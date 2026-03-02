use anyhow::{bail, Result};
use tracing::warn;

use crate::cli::LandCmd;
use crate::git::{gh_rw, git_ro, git_rw, sanitize_gh_base_ref, to_remote_ref};
use crate::github::{
    fetch_pr_bodies_graphql, fetch_pr_ci_review_status, graphql_escape, list_open_prs_for_heads,
};
use crate::parsing::derive_local_groups;
use crate::selectors::{resolve_inclusive_count, InclusiveSelector};

fn resolve_land_take_count(
    groups: &[crate::parsing::Group],
    until: &InclusiveSelector,
) -> Result<usize> {
    resolve_inclusive_count(groups, until)
}

fn resolve_landed_pr_segment<'a>(
    groups: &[crate::parsing::Group],
    prefix: &str,
    prs: &'a [crate::github::PrInfo],
    until: &InclusiveSelector,
) -> Result<(usize, Vec<&'a crate::github::PrInfo>)> {
    let take_n = resolve_land_take_count(groups, until)?;
    let mut segment: Vec<&crate::github::PrInfo> = Vec::with_capacity(take_n);
    for g in &groups[..take_n] {
        let head_branch = format!("{}{}", prefix, g.tag);
        if let Some(pr) = prs.iter().find(|p| p.head == head_branch) {
            segment.push(pr);
        } else {
            bail!(
                "No open PR found for local group '{}' (branch '{}')",
                g.tag,
                head_branch
            );
        }
    }
    Ok((take_n, segment))
}

pub fn land_until(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
    dry: bool,
    mode: LandCmd,
    bypass_safety: bool,
) -> Result<usize> {
    // Local stack is the source of truth: derive order from local groups
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        bail!("No local groups found; nothing to land.");
    }
    let take_n = resolve_land_take_count(&groups, until)?;
    let heads: Vec<String> = groups[..take_n]
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }
    let (_, ordered) = resolve_landed_pr_segment(&groups, prefix, &prs, until)?;
    let segment = ordered.as_slice();

    // Safety validation: CI and Reviews must be passing/approved for all PRs being landed
    let numbers: Vec<u64> = segment.iter().map(|p| p.number).collect();
    if !numbers.is_empty() {
        if let Ok(status_map) = fetch_pr_ci_review_status(&numbers) {
            let mut ci_bad: Vec<u64> = vec![];
            let mut rv_bad: Vec<u64> = vec![];
            for n in &numbers {
                if let Some(st) = status_map.get(n) {
                    if st.ci_state.as_str() != "SUCCESS" {
                        ci_bad.push(*n);
                    }
                    if st.review_decision.as_str() != "APPROVED" {
                        rv_bad.push(*n);
                    }
                } else {
                    // Unknown status → treat as failing both
                    ci_bad.push(*n);
                    rv_bad.push(*n);
                }
            }
            if !ci_bad.is_empty() || !rv_bad.is_empty() {
                let ci_str = if ci_bad.is_empty() {
                    String::from("none")
                } else {
                    ci_bad
                        .iter()
                        .map(|x| format!("#{}", x))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let rv_str = if rv_bad.is_empty() {
                    String::from("none")
                } else {
                    rv_bad
                        .iter()
                        .map(|x| format!("#{}", x))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                if bypass_safety {
                    warn!(
                        "Bypassing safety checks (--unsafe). CI not passing: {}; Reviews not approved: {}",
                        ci_str, rv_str
                    );
                } else {
                    bail!(
                        "Refusing to land: CI not passing: {}; Reviews not approved: {}. Use --unsafe to override.",
                        ci_str, rv_str
                    );
                }
            }
        }
    }

    if let LandCmd::PerPr = mode {
        // Verify each has exactly one unique commit over its parent
        git_rw(dry, ["fetch", "origin"].as_slice())?; // ensure remotes up to date
        let mut offenders: Vec<u64> = vec![];
        for (i, pr) in segment.iter().enumerate() {
            let parent = if i == 0 {
                base.to_string()
            } else {
                segment[i - 1].head.clone()
            };
            let parent_ref = to_remote_ref(&parent);
            let child_ref = to_remote_ref(&pr.head);
            let cnt_s = git_ro(
                [
                    "rev-list",
                    "--count",
                    &format!("{}..{}", parent_ref, child_ref),
                ]
                .as_slice(),
            )?;
            let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
            if cnt != 1 {
                offenders.push(pr.number);
            }
        }
        if !offenders.is_empty() {
            warn!(
                "The following PRs have != 1 commit: {}",
                offenders
                    .iter()
                    .map(|x| format!("#{}", x))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            bail!("Run `spr prep` to squash them first");
        }
    }

    // Batch: set base of Nth PR, merge it (rebase or squash), and close others with a comment via GraphQL
    let nth = segment[take_n - 1];
    let mut nums: Vec<u64> = vec![nth.number];
    for pr in &segment[..take_n - 1] {
        nums.push(pr.number);
    }
    let bodies = fetch_pr_bodies_graphql(&nums)?;
    let nth_id = bodies
        .get(&nth.number)
        .map(|x| x.id.clone())
        .unwrap_or_default();
    if nth_id.is_empty() {
        bail!("Failed to fetch GraphQL id for PR #{}", nth.number);
    }

    let mut m = String::from("mutation {");
    // Ensure base for nth
    m.push_str(&format!(
        "b0: updatePullRequest(input:{{pullRequestId:\"{}\", baseRefName:\"{}\"}}){{ clientMutationId }} ",
        nth_id,
        graphql_escape(&sanitize_gh_base_ref(base))
    ));
    if let LandCmd::PerPr = mode {
        // Merge nth with REBASE
        m.push_str(&format!(
            "m0: mergePullRequest(input:{{pullRequestId:\"{}\", mergeMethod:REBASE}}){{ clientMutationId }} ",
            nth_id
        ));
    } else {
        // Merge nth with SQUASH
        m.push_str(&format!(
            "m0: mergePullRequest(input:{{pullRequestId:\"{}\", mergeMethod:SQUASH}}){{ clientMutationId }} ",
            nth_id
        ));
    }
    // Close others with a comment
    for (i, pr) in segment[..take_n - 1].iter().enumerate() {
        let id = bodies
            .get(&pr.number)
            .map(|x| x.id.clone())
            .unwrap_or_default();
        if id.is_empty() {
            continue;
        }
        let idx = i + 1;
        let comment = format!("Merged as part of PR #{}", nth.number);
        m.push_str(&format!(
            "c{}: addComment(input:{{subjectId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
            idx,
            id,
            graphql_escape(&comment)
        ));
        m.push_str(&format!(
            "x{}: closePullRequest(input:{{pullRequestId:\"{}\"}}){{ clientMutationId }} ",
            idx, id
        ));
    }
    m.push('}');
    tracing::info!(
        "Merging PR #{} and closing {} other PR(s) on GitHub... this might take a few seconds.",
        nth.number,
        take_n - 1
    );
    gh_rw(
        dry,
        ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
    )?;

    Ok(take_n)
}

/// Per-PR: land N PRs bottom-up, each PR as its own commit using rebase merge.
/// Each PR must have exactly one commit over its parent.
pub fn land_per_pr_until(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
    dry: bool,
    bypass_safety: bool,
) -> Result<usize> {
    land_until(
        base,
        prefix,
        ignore_tag,
        until,
        dry,
        LandCmd::PerPr,
        bypass_safety,
    )
}

/// Flatten: behave like per-pr landing but squash-merge the Nth PR and set its base to the actual base.
pub fn land_flatten_until(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
    dry: bool,
    bypass_safety: bool,
) -> Result<usize> {
    land_until(
        base,
        prefix,
        ignore_tag,
        until,
        dry,
        LandCmd::Flatten,
        bypass_safety,
    )
}

#[cfg(test)]
mod tests {
    use super::{resolve_land_take_count, resolve_landed_pr_segment};
    use crate::github::PrInfo;
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, InclusiveSelector, StableHandle};

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter()
            .map(|tag| Group {
                tag: tag.to_string(),
                subjects: vec![format!("feat: {tag}")],
                commits: vec![format!("{tag}1")],
                first_message: Some(format!("feat: {tag} pr:{tag}")),
                ignored_after: Vec::new(),
            })
            .collect()
    }

    fn pr(number: u64, head: &str) -> PrInfo {
        PrInfo {
            number,
            head: head.to_string(),
            base: "main".to_string(),
        }
    }

    #[test]
    fn land_until_resolves_stable_handle_as_inclusive_prefix() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let until = InclusiveSelector::Group(GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        }));

        assert_eq!(resolve_land_take_count(&groups, &until).unwrap(), 2);
    }

    #[test]
    fn land_until_only_requires_open_prs_for_landed_prefix() {
        let groups = groups(&["rho", "sigma"]);
        let prs = vec![pr(14, "skilltest/rho")];
        let until = InclusiveSelector::Group(GroupSelector::LocalPr(1));

        let (take_n, ordered) =
            resolve_landed_pr_segment(&groups, "skilltest/", &prs, &until).unwrap();

        assert_eq!(take_n, 1);
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].number, 14);
        assert_eq!(ordered[0].head, "skilltest/rho");
    }

    #[test]
    fn land_until_still_requires_prs_for_every_group_being_landed() {
        let groups = groups(&["rho", "sigma"]);
        let prs = vec![pr(14, "skilltest/rho")];
        let until = InclusiveSelector::Group(GroupSelector::LocalPr(2));

        let err = resolve_landed_pr_segment(&groups, "skilltest/", &prs, &until).unwrap_err();

        assert_eq!(
            err.to_string(),
            "No open PR found for local group 'sigma' (branch 'skilltest/sigma')"
        );
    }
}
