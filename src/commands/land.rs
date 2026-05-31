use anyhow::{bail, Result};
use std::collections::HashMap;
use tracing::warn;

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::cli::LandCmd;
use crate::execution::ExecutionMode;
use crate::git::{gh_rw, git_ro, git_rw, sanitize_gh_base_ref, to_remote_ref};
use crate::github::{
    fetch_pr_bodies_graphql, fetch_pr_ci_review_status, graphql_escape, list_open_prs_for_heads,
    PrCiState, PrReviewDecision,
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
    branch_identities: &[crate::branch_names::GroupBranchIdentity],
    prs_by_head: &HashMap<
        crate::branch_names::CanonicalBranchConflictKey,
        &'a crate::github::PrInfo,
    >,
    until: &InclusiveSelector,
) -> Result<(usize, Vec<&'a crate::github::PrInfo>)> {
    let take_n = resolve_land_take_count(groups, until)?;
    let mut segment: Vec<&crate::github::PrInfo> = Vec::with_capacity(take_n);
    for (g, identity) in groups[..take_n].iter().zip(branch_identities.iter()) {
        let head_branch = &identity.exact;
        if let Some(pr) = prs_by_head.get(&identity.conflict_key).copied() {
            segment.push(pr);
        } else {
            bail!(
                "No open PR found for local group '{}' (branch '{}')",
                g.selector_text(),
                head_branch
            );
        }
    }
    Ok((take_n, segment))
}

// Each older PR adds two mutative aliases: one comment and one close. GitHub does not publish a
// safe alias count for this shape, so keep each write request deliberately small.
const MAX_CLOSE_COMMENT_PRS_PER_MUTATION: usize = 3;

fn build_land_merge_mutation(nth_id: &str, base: &str, mode: LandCmd) -> String {
    let merge_method = match mode {
        LandCmd::PerPr => "REBASE",
        LandCmd::Flatten => "SQUASH",
    };
    format!(
        "mutation {{b0: updatePullRequest(input:{{pullRequestId:\"{}\", baseRefName:\"{}\"}}){{ clientMutationId }} m0: mergePullRequest(input:{{pullRequestId:\"{}\", mergeMethod:{}}}){{ clientMutationId }} }}",
        nth_id,
        graphql_escape(&sanitize_gh_base_ref(base)),
        nth_id,
        merge_method,
    )
}

fn build_close_comment_mutation(
    prs: &[&crate::github::PrInfo],
    ids_by_number: &HashMap<u64, String>,
    merged_pr_number: u64,
) -> Option<String> {
    let mut mutation = String::from("mutation {");
    let mut has_operations = false;
    for (i, pr) in prs.iter().enumerate() {
        let Some(id) = ids_by_number.get(&pr.number).filter(|id| !id.is_empty()) else {
            continue;
        };
        has_operations = true;
        let comment = format!("Merged as part of PR #{}", merged_pr_number);
        mutation.push_str(&format!(
            "c{}: addComment(input:{{subjectId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
            i,
            id,
            graphql_escape(&comment)
        ));
        mutation.push_str(&format!(
            "x{}: closePullRequest(input:{{pullRequestId:\"{}\"}}){{ clientMutationId }} ",
            i, id
        ));
    }
    mutation.push('}');
    has_operations.then_some(mutation)
}

pub fn land_until(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
    execution_mode: ExecutionMode,
    mode: LandCmd,
    bypass_safety: bool,
) -> Result<usize> {
    // Local stack is the source of truth: derive order from local groups
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        bail!("No local groups found; nothing to land.");
    }
    let branch_identities = group_branch_identities(&groups, prefix)?;
    let take_n = resolve_land_take_count(&groups, until)?;
    let heads: Vec<String> = branch_identities[..take_n]
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_prs_for_heads(&heads)?;
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }
    let (_, ordered) = resolve_landed_pr_segment(&groups, &branch_identities, &prs_by_head, until)?;
    let segment = ordered.as_slice();

    // Safety validation: CI and Reviews must be passing/approved for all PRs being landed
    let numbers: Vec<u64> = segment.iter().map(|p| p.number).collect();
    if !numbers.is_empty() {
        if let Ok(status_map) = fetch_pr_ci_review_status(&numbers) {
            let mut ci_bad: Vec<u64> = vec![];
            let mut rv_bad: Vec<u64> = vec![];
            for n in &numbers {
                if let Some(st) = status_map.get(n) {
                    if st.ci_state != PrCiState::Success {
                        ci_bad.push(*n);
                    }
                    if st.review_decision != PrReviewDecision::Approved {
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
        git_rw(execution_mode, ["fetch", "origin"].as_slice())?; // ensure remotes up to date
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

    // Set base of Nth PR, merge it (rebase or squash), then close older PRs in bounded chunks.
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

    let ids_by_number: HashMap<u64, String> = bodies
        .iter()
        .map(|(number, info)| (*number, info.id.clone()))
        .collect();
    tracing::info!(
        "Merging PR #{} and closing {} other PR(s) on GitHub... this might take a few seconds.",
        nth.number,
        take_n - 1
    );
    let merge_mutation = build_land_merge_mutation(&nth_id, base, mode);
    gh_rw(
        execution_mode,
        ["api", "graphql", "-f", &format!("query={}", merge_mutation)].as_slice(),
    )?;
    for chunk in segment[..take_n - 1].chunks(MAX_CLOSE_COMMENT_PRS_PER_MUTATION) {
        if let Some(close_mutation) =
            build_close_comment_mutation(chunk, &ids_by_number, nth.number)
        {
            gh_rw(
                execution_mode,
                ["api", "graphql", "-f", &format!("query={}", close_mutation)].as_slice(),
            )?;
        }
    }

    Ok(take_n)
}

/// Per-PR: land N PRs bottom-up, each PR as its own commit using rebase merge.
/// Each PR must have exactly one commit over its parent.
pub fn land_per_pr_until(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
    execution_mode: ExecutionMode,
    bypass_safety: bool,
) -> Result<usize> {
    land_until(
        base,
        prefix,
        ignore_tag,
        until,
        execution_mode,
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
    execution_mode: ExecutionMode,
    bypass_safety: bool,
) -> Result<usize> {
    land_until(
        base,
        prefix,
        ignore_tag,
        until,
        execution_mode,
        LandCmd::Flatten,
        bypass_safety,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        build_close_comment_mutation, build_land_merge_mutation, land_until,
        resolve_land_take_count, resolve_landed_pr_segment,
    };
    use crate::cli::LandCmd;
    use crate::execution::ExecutionMode;
    use crate::github::PrInfo;
    use crate::parsing::Group;
    use crate::selectors::{ExplicitGroupSelector, GroupSelector, InclusiveSelector};
    use crate::test_support::{init_case_conflicting_stack_repo, lock_cwd, DirGuard};
    use std::collections::HashMap;

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter()
            .map(|tag| Group {
                marker: crate::group_markers::GroupMarker::PrLabel(tag.to_string()),
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
        let until = InclusiveSelector::Group(GroupSelector::Explicit(
            ExplicitGroupSelector::PrLabel("beta".to_string()),
        ));

        assert_eq!(resolve_land_take_count(&groups, &until).unwrap(), 2);
    }

    #[test]
    fn land_until_only_requires_open_prs_for_landed_prefix() {
        let groups = groups(&["rho", "sigma"]);
        let branch_identities =
            crate::branch_names::group_branch_identities(&groups, "skilltest/").unwrap();
        let prs = [pr(14, "skilltest/rho")];
        let prs_by_head: HashMap<_, _> = prs
            .iter()
            .map(|pr| {
                (
                    crate::branch_names::canonical_branch_conflict_key(&pr.head),
                    pr,
                )
            })
            .collect();
        let until = InclusiveSelector::Group(GroupSelector::LocalPr(1));

        let (take_n, ordered) =
            resolve_landed_pr_segment(&groups, &branch_identities, &prs_by_head, &until).unwrap();

        assert_eq!(take_n, 1);
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].number, 14);
        assert_eq!(ordered[0].head, "skilltest/rho");
    }

    #[test]
    fn land_until_still_requires_prs_for_every_group_being_landed() {
        let groups = groups(&["rho", "sigma"]);
        let branch_identities =
            crate::branch_names::group_branch_identities(&groups, "skilltest/").unwrap();
        let prs = [pr(14, "skilltest/rho")];
        let prs_by_head: HashMap<_, _> = prs
            .iter()
            .map(|pr| {
                (
                    crate::branch_names::canonical_branch_conflict_key(&pr.head),
                    pr,
                )
            })
            .collect();
        let until = InclusiveSelector::Group(GroupSelector::LocalPr(2));

        let err = resolve_landed_pr_segment(&groups, &branch_identities, &prs_by_head, &until)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "No open PR found for local group 'pr:sigma' (branch 'skilltest/sigma')"
        );
    }

    #[test]
    fn land_until_rejects_case_colliding_branch_names_from_local_stack() {
        let _lock = lock_cwd();
        let dir = init_case_conflicting_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let err = land_until(
            "main",
            "dank-spr/",
            "ignore",
            &InclusiveSelector::All,
            ExecutionMode::DryRun,
            LandCmd::Flatten,
            false,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("pr:alpha and pr:Alpha derive conflicting branch names"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn land_merge_mutation_only_updates_and_merges_target_pr() {
        let mutation = build_land_merge_mutation("PR_target", "origin/main", LandCmd::Flatten);

        assert!(mutation.contains("updatePullRequest"));
        assert!(mutation.contains("mergePullRequest"));
        assert!(!mutation.contains("closePullRequest"));
        assert!(!mutation.contains("addComment"));
        assert!(mutation.contains("baseRefName:\"main\""));
        assert!(mutation.contains("mergeMethod:SQUASH"));
    }

    #[test]
    fn close_comment_mutation_only_closes_supplied_prs() {
        let prs = [pr(1, "skilltest/alpha"), pr(2, "skilltest/beta")];
        let ids = HashMap::from([(1, "PR_alpha".to_string()), (2, "PR_beta".to_string())]);

        let mutation = build_close_comment_mutation(&[&prs[0]], &ids, 3).unwrap();

        assert!(mutation.contains("PR_alpha"));
        assert!(!mutation.contains("PR_beta"));
        assert!(mutation.contains("Merged as part of PR #3"));
        assert!(mutation.contains("addComment"));
        assert!(mutation.contains("closePullRequest"));
    }

    #[test]
    fn close_comment_mutation_skips_missing_and_empty_ids() {
        let prs = [pr(1, "skilltest/alpha"), pr(2, "skilltest/beta")];
        let ids = HashMap::from([(1, String::new())]);

        assert!(build_close_comment_mutation(&[&prs[0], &prs[1]], &ids, 3).is_none());
    }
}
