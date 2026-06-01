use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use tracing::warn;

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::cli::LandCmd;
use crate::execution::ExecutionMode;
use crate::git::{gh_rw, git_ro, git_rw, sanitize_gh_base_ref, to_remote_ref};
use crate::github::{
    fetch_pr_bodies_graphql, fetch_pr_ci_review_status, fetch_pr_issue_comment_bodies_graphql,
    graphql_escape, list_open_or_merged_prs_for_heads, PrCiState, PrInfoWithState,
    PrReviewDecision, PrState,
};
use crate::parsing::derive_local_groups;
use crate::selectors::{resolve_inclusive_count, InclusiveSelector};

fn resolve_land_take_count(
    groups: &[crate::parsing::Group],
    until: &InclusiveSelector,
) -> Result<usize> {
    resolve_inclusive_count(groups, until)
}

#[derive(Debug)]
enum LandPlan<'a> {
    Fresh {
        segment: Vec<&'a PrInfoWithState>,
    },
    Recovery {
        target: &'a PrInfoWithState,
        open_older_prs: Vec<&'a PrInfoWithState>,
    },
}

fn resolve_land_plan<'a>(
    groups: &[crate::parsing::Group],
    branch_identities: &[crate::branch_names::GroupBranchIdentity],
    prs_by_head: &HashMap<crate::branch_names::CanonicalBranchConflictKey, &'a PrInfoWithState>,
    until: &InclusiveSelector,
) -> Result<(usize, LandPlan<'a>)> {
    let take_n = resolve_land_take_count(groups, until)?;
    let target_identity = &branch_identities[take_n - 1];
    let target = prs_by_head
        .get(&target_identity.conflict_key)
        .copied()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No open PR found for local group '{}' (branch '{}')",
                groups[take_n - 1].selector_text(),
                target_identity.exact
            )
        })?;
    if target.state == PrState::Merged {
        let open_older_prs = branch_identities[..take_n - 1]
            .iter()
            .filter_map(|identity| prs_by_head.get(&identity.conflict_key).copied())
            .filter(|pr| pr.state == PrState::Open)
            .collect();
        return Ok((
            take_n,
            LandPlan::Recovery {
                target,
                open_older_prs,
            },
        ));
    }
    let mut segment = Vec::with_capacity(take_n);
    for (g, identity) in groups[..take_n].iter().zip(branch_identities.iter()) {
        let head_branch = &identity.exact;
        if let Some(pr) = prs_by_head
            .get(&identity.conflict_key)
            .copied()
            .filter(|pr| pr.state == PrState::Open)
        {
            segment.push(pr);
        } else {
            bail!(
                "No open PR found for local group '{}' (branch '{}')",
                g.selector_text(),
                head_branch
            );
        }
    }
    Ok((take_n, LandPlan::Fresh { segment }))
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

fn cleanup_comment(merged_pr_number: u64) -> String {
    format!("Merged as part of PR #{merged_pr_number}")
}

fn build_close_comment_mutation(
    prs: &[&PrInfoWithState],
    ids_by_number: &HashMap<u64, String>,
    merged_pr_number: u64,
    add_comment_numbers: &HashSet<u64>,
) -> Option<String> {
    let mut mutation = String::from("mutation {");
    let mut has_operations = false;
    for (i, pr) in prs.iter().enumerate() {
        let Some(id) = ids_by_number.get(&pr.number).filter(|id| !id.is_empty()) else {
            continue;
        };
        has_operations = true;
        if add_comment_numbers.contains(&pr.number) {
            mutation.push_str(&format!(
                "c{}: addComment(input:{{subjectId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
                i,
                id,
                graphql_escape(&cleanup_comment(merged_pr_number))
            ));
        }
        mutation.push_str(&format!(
            "x{}: closePullRequest(input:{{pullRequestId:\"{}\"}}){{ clientMutationId }} ",
            i, id
        ));
    }
    mutation.push('}');
    has_operations.then_some(mutation)
}

struct LandMutationPlan<'a> {
    base: &'a str,
    mode: LandCmd,
    target: &'a PrInfoWithState,
    target_id: Option<&'a str>,
    open_older_prs: &'a [&'a PrInfoWithState],
    ids_by_number: &'a HashMap<u64, String>,
    add_comment_numbers: &'a HashSet<u64>,
}

fn run_land_mutations<F>(plan: LandMutationPlan<'_>, mut run: F) -> Result<()>
where
    F: FnMut(String) -> Result<()>,
{
    if let Some(target_id) = plan.target_id {
        run(build_land_merge_mutation(target_id, plan.base, plan.mode))?;
    }
    for chunk in plan
        .open_older_prs
        .chunks(MAX_CLOSE_COMMENT_PRS_PER_MUTATION)
    {
        if let Some(mutation) = build_close_comment_mutation(
            chunk,
            plan.ids_by_number,
            plan.target.number,
            plan.add_comment_numbers,
        ) {
            run(mutation)?;
        }
    }
    Ok(())
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
    let prs = list_open_or_merged_prs_for_heads(&heads)?;
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();
    let (_, plan) = resolve_land_plan(&groups, &branch_identities, &prs_by_head, until)?;
    let LandPlan::Fresh { segment } = &plan else {
        let LandPlan::Recovery {
            target,
            open_older_prs,
        } = &plan
        else {
            unreachable!()
        };
        if open_older_prs.is_empty() {
            tracing::info!(
                "PR #{} is already merged and its older PR cleanup is complete.",
                target.number
            );
            return Ok(take_n);
        }
        let numbers = open_older_prs
            .iter()
            .map(|pr| pr.number)
            .collect::<Vec<_>>();
        let bodies = fetch_pr_bodies_graphql(&numbers)?;
        let ids_by_number = bodies
            .iter()
            .map(|(number, info)| (*number, info.id.clone()))
            .collect::<HashMap<_, _>>();
        let expected_comment = cleanup_comment(target.number);
        let mut add_comment_numbers = std::collections::HashSet::new();
        for pr in open_older_prs {
            let comments = fetch_pr_issue_comment_bodies_graphql(pr.number)?;
            if !comments.iter().any(|comment| comment == &expected_comment) {
                add_comment_numbers.insert(pr.number);
            }
        }
        tracing::info!(
            "PR #{} is already merged; closing {} remaining older PR(s).",
            target.number,
            open_older_prs.len()
        );
        return run_land_mutations(
            LandMutationPlan {
                base,
                mode,
                target,
                target_id: None,
                open_older_prs,
                ids_by_number: &ids_by_number,
                add_comment_numbers: &add_comment_numbers,
            },
            |mutation| {
                gh_rw(
                    execution_mode,
                    ["api", "graphql", "-f", &format!("query={mutation}")].as_slice(),
                )?;
                Ok(())
            },
        )
        .map(|()| take_n);
    };
    let segment = segment.as_slice();

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
    let add_comment_numbers = segment[..take_n - 1]
        .iter()
        .map(|pr| pr.number)
        .collect::<std::collections::HashSet<_>>();
    run_land_mutations(
        LandMutationPlan {
            base,
            mode,
            target: nth,
            target_id: Some(&nth_id),
            open_older_prs: &segment[..take_n - 1],
            ids_by_number: &ids_by_number,
            add_comment_numbers: &add_comment_numbers,
        },
        |mutation| {
            gh_rw(
                execution_mode,
                ["api", "graphql", "-f", &format!("query={mutation}")].as_slice(),
            )?;
            Ok(())
        },
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
        build_close_comment_mutation, build_land_merge_mutation, land_until, resolve_land_plan,
        resolve_land_take_count, run_land_mutations, LandMutationPlan, LandPlan,
    };
    use crate::branch_names::canonical_branch_conflict_key;
    use crate::cli::LandCmd;
    use crate::execution::ExecutionMode;
    use crate::github::{PrInfoWithState, PrState};
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

    fn pr(number: u64, head: &str) -> PrInfoWithState {
        PrInfoWithState {
            number,
            head: head.to_string(),
            base: "main".to_string(),
            state: PrState::Open,
            url: format!("https://github.com/o/r/pull/{number}"),
        }
    }

    fn merged_pr(number: u64, head: &str) -> PrInfoWithState {
        PrInfoWithState {
            state: PrState::Merged,
            ..pr(number, head)
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
            resolve_land_plan(&groups, &branch_identities, &prs_by_head, &until).unwrap();

        assert_eq!(take_n, 1);
        let LandPlan::Fresh { segment } = ordered else {
            panic!("expected fresh land plan");
        };
        assert_eq!(segment.len(), 1);
        assert_eq!(segment[0].number, 14);
        assert_eq!(segment[0].head, "skilltest/rho");
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

        let err = resolve_land_plan(&groups, &branch_identities, &prs_by_head, &until).unwrap_err();

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

        let mutation = build_close_comment_mutation(
            &[&prs[0]],
            &ids,
            3,
            &std::collections::HashSet::from([1]),
        )
        .unwrap();

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

        assert!(build_close_comment_mutation(
            &[&prs[0], &prs[1]],
            &ids,
            3,
            &std::collections::HashSet::from([1, 2]),
        )
        .is_none());
    }

    #[test]
    fn recovery_plan_skips_merge_and_closes_only_remaining_open_prs() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let branch_identities =
            crate::branch_names::group_branch_identities(&groups, "skilltest/").unwrap();
        let prs = [
            pr(1, "skilltest/alpha"),
            merged_pr(2, "skilltest/beta"),
            merged_pr(3, "skilltest/gamma"),
        ];
        let prs_by_head = prs
            .iter()
            .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
            .collect();

        let (_, plan) = resolve_land_plan(
            &groups,
            &branch_identities,
            &prs_by_head,
            &InclusiveSelector::All,
        )
        .unwrap();
        let LandPlan::Recovery {
            target,
            open_older_prs,
        } = plan
        else {
            panic!("expected recovery plan");
        };

        assert_eq!(target.number, 3);
        assert_eq!(
            open_older_prs
                .iter()
                .map(|pr| pr.number)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn partial_land_retry_skips_merge_and_does_not_duplicate_comment() {
        let target = pr(3, "skilltest/gamma");
        let older = pr(1, "skilltest/alpha");
        let ids = HashMap::from([(1, "PR_alpha".to_string()), (3, "PR_gamma".to_string())]);
        let comments = std::collections::HashSet::from([1]);
        let mut first_calls = Vec::new();
        let err = run_land_mutations(
            LandMutationPlan {
                base: "main",
                mode: LandCmd::Flatten,
                target: &target,
                target_id: Some("PR_gamma"),
                open_older_prs: &[&older],
                ids_by_number: &ids,
                add_comment_numbers: &comments,
            },
            |mutation| {
                first_calls.push(mutation);
                if first_calls.len() == 2 {
                    anyhow::bail!("transient cleanup failure");
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("transient cleanup failure"));
        assert!(first_calls[0].contains("mergePullRequest"));

        let mut retry_calls = Vec::new();
        run_land_mutations(
            LandMutationPlan {
                base: "main",
                mode: LandCmd::Flatten,
                target: &target,
                target_id: None,
                open_older_prs: &[&older],
                ids_by_number: &ids,
                add_comment_numbers: &std::collections::HashSet::new(),
            },
            |mutation| {
                retry_calls.push(mutation);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(retry_calls.len(), 1);
        assert!(!retry_calls[0].contains("mergePullRequest"));
        assert!(!retry_calls[0].contains("addComment"));
        assert!(retry_calls[0].contains("closePullRequest"));
    }
}
