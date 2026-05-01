use anyhow::{anyhow, Result};
use tracing::info;

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::execution::ExecutionMode;
use crate::git::{git_ro, git_rw};
use crate::github::{append_warning_to_pr, list_open_prs_for_heads};
use crate::limit::Limit;
use crate::maintenance_output::{
    PrepNextChildAction, PrepNextChildData, PrepOptions, PrepRepoContext, PrepSummaryData,
    PreparedGroupAction, PreparedGroupData, ResolvedPrepSelection,
};
use crate::parsing::{
    derive_groups_between_with_ignored, derive_local_groups, split_groups_for_update,
};
use crate::selectors::{
    resolve_group_ordinal, resolve_inclusive_count, GroupSelector, InclusiveSelector,
};
use crate::update_output::{
    ResolvedUpdateLimit, UpdateOptions, UpdateRepoContext, UpdateSummaryData,
};

pub struct PrepExecutionOptions {
    pub pr_description_mode: crate::config::PrDescriptionMode,
    pub list_order: crate::config::ListOrder,
    pub local_pr_branch_policy: crate::config::LocalPrBranchSyncPolicy,
    pub selection: crate::cli::PrepSelection,
    pub execution_mode: ExecutionMode,
}

fn resolve_prep_window(
    groups: &[crate::parsing::Group],
    selection: &crate::cli::PrepSelection,
) -> Result<(usize, usize)> {
    match selection {
        crate::cli::PrepSelection::All => Ok((0, groups.len())),
        crate::cli::PrepSelection::Until(selector) => {
            let end = resolve_inclusive_count(groups, selector)?;
            Ok((0, end.min(groups.len())))
        }
        crate::cli::PrepSelection::Exact(selector) => {
            let ordinal = resolve_group_ordinal(groups, selector)?;
            Ok((ordinal - 1, ordinal))
        }
    }
}

fn selector_text(selector: &GroupSelector) -> String {
    selector.to_string()
}

fn resolved_selection(
    selection: &crate::cli::PrepSelection,
    end_idx_exclusive: usize,
) -> ResolvedPrepSelection {
    match selection {
        crate::cli::PrepSelection::All => ResolvedPrepSelection::All,
        crate::cli::PrepSelection::Until(selector) => match selector {
            InclusiveSelector::All => ResolvedPrepSelection::All,
            InclusiveSelector::Group(selector) => ResolvedPrepSelection::Until {
                selector: selector_text(selector),
                last_local_pr_number: end_idx_exclusive,
            },
        },
        crate::cli::PrepSelection::Exact(selector) => ResolvedPrepSelection::Exact {
            selector: selector_text(selector),
            local_pr_number: end_idx_exclusive,
        },
    }
}

fn limit_and_next_idx(
    groups: &[crate::parsing::Group],
    selection: &crate::cli::PrepSelection,
) -> Result<(Option<Limit>, Option<usize>, ResolvedUpdateLimit)> {
    match selection {
        crate::cli::PrepSelection::All => Ok((None, None, ResolvedUpdateLimit::All)),
        crate::cli::PrepSelection::Until(selector) => {
            let count = resolve_inclusive_count(groups, selector)?;
            if count == 0 {
                Ok((None, None, ResolvedUpdateLimit::All))
            } else {
                Ok((
                    Some(Limit::ByPr(count)),
                    Some(count),
                    ResolvedUpdateLimit::ByPr { count },
                ))
            }
        }
        crate::cli::PrepSelection::Exact(selector) => {
            let count = resolve_group_ordinal(groups, selector)?;
            Ok((
                Some(Limit::ByPr(count)),
                Some(count),
                ResolvedUpdateLimit::ByPr { count },
            ))
        }
    }
}

fn render_prepared_group_action(action: PreparedGroupAction) -> &'static str {
    match action {
        PreparedGroupAction::Squashed => "squashed",
        PreparedGroupAction::PreservedSingleCommit => "preserved single commit",
        PreparedGroupAction::SkippedEmpty => "skipped empty rewrite",
    }
}

fn render_prep_next_child_action(action: PrepNextChildAction) -> &'static str {
    match action {
        PrepNextChildAction::WouldAppendWarning => "would append warning to next child PR",
        PrepNextChildAction::WarningAppended => "appended warning to next child PR",
        PrepNextChildAction::SkippedStackOnly => "skipped next child warning in stack_only mode",
        PrepNextChildAction::MissingOpenPr => "next child branch has no open PR",
    }
}

fn render_update_push_action(action: crate::update_output::UpdatePushAction) -> &'static str {
    match action {
        crate::update_output::UpdatePushAction::Unchanged => "unchanged",
        crate::update_output::UpdatePushAction::CreateBranch => "create branch",
        crate::update_output::UpdatePushAction::FastForwardBranch => "fast-forward branch",
        crate::update_output::UpdatePushAction::ForcePushBranch => "force-push branch",
    }
}

fn render_update_pr_action(action: crate::update_output::UpdatePrAction) -> &'static str {
    match action {
        crate::update_output::UpdatePrAction::NotRequested => "no pr action",
        crate::update_output::UpdatePrAction::Created => "create pr",
        crate::update_output::UpdatePrAction::Existing => "existing pr",
    }
}

pub fn render_prep_summary(summary: &PrepSummaryData) -> Vec<String> {
    if summary.selected_groups.is_empty() && summary.update.is_none() {
        return vec!["Nothing to prep".to_string()];
    }

    let mut lines: Vec<String> = summary
        .selected_groups
        .iter()
        .map(|group| {
            format!(
                "Prepared LPR #{} / {}: {}",
                group.local_pr_number,
                group.stable_handle,
                render_prepared_group_action(group.action)
            )
        })
        .collect();

    lines.push(format!(
        "Replayed {} commit(s); skipped {} empty replay commit(s)",
        summary.replayed_commit_count, summary.skipped_replay_commit_count
    ));

    if let Some(next_child) = &summary.next_child {
        lines.push(format!(
            "Next child {} ({})",
            next_child.stable_handle,
            render_prep_next_child_action(next_child.action)
        ));
    }

    if let Some(update) = &summary.update {
        for group in &update.groups {
            lines.push(format!(
                "Update {}: {}, {}",
                group.head_branch,
                render_update_push_action(group.push_action),
                render_update_pr_action(group.pr_action)
            ));
        }
    }

    lines
}

/// Squash PRs according to selection; operate locally then run update for the affected groups.
///
/// `list_order` is forwarded to the nested update step so selection-sensitive summaries keep the
/// same local ordering conventions as `spr list`.
pub fn prep_squash(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    options: PrepExecutionOptions,
) -> Result<PrepSummaryData> {
    let PrepExecutionOptions {
        pr_description_mode,
        list_order,
        local_pr_branch_policy,
        selection,
        execution_mode,
    } = options;
    let dry_run = execution_mode == ExecutionMode::DryRun;
    let (merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        return Ok(PrepSummaryData {
            repo: PrepRepoContext {
                base: base.to_string(),
                prefix: prefix.to_string(),
                ignore_tag: ignore_tag.to_string(),
            },
            options: PrepOptions {
                dry_run,
                pr_description_mode,
            },
            selection: ResolvedPrepSelection::All,
            selected_groups: Vec::new(),
            rewritten_head_sha: None,
            replayed_commit_count: 0,
            skipped_replay_commit_count: 0,
            next_child: None,
            update: None,
        });
    }
    let branch_identities = group_branch_identities(&groups, prefix)?;
    let (start_idx, end_idx_exclusive) = resolve_prep_window(&groups, &selection)?;
    let resolved_selection = resolved_selection(&selection, end_idx_exclusive);

    let mut parent_sha = if start_idx == 0 {
        merge_base.clone()
    } else {
        groups[start_idx - 1]
            .commits
            .last()
            .cloned()
            .expect("group has at least one commit")
    };
    let mut selected_groups: Vec<PreparedGroupData> = Vec::new();

    if start_idx < end_idx_exclusive {
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for group in &groups[start_idx..end_idx_exclusive] {
            let tip = group
                .commits
                .last()
                .ok_or_else(|| anyhow!("Empty group {}", group.tag))?;
            args.push(format!("{}^{{tree}}", tip));
        }
        let ref_args: Vec<&str> = args.iter().map(String::as_str).collect();
        let trees_out = git_ro(&ref_args)?;
        let selected_trees: Vec<&str> = trees_out.lines().collect();

        let mut msg_args: Vec<&str> = vec!["log", "--no-walk=unsorted", "--format=%B%x1e"];
        let mut single_tip_shas: Vec<&str> = Vec::new();
        for group in &groups[start_idx..end_idx_exclusive] {
            if group.commits.len() == 1 {
                if let Some(tip) = group.commits.last() {
                    single_tip_shas.push(tip);
                }
            }
        }
        if !single_tip_shas.is_empty() {
            msg_args.extend(single_tip_shas.clone());
        }
        let single_messages_raw = if single_tip_shas.is_empty() {
            String::new()
        } else {
            git_ro(&msg_args)?
        };
        let single_messages: Vec<&str> = if single_tip_shas.is_empty() {
            Vec::new()
        } else {
            single_messages_raw
                .split('\u{001e}')
                .map(|message| message.trim_end_matches('\n'))
                .collect()
        };
        let mut single_idx = 0usize;

        for (offset, group) in groups[start_idx..end_idx_exclusive].iter().enumerate() {
            let tree = selected_trees.get(offset).copied().unwrap_or("");
            let message = if group.commits.len() > 1 {
                group.squash_commit_message()?
            } else {
                let message = single_messages.get(single_idx).copied().unwrap_or("");
                single_idx += 1;
                message.to_string()
            };
            let parent_tree =
                git_ro(["rev-parse", &format!("{}^{{tree}}", parent_sha)].as_slice())?
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string();
            if tree != parent_tree {
                let new_commit = git_rw(
                    execution_mode,
                    ["commit-tree", tree, "-p", &parent_sha, "-m", &message].as_slice(),
                )?
                .trim()
                .to_string();
                let action = if group.commits.len() > 1 {
                    PreparedGroupAction::Squashed
                } else {
                    PreparedGroupAction::PreservedSingleCommit
                };
                parent_sha = new_commit.clone();
                selected_groups.push(PreparedGroupData {
                    local_pr_number: start_idx + offset + 1,
                    stable_handle: crate::commands::common::stable_handle_text(&group.tag),
                    source_commit_count: group.commits.len(),
                    action,
                    target_sha: Some(new_commit),
                });
            } else {
                selected_groups.push(PreparedGroupData {
                    local_pr_number: start_idx + offset + 1,
                    stable_handle: crate::commands::common::stable_handle_text(&group.tag),
                    source_commit_count: group.commits.len(),
                    action: PreparedGroupAction::SkippedEmpty,
                    target_sha: None,
                });
            }
        }
    }

    let remainder: Vec<String> = groups
        .iter()
        .skip(end_idx_exclusive)
        .flat_map(|group| group.commits.iter().cloned())
        .collect();
    let mut replayed_commit_count = 0usize;
    let mut skipped_replay_commit_count = 0usize;
    if !remainder.is_empty() {
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for sha in &remainder {
            args.push(format!("{}^{{tree}}", sha));
        }
        let ref_args: Vec<&str> = args.iter().map(String::as_str).collect();
        let trees_out = git_ro(&ref_args)?;
        let trees: Vec<&str> = trees_out.lines().collect();
        let mut log_args: Vec<&str> = vec!["log", "--no-walk=unsorted", "--format=%B%x1e"];
        let remainder_refs: Vec<&str> = remainder.iter().map(String::as_str).collect();
        log_args.extend(remainder_refs);
        let bodies_raw = git_ro(&log_args)?;
        let bodies: Vec<&str> = bodies_raw
            .split('\u{001e}')
            .map(|body| body.trim_end_matches('\n'))
            .collect();
        for index in 0..remainder.len() {
            let tree = trees.get(index).copied().unwrap_or("");
            let message = bodies.get(index).copied().unwrap_or("");
            let parent_tree =
                git_ro(["rev-parse", &format!("{}^{{tree}}", parent_sha)].as_slice())?
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string();
            if tree == parent_tree {
                skipped_replay_commit_count += 1;
            } else {
                let new_commit = git_rw(
                    execution_mode,
                    ["commit-tree", tree, "-p", &parent_sha, "-m", message].as_slice(),
                )?
                .trim()
                .to_string();
                parent_sha = new_commit;
                replayed_commit_count += 1;
            }
        }
    }

    let current_branch = git_ro(["symbolic-ref", "--quiet", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    git_rw(
        execution_mode,
        [
            "update-ref",
            &format!("refs/heads/{}", current_branch),
            &parent_sha,
        ]
        .as_slice(),
    )?;

    let (limit, next_idx_opt, resolved_extent) = limit_and_next_idx(&groups, &selection)?;
    let (_merge_base, leading_ignored, updated_groups) =
        derive_groups_between_with_ignored(base, &parent_sha, ignore_tag)?;
    let (updated_groups, skipped_handles) =
        split_groups_for_update(&leading_ignored, updated_groups);
    let update_execution = crate::commands::build_from_groups_with_summary(
        base,
        prefix,
        &skipped_handles,
        false,
        execution_mode,
        pr_description_mode,
        limit,
        updated_groups,
        list_order,
        true,
        0,
        local_pr_branch_policy,
    )?;
    let update_summary = UpdateSummaryData::from_execution(
        UpdateRepoContext {
            base: base.to_string(),
            from: "HEAD".to_string(),
            prefix: prefix.to_string(),
            ignore_tag: ignore_tag.to_string(),
        },
        UpdateOptions {
            dry_run,
            no_pr: false,
            pr_description_mode,
            local_pr_branches: local_pr_branch_policy,
        },
        resolved_extent,
        update_execution,
    );

    let next_child = if let Some(next_idx) = next_idx_opt {
        if next_idx < groups.len() {
            let next_branch = branch_identities[next_idx].exact.clone();
            let prs = list_open_prs_for_heads(std::slice::from_ref(&next_branch))?;
            let next_key = canonical_branch_conflict_key(&next_branch);
            let matching_pr = prs
                .iter()
                .find(|pr| canonical_branch_conflict_key(&pr.head) == next_key);
            match matching_pr {
                Some(pr) => {
                    let action = match pr_description_mode {
                        crate::config::PrDescriptionMode::Overwrite => {
                            append_warning_to_pr(
                                pr.number,
                                "🚨🚨 parent PRs have changed, this PR may show extra diffs from parent PR 🚨🚨",
                                execution_mode,
                            )?;
                            if dry_run {
                                PrepNextChildAction::WouldAppendWarning
                            } else {
                                PrepNextChildAction::WarningAppended
                            }
                        }
                        crate::config::PrDescriptionMode::StackOnly => {
                            PrepNextChildAction::SkippedStackOnly
                        }
                    };
                    Some(PrepNextChildData {
                        local_pr_number: next_idx + 1,
                        stable_handle: crate::commands::common::stable_handle_text(
                            &groups[next_idx].tag,
                        ),
                        head_branch: next_branch,
                        remote_pr_number: Some(pr.number),
                        action,
                    })
                }
                None => Some(PrepNextChildData {
                    local_pr_number: next_idx + 1,
                    stable_handle: crate::commands::common::stable_handle_text(
                        &groups[next_idx].tag,
                    ),
                    head_branch: next_branch,
                    remote_pr_number: None,
                    action: PrepNextChildAction::MissingOpenPr,
                }),
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(PrepSummaryData {
        repo: PrepRepoContext {
            base: base.to_string(),
            prefix: prefix.to_string(),
            ignore_tag: ignore_tag.to_string(),
        },
        options: PrepOptions {
            dry_run,
            pr_description_mode,
        },
        selection: resolved_selection,
        selected_groups,
        rewritten_head_sha: Some(parent_sha),
        replayed_commit_count,
        skipped_replay_commit_count,
        next_child,
        update: Some(update_summary),
    })
}

pub fn print_prep_summary(summary: &PrepSummaryData) {
    for line in render_prep_summary(summary) {
        info!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::{prep_squash, render_prep_summary, resolve_prep_window, PrepExecutionOptions};
    use crate::cli::PrepSelection;
    use crate::config::{ListOrder, PrDescriptionMode};
    use crate::execution::ExecutionMode;
    use crate::maintenance_output::{PreparedGroupAction, ResolvedPrepSelection};
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, InclusiveSelector, StableHandle};
    use crate::test_support::{init_case_conflicting_stack_repo, lock_cwd, DirGuard};

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

    #[test]
    fn prep_until_stable_handle_resolves_inclusive_window() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let selection = PrepSelection::Until(InclusiveSelector::Group(GroupSelector::Stable(
            StableHandle {
                tag: "beta".to_string(),
            },
        )));

        assert_eq!(resolve_prep_window(&groups, &selection).unwrap(), (0, 2));
    }

    #[test]
    fn prep_exact_stable_handle_resolves_single_group_window() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let selection = PrepSelection::Exact(GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        }));

        assert_eq!(resolve_prep_window(&groups, &selection).unwrap(), (1, 2));
    }

    #[test]
    fn render_prep_summary_mentions_selected_groups() {
        let summary = crate::maintenance_output::PrepSummaryData {
            repo: crate::maintenance_output::PrepRepoContext {
                base: "main".to_string(),
                prefix: "dank-spr/".to_string(),
                ignore_tag: "ignore".to_string(),
            },
            options: crate::maintenance_output::PrepOptions {
                dry_run: true,
                pr_description_mode: PrDescriptionMode::Overwrite,
            },
            selection: ResolvedPrepSelection::All,
            selected_groups: vec![crate::maintenance_output::PreparedGroupData {
                local_pr_number: 1,
                stable_handle: "pr:alpha".to_string(),
                source_commit_count: 2,
                action: PreparedGroupAction::Squashed,
                target_sha: Some("abc123".to_string()),
            }],
            rewritten_head_sha: Some("abc123".to_string()),
            replayed_commit_count: 0,
            skipped_replay_commit_count: 0,
            next_child: None,
            update: None,
        };

        let lines = render_prep_summary(&summary);

        assert!(lines
            .iter()
            .any(|line| line.contains("Prepared LPR #1 / pr:alpha")));
    }

    #[test]
    fn prep_squash_rejects_case_colliding_branch_names_from_local_stack() {
        let _lock = lock_cwd();
        let dir = init_case_conflicting_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let err = prep_squash(
            "main",
            "dank-spr/",
            "ignore",
            PrepExecutionOptions {
                pr_description_mode: PrDescriptionMode::Overwrite,
                list_order: ListOrder::RecentOnBottom,
                local_pr_branch_policy: crate::config::LocalPrBranchSyncPolicy::Off,
                selection: PrepSelection::All,
                execution_mode: ExecutionMode::DryRun,
            },
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("pr:alpha and pr:Alpha derive conflicting synthetic branch names"),
            "unexpected error: {err}"
        );
    }
}
