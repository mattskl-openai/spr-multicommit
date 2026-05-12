//! Restack a local PR stack while keeping ignored commits on the branch.
//!
//! This command rebuilds the portion of the stack that comes after the first
//! `N` PR groups onto the latest `base`. Clean suffix-shaped plans run as an
//! in-place Git rebase. Other plans, or rebase attempts that do not complete
//! cleanly, fall back to the temp-worktree cherry-pick executor.
//!
//! Conflict handling is policy-driven via the `restack_conflict` config key.
//! The default `halt` behavior suspends the replay, leaves the temp worktree
//! in place, and writes a resume file for `spr resume <path>`. The `rollback`
//! behavior preserves the historical cleanup-on-conflict path.

use anyhow::{bail, Context, Result};
use std::path::Path;
use tracing::{info, warn};

use crate::commands::common;
use crate::commands::common::{CherryPickEmptyPolicy, CherryPickOp, NativeRebaseOutcome};
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteDestinationKind,
    RewriteSession,
};
use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
use crate::execution::ExecutionMode;
use crate::git::git_rev_list_range;
use crate::git::git_rev_parse;
use crate::git::git_ro;
use crate::git::git_rw;
use crate::parsing::{derive_local_groups_with_ignored, Group};
use crate::restack_output::{
    render_human_preview, RestackExecutorPlan, RestackPreviewData, RestackPreviewGroup,
};
use crate::selectors::{resolve_after_count, AfterSelector};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestackExecutionOptions {
    safe: bool,
    execution_mode: ExecutionMode,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
}

#[derive(Debug, Clone)]
pub struct RestackPlan {
    pub base_ref: String,
    pub base_sha: Option<String>,
    pub base_ref_was_refreshed: bool,
    pub current_branch: String,
    pub original_head: String,
    pub after_selector: String,
    pub resolved_after_count: usize,
    pub dropped_groups: Vec<Group>,
    pub remaining_groups: Vec<Group>,
    pub kept_ignored_segments: Vec<Vec<String>>,
    pub operations: Vec<CherryPickOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FastRestackPlan {
    upstream_exclusive: String,
    original_head: String,
    commit_count: usize,
}

impl FastRestackPlan {
    fn executor_plan(&self) -> RestackExecutorPlan {
        RestackExecutorPlan::NativeRebase {
            upstream_exclusive: self.upstream_exclusive.clone(),
            commit_count: self.commit_count,
        }
    }
}

/// Build the ordered cherry-pick plan that reconstructs the restacked history.
///
/// The plan applies:
/// 1. Ignored commits attached to dropped groups, kept before the remaining stack.
/// 2. Each remaining PR group's commits.
/// 3. Each remaining group's trailing ignored block.
fn build_cherry_pick_plan(
    kept_ignored_segments: &[Vec<String>],
    remaining: &[Group],
) -> Vec<CherryPickOp> {
    let mut ops: Vec<CherryPickOp> = kept_ignored_segments
        .iter()
        .filter_map(|segment| CherryPickOp::from_commits(segment))
        .collect();

    for g in remaining {
        ops.extend(CherryPickOp::from_commits(&g.commits));
        ops.extend(CherryPickOp::from_commits(&g.ignored_after));
    }

    ops
}

fn build_kept_ignored_segments(
    leading_ignored: Vec<String>,
    groups: &[Group],
    after: usize,
) -> Vec<Vec<String>> {
    let mut segments: Vec<Vec<String>> = if leading_ignored.is_empty() {
        Vec::new()
    } else {
        vec![leading_ignored]
    };

    segments.extend(
        groups
            .iter()
            .take(after)
            .filter(|group| !group.ignored_after.is_empty())
            .map(|group| group.ignored_after.clone()),
    );

    segments
}

fn commit_parent(sha: &str) -> Result<String> {
    git_rev_parse(&format!("{sha}^")).with_context(|| format!("failed to find parent of {sha}"))
}

fn expand_cherry_pick_op(operation: &CherryPickOp) -> Result<Option<Vec<String>>> {
    match operation {
        CherryPickOp::Commit { sha, empty_policy } => {
            if *empty_policy == CherryPickEmptyPolicy::StopOnEmpty {
                Ok(Some(vec![sha.clone()]))
            } else {
                Ok(None)
            }
        }
        CherryPickOp::Range {
            first,
            last,
            empty_policy,
        } => {
            if *empty_policy != CherryPickEmptyPolicy::StopOnEmpty {
                Ok(None)
            } else {
                let upstream_exclusive = commit_parent(first)?;
                let expanded = git_rev_list_range(&upstream_exclusive, last)?;
                if expanded.first() == Some(first) && expanded.last() == Some(last) {
                    Ok(Some(expanded))
                } else {
                    bail!(
                        "cherry-pick range {}^..{} expanded to unexpected endpoints {:?}..{:?}",
                        first,
                        last,
                        expanded.first(),
                        expanded.last()
                    );
                }
            }
        }
    }
}

fn expand_cherry_pick_ops(operations: &[CherryPickOp]) -> Result<Option<Vec<String>>> {
    let mut expanded = Some(Vec::new());
    for operation in operations {
        expanded = if let Some(mut current) = expanded {
            if let Some(mut operation_commits) = expand_cherry_pick_op(operation)? {
                current.append(&mut operation_commits);
                Some(current)
            } else {
                None
            }
        } else {
            None
        };
    }
    Ok(expanded)
}

fn plan_fast_suffix_rebase(
    original_head: &str,
    operations: &[CherryPickOp],
) -> Result<Option<FastRestackPlan>> {
    if operations.is_empty() {
        Ok(None)
    } else if let Some(expanded_operations) = expand_cherry_pick_ops(operations)? {
        let first_replayed = expanded_operations
            .first()
            .expect("non-empty operations expand to non-empty commits");
        let upstream_exclusive = commit_parent(first_replayed)?;
        let suffix = git_rev_list_range(&upstream_exclusive, original_head)?;
        if suffix == expanded_operations {
            Ok(Some(FastRestackPlan {
                upstream_exclusive,
                original_head: original_head.to_string(),
                commit_count: expanded_operations.len(),
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Emit user-facing rollback and manual-continue instructions for a halted restack.
///
/// The instructions are explicit about the temp worktree location because a
/// common mistake is to resolve conflicts in the original worktree, which does
/// not affect the halted cherry-pick sequence.
fn resolve_restack_after_count(groups: &[Group], after: &AfterSelector) -> Result<usize> {
    resolve_after_count(groups, after)
}

fn group_preview(group: &Group) -> RestackPreviewGroup {
    RestackPreviewGroup {
        stable_handle: group.selector_text(),
        commit_count: group.commits.len(),
        ignored_after_count: group.ignored_after.len(),
    }
}

impl RestackPlan {
    fn planned_executor(&self, allow_native_rebase: bool) -> Result<RestackExecutorPlan> {
        if self.dropped_groups.is_empty() && self.remaining_groups.is_empty() {
            Ok(RestackExecutorPlan::Noop)
        } else if self.remaining_groups.is_empty() && self.kept_ignored_segments.is_empty() {
            Ok(RestackExecutorPlan::ResetToBase)
        } else if allow_native_rebase {
            if let Some(fast_plan) = plan_fast_suffix_rebase(&self.original_head, &self.operations)?
            {
                Ok(fast_plan.executor_plan())
            } else {
                Ok(RestackExecutorPlan::TempWorktreeCherryPick {
                    operation_count: self.operations.len(),
                })
            }
        } else {
            Ok(RestackExecutorPlan::TempWorktreeCherryPick {
                operation_count: self.operations.len(),
            })
        }
    }

    fn preview_data(
        &self,
        safe_requested: bool,
        planned_executor: RestackExecutorPlan,
    ) -> RestackPreviewData {
        let would_change_branch =
            !self.dropped_groups.is_empty() || !self.remaining_groups.is_empty();
        let would_create_temp_worktree = matches!(
            planned_executor,
            RestackExecutorPlan::TempWorktreeCherryPick { .. }
        );
        let mut not_validated = if self.base_ref_was_refreshed {
            Vec::new()
        } else {
            vec!["remote freshness".to_string()]
        };
        not_validated.extend(
            [
                "cherry-pick conflicts",
                "tests",
                "pre-push hooks",
                "GitHub mergeability",
                "spr update result",
            ]
            .into_iter()
            .map(str::to_string),
        );

        RestackPreviewData {
            base_ref: self.base_ref.clone(),
            base_sha: self.base_sha.clone(),
            base_ref_was_refreshed: self.base_ref_was_refreshed,
            current_branch: self.current_branch.clone(),
            original_head: self.original_head.clone(),
            after_selector: self.after_selector.clone(),
            resolved_after_count: self.resolved_after_count,
            dropped_groups: self.dropped_groups.iter().map(group_preview).collect(),
            remaining_groups: self.remaining_groups.iter().map(group_preview).collect(),
            kept_ignored_segment_count: self.kept_ignored_segments.len(),
            planned_cherry_pick_operation_count: self.operations.len(),
            planned_executor,
            would_fetch_origin_when_executed: !self.base_ref_was_refreshed,
            would_create_backup_tag: safe_requested && would_change_branch,
            would_create_temp_worktree,
            would_reset_current_branch: would_change_branch,
            would_refresh_stack_metadata: would_change_branch,
            not_validated,
        }
    }
}

fn build_restack_plan(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    leading_ignored: Vec<String>,
    groups: Vec<Group>,
    after: usize,
    after_selector: String,
    base_ref_was_refreshed: bool,
) -> Result<RestackPlan> {
    let after = std::cmp::min(after, groups.len());
    let kept_ignored_segments = build_kept_ignored_segments(leading_ignored, &groups, after);
    let dropped_groups = groups[..after].to_vec();
    let remaining_groups = groups[after..].to_vec();
    let operations = build_cherry_pick_plan(&kept_ignored_segments, &remaining_groups);
    let (current_branch, _) = common::get_current_branch_and_short()?;
    let original_head = git_rev_parse("HEAD")?;

    Ok(RestackPlan {
        base_ref: metadata_context.base.clone(),
        base_sha: git_rev_parse(&metadata_context.base).ok(),
        base_ref_was_refreshed,
        current_branch,
        original_head,
        after_selector,
        resolved_after_count: after,
        dropped_groups,
        remaining_groups,
        kept_ignored_segments,
        operations,
    })
}

fn collect_restack_plan(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    after: &AfterSelector,
    base_ref_was_refreshed: bool,
) -> Result<Option<RestackPlan>> {
    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(&metadata_context.base, &metadata_context.ignore_tag)?;
    if groups.is_empty() {
        Ok(None)
    } else {
        let resolved_after_count = resolve_restack_after_count(&groups, after)?;
        build_restack_plan(
            metadata_context,
            leading_ignored,
            groups,
            resolved_after_count,
            after.to_string(),
            base_ref_was_refreshed,
        )
        .map(Some)
    }
}

fn collect_restack_plan_after_count(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    after: usize,
    base_ref_was_refreshed: bool,
) -> Result<Option<RestackPlan>> {
    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(&metadata_context.base, &metadata_context.ignore_tag)?;
    if groups.is_empty() {
        Ok(None)
    } else {
        build_restack_plan(
            metadata_context,
            leading_ignored,
            groups,
            after,
            after.to_string(),
            base_ref_was_refreshed,
        )
        .map(Some)
    }
}

pub fn preview_restack_after(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    after: &AfterSelector,
    safe_requested: bool,
) -> Result<RestackPreviewData> {
    if let Some(plan) = collect_restack_plan(metadata_context, after, false)? {
        let planned_executor = plan.planned_executor(true)?;
        Ok(plan.preview_data(safe_requested, planned_executor))
    } else {
        let (current_branch, _) = common::get_current_branch_and_short()?;
        let original_head = git_rev_parse("HEAD")?;
        let plan = RestackPlan {
            base_ref: metadata_context.base.clone(),
            base_sha: git_rev_parse(&metadata_context.base).ok(),
            base_ref_was_refreshed: false,
            current_branch,
            original_head,
            after_selector: after.to_string(),
            resolved_after_count: 0,
            dropped_groups: Vec::new(),
            remaining_groups: Vec::new(),
            kept_ignored_segments: Vec::new(),
            operations: Vec::new(),
        };
        let planned_executor = plan.planned_executor(true)?;
        Ok(plan.preview_data(safe_requested, planned_executor))
    }
}

fn log_human_restack_plan(
    plan: &RestackPlan,
    safe_requested: bool,
    planned_executor: RestackExecutorPlan,
) {
    let data = plan.preview_data(safe_requested, planned_executor);
    for line in render_human_preview("Restack plan", &data).lines() {
        info!("{line}");
    }
}

fn git_path_exists(path_key: &str) -> Result<bool> {
    let path = git_ro(["rev-parse", "--git-path", path_key].as_slice())?
        .trim()
        .to_string();
    Ok(Path::new(&path).exists())
}

fn verify_no_rebase_state() -> Result<()> {
    let stale_paths = ["rebase-apply", "rebase-merge"]
        .into_iter()
        .filter_map(|path_key| match git_path_exists(path_key) {
            Ok(true) => Some(Ok(path_key)),
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<Vec<_>>>()?;
    if stale_paths.is_empty() {
        Ok(())
    } else {
        bail!(
            "git rebase abort returned successfully, but rebase state still exists: {}",
            stale_paths.join(", ")
        );
    }
}

fn verify_fast_rebase_abort_cleanup(original_head: &str) -> Result<()> {
    let current_head = git_rev_parse("HEAD")?;
    let status = git_ro(["status", "--porcelain=v1"].as_slice())?;
    verify_no_rebase_state()?;
    if current_head != original_head {
        bail!(
            "git rebase abort did not restore HEAD: original {}, current {}",
            original_head,
            current_head
        );
    } else if !status.trim().is_empty() {
        bail!("git rebase abort left uncommitted work:\n{}", status);
    } else {
        Ok(())
    }
}

fn try_fast_suffix_rebase(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    metadata_refresh_context: &crate::stack_metadata::RefreshMetadataContext,
    fast_plan: &FastRestackPlan,
    cur_branch: &str,
    original_worktree_root: &str,
) -> Result<NativeRebaseOutcome> {
    let rebase_args = [
        "rebase",
        "--reapply-cherry-picks",
        "--empty=stop",
        "--onto",
        metadata_context.base.as_str(),
        fast_plan.upstream_exclusive.as_str(),
        cur_branch,
    ];
    match common::run_native_rebase_with_abort(
        ExecutionMode::Apply,
        rebase_args.as_slice(),
        "fast restack",
    )? {
        NativeRebaseOutcome::Aborted => {
            verify_fast_rebase_abort_cleanup(&fast_plan.original_head).with_context(|| {
                format!(
                    "fast restack rebase failed and abort returned, but the original stack checkout was not restored cleanly; inspect {}",
                    original_worktree_root
                )
            })?;
            warn!(
                "Fast restack rebase failed and was aborted; falling back to temp-worktree replay"
            );
            Ok(NativeRebaseOutcome::Aborted)
        }
        NativeRebaseOutcome::Completed => {
            crate::stack_metadata::refresh_metadata_for_branch(
                original_worktree_root,
                cur_branch,
                metadata_refresh_context,
                None,
            )?;
            info!(
                "Fast-restacked {} commit(s) with git rebase --onto {} {} {}",
                fast_plan.commit_count,
                metadata_context.base,
                fast_plan.upstream_exclusive,
                cur_branch
            );
            Ok(NativeRebaseOutcome::Completed)
        }
    }
}

fn restack_after_resolved(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    plan: RestackPlan,
    options: RestackExecutionOptions,
) -> Result<RewriteCommandOutcome> {
    let planned_executor = plan.planned_executor(options.execution_mode == ExecutionMode::Apply)?;
    log_human_restack_plan(&plan, options.safe, planned_executor.clone());

    common::with_dirty_worktree_policy(
        options.execution_mode,
        "spr restack",
        options.dirty_worktree_policy,
        |deferred_dirty_worktree_restore| {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            let original_head = git_rev_parse("HEAD")?;
            let original_worktree_root = rewrite_resume::current_repo_root()?;
            let metadata_refresh_context = metadata_context.clone();
            if plan.remaining_groups.is_empty() && plan.kept_ignored_segments.is_empty() {
                if options.safe {
                    let _ = common::create_backup_tag(
                        options.execution_mode,
                        "restack",
                        &cur_branch,
                        &short,
                    )?;
                }
                info!(
                    "Skipping all {} PR(s); syncing current branch {} to {}",
                    plan.dropped_groups.len(),
                    cur_branch,
                    metadata_context.base
                );
                common::reset_current_branch_to(options.execution_mode, &metadata_context.base)?;
                if options.execution_mode == ExecutionMode::Apply {
                    crate::stack_metadata::refresh_metadata_for_branch(
                        &original_worktree_root,
                        &cur_branch,
                        &metadata_refresh_context,
                        None,
                    )?;
                }
                Ok(RewriteCommandOutcome::Completed)
            } else {
                let (fast_outcome, existing_backup_tag) = match &planned_executor {
                    RestackExecutorPlan::NativeRebase {
                        upstream_exclusive,
                        commit_count,
                    } => {
                        let backup_tag = if options.safe {
                            Some(common::create_backup_tag(
                                ExecutionMode::Apply,
                                "restack",
                                &cur_branch,
                                &short,
                            )?)
                        } else {
                            None
                        };
                        let fast_plan = FastRestackPlan {
                            upstream_exclusive: upstream_exclusive.clone(),
                            original_head: original_head.clone(),
                            commit_count: *commit_count,
                        };
                        let outcome = try_fast_suffix_rebase(
                            metadata_context,
                            &metadata_refresh_context,
                            &fast_plan,
                            &cur_branch,
                            &original_worktree_root,
                        )?;
                        (outcome, backup_tag)
                    }
                    RestackExecutorPlan::Noop
                    | RestackExecutorPlan::ResetToBase
                    | RestackExecutorPlan::TempWorktreeCherryPick { .. } => {
                        (NativeRebaseOutcome::Aborted, None)
                    }
                };
                if fast_outcome == NativeRebaseOutcome::Completed {
                    Ok(RewriteCommandOutcome::Completed)
                } else {
                    let resume_path = rewrite_resume::prepare_resume_path_for_new_session(
                        options.execution_mode,
                        RewriteCommandKind::Restack,
                        &cur_branch,
                        &original_head,
                    )?;
                    let backup_tag = if existing_backup_tag.is_some() {
                        existing_backup_tag
                    } else if options.safe {
                        Some(common::create_backup_tag(
                            options.execution_mode,
                            "restack",
                            &cur_branch,
                            &short,
                        )?)
                    } else {
                        None
                    };

                    let (tmp_path, tmp_branch) = common::create_temp_worktree(
                        options.execution_mode,
                        "restack",
                        &metadata_context.base,
                        &short,
                    )?;
                    let outcome = rewrite_resume::run_rewrite_session(
                        options.execution_mode,
                        RewriteSession {
                            command_kind: RewriteCommandKind::Restack,
                            conflict_policy: if options.conflict_policy
                                == RestackConflictPolicy::Rollback
                            {
                                RewriteConflictPolicy::Rollback
                            } else {
                                RewriteConflictPolicy::Suspend
                            },
                            original_worktree_root,
                            original_branch: cur_branch.clone(),
                            original_head,
                            destination_kind: RewriteDestinationKind::CheckedOutBranch,
                            resume_path,
                            temp_branch: tmp_branch,
                            temp_worktree_path: tmp_path,
                            backup_tag,
                            operations: plan.operations.clone(),
                            deferred_dirty_worktree_restore,
                            post_success_hint: None,
                            metadata_refresh_context: Some(metadata_refresh_context),
                        },
                    )?;
                    if outcome == RewriteCommandOutcome::Completed {
                        info!(
                        "Rebased commits after first {} PR(s) of {} onto {} (including ignored commits)",
                        plan.resolved_after_count, cur_branch, metadata_context.base
                    );
                    }
                    Ok(outcome)
                }
            }
        },
    )
}

/// Restack the local stack by rebasing commits after the first `after` PRs onto `base`.
///
/// This preserves ignored commits (`pr:ignore` blocks) by carrying them into the
/// rebuilt history. Ignored commits that appear between dropped PR groups are kept
/// before the remaining stack.
///
/// With the default `Halt` conflict policy, any cherry-pick conflict suspends
/// the restack, leaves the temporary worktree and branch in place, and writes
/// a resume file for `spr resume <path>`.
///
/// With the `Rollback` conflict policy, restack preserves the historical
/// cleanup-on-conflict behavior and attempts to remove the temp worktree and
/// branch after aborting the failed cherry-pick.
///
/// # Errors
///
/// Returns errors from git operations (fetch, worktree creation, cherry-picks, reset).
pub fn restack_after(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    after: &AfterSelector,
    safe: bool,
    execution_mode: ExecutionMode,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(execution_mode, ["fetch", "origin"].as_slice())?;

    if let Some(plan) = collect_restack_plan(metadata_context, after, true)? {
        restack_after_resolved(
            metadata_context,
            plan,
            RestackExecutionOptions {
                safe,
                execution_mode,
                conflict_policy,
                dirty_worktree_policy,
            },
        )
    } else {
        info!("No local PR groups found; nothing to restack.");
        Ok(RewriteCommandOutcome::Completed)
    }
}

/// Restack the local stack by keeping the first `after` groups in place.
pub fn restack_after_count(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    after: usize,
    safe: bool,
    execution_mode: ExecutionMode,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(execution_mode, ["fetch", "origin"].as_slice())?;

    if let Some(plan) = collect_restack_plan_after_count(metadata_context, after, true)? {
        restack_after_resolved(
            metadata_context,
            plan,
            RestackExecutionOptions {
                safe,
                execution_mode,
                conflict_policy,
                dirty_worktree_policy,
            },
        )
    } else {
        Ok(RewriteCommandOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_cherry_pick_plan, build_kept_ignored_segments, build_restack_plan,
        plan_fast_suffix_rebase, preview_restack_after, resolve_restack_after_count,
    };
    use crate::commands::common::{CherryPickEmptyPolicy, CherryPickOp};
    use crate::commands::rewrite_resume::{resume_rewrite, RewriteResumeState};
    use crate::commands::RewriteCommandOutcome;
    use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
    use crate::execution::ExecutionMode;
    use crate::parsing::Group;
    use crate::restack_output::RestackExecutorPlan;
    use crate::selectors::{AfterSelector, ExplicitGroupSelector, GroupSelector};
    use crate::test_support::{commit_file, git, lock_cwd, write_file, DirGuard};
    use std::env;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn metadata_context() -> crate::stack_metadata::RefreshMetadataContext {
        crate::stack_metadata::RefreshMetadataContext {
            base: "main".to_string(),
            prefix: "dank-spr/".to_string(),
            ignore_tag: "ignore".to_string(),
        }
    }

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

    #[test]
    fn restack_after_stable_handle_keeps_that_group_and_lower_groups() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let after = AfterSelector::Group(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
            "beta".to_string(),
        )));

        assert_eq!(resolve_restack_after_count(&groups, &after).unwrap(), 2);
    }

    #[test]
    fn kept_ignored_segments_preserve_group_boundaries() {
        let groups = vec![
            Group {
                marker: crate::group_markers::GroupMarker::PrLabel("alpha".to_string()),
                subjects: vec!["feat: alpha".to_string()],
                commits: vec!["a1".to_string()],
                first_message: Some("feat: alpha pr:alpha".to_string()),
                ignored_after: vec!["i1".to_string(), "i2".to_string()],
            },
            Group {
                marker: crate::group_markers::GroupMarker::PrLabel("beta".to_string()),
                subjects: vec!["feat: beta".to_string()],
                commits: vec!["b1".to_string()],
                first_message: Some("feat: beta pr:beta".to_string()),
                ignored_after: vec!["i3".to_string(), "i4".to_string()],
            },
        ];

        assert_eq!(
            build_kept_ignored_segments(vec!["l1".to_string()], &groups, 2),
            vec![
                vec!["l1".to_string()],
                vec!["i1".to_string(), "i2".to_string()],
                vec!["i3".to_string(), "i4".to_string()],
            ]
        );
    }

    #[test]
    fn build_cherry_pick_plan_keeps_ignored_segments_separate() {
        let remaining = groups(&["gamma"]);

        assert_eq!(
            build_cherry_pick_plan(
                &[
                    vec!["i1".to_string(), "i2".to_string()],
                    vec!["i3".to_string(), "i4".to_string()],
                ],
                &remaining,
            ),
            vec![
                CherryPickOp::Range {
                    first: "i1".to_string(),
                    last: "i2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Range {
                    first: "i3".to_string(),
                    last: "i4".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "gamma1".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
            ]
        );
    }

    #[test]
    fn build_restack_plan_selects_dropped_remaining_ignored_and_ops() {
        let plan = build_restack_plan(
            &metadata_context(),
            vec!["lead1".to_string()],
            groups(&["alpha", "beta", "gamma"]),
            2,
            "pr:beta".to_string(),
            false,
        )
        .unwrap();

        assert_eq!(plan.after_selector, "pr:beta");
        assert_eq!(plan.resolved_after_count, 2);
        assert_eq!(
            plan.dropped_groups
                .iter()
                .map(Group::bare_selector_text)
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(
            plan.remaining_groups
                .iter()
                .map(Group::bare_selector_text)
                .collect::<Vec<_>>(),
            vec!["gamma"]
        );
        assert_eq!(plan.kept_ignored_segments, vec![vec!["lead1".to_string()]]);
        assert_eq!(plan.operations.len(), 2);
    }

    fn init_restack_conflict_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).expect("create repo dir");
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(&repo, "story.txt", "base\n");
        git(&repo, ["add", "story.txt"].as_slice());
        git(&repo, ["commit", "-m", "init"].as_slice());

        let origin = dir.path().join("origin.git");
        git(
            &repo,
            ["init", "--bare", origin.to_str().unwrap()].as_slice(),
        );
        git(
            &repo,
            ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
        );
        git(&repo, ["push", "-u", "origin", "main"].as_slice());

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha-1\n", "feat: alpha pr:alpha");
        commit_file(&repo, "story.txt", "stack-beta\n", "feat: beta pr:beta");
        git(&repo, ["checkout", "main"].as_slice());
        commit_file(&repo, "story.txt", "base-updated\n", "feat: base update");
        git(&repo, ["checkout", "stack"].as_slice());
        dir
    }

    fn init_restack_preview_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).expect("create repo dir");
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        commit_file(&repo, "base.txt", "base\n", "init");

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha pr:alpha");
        commit_file(&repo, "beta.txt", "beta\n", "feat: beta pr:beta");
        dir
    }

    fn init_suffix_planning_repo() -> (TempDir, Vec<String>) {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let shas = [
            commit_file(repo, "alpha.txt", "alpha\n", "feat: alpha pr:alpha"),
            commit_file(
                repo,
                "ignore-a.txt",
                "ignore-a\n",
                "local ignore A pr:ignore",
            ),
            commit_file(repo, "beta.txt", "beta\n", "feat: beta pr:beta"),
            commit_file(
                repo,
                "ignore-b.txt",
                "ignore-b\n",
                "local ignore B pr:ignore",
            ),
            commit_file(repo, "gamma.txt", "gamma\n", "feat: gamma pr:gamma"),
        ]
        .to_vec();
        (dir, shas)
    }

    #[test]
    fn fast_suffix_plan_accepts_exact_contiguous_suffix() {
        let _lock = lock_cwd();
        let (dir, shas) = init_suffix_planning_repo();
        let _guard = DirGuard::change_to(dir.path());

        let plan = plan_fast_suffix_rebase(
            &shas[4],
            &[CherryPickOp::Range {
                first: shas[2].clone(),
                last: shas[4].clone(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }],
        )
        .unwrap()
        .expect("beta..gamma is the top suffix");

        assert_eq!(plan.upstream_exclusive, shas[1]);
        assert_eq!(plan.original_head, shas[4]);
        assert_eq!(plan.commit_count, 3);
    }

    #[test]
    fn fast_suffix_plan_accepts_dropped_group_ignored_commit_when_it_is_in_suffix() {
        let _lock = lock_cwd();
        let (dir, shas) = init_suffix_planning_repo();
        let _guard = DirGuard::change_to(dir.path());

        let plan = plan_fast_suffix_rebase(
            &shas[4],
            &[
                CherryPickOp::Commit {
                    sha: shas[1].clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Range {
                    first: shas[2].clone(),
                    last: shas[4].clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
            ],
        )
        .unwrap()
        .expect("ignore-A..gamma is the top suffix");

        assert_eq!(plan.upstream_exclusive, shas[0]);
        assert_eq!(plan.commit_count, 4);
    }

    #[test]
    fn fast_suffix_plan_rejects_noncontiguous_kept_ignored_segments() {
        let _lock = lock_cwd();
        let (dir, shas) = init_suffix_planning_repo();
        let _guard = DirGuard::change_to(dir.path());

        let plan = plan_fast_suffix_rebase(
            &shas[4],
            &[
                CherryPickOp::Commit {
                    sha: shas[1].clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: shas[3].clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: shas[4].clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
            ],
        )
        .unwrap();

        assert!(plan.is_none());
    }

    #[test]
    fn fast_suffix_plan_rejects_partial_suffix() {
        let _lock = lock_cwd();
        let (dir, shas) = init_suffix_planning_repo();
        let _guard = DirGuard::change_to(dir.path());

        let plan = plan_fast_suffix_rebase(
            &shas[4],
            &[CherryPickOp::Commit {
                sha: shas[3].clone(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }],
        )
        .unwrap();

        assert!(plan.is_none());
    }

    fn read_preview_side_effect_snapshot(repo: &Path) -> String {
        [
            git(repo, ["rev-parse", "HEAD"].as_slice()),
            git(repo, ["branch", "--format=%(refname:short)"].as_slice()),
            git(repo, ["tag", "--list"].as_slice()),
            git(repo, ["worktree", "list", "--porcelain"].as_slice()),
            fs::read_dir(repo.join(".git/spr/resume"))
                .map(|entries| entries.count().to_string())
                .unwrap_or_else(|_| "<no-resume-dir>".to_string()),
        ]
        .join("\n---\n")
    }

    #[test]
    fn restack_preview_reports_plan_and_leaves_git_state_unchanged() {
        let _lock = lock_cwd();
        let dir = init_restack_preview_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let before = read_preview_side_effect_snapshot(&repo);
        let alpha_tip = git(&repo, ["rev-parse", "HEAD^"].as_slice())
            .trim()
            .to_string();

        let preview = preview_restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
                "alpha".to_string(),
            ))),
            true,
        )
        .unwrap();

        assert_eq!(preview.current_branch, "stack");
        assert_eq!(preview.after_selector, "pr:alpha");
        assert_eq!(preview.resolved_after_count, 1);
        assert_eq!(preview.dropped_groups[0].stable_handle, "pr:alpha");
        assert_eq!(preview.remaining_groups[0].stable_handle, "pr:beta");
        assert!(preview.would_fetch_origin_when_executed);
        assert!(preview.would_create_backup_tag);
        assert_eq!(
            preview.planned_executor,
            RestackExecutorPlan::NativeRebase {
                upstream_exclusive: alpha_tip,
                commit_count: 1,
            }
        );
        assert!(!preview.would_create_temp_worktree);
        assert!(preview.would_reset_current_branch);
        assert!(preview
            .not_validated
            .contains(&"cherry-pick conflicts".to_string()));
        assert_eq!(read_preview_side_effect_snapshot(&repo), before);
    }

    fn resolve_restack_conflict(temp_repo: &Path) {
        fs::write(temp_repo.join("story.txt"), "base-updated\nstack-beta\n")
            .expect("resolve restack conflict");
        git(temp_repo, ["add", "story.txt"].as_slice());
    }

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: String) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(original) = &self.original {
                env::set_var(self.key, original);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn init_clean_suffix_restack_repo() -> (TempDir, String, String) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).expect("create repo dir");
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(&repo, "story.txt", "base\n");
        git(&repo, ["add", "story.txt"].as_slice());
        git(&repo, ["commit", "-m", "init"].as_slice());

        let origin = dir.path().join("origin.git");
        git(
            &repo,
            ["init", "--bare", origin.to_str().unwrap()].as_slice(),
        );
        git(
            &repo,
            ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
        );
        git(&repo, ["push", "-u", "origin", "main"].as_slice());

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha-1\n", "feat: alpha pr:alpha");
        commit_file(&repo, "beta.txt", "beta-1\n", "feat: beta pr:beta");
        let original_stack_tip = git(&repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();
        let original_short = git(&repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();

        git(&repo, ["checkout", "main"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha-1\n", "feat: base has alpha");
        git(&repo, ["push", "origin", "main"].as_slice());
        git(&repo, ["checkout", "stack"].as_slice());

        (dir, original_stack_tip, original_short)
    }

    #[test]
    fn restack_clean_suffix_uses_native_rebase_without_temp_worktree() {
        let _lock = lock_cwd();
        let (dir, original_stack_tip, original_short) = init_clean_suffix_restack_repo();
        let repo = dir.path().join("repo");
        let trace_path = dir.path().join("restack-trace.jsonl");
        let _trace_guard =
            EnvVarGuard::set("GIT_TRACE2_EVENT", trace_path.to_string_lossy().to_string());
        let _guard = DirGuard::change_to(&repo);
        let preview = preview_restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::LocalPr(1)),
            true,
        )
        .expect("preview clean suffix restack");
        assert!(
            matches!(
                preview.planned_executor,
                RestackExecutorPlan::NativeRebase {
                    commit_count: 1,
                    ..
                }
            ),
            "clean suffix preview should name the native-rebase executor: {:?}",
            preview.planned_executor
        );
        assert!(
            !preview.would_create_temp_worktree,
            "clean suffix preview should predict the native-rebase executor"
        );

        let outcome = super::restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::LocalPr(1)),
            true,
            ExecutionMode::Apply,
            RestackConflictPolicy::Halt,
            DirtyWorktreePolicy::Halt,
        )
        .expect("restack should complete");

        assert_eq!(outcome, RewriteCommandOutcome::Completed);
        let trace = fs::read_to_string(&trace_path).expect("read git trace");
        assert!(
            trace.contains("\"argv\":[\"git\",\"rebase\""),
            "trace should contain native git rebase\n{trace}"
        );
        assert!(
            !trace.contains("\"argv\":[\"git\",\"worktree\",\"add\""),
            "clean suffix restack should not create a rewrite worktree\n{trace}"
        );
        assert_eq!(
            git(
                &repo,
                ["log", "--format=%s", "--reverse", "origin/main..HEAD"].as_slice()
            )
            .lines()
            .collect::<Vec<_>>(),
            vec!["feat: beta pr:beta"]
        );
        assert_eq!(
            git(&repo, ["merge-base", "origin/main", "HEAD"].as_slice())
                .trim()
                .to_string(),
            git(&repo, ["rev-parse", "origin/main"].as_slice())
                .trim()
                .to_string()
        );
        assert_eq!(
            git(
                &repo,
                [
                    "rev-parse",
                    &format!("backup/restack/stack-{original_short}")
                ]
                .as_slice()
            )
            .trim(),
            original_stack_tip
        );
        let metadata = fs::read_to_string(repo.join(".git/spr/stack_metadata_v1.json"))
            .expect("read refreshed metadata");
        assert!(metadata.contains("\"stack\""));
        assert!(metadata.contains("\"dank-spr/beta\""));
        assert!(!metadata.contains("\"dank-spr/alpha\""));
    }

    #[test]
    fn restack_halt_policy_suspends_and_resumes_conflict() {
        let _lock = lock_cwd();
        let dir = init_restack_conflict_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let original_head = git(&repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();

        let outcome = super::restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::LocalPr(1)),
            false,
            ExecutionMode::Apply,
            RestackConflictPolicy::Halt,
            DirtyWorktreePolicy::Halt,
        )
        .expect("restack should suspend");
        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended restack"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };
        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        assert_eq!(
            git(&repo, ["rev-parse", "HEAD"].as_slice()).trim(),
            original_head
        );
        assert!(
            git(&repo, ["status", "--porcelain=v1"].as_slice()).is_empty(),
            "fast rebase abort plus fallback suspend should leave the original worktree clean"
        );
        resolve_restack_conflict(Path::new(&resume_state.temp_worktree_path));

        let resumed = resume_rewrite(&resume_path).expect("resume restack");
        assert_eq!(resumed, RewriteCommandOutcome::Completed);
        assert!(
            !resume_path.exists(),
            "successful restack resume should delete the resume file"
        );
        assert_eq!(
            fs::read_to_string(repo.join("story.txt")).expect("read final file"),
            "base-updated\nstack-beta\n"
        );
        assert_eq!(
            crate::test_support::log_subjects(&repo, 2),
            vec![
                "feat: beta pr:beta".to_string(),
                "feat: base update".to_string(),
            ]
        );
    }
}
