//! Restack a local PR stack while keeping ignored commits on the branch.
//!
//! This command rebuilds the portion of the stack that comes after the first
//! `N` PR groups by cherry-picking those commits onto the latest `base` in a
//! temporary worktree and branch. Once the cherry-picks succeed, the current
//! branch is hard-reset to the rebuilt tip and the temp state is removed.
//!
//! Conflict handling is policy-driven via the `restack_conflict` config key.
//! The default `halt` behavior suspends the replay, leaves the temp worktree
//! in place, and writes a resume file for `spr resume <path>`. The `rollback`
//! behavior preserves the historical cleanup-on-conflict path.

use anyhow::Result;
use tracing::info;

use crate::commands::common;
use crate::commands::common::CherryPickOp;
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteSession,
};
use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
use crate::git::git_rev_parse;
use crate::git::git_rw;
use crate::parsing::{derive_local_groups_with_ignored, Group};
use crate::restack_output::{render_human_preview, RestackPreviewData, RestackPreviewGroup};
use crate::selectors::{resolve_after_count, AfterSelector};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestackExecutionOptions {
    safe: bool,
    dry: bool,
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
        stable_handle: format!("pr:{}", group.tag),
        commit_count: group.commits.len(),
        ignored_after_count: group.ignored_after.len(),
    }
}

impl RestackPlan {
    fn preview_data(&self, safe_requested: bool) -> RestackPreviewData {
        let would_change_branch =
            !self.dropped_groups.is_empty() || !self.remaining_groups.is_empty();
        let would_create_temp_worktree =
            !self.remaining_groups.is_empty() || !self.kept_ignored_segments.is_empty();
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
        Ok(plan.preview_data(safe_requested))
    } else {
        let (current_branch, _) = common::get_current_branch_and_short()?;
        let original_head = git_rev_parse("HEAD")?;
        Ok(RestackPlan {
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
        }
        .preview_data(safe_requested))
    }
}

fn log_human_restack_plan(plan: &RestackPlan, safe_requested: bool) {
    let data = plan.preview_data(safe_requested);
    for line in render_human_preview("Restack plan", &data).lines() {
        info!("{line}");
    }
}

fn restack_after_resolved(
    metadata_context: &crate::stack_metadata::RefreshMetadataContext,
    plan: RestackPlan,
    options: RestackExecutionOptions,
) -> Result<RewriteCommandOutcome> {
    log_human_restack_plan(&plan, options.safe);

    common::with_dirty_worktree_policy(
        options.dry,
        "spr restack",
        options.dirty_worktree_policy,
        |deferred_dirty_worktree_restore| {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            let original_head = git_rev_parse("HEAD")?;
            let original_worktree_root = rewrite_resume::current_repo_root()?;
            let metadata_refresh_context = metadata_context.clone();
            if plan.remaining_groups.is_empty() && plan.kept_ignored_segments.is_empty() {
                if options.safe {
                    let _ = common::create_backup_tag(options.dry, "restack", &cur_branch, &short)?;
                }
                info!(
                    "Skipping all {} PR(s); syncing current branch {} to {}",
                    plan.dropped_groups.len(),
                    cur_branch,
                    metadata_context.base
                );
                common::reset_current_branch_to(options.dry, &metadata_context.base)?;
                if !options.dry {
                    crate::stack_metadata::refresh_metadata_for_branch(
                        &original_worktree_root,
                        &cur_branch,
                        &metadata_refresh_context,
                        None,
                    )?;
                }
                Ok(RewriteCommandOutcome::Completed)
            } else {
                let resume_path = rewrite_resume::prepare_resume_path_for_new_session(
                    options.dry,
                    RewriteCommandKind::Restack,
                    &cur_branch,
                    &original_head,
                )?;
                let backup_tag = if options.safe {
                    Some(common::create_backup_tag(
                        options.dry,
                        "restack",
                        &cur_branch,
                        &short,
                    )?)
                } else {
                    None
                };

                let (tmp_path, tmp_branch) = common::create_temp_worktree(
                    options.dry,
                    "restack",
                    &metadata_context.base,
                    &short,
                )?;
                let outcome = rewrite_resume::run_rewrite_session(
                    options.dry,
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
    dry: bool,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    if let Some(plan) = collect_restack_plan(metadata_context, after, true)? {
        restack_after_resolved(
            metadata_context,
            plan,
            RestackExecutionOptions {
                safe,
                dry,
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
    dry: bool,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    if let Some(plan) = collect_restack_plan_after_count(metadata_context, after, true)? {
        restack_after_resolved(
            metadata_context,
            plan,
            RestackExecutionOptions {
                safe,
                dry,
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
        preview_restack_after, resolve_restack_after_count,
    };
    use crate::commands::common::{CherryPickEmptyPolicy, CherryPickOp};
    use crate::commands::rewrite_resume::{resume_rewrite, RewriteResumeState};
    use crate::commands::RewriteCommandOutcome;
    use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
    use crate::parsing::Group;
    use crate::selectors::{AfterSelector, GroupSelector, StableHandle};
    use crate::test_support::{commit_file, git, lock_cwd, write_file, DirGuard};
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
                tag: tag.to_string(),
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
        let after = AfterSelector::Group(GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        }));

        assert_eq!(resolve_restack_after_count(&groups, &after).unwrap(), 2);
    }

    #[test]
    fn kept_ignored_segments_preserve_group_boundaries() {
        let groups = vec![
            Group {
                tag: "alpha".to_string(),
                subjects: vec!["feat: alpha".to_string()],
                commits: vec!["a1".to_string()],
                first_message: Some("feat: alpha pr:alpha".to_string()),
                ignored_after: vec!["i1".to_string(), "i2".to_string()],
            },
            Group {
                tag: "beta".to_string(),
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
                .map(|group| group.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(
            plan.remaining_groups
                .iter()
                .map(|group| group.tag.as_str())
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

        let preview = preview_restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::Stable(StableHandle {
                tag: "alpha".to_string(),
            })),
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
        assert!(preview.would_create_temp_worktree);
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

    #[test]
    fn restack_halt_policy_suspends_and_resumes_conflict() {
        let _lock = lock_cwd();
        let dir = init_restack_conflict_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);

        let outcome = super::restack_after(
            &metadata_context(),
            &AfterSelector::Group(GroupSelector::LocalPr(1)),
            false,
            false,
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
        resolve_restack_conflict(Path::new(&resume_state.temp_worktree_path));

        let resumed = resume_rewrite(false, &resume_path).expect("resume restack");
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
