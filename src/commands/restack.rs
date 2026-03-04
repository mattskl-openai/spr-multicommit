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
use crate::selectors::{resolve_after_count, AfterSelector};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestackExecutionOptions {
    safe: bool,
    dry: bool,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
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

fn restack_after_resolved(
    base: &str,
    leading_ignored: Vec<String>,
    groups: Vec<Group>,
    after: usize,
    options: RestackExecutionOptions,
) -> Result<RewriteCommandOutcome> {
    let after = std::cmp::min(after, groups.len());
    let kept_ignored_segments = build_kept_ignored_segments(leading_ignored, &groups, after);
    let remaining = groups[after..].to_vec();

    common::with_dirty_worktree_policy(
        options.dry,
        "spr restack",
        options.dirty_worktree_policy,
        |deferred_dirty_worktree_restore| {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            let original_head = git_rev_parse("HEAD")?;
            let original_worktree_root = rewrite_resume::current_repo_root()?;
            if remaining.is_empty() && kept_ignored_segments.is_empty() {
                if options.safe {
                    let _ = common::create_backup_tag(options.dry, "restack", &cur_branch, &short)?;
                }
                info!(
                    "Skipping all {} PR(s); syncing current branch {} to {}",
                    groups.len(),
                    cur_branch,
                    base
                );
                common::reset_current_branch_to(options.dry, base)?;
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

                let (tmp_path, tmp_branch) =
                    common::create_temp_worktree(options.dry, "restack", base, &short)?;
                let ops = build_cherry_pick_plan(&kept_ignored_segments, &remaining);
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
                        operations: ops,
                        deferred_dirty_worktree_restore,
                        post_success_hint: None,
                    },
                )?;
                if outcome == RewriteCommandOutcome::Completed {
                    info!(
                        "Rebased commits after first {} PR(s) of {} onto {} (including ignored commits)",
                        after, cur_branch, base
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
    base: &str,
    ignore_tag: &str,
    after: &AfterSelector,
    safe: bool,
    dry: bool,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No local PR groups found; nothing to restack.");
        Ok(RewriteCommandOutcome::Completed)
    } else {
        let after = resolve_restack_after_count(&groups, after)?;
        restack_after_resolved(
            base,
            leading_ignored,
            groups,
            after,
            RestackExecutionOptions {
                safe,
                dry,
                conflict_policy,
                dirty_worktree_policy,
            },
        )
    }
}

/// Restack the local stack by keeping the first `after` groups in place.
pub fn restack_after_count(
    base: &str,
    ignore_tag: &str,
    after: usize,
    safe: bool,
    dry: bool,
    conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        Ok(RewriteCommandOutcome::Completed)
    } else {
        restack_after_resolved(
            base,
            leading_ignored,
            groups,
            after,
            RestackExecutionOptions {
                safe,
                dry,
                conflict_policy,
                dirty_worktree_policy,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{build_cherry_pick_plan, build_kept_ignored_segments, resolve_restack_after_count};
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
            "main",
            "ignore",
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
