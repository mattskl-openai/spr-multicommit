//! Restack a local PR stack while keeping ignored commits on the branch.
//!
//! This command rebuilds the portion of the stack that comes after the first
//! `N` PR groups by cherry-picking those commits onto the latest `base` in a
//! temporary worktree and branch. Once the cherry-picks succeed, the current
//! branch is hard-reset to the rebuilt tip and the temp state is removed.
//!
//! Conflict handling is policy-driven via the `restack_conflict` config key.
//! The default `rollback` behavior cleans up the temp state on conflict; the
//! `halt` behavior leaves the temp worktree and branch in place and prints the
//! exact commands needed to either roll back or continue manually.

use std::process::Command;

use anyhow::{anyhow, Context, Result};
use tracing::{info, warn};

use crate::commands::common;
use crate::commands::common::CherryPickOp;
use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
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

fn cherry_pick_head_exists(tmp_path: &str) -> bool {
    Command::new("git")
        .args([
            "-C",
            tmp_path,
            "rev-parse",
            "-q",
            "--verify",
            "CHERRY_PICK_HEAD",
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn abort_cherry_pick_best_effort(dry: bool, tmp_path: &str) {
    if let Err(e) = git_rw(dry, ["-C", tmp_path, "cherry-pick", "--abort"].as_slice()) {
        warn!("Failed to abort cherry-pick in {}: {}", tmp_path, e);
    }
}

fn cleanup_temp_worktree_best_effort(dry: bool, tmp_path: &str, tmp_branch: &str) {
    if let Err(e) = common::cleanup_temp_worktree(dry, tmp_path, tmp_branch) {
        warn!(
            "Failed to clean up temp restack state ({} / {}): {}",
            tmp_path, tmp_branch, e
        );
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

/// Emit user-facing rollback and manual-continue instructions for a halted restack.
///
/// The instructions are explicit about the temp worktree location because a
/// common mistake is to resolve conflicts in the original worktree, which does
/// not affect the halted cherry-pick sequence.
fn emit_halt_instructions(
    cur_branch: &str,
    backup_tag: Option<&str>,
    base: &str,
    tmp_path: &str,
    tmp_branch: &str,
    op_index: usize,
    ops: &[CherryPickOp],
) {
    info!("Restack halted due to a cherry-pick conflict.");
    info!("Base: {}", base);
    info!("Temp worktree: {}", tmp_path);
    info!("Temp branch: {}", tmp_branch);
    info!("You remain on branch: {}", cur_branch);

    info!("To roll back and clean up temp restack state:");
    info!("  # if a cherry-pick is in progress, abort it first");
    info!("  git -C {} cherry-pick --abort", tmp_path);
    info!("  git worktree remove -f {}", tmp_path);
    info!("  git branch -D {}", tmp_branch);
    info!("  git checkout {}", cur_branch);
    if let Some(backup) = backup_tag {
        info!("  # Optional: restore the --safe backup tag onto your current branch");
        info!("  git reset --hard refs/tags/{}", backup);
    }

    info!("To resolve and continue the restack manually:");
    info!("  cd {}", tmp_path);
    info!("  git status");
    info!("  # resolve conflicts, then stage the resolutions");
    info!("  git add <paths>");
    info!("  git cherry-pick --continue");

    let remaining_ops = ops.iter().skip(op_index + 1).collect::<Vec<_>>();
    if !remaining_ops.is_empty() {
        info!("  # then apply the remaining cherry-picks:");
        for op in remaining_ops {
            info!("  {}", op.command_for_user(tmp_path));
        }
    }

    info!("  # finalize by moving your branch to the temp restack tip:");
    info!("  git reset --hard {}", tmp_branch);
    info!("  git worktree remove -f {}", tmp_path);
    info!("  git branch -D {}", tmp_branch);
    info!("  spr update");
}

fn resolve_restack_after_count(groups: &[Group], after: &AfterSelector) -> Result<usize> {
    resolve_after_count(groups, after)
}

fn restack_after_resolved(
    base: &str,
    leading_ignored: Vec<String>,
    groups: Vec<Group>,
    after: usize,
    options: RestackExecutionOptions,
) -> Result<()> {
    let after = std::cmp::min(after, groups.len());
    let kept_ignored_segments = build_kept_ignored_segments(leading_ignored, &groups, after);
    let remaining = groups[after..].to_vec();

    common::with_dirty_worktree_policy(
        options.dry,
        "spr restack",
        options.dirty_worktree_policy,
        || {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
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
                Ok(())
            } else {
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
                for (idx, op) in ops.iter().enumerate() {
                    if let Err(err) = op.run(options.dry, &tmp_path) {
                        let conflict = cherry_pick_head_exists(&tmp_path);
                        if conflict && options.conflict_policy == RestackConflictPolicy::Halt {
                            emit_halt_instructions(
                                &cur_branch,
                                backup_tag.as_deref(),
                                base,
                                &tmp_path,
                                &tmp_branch,
                                idx,
                                &ops,
                            );
                            return Err(anyhow!(
                                "restack halted due to conflict; resolve in temp worktree and continue manually"
                            ));
                        }

                        if conflict {
                            abort_cherry_pick_best_effort(options.dry, &tmp_path);
                        }
                        cleanup_temp_worktree_best_effort(options.dry, &tmp_path, &tmp_branch);
                        return Err(err)
                            .context("restack failed; temp restack state was cleaned up");
                    }
                }

                let new_tip = common::tip_of_tmp(&tmp_path)?;
                info!(
                    "Rebased commits after first {} PR(s) of {} onto {} (including ignored commits)",
                    after, cur_branch, base
                );
                common::reset_current_branch_to(options.dry, &new_tip)?;
                cleanup_temp_worktree_best_effort(options.dry, &tmp_path, &tmp_branch);

                Ok(())
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
/// With the default `Rollback` conflict policy, any cherry-pick conflict aborts
/// the restack attempt and `spr` *attempts* to clean up the temporary worktree
/// and branch. Cleanup failures are logged as warnings and may require manual
/// cleanup.
///
/// With the `Halt` conflict policy, restack stops at the first conflict and
/// prints step-by-step instructions for resolving the conflict in the temp
/// worktree and finishing the cherry-pick sequence manually before resetting
/// the original branch to the rebuilt tip.
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
) -> Result<()> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No local PR groups found; nothing to restack.");
        Ok(())
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
) -> Result<()> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        Ok(())
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
    use crate::parsing::Group;
    use crate::selectors::{AfterSelector, GroupSelector, StableHandle};

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
}
