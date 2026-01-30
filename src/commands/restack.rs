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
use crate::config::RestackConflictPolicy;
use crate::git::git_rw;
use crate::parsing::{derive_local_groups_with_ignored, Group};

/// A single planned cherry-pick step used to rebuild the restacked history.
///
/// `spr restack` constructs an ordered plan of these operations and executes
/// them inside a temporary worktree and branch. A `Range` represents a
/// contiguous commit interval (inclusive) expressed as `first^..last`.
///
/// Callers should treat this as an execution primitive: errors are surfaced to
/// the caller, and cleanup/rollback decisions are intentionally handled at a
/// higher level where the conflict policy is known.
#[derive(Debug, Clone)]
enum CherryPickOp {
    /// Cherry-pick a single commit SHA.
    Commit { sha: String },
    /// Cherry-pick an inclusive range from `first` through `last`.
    Range { first: String, last: String },
}

impl CherryPickOp {
    /// Execute this cherry-pick operation in the temp worktree at `tmp_path`.
    ///
    /// This method is intentionally thin and does not attempt to detect or
    /// resolve conflicts; callers must check for conflict state and decide
    /// whether to halt or roll back based on policy.
    fn run(&self, dry: bool, tmp_path: &str) -> Result<()> {
        match self {
            CherryPickOp::Commit { sha } => common::cherry_pick_commit(dry, tmp_path, sha),
            CherryPickOp::Range { first, last } => {
                common::cherry_pick_range(dry, tmp_path, first, last)
            }
        }
    }

    /// Render a user-facing git command that mirrors this operation.
    ///
    /// This string is used in halt instructions so a human can continue the
    /// remaining plan manually from the temp worktree.
    fn command_for_user(&self, tmp_path: &str) -> String {
        match self {
            CherryPickOp::Commit { sha } => {
                format!("git -C {} cherry-pick {}", tmp_path, sha)
            }
            CherryPickOp::Range { first, last } => {
                format!("git -C {} cherry-pick {}^..{}", tmp_path, first, last)
            }
        }
    }
}

fn cherry_pick_head_exists(tmp_path: &str) -> bool {
    Command::new("git")
        .args(["-C", tmp_path, "rev-parse", "-q", "--verify", "CHERRY_PICK_HEAD"])
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
fn build_cherry_pick_plan(kept_ignored: &[String], remaining: &[Group]) -> Vec<CherryPickOp> {
    let mut ops: Vec<CherryPickOp> = vec![];

    if let (Some(first), Some(last)) = (kept_ignored.first(), kept_ignored.last()) {
        if kept_ignored.len() == 1 {
            ops.push(CherryPickOp::Commit { sha: first.clone() });
        } else {
            ops.push(CherryPickOp::Range {
                first: first.clone(),
                last: last.clone(),
            });
        }
    }

    for g in remaining {
        if let (Some(first), Some(last)) = (g.commits.first(), g.commits.last()) {
            ops.push(CherryPickOp::Range {
                first: first.clone(),
                last: last.clone(),
            });
        }
        if let (Some(first), Some(last)) = (g.ignored_after.first(), g.ignored_after.last()) {
            if g.ignored_after.len() == 1 {
                ops.push(CherryPickOp::Commit { sha: first.clone() });
            } else {
                ops.push(CherryPickOp::Range {
                    first: first.clone(),
                    last: last.clone(),
                });
            }
        }
    }

    ops
}

/// Emit user-facing rollback and manual-continue instructions for a halted restack.
///
/// The instructions are explicit about the temp worktree location because a
/// common mistake is to resolve conflicts in the original worktree, which does
/// not affect the halted cherry-pick sequence.
fn emit_halt_instructions(
    cur_branch: &str,
    backup_branch: Option<&str>,
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
    if let Some(backup) = backup_branch {
        info!("  # Optional: restore the backup created by --safe");
        info!("  git checkout {}", backup);
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
    after: usize,
    safe: bool,
    dry: bool,
    conflict_policy: RestackConflictPolicy,
) -> Result<()> {
    // Ensure we operate against the latest remote state
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No local PR groups found; nothing to restack.");
        return Ok(());
    }
    let (cur_branch, short) = common::get_current_branch_and_short()?;
    // Clamp 'after' to the number of groups; if equal, there is nothing to move
    let after = std::cmp::min(after, groups.len());
    let mut kept_ignored: Vec<String> = leading_ignored;
    for g in groups.iter().take(after) {
        kept_ignored.extend(g.ignored_after.iter().cloned());
    }
    let remaining = &groups[after..];
    if remaining.is_empty() && kept_ignored.is_empty() {
        // Nothing to move; sync current branch to base
        if safe {
            let _ = common::create_backup_branch(dry, "restack", &cur_branch, &short)?;
        }
        info!(
            "Skipping all {} PR(s); syncing current branch {} to {}",
            groups.len(),
            cur_branch,
            base
        );
        common::reset_current_branch_to(dry, base)?;
        return Ok(());
    }

    // Create a local backup branch pointing to current HEAD before rewriting
    let backup_branch = if safe {
        Some(common::create_backup_branch(dry, "restack", &cur_branch, &short)?)
    } else {
        None
    };

    let (tmp_path, tmp_branch) = common::create_temp_worktree(dry, "restack", base, &short)?;
    let ops = build_cherry_pick_plan(&kept_ignored, remaining);
    for (idx, op) in ops.iter().enumerate() {
        if let Err(err) = op.run(dry, &tmp_path) {
            let conflict = cherry_pick_head_exists(&tmp_path);
            if conflict && conflict_policy == RestackConflictPolicy::Halt {
                emit_halt_instructions(
                    &cur_branch,
                    backup_branch.as_deref(),
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
                abort_cherry_pick_best_effort(dry, &tmp_path);
            }
            cleanup_temp_worktree_best_effort(dry, &tmp_path, &tmp_branch);
            return Err(err).context("restack failed; temp restack state was cleaned up");
        }
    }

    let new_tip = common::tip_of_tmp(&tmp_path)?;
    info!(
        "Rebased commits after first {} PR(s) of {} onto {} (including ignored commits)",
        after, cur_branch, base
    );
    common::reset_current_branch_to(dry, &new_tip)?;
    cleanup_temp_worktree_best_effort(dry, &tmp_path, &tmp_branch);

    Ok(())
}
