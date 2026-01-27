//! Restack a local PR stack while keeping ignored commits on the branch.

use anyhow::Result;
use tracing::info;

use crate::commands::common;
use crate::git::git_rw;
use crate::parsing::derive_local_groups_with_ignored;

/// Restack the local stack by rebasing commits after the first `after` PRs onto `base`.
///
/// This preserves ignored commits (`pr:ignore` blocks) by carrying them into the
/// rebuilt history. Ignored commits that appear between dropped PR groups are kept
/// before the remaining stack.
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
    if safe {
        let _ = common::create_backup_branch(dry, "restack", &cur_branch, &short)?;
    }

    let (tmp_path, tmp_branch) = common::create_temp_worktree(dry, "restack", base, &short)?;
    // Preserve ignored commits from dropped groups before the remaining stack.
    if !kept_ignored.is_empty() {
        let first = kept_ignored.first().expect("kept_ignored not empty");
        let last = kept_ignored.last().expect("kept_ignored not empty");
        if kept_ignored.len() == 1 {
            common::cherry_pick_commit(dry, &tmp_path, first)?;
        } else {
            common::cherry_pick_range(dry, &tmp_path, first, last)?;
        }
    }
    for g in remaining {
        if let (Some(first), Some(last)) = (g.commits.first(), g.commits.last()) {
            common::cherry_pick_range(dry, &tmp_path, first, last)?;
        }
        if !g.ignored_after.is_empty() {
            let first = g.ignored_after.first().expect("ignored_after not empty");
            let last = g.ignored_after.last().expect("ignored_after not empty");
            if g.ignored_after.len() == 1 {
                common::cherry_pick_commit(dry, &tmp_path, first)?;
            } else {
                common::cherry_pick_range(dry, &tmp_path, first, last)?;
            }
        }
    }

    let new_tip = common::tip_of_tmp(&tmp_path)?;
    info!(
        "Rebased commits after first {} PR(s) of {} onto {} (including ignored commits)",
        after, cur_branch, base
    );
    common::reset_current_branch_to(dry, &new_tip)?;
    common::cleanup_temp_worktree(dry, &tmp_path, &tmp_branch)?;

    Ok(())
}
