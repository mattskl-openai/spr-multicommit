use anyhow::Result;
use tracing::info;

use crate::commands::common;
use crate::git::git_rw;
use crate::parsing::derive_local_groups;

/// Restack the local stack by rebasing all commits after the first `after` PRs onto `base`.
///
/// How it works:
/// - Compute PR groups from `base..HEAD` (via `pr:<tag>` markers), bottom→top.
/// - If `after == 0`: set `upstream = merge-base(base, HEAD)`.
/// - Else: set `upstream = <first_commit_of_group_{after+1}>^` (parent of the first commit after the first N groups).
/// - Run: `git rebase --onto <base> <upstream> <current-branch>`.
///
/// This moves the entire range starting at the first commit of group N+1 onto `base`, leaving the first N PRs untouched.
pub fn restack_after(base: &str, after: usize, safe: bool, dry: bool) -> Result<()> {
    // Ensure we operate against the latest remote state
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (merge_base, groups) = derive_local_groups(base)?;
    if groups.is_empty() {
        info!("No local PR groups found; nothing to restack.");
        return Ok(());
    }
    let (cur_branch, short) = common::get_current_branch_and_short()?;
    // Clamp 'after' to the number of groups; if equal, there is nothing to move
    let after = std::cmp::min(after, groups.len());
    if after == groups.len() {
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

    // Determine upstream for rebase. To include the first commit of group N+1 in the rebase,
    // set upstream to the parent of that commit (i.e., `<first>^`). For N == 0, use merge-base.
    let upstream: String = if after == 0 {
        merge_base
    } else {
        let next = &groups[after];
        if let Some(first) = next.commits.first() {
            format!("{}^", first)
        } else {
            merge_base
        }
    };

    // Create a local backup branch pointing to current HEAD before rebasing
    if safe {
        let _ = common::create_backup_branch(dry, "restack", &cur_branch, &short)?;
    }
    info!(
        "Rebasing commits after first {} PR(s) of {} onto {} (upstream = {})",
        after, cur_branch, base, upstream
    );
    git_rw(
        dry,
        ["rebase", "--onto", base, &upstream, &cur_branch].as_slice(),
    )?;

    Ok(())
}
