use anyhow::Result;
use tracing::info;

use crate::git::{git_ro, git_rw};
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
    if after >= groups.len() {
        info!(
            "after={} >= {} group(s); nothing to restack.",
            after,
            groups.len()
        );
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

    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    if safe {
        // Create a local backup branch pointing to current HEAD before rebasing
        let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
            .trim()
            .to_string();
        let backup = format!("backup/restack/{}-{}", cur_branch, short);
        info!("Creating backup branch at HEAD: {}", backup);
        let _ = git_rw(dry, ["branch", &backup, "HEAD"].as_slice())?;
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
