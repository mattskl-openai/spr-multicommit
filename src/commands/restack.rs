use anyhow::Result;
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::parsing::parse_groups;

/// Restack the local stack by rebasing all commits after the first `after` PRs onto `base`.
///
/// How it works:
/// - Compute PR groups from `base..HEAD` (via `pr:<tag>` markers), bottom→top.
/// - If `after == 0`: set `upstream = merge-base(base, HEAD)`.
/// - Else: set `upstream = <first_commit_of_group_{after+1}>^` (parent of the first commit after the first N groups).
/// - Run: `git rebase --onto <base> <upstream> <current-branch>`.
///
/// This moves the entire range starting at the first commit of group N+1 onto `base`, leaving the first N PRs untouched.
pub fn restack_after(base: &str, _prefix: &str, after: usize, dry: bool) -> Result<()> {
    let merge_base = git_ro(["merge-base", base, "HEAD"].as_slice())?
        .trim()
        .to_string();
    let lines = git_ro(
        [
            "log",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &format!("{}..HEAD", merge_base),
        ]
        .as_slice(),
    )?;
    let groups = parse_groups(&lines)?;
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
