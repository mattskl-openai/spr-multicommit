use anyhow::{anyhow, bail, Result};
use regex::Regex;
use tracing::info;

use crate::commands::common;
use crate::git::git_ro;
use crate::parsing::derive_local_groups;

/// Move the last `tail_count` commits (top-of-stack) to become the tail of PR `n` (1-based, bottom→top).
pub fn fix_pr_tail(
    base: &str,
    ignore_tag: &str,
    n: usize,
    tail_count: usize,
    safe: bool,
    dry: bool,
) -> Result<()> {
    if tail_count == 0 {
        return Ok(());
    }

    let (merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    let total_groups = groups.len();
    if total_groups == 0 {
        info!("No local PR groups found; nothing to fix.");
        return Ok(());
    }

    // Validate target PR index (1-based)
    if n == 0 || n > total_groups {
        return Err(anyhow!(
            "Target PR N={} out of bounds (1..={})",
            n,
            total_groups
        ));
    }
    let target_n = n;

    // Flatten commits bottom→top
    let mut all_commits: Vec<String> = groups
        .iter()
        .flat_map(|g| g.commits.iter().cloned())
        .collect();
    if all_commits.is_empty() {
        info!("No commits found; nothing to fix.");
        return Ok(());
    }

    // Determine top M commits (trim if M > total)
    let m = tail_count.min(all_commits.len());
    let top_commits: Vec<String> = all_commits.split_off(all_commits.len() - m);

    // Validate: moved commits must NOT contain pr:<tag> markers
    let re_tag = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
    let mut offenders: Vec<String> = vec![];
    for sha in &top_commits {
        let msg = git_ro(["log", "-n", "1", "--format=%B", sha].as_slice())?;
        if re_tag.is_match(&msg) {
            offenders.push(sha.clone());
        }
    }
    if !offenders.is_empty() {
        bail!(
            "Selected tail commit(s) contain pr:<tag> markers; cannot move commits that start or belong to PR groups: {}",
            offenders
                .iter()
                .map(|s| s.chars().take(8).collect::<String>())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Determine insertion index after last commit of PR N within the remainder
    let last_of_n = groups
        .get(target_n - 1)
        .and_then(|g| g.commits.last())
        .ok_or_else(|| anyhow!("PR {} has no commits", target_n))?
        .clone();
    let insert_pos = all_commits
        .iter()
        .position(|sha| sha == &last_of_n)
        .ok_or_else(|| anyhow!("Could not locate last commit of PR {} in stream", target_n))?;

    // Build new order: remainder with top commits inserted after PR N's tail
    let mut new_order: Vec<String> = Vec::with_capacity(all_commits.len() + top_commits.len());
    new_order.extend(all_commits[..=insert_pos].iter().cloned());
    new_order.extend(top_commits.iter().cloned());
    if insert_pos + 1 < all_commits.len() {
        new_order.extend(all_commits[insert_pos + 1..].iter().cloned());
    }

    // Optionally create a backup branch at current HEAD (safety)
    let (cur_branch, short) = common::get_current_branch_and_short()?;
    if safe {
        let _ = common::create_backup_branch(dry, "fix-pr", &cur_branch, &short)?;
    }

    // Build the new history in a temporary worktree off merge-base
    let (tmp_path, tmp_branch) = common::create_temp_worktree(dry, "fix", &merge_base, &short)?;

    for sha in &new_order {
        // Cherry-pick the commit onto tmp
        common::cherry_pick_commit(dry, &tmp_path, sha)?;
    }

    // Point current branch to new tip
    let new_tip = common::tip_of_tmp(&tmp_path)?;
    info!(
        "Updating current branch {} to new tip {} (fix-pr applied)…",
        cur_branch, new_tip
    );
    common::reset_current_branch_to(dry, &new_tip)?;

    // Cleanup temp worktree/branch
    common::cleanup_temp_worktree(dry, &tmp_path, &tmp_branch)?;

    Ok(())
}
