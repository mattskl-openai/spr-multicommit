use anyhow::{anyhow, bail, Result};
use regex::Regex;
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::parsing::derive_local_groups;

/// Move the last `tail_count` commits (top-of-stack) to become the tail of PR `n` (1-based, bottom→top).
pub fn fix_pr_tail(base: &str, n: usize, tail_count: usize, safe: bool, dry: bool) -> Result<()> {
    if tail_count == 0 {
        return Ok(());
    }

    let (merge_base, groups) = derive_local_groups(base)?;
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
                .map(|s| format!("{}", &s.chars().take(8).collect::<String>()))
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
    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    if safe {
        let backup = format!("backup/fix-pr/{}-{}", cur_branch, short);
        info!("Creating backup branch at HEAD: {}", backup);
        let _ = git_rw(dry, ["branch", &backup, "HEAD"].as_slice())?;
    }

    // Build the new history in a temporary worktree off merge-base
    let tmp_branch = format!("spr/tmp-fix-{}", short);
    let tmp_path = format!("/tmp/spr-fix-{}", short);
    info!(
        "Rewriting stack in temp worktree {} on branch {}…",
        tmp_path, tmp_branch
    );
    let _ = git_rw(
        dry,
        [
            "worktree",
            "add",
            "-f",
            "-b",
            &tmp_branch,
            &tmp_path,
            &merge_base,
        ]
        .as_slice(),
    )?;

    for sha in &new_order {
        // Cherry-pick the commit onto tmp
        git_rw(dry, ["-C", &tmp_path, "cherry-pick", sha].as_slice())?;
    }

    // Point current branch to new tip
    let new_tip = git_ro(["-C", &tmp_path, "rev-parse", "HEAD"].as_slice())?
        .trim()
        .to_string();
    info!(
        "Updating current branch {} to new tip {} (fix-pr applied)…",
        cur_branch, new_tip
    );
    let _ = git_rw(dry, ["reset", "--hard", &new_tip].as_slice())?;

    // Cleanup temp worktree/branch
    let _ = git_rw(dry, ["worktree", "remove", "-f", &tmp_path].as_slice())?;
    let _ = git_rw(dry, ["branch", "-D", &tmp_branch].as_slice())?;

    Ok(())
}
