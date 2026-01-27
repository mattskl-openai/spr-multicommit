//! Reorder local PR groups while preserving ignore blocks.

use anyhow::{anyhow, Result};
use tracing::info;

use crate::commands::common;
use crate::parsing::derive_local_groups_with_ignored;

fn parse_range(input: &str) -> Result<(usize, usize)> {
    if let Some(dots) = input.find("..") {
        let (a, b) = input.split_at(dots);
        let b = &b[2..];
        let ai: usize = a.trim().parse()?;
        let bi: usize = b.trim().parse()?;
        Ok((ai, bi))
    } else {
        let ai: usize = input.trim().parse()?;
        Ok((ai, ai))
    }
}

fn format_simple_plan(old: &[usize], new: &[usize], a: usize, b: usize, c: usize) -> String {
    let lhs = if a == b {
        format!("{}", a)
    } else {
        format!("{}..{}", a, b)
    };
    format!(
        "{}→{}: [{}] → [{}]",
        lhs,
        c,
        old.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(","),
        new.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// Cherry-pick a contiguous block of commits, preserving local-only history.
fn cherry_pick_block(dry: bool, tmp_path: &str, commits: &[String]) -> Result<()> {
    if commits.is_empty() {
        return Ok(());
    }
    let first = commits.first().expect("commits not empty");
    let last = commits.last().expect("commits not empty");
    if commits.len() == 1 {
        common::cherry_pick_commit(dry, tmp_path, first)
    } else {
        common::cherry_pick_range(dry, tmp_path, first, last)
    }
}

/// Move a group (or group range) to come after a target group index.
///
/// Ignore blocks (`pr:ignore` and its configured alias) remain attached to the
/// group that precedes them, so local-only commits move with their owning group.
///
/// # Errors
///
/// Returns errors for invalid ranges, invalid `--after` positions, or when Git
/// operations (worktree creation, cherry-picks, reset) fail.
pub fn move_groups_after(
    base: &str,
    ignore_tag: &str,
    range: &str,
    after: &str,
    safe: bool,
    dry: bool,
) -> Result<()> {
    // Discover groups from local commits bottom→top
    let (merge_base, leading_ignored, groups) = derive_local_groups_with_ignored(base, ignore_tag)?;
    let n = groups.len();
    if n == 0 {
        info!("No local PR groups found; nothing to move.");
        return Ok(());
    }

    let (a, b) = parse_range(range)?; // 1-based inclusive
    if a == 0 || b == 0 || a > n || b > n {
        return Err(anyhow!(
            "Range out of bounds: {}..{} with N={} groups",
            a,
            b,
            n
        ));
    }
    let c: usize = match after.trim().to_lowercase().as_str() {
        "bottom" => 0,
        "top" => n,
        s => s.parse::<usize>().map_err(|_| {
            anyhow!(
                "--after must be a number in 0..={} or one of: bottom, top (got '{}')",
                n,
                after
            )
        })?,
    };
    if c > n {
        return Err(anyhow!("--after must be in 0..={} (got {})", n, c));
    }

    if a == b {
        if a == c {
            info!("Already in desired position: {}", a);
            return Ok(());
        }
    } else if a >= b {
        return Err(anyhow!("Invalid range: require A<B (got {}..{})", a, b));
    }
    if c != 0 && c >= a && c <= b {
        return Err(anyhow!(
            "--after target C={} must not be within [{}..{}]",
            c,
            a,
            b
        ));
    }

    // Compute new order by removing [a..b] and inserting AFTER position c
    let mut old_order: Vec<usize> = (1..=n).collect();
    let removed: Vec<usize> = old_order.drain(a - 1..b).collect();
    let mut new_order: Vec<usize> = Vec::with_capacity(n);
    // Determine insertion point in remaining list
    let len_removed = b - a + 1;
    // after C: insert index is C in remaining (0 means bottom)
    let insert_pos = if c < a {
        c
    } else {
        c.saturating_sub(len_removed)
    };
    let mut i = 0usize;
    while i < old_order.len() && i < insert_pos {
        new_order.push(old_order[i]);
        i += 1;
    }
    // Insert removed block
    new_order.extend_from_slice(&removed);
    // Remainder
    while i < old_order.len() {
        new_order.push(old_order[i]);
        i += 1;
    }

    let plan = format_simple_plan(&((1..=n).collect::<Vec<_>>()), &new_order, a, b, c);
    info!("Plan: {}", plan);

    if new_order == (1..=n).collect::<Vec<_>>() {
        info!("Order unchanged; nothing to do.");
        return Ok(());
    }

    // Optionally create a backup branch at current HEAD
    let (cur_branch, short) = common::get_current_branch_and_short()?;
    if safe {
        let _ = common::create_backup_branch(dry, "move", &cur_branch, &short)?;
    }

    // Build the new history in a temporary worktree off merge-base
    let (tmp_path, tmp_branch) = common::create_temp_worktree(dry, "move", &merge_base, &short)?;

    // Preserve ignored commits that appear before the first group
    cherry_pick_block(dry, &tmp_path, &leading_ignored)?;

    // Cherry-pick commits in the new order, group by group (batched per-group)
    for idx in &new_order {
        let g = &groups[*idx - 1];
        if let (Some(first), Some(last)) = (g.commits.first(), g.commits.last()) {
            common::cherry_pick_range(dry, &tmp_path, first, last)?;
        }
        cherry_pick_block(dry, &tmp_path, &g.ignored_after)?;
    }

    let new_tip = common::tip_of_tmp(&tmp_path)?;
    info!(
        "Updating current branch {} to new tip {} (stack reordered)…",
        cur_branch, new_tip
    );
    common::reset_current_branch_to(dry, &new_tip)?;

    // Cleanup temp worktree/branch
    common::cleanup_temp_worktree(dry, &tmp_path, &tmp_branch)?;

    Ok(())
}
