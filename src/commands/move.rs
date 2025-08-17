use anyhow::{anyhow, Result};
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::parsing::derive_local_groups;

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

pub fn move_groups_after(
    base: &str,
    range: &str,
    after: &str,
    safe: bool,
    dry: bool,
) -> Result<()> {
    // Discover groups from local commits bottom→top
    let (merge_base, groups) = derive_local_groups(base)?;
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
    } else if !(a < b) {
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
    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    if safe {
        let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
            .trim()
            .to_string();
        let backup = format!("backup/move/{}-{}", cur_branch, short);
        info!("Creating backup branch at HEAD: {}", backup);
        let _ = git_rw(dry, ["branch", &backup, "HEAD"].as_slice())?;
    }

    // Build the new history in a temporary worktree off merge-base
    let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    let tmp_branch = format!("spr/tmp-move-{}", short);
    let tmp_path = format!("/tmp/spr-move-{}", short);
    info!(
        "Rebuilding stack in temp worktree {} on branch {}…",
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

    // Cherry-pick commits in the new order, group by group (batched per-group)
    for idx in &new_order {
        let g = &groups[*idx - 1];
        if let (Some(first), Some(last)) = (g.commits.first(), g.commits.last()) {
            let range = format!("{}^..{}", first, last);
            git_rw(dry, ["-C", &tmp_path, "cherry-pick", &range].as_slice())?;
        }
    }

    let new_tip = git_ro(["-C", &tmp_path, "rev-parse", "HEAD"].as_slice())?
        .trim()
        .to_string();
    info!(
        "Updating current branch {} to new tip {} (stack reordered)…",
        cur_branch, new_tip
    );
    let _ = git_rw(dry, ["reset", "--hard", &new_tip].as_slice())?;

    // Cleanup temp worktree/branch
    let _ = git_rw(dry, ["worktree", "remove", "-f", &tmp_path].as_slice())?;
    let _ = git_rw(dry, ["branch", "-D", &tmp_branch].as_slice())?;

    Ok(())
}
