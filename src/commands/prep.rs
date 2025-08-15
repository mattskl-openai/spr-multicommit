use anyhow::{anyhow, bail, Result};
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::limit::Limit;
use crate::parsing::parse_groups;

/// Squash PRs according to selection; operate locally then run update for the affected groups.
pub fn prep_squash(
    base: &str,
    prefix: &str,
    selection: crate::cli::PrepSelection,
    dry: bool,
) -> Result<()> {
    // Work purely on local commit stack: build groups from base..HEAD
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
        info!("Nothing to prep");
        return Ok(());
    }

    let total_groups = groups.len();
    let num_to_prep = match selection {
        crate::cli::PrepSelection::All => total_groups,
        crate::cli::PrepSelection::Until(n) => n.min(total_groups),
        crate::cli::PrepSelection::Exact(i) => {
            if i == 0 || i > total_groups {
                bail!("--exact out of range (1..={})", total_groups);
            }
            1
        }
    };
    let to_prep = match selection {
        crate::cli::PrepSelection::Exact(i) => groups
            .iter()
            .skip(i - 1)
            .take(1)
            .cloned()
            .collect::<Vec<_>>(),
        _ => groups.iter().take(num_to_prep).cloned().collect::<Vec<_>>(),
    };
    info!("Locally squashing {} group(s)", to_prep.len());
    for (i, g) in to_prep.iter().enumerate() {
        info!("Group {}: tag = {}", i + 1, g.tag);
        for (j, sha) in g.commits.iter().enumerate() {
            info!("  Commit {}: {}", j + 1, sha);
        }
    }

    // Build a new chain: starting parent depends on selection
    let mut parent_sha = match selection {
        crate::cli::PrepSelection::Exact(i) => {
            if i == 1 {
                merge_base.clone()
            } else {
                groups[i - 2].commits.last().unwrap().clone()
            }
        }
        _ => merge_base.clone(),
    };
    // Batch tip trees
    if !to_prep.is_empty() {
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for g in &to_prep {
            let tip = g
                .commits
                .last()
                .ok_or_else(|| anyhow!("Empty group {}", g.tag))?;
            args.push(format!("{}^{{tree}}", tip));
        }
        let ref_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let trees_out = git_ro(&ref_args)?;
        let trees: Vec<&str> = trees_out.lines().collect();
        for (idx, g) in to_prep.iter().enumerate() {
            let tree = trees.get(idx).copied().unwrap_or("");
            let msg = g.squash_commit_message()?;
            let new_commit = git_rw(
                dry,
                ["commit-tree", tree, "-p", &parent_sha, "-m", &msg].as_slice(),
            )?
            .trim()
            .to_string();
            parent_sha = new_commit;
        }
    }

    // Replay the remaining commits (not prepped) on top to preserve the rest of the stack
    let skip_after = match selection {
        crate::cli::PrepSelection::Exact(i) => i,
        _ => num_to_prep,
    };
    let remainder: Vec<String> = groups
        .iter()
        .skip(skip_after)
        .flat_map(|g| g.commits.iter().cloned())
        .collect();
    if !remainder.is_empty() {
        // Batch trees
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for sha in &remainder {
            args.push(format!("{}^{{tree}}", sha));
        }
        let ref_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let trees_out = git_ro(&ref_args)?;
        let trees: Vec<&str> = trees_out.lines().collect();
        // Batch bodies
        let mut log_args: Vec<&str> = vec!["log", "-1", "--format=%B%x1e"]; // RS-separated
        let rem_refs: Vec<&str> = remainder.iter().map(|s| s.as_str()).collect();
        log_args.extend(rem_refs);
        let bodies_raw = git_ro(&log_args)?;
        let bodies: Vec<&str> = bodies_raw
            .split('\u{001e}')
            .map(|s| s.trim_end_matches('\n'))
            .filter(|s| !s.is_empty())
            .collect();
        for i in 0..remainder.len() {
            let tree = trees.get(i).copied().unwrap_or("");
            let msg = bodies.get(i).copied().unwrap_or("");
            let new_commit = git_rw(
                dry,
                ["commit-tree", tree, "-p", &parent_sha, "-m", msg].as_slice(),
            )?
            .trim()
            .to_string();
            parent_sha = new_commit;
        }
    }

    // Move current branch to new head (includes squashed N groups + unchanged remainder)
    let cur_branch = git_ro(["symbolic-ref", "--quiet", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    git_rw(
        dry,
        [
            "update-ref",
            &format!("refs/heads/{}", cur_branch),
            &parent_sha,
        ]
        .as_slice(),
    )?;

    // Immediately run update for affected groups
    let limit = match selection {
        crate::cli::PrepSelection::All => None,
        crate::cli::PrepSelection::Until(_) => Some(Limit::ByPr(num_to_prep)),
        crate::cli::PrepSelection::Exact(_) => None,
    };
    crate::commands::update::build_from_tags(base, "HEAD", prefix, false, dry, limit)
}
