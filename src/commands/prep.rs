use anyhow::{anyhow, bail, Result};
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::github::{append_warning_to_pr, list_spr_prs};
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

    // Decide which index to operate on, honoring selection
    let k_opt: Option<usize> = match selection {
        crate::cli::PrepSelection::All => groups
            .iter()
            .enumerate()
            .find(|(_i, g)| g.commits.len() > 1)
            .map(|(i, _)| i),
        crate::cli::PrepSelection::Until(n) => {
            let upper = n.min(groups.len());
            groups
                .iter()
                .take(upper)
                .enumerate()
                .find(|(_i, g)| g.commits.len() > 1)
                .map(|(i, _)| i)
        }
        crate::cli::PrepSelection::Exact(i) => {
            if i == 0 || i > groups.len() { bail!("--exact out of range (1..={})", groups.len()); }
            let idx = i - 1;
            if groups[idx].commits.len() > 1 { Some(idx) } else { None }
        }
    };

    // If no multi-commit group in range, nothing to do
    let Some(k) = k_opt else {
        info!("No multi-commit PR found in the selected range; nothing to squash");
        return Ok(());
    };

    // Start rebuilding history from just before the first multi-commit group
    let mut parent_sha = if k == 0 {
        merge_base.clone()
    } else {
        groups[k - 1]
            .commits
            .last()
            .cloned()
            .expect("group has at least one commit")
    };

    // Squash the first multi-commit group into a single commit
    let tip = groups[k]
        .commits
        .last()
        .ok_or_else(|| anyhow!("Empty group {}", groups[k].tag))?;
    let tip_tree = git_ro(["rev-parse", &format!("{}^{{tree}}", tip)].as_slice())?
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    // Skip creating a commit if tree equals parent's tree (no changes)
    let parent_tree = git_ro(["rev-parse", &format!("{}^{{tree}}", parent_sha)].as_slice())?
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    if tip_tree != parent_tree {
        let msg = groups[k].squash_commit_message()?;
        let new_commit = git_rw(
            dry,
            ["commit-tree", &tip_tree, "-p", &parent_sha, "-m", &msg].as_slice(),
        )?
        .trim()
        .to_string();
        parent_sha = new_commit;
    } else {
        info!("Skipping empty squashed commit for group {} (no tree changes)", groups[k].tag);
    }

    // Replay the remaining commits (above k) as-is on top to preserve their messages
    let remainder: Vec<String> = groups
        .iter()
        .skip(k + 1)
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
        // Batch bodies: ensure 1:1 with input order and include empty messages
        let mut log_args: Vec<&str> = vec!["log", "--no-walk=unsorted", "--format=%B%x1e"]; // RS-separated
        let rem_refs: Vec<&str> = remainder.iter().map(|s| s.as_str()).collect();
        log_args.extend(rem_refs);
        let bodies_raw = git_ro(&log_args)?;
        let bodies: Vec<&str> = bodies_raw
            .split('\u{001e}')
            .map(|s| s.trim_end_matches('\n'))
            .collect();
        for i in 0..remainder.len() {
            let tree = trees.get(i).copied().unwrap_or("");
            let msg = bodies.get(i).copied().unwrap_or("");
            // Skip creating a commit if this commit's tree equals parent's tree
            let parent_tree = git_ro(["rev-parse", &format!("{}^{{tree}}", parent_sha)].as_slice())?
                .lines()
                .next()
                .unwrap_or("")
                .to_string();
            if tree == parent_tree {
                info!("Skipping empty replay commit {} of {} (no changes)", i + 1, remainder.len());
                continue;
            }
            let new_commit = git_rw(
                dry,
                ["commit-tree", tree, "-p", &parent_sha, "-m", msg].as_slice(),
            )?
            .trim()
            .to_string();
            parent_sha = new_commit;
        }
    }

    // Move current branch to new head (includes squashed group + unchanged remainder reparented)
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

    // Decide limit for pushing and whether to warn the next PR
    let (limit, next_idx_opt) = match selection {
        crate::cli::PrepSelection::All => (None, None),
        crate::cli::PrepSelection::Until(n) => {
            if n == 0 { (None, None) } else { (Some(Limit::ByPr(n)), Some(n)) }
        }
        crate::cli::PrepSelection::Exact(i) => (Some(Limit::ByPr(i)), Some(i)),
    };

    // Push updates for the selected scope
    crate::commands::update::build_from_tags(base, "HEAD", prefix, false, dry, limit)?;

    // Add a warning to the first PR not included in the push
    if let Some(next_idx) = next_idx_opt {
        if next_idx < groups.len() {
            let next_branch = format!("{}{}", prefix, groups[next_idx].tag);
            let prs = list_spr_prs(prefix)?;
            if let Some(pr) = prs.iter().find(|p| p.head == next_branch) {
                append_warning_to_pr(
                    pr.number,
                    "🚨🚨 parent PRs have changed, this PR may show extra diffs from parent PR 🚨🚨",
                    dry,
                )?;
            }
        }
    }

    Ok(())
}
