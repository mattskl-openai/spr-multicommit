//! Adjust the tail of a PR group while preserving ignore blocks.

use anyhow::{anyhow, bail, Result};
use std::collections::HashSet;
use tracing::info;

use crate::commands::common;
use crate::git::git_ro;
use crate::parsing::derive_local_groups_with_ignored;
use crate::selectors::{resolve_group_ordinal, GroupSelector};

fn resolve_fix_pr_target(
    groups: &[crate::parsing::Group],
    target: &GroupSelector,
) -> Result<usize> {
    resolve_group_ordinal(groups, target)
}

/// Move the last `tail_count` commits (top-of-stack) to become the tail of PR `n` (1-based, bottom→top).
///
/// Ignore blocks are treated as part of the preceding group and are never moved.
/// If the selected tail commits intersect an ignore block, the operation aborts.
///
/// # Errors
///
/// Returns errors when the target index is out of range, when the tail contains
/// `pr:<tag>` markers, when the tail intersects ignored commits, or when Git
/// operations (worktree creation, cherry-picks, reset) fail.
pub fn fix_pr_tail(
    base: &str,
    ignore_tag: &str,
    target: &GroupSelector,
    tail_count: usize,
    safe: bool,
    dry: bool,
) -> Result<()> {
    if tail_count == 0 {
        return Ok(());
    }

    let (merge_base, leading_ignored, groups) = derive_local_groups_with_ignored(base, ignore_tag)?;
    let total_groups = groups.len();
    if total_groups == 0 {
        info!("No local PR groups found; nothing to fix.");
        return Ok(());
    }

    let target_n = resolve_fix_pr_target(&groups, target)?;

    // Flatten commits bottom→top
    let mut all_commits: Vec<String> = Vec::new();
    all_commits.extend(leading_ignored.iter().cloned());
    for g in &groups {
        all_commits.extend(g.commits.iter().cloned());
        all_commits.extend(g.ignored_after.iter().cloned());
    }
    if all_commits.is_empty() {
        info!("No commits found; nothing to fix.");
        return Ok(());
    }

    // Determine top M commits (trim if M > total)
    let m = tail_count.min(all_commits.len());
    let top_commits: Vec<String> = all_commits.split_off(all_commits.len() - m);

    // Validate: moved commits must NOT contain pr:<tag> markers
    let mut offenders: Vec<String> = vec![];
    for sha in &top_commits {
        let msg = git_ro(["log", "-n", "1", "--format=%B", sha].as_slice())?;
        if crate::pr_labels::candidate_marker_regex().is_match(&msg) {
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

    // Disallow moving commits from ignore blocks; they must stay attached to their PR.
    let mut ignored_set: HashSet<String> = HashSet::new();
    ignored_set.extend(leading_ignored.iter().cloned());
    for g in &groups {
        ignored_set.extend(g.ignored_after.iter().cloned());
    }
    let mut ignored_moved: Vec<String> = top_commits
        .iter()
        .filter(|sha| ignored_set.contains(*sha))
        .cloned()
        .collect();
    if !ignored_moved.is_empty() {
        ignored_moved.sort();
        ignored_moved.dedup();
        bail!(
            "Selected tail commit(s) are in an ignored block; adjust --tail to avoid moving ignored commits: {}",
            ignored_moved
                .iter()
                .map(|s| s.chars().take(8).collect::<String>())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Determine insertion index after last commit of PR N (including its ignore block) within the remainder
    let target = groups
        .get(target_n - 1)
        .ok_or_else(|| anyhow!("PR {} has no commits", target_n))?;
    let last_of_n = if let Some(last_ignored) = target.ignored_after.last() {
        last_ignored.clone()
    } else {
        target
            .commits
            .last()
            .ok_or_else(|| anyhow!("PR {} has no commits", target_n))?
            .clone()
    };
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

    // Optionally create a backup tag at current HEAD (safety)
    let (cur_branch, short) = common::get_current_branch_and_short()?;
    if safe {
        let _ = common::create_backup_tag(dry, "fix-pr", &cur_branch, &short)?;
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

#[cfg(test)]
mod tests {
    use super::resolve_fix_pr_target;
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, StableHandle};

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter()
            .map(|tag| Group {
                tag: tag.to_string(),
                subjects: vec![format!("feat: {tag}")],
                commits: vec![format!("{tag}1")],
                first_message: Some(format!("feat: {tag} pr:{tag}")),
                ignored_after: Vec::new(),
            })
            .collect()
    }

    #[test]
    fn fix_pr_resolves_stable_handle_to_current_local_ordinal() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let target = GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        });

        assert_eq!(resolve_fix_pr_target(&groups, &target).unwrap(), 2);
    }
}
