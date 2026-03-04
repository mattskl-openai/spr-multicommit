//! Reorder local PR groups while preserving ignore blocks.

use anyhow::{anyhow, Result};
use tracing::info;

use crate::commands::common;
use crate::commands::common::CherryPickOp;
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteSession,
};
use crate::config::DirtyWorktreePolicy;
use crate::git::git_rev_parse;
use crate::github::get_open_pr_automerge_for_head;
use crate::parsing::derive_local_groups_with_ignored;
use crate::selectors::{
    resolve_after_count, resolve_group_range, AfterSelector, GroupRangeSelector,
};

/// Execution controls for `spr move`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveExecutionOptions {
    pub safe: bool,
    pub dry: bool,
    pub dirty_worktree_policy: DirtyWorktreePolicy,
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

fn build_move_operations(
    leading_ignored: &[String],
    groups: &[crate::parsing::Group],
    new_order: &[usize],
) -> Vec<CherryPickOp> {
    let mut operations = Vec::new();
    operations.extend(CherryPickOp::from_commits(leading_ignored));
    for idx in new_order {
        let group = &groups[*idx - 1];
        operations.extend(CherryPickOp::from_commits(&group.commits));
        operations.extend(CherryPickOp::from_commits(&group.ignored_after));
    }
    operations
}

fn resolve_move_targets(
    groups: &[crate::parsing::Group],
    range: &GroupRangeSelector,
    after: &AfterSelector,
) -> Result<(usize, usize, usize)> {
    let (a, b) = resolve_group_range(groups, range)?;
    let c = resolve_after_count(groups, after)?;
    Ok((a, b, c))
}

fn changes_stack_bottom(new_order: &[usize]) -> bool {
    new_order
        .first()
        .map(|bottom| *bottom != 1)
        .unwrap_or(false)
}

fn should_block_for_bottom_pr_automerge(
    bottom_pr_automerge_enabled: bool,
    new_order: &[usize],
) -> bool {
    if bottom_pr_automerge_enabled {
        changes_stack_bottom(new_order)
    } else {
        false
    }
}

fn enforce_bottom_pr_automerge_guard(
    prefix: &str,
    groups: &[crate::parsing::Group],
    new_order: &[usize],
) -> Result<()> {
    if changes_stack_bottom(new_order) {
        let bottom_group = &groups[0];
        let bottom_head = format!("{}{}", prefix, bottom_group.tag);
        if let Some(bottom_pr) = get_open_pr_automerge_for_head(&bottom_head)? {
            if should_block_for_bottom_pr_automerge(bottom_pr.auto_merge_enabled, new_order) {
                Err(anyhow!(
                    "Refusing to change the stack bottom because {} (#{} / pr:{}) has auto-merge enabled. Disable auto-merge on that bottom PR before moving any PR below it.",
                    bottom_head,
                    bottom_pr.number,
                    bottom_group.tag
                ))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
}

/// Move a group (or group range) to come after a target group index.
///
/// Ignore blocks (`pr:ignore` and its configured alias) remain attached to the
/// group that precedes them, so local-only commits move with their owning group.
/// If the current bottom PR has GitHub auto-merge enabled, this command also
/// refuses any move that would place another PR below it.
///
/// # Errors
///
/// Returns errors for invalid ranges, invalid `--after` positions, or when Git
/// operations (worktree creation, cherry-picks, reset) fail.
pub fn move_groups_after(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    range: &GroupRangeSelector,
    after: &AfterSelector,
    options: MoveExecutionOptions,
) -> Result<RewriteCommandOutcome> {
    // Discover groups from local commits bottom→top
    let (merge_base, leading_ignored, groups) = derive_local_groups_with_ignored(base, ignore_tag)?;
    let n = groups.len();
    if n == 0 {
        info!("No local PR groups found; nothing to move.");
        return Ok(RewriteCommandOutcome::Completed);
    }

    let (a, b, c) = resolve_move_targets(&groups, range, after)?;
    if a == 0 || b == 0 || a > n || b > n {
        return Err(anyhow!(
            "Range out of bounds: {}..{} with N={} groups",
            a,
            b,
            n
        ));
    }
    if c > n {
        return Err(anyhow!("--after must be in 0..={} (got {})", n, c));
    }

    if a == b {
        if a == c {
            info!("Already in desired position: {}", a);
            return Ok(RewriteCommandOutcome::Completed);
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

    enforce_bottom_pr_automerge_guard(prefix, &groups, &new_order)?;

    let plan = format_simple_plan(&((1..=n).collect::<Vec<_>>()), &new_order, a, b, c);
    info!("Plan: {}", plan);

    if new_order == (1..=n).collect::<Vec<_>>() {
        info!("Order unchanged; nothing to do.");
        return Ok(RewriteCommandOutcome::Completed);
    }

    common::with_dirty_worktree_policy(
        options.dry,
        "spr move",
        options.dirty_worktree_policy,
        || {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            let original_head = git_rev_parse("HEAD")?;
            let original_worktree_root = rewrite_resume::current_repo_root()?;
            let backup_tag = if options.safe {
                Some(common::create_backup_tag(
                    options.dry,
                    "move",
                    &cur_branch,
                    &short,
                )?)
            } else {
                None
            };

            let (tmp_path, tmp_branch) =
                common::create_temp_worktree(options.dry, "move", &merge_base, &short)?;
            let steps = rewrite_resume::build_replay_steps(&build_move_operations(
                &leading_ignored,
                &groups,
                &new_order,
            ))?;
            rewrite_resume::run_rewrite_session(
                options.dry,
                RewriteSession {
                    command_kind: RewriteCommandKind::Move,
                    conflict_policy: RewriteConflictPolicy::Suspend,
                    original_worktree_root,
                    original_branch: cur_branch,
                    original_head,
                    temp_branch: tmp_branch,
                    temp_worktree_path: tmp_path,
                    backup_tag,
                    steps,
                    post_success_hint: Some(
                        "No GitHub changes were made. Run `spr update` after inspecting the rewritten stack."
                            .to_string(),
                    ),
                },
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{changes_stack_bottom, resolve_move_targets, should_block_for_bottom_pr_automerge};
    use crate::commands::rewrite_resume::{resume_rewrite, RewriteResumeState};
    use crate::commands::{move_groups_after, MoveExecutionOptions, RewriteCommandOutcome};
    use crate::config::DirtyWorktreePolicy;
    use crate::parsing::Group;
    use crate::selectors::{AfterSelector, GroupRangeSelector, GroupSelector, StableHandle};
    use crate::test_support::{commit_file, git, lock_cwd, log_subjects, write_file, DirGuard};
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

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
    fn move_range_and_after_resolve_from_stable_handles() {
        let groups = groups(&["alpha", "beta", "gamma"]);
        let range = GroupRangeSelector::Inclusive {
            start: GroupSelector::Stable(StableHandle {
                tag: "beta".to_string(),
            }),
            end: GroupSelector::Stable(StableHandle {
                tag: "gamma".to_string(),
            }),
        };
        let after = AfterSelector::Group(GroupSelector::Stable(StableHandle {
            tag: "alpha".to_string(),
        }));

        assert_eq!(
            resolve_move_targets(&groups, &range, &after).unwrap(),
            (2, 3, 1)
        );
    }

    #[test]
    fn changes_stack_bottom_detects_bottom_replacement() {
        assert!(!changes_stack_bottom(&[1, 3, 2]));
        assert!(changes_stack_bottom(&[2, 1, 3]));
    }

    #[test]
    fn bottom_pr_automerge_only_blocks_when_bottom_would_change() {
        assert!(should_block_for_bottom_pr_automerge(true, &[2, 1, 3]));
        assert!(!should_block_for_bottom_pr_automerge(true, &[1, 3, 2]));
        assert!(!should_block_for_bottom_pr_automerge(false, &[2, 1, 3]));
    }

    fn init_move_conflict_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(repo, "story.txt", "base\n");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "alpha-1\n", "feat: alpha pr:alpha");
        commit_file(repo, "story.txt", "beta-1\n", "feat: beta pr:beta");
        commit_file(repo, "story.txt", "gamma-1\n", "feat: gamma pr:gamma");
        dir
    }

    #[test]
    fn move_suspends_and_resumes_conflict_without_github_lookup() {
        let _lock = lock_cwd();
        let dir = init_move_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let outcome = move_groups_after(
            "main",
            "dank-spr/",
            "ignore",
            &GroupRangeSelector::Single(GroupSelector::LocalPr(3)),
            &AfterSelector::Group(GroupSelector::LocalPr(1)),
            MoveExecutionOptions {
                safe: false,
                dry: false,
                dirty_worktree_policy: DirtyWorktreePolicy::Halt,
            },
        )
        .expect("move should suspend");
        let mut current = outcome;
        let mut last_resume_path = None;
        while let RewriteCommandOutcome::Suspended(state) = current {
            let resume_path = state.resume_path.clone();
            let resume_state: RewriteResumeState =
                serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                    .expect("parse resume state");
            let resolved_contents = if resume_state.suspended_step_index == 1 {
                "gamma-1\n"
            } else {
                "gamma-1\nbeta-1\n"
            };
            fs::write(
                Path::new(&resume_state.temp_worktree_path).join("story.txt"),
                resolved_contents,
            )
            .expect("resolve move conflict");
            git(
                Path::new(&resume_state.temp_worktree_path),
                ["add", "story.txt"].as_slice(),
            );
            last_resume_path = Some(resume_path.clone());
            current = resume_rewrite(false, &resume_path).expect("resume move");
        }

        assert_eq!(current, RewriteCommandOutcome::Completed);
        if let Some(resume_path) = last_resume_path {
            assert!(
                !resume_path.exists(),
                "successful move resume should delete the resume file"
            );
        }
        assert_eq!(
            fs::read_to_string(repo.join("story.txt")).expect("read final file"),
            "gamma-1\nbeta-1\n"
        );
        assert_eq!(
            log_subjects(&repo, 3),
            vec![
                "feat: beta pr:beta".to_string(),
                "feat: gamma pr:gamma".to_string(),
                "feat: alpha pr:alpha".to_string(),
            ]
        );
    }
}
