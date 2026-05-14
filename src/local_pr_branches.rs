//! Optional synchronization for local per-PR branches.
//!
//! Each group's canonical branch name is resolved from its seed marker. Remote
//! updates already use those names, and this module optionally keeps matching
//! local branches pointed at the same group tips.

use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashSet;
use tracing::info;

use crate::branch_names::group_branch_name;
use crate::config::LocalPrBranchSyncPolicy;
use crate::execution::ExecutionMode;
use crate::git::{git_is_ancestor, git_local_branch_tip, git_rw, worktree_entries};
use crate::parsing::Group;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPrBranchTarget {
    pub stable_handle: String,
    pub branch_name: String,
    pub tip: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalPrBranchActionKind {
    Created,
    Updated,
    Skipped,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalPrBranchAction {
    pub stable_handle: String,
    pub branch: String,
    pub old_tip: Option<String>,
    pub new_tip: String,
    pub action: LocalPrBranchActionKind,
    pub reason: String,
    pub backup_tag: Option<String>,
}

struct PlannedLocalPrBranchAction {
    target: LocalPrBranchTarget,
    old_tip: Option<String>,
    action: LocalPrBranchActionKind,
    reason: String,
}

pub fn targets_from_groups(prefix: &str, groups: &[Group]) -> Result<Vec<LocalPrBranchTarget>> {
    groups
        .iter()
        .map(|group| {
            let tip =
                group.commits.last().cloned().ok_or_else(|| {
                    anyhow::anyhow!("Group {} has no commits", group.selector_text())
                })?;
            Ok(LocalPrBranchTarget {
                stable_handle: crate::commands::common::group_selector_text(group),
                branch_name: group_branch_name(prefix, group),
                tip,
            })
        })
        .collect()
}

pub fn sync_local_pr_branches(
    policy: LocalPrBranchSyncPolicy,
    execution_mode: ExecutionMode,
    targets: &[LocalPrBranchTarget],
) -> Result<Vec<LocalPrBranchAction>> {
    if policy == LocalPrBranchSyncPolicy::Off || targets.is_empty() {
        return Ok(Vec::new());
    }

    let checked_out_branches = checked_out_local_branches()?;
    let planned = targets
        .iter()
        .map(|target| plan_action(policy, target, &checked_out_branches))
        .collect::<Result<Vec<_>>>()?;
    let actions = planned
        .into_iter()
        .map(|planned| apply_action(execution_mode, planned))
        .collect::<Result<Vec<_>>>()?;
    emit_actions(&actions);
    Ok(actions)
}

pub fn plan_local_pr_branch_drift(
    policy: LocalPrBranchSyncPolicy,
    targets: &[LocalPrBranchTarget],
) -> Result<Vec<LocalPrBranchAction>> {
    if policy == LocalPrBranchSyncPolicy::Off || targets.is_empty() {
        return Ok(Vec::new());
    }

    let checked_out_branches = checked_out_local_branches()?;
    targets
        .iter()
        .map(|target| plan_action(policy, target, &checked_out_branches))
        .filter_map(|planned| match planned {
            Ok(planned)
                if matches!(
                    planned.action,
                    LocalPrBranchActionKind::Created
                        | LocalPrBranchActionKind::Updated
                        | LocalPrBranchActionKind::Blocked
                ) =>
            {
                Some(Ok(planned.into_action_without_backup_tag()))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

fn plan_action(
    policy: LocalPrBranchSyncPolicy,
    target: &LocalPrBranchTarget,
    checked_out_branches: &HashSet<String>,
) -> Result<PlannedLocalPrBranchAction> {
    let old_tip = git_local_branch_tip(&target.branch_name)?;
    let (action, reason) = if let Some(old_tip) = old_tip.as_deref() {
        if old_tip == target.tip {
            (
                LocalPrBranchActionKind::Skipped,
                "already at target".to_string(),
            )
        } else if checked_out_branches.contains(&target.branch_name) {
            (
                LocalPrBranchActionKind::Blocked,
                "branch is checked out in a worktree".to_string(),
            )
        } else {
            (
                LocalPrBranchActionKind::Updated,
                "move existing local branch to target".to_string(),
            )
        }
    } else if policy == LocalPrBranchSyncPolicy::CreateOrUpdate {
        (
            LocalPrBranchActionKind::Created,
            "create missing local branch at target".to_string(),
        )
    } else {
        (
            LocalPrBranchActionKind::Skipped,
            "missing local branch and policy is update-existing".to_string(),
        )
    };

    Ok(PlannedLocalPrBranchAction {
        target: target.clone(),
        old_tip,
        action,
        reason,
    })
}

impl PlannedLocalPrBranchAction {
    fn into_action_without_backup_tag(self) -> LocalPrBranchAction {
        LocalPrBranchAction {
            stable_handle: self.target.stable_handle,
            branch: self.target.branch_name,
            old_tip: self.old_tip,
            new_tip: self.target.tip,
            action: self.action,
            reason: self.reason,
            backup_tag: None,
        }
    }
}

fn apply_action(
    execution_mode: ExecutionMode,
    planned: PlannedLocalPrBranchAction,
) -> Result<LocalPrBranchAction> {
    let backup_tag = match planned.action {
        LocalPrBranchActionKind::Created => {
            git_rw(
                execution_mode,
                [
                    "branch",
                    planned.target.branch_name.as_str(),
                    planned.target.tip.as_str(),
                ]
                .as_slice(),
            )
            .with_context(|| {
                format!(
                    "failed to create local PR branch {} at {}",
                    planned.target.branch_name, planned.target.tip
                )
            })?;
            None
        }
        LocalPrBranchActionKind::Updated => {
            let backup_tag = backup_tag_for_non_fast_forward(
                execution_mode,
                &planned.target.branch_name,
                planned.old_tip.as_deref(),
                &planned.target.tip,
            )?;
            git_rw(
                execution_mode,
                [
                    "branch",
                    "-f",
                    planned.target.branch_name.as_str(),
                    planned.target.tip.as_str(),
                ]
                .as_slice(),
            )
            .with_context(|| {
                format!(
                    "failed to move local PR branch {} to {}",
                    planned.target.branch_name, planned.target.tip
                )
            })?;
            backup_tag
        }
        LocalPrBranchActionKind::Skipped | LocalPrBranchActionKind::Blocked => None,
    };

    Ok(LocalPrBranchAction {
        stable_handle: planned.target.stable_handle,
        branch: planned.target.branch_name,
        old_tip: planned.old_tip,
        new_tip: planned.target.tip,
        action: planned.action,
        reason: planned.reason,
        backup_tag,
    })
}

fn backup_tag_for_non_fast_forward(
    execution_mode: ExecutionMode,
    branch_name: &str,
    old_tip: Option<&str>,
    new_tip: &str,
) -> Result<Option<String>> {
    let Some(old_tip) = old_tip else {
        return Ok(None);
    };
    if git_is_ancestor(old_tip, new_tip)? {
        return Ok(None);
    }

    let backup_tag = format!(
        "backup/local-pr-branches/{}-{}",
        branch_name,
        short_sha(old_tip)
    );
    git_rw(
        execution_mode,
        ["tag", "-f", &backup_tag, old_tip].as_slice(),
    )
    .with_context(|| {
        format!(
            "failed to create backup tag {} for local PR branch {} at {}",
            backup_tag, branch_name, old_tip
        )
    })?;
    Ok(Some(backup_tag))
}

fn checked_out_local_branches() -> Result<HashSet<String>> {
    Ok(worktree_entries()
        .context("failed to list git worktrees")?
        .into_iter()
        .filter_map(|entry| entry.branch)
        .collect())
}

fn emit_actions(actions: &[LocalPrBranchAction]) {
    for action in actions {
        let old_tip = action
            .old_tip
            .as_deref()
            .map(short_sha)
            .unwrap_or("missing");
        let new_tip = short_sha(&action.new_tip);
        let verb = match action.action {
            LocalPrBranchActionKind::Created => "created",
            LocalPrBranchActionKind::Updated => "updated",
            LocalPrBranchActionKind::Skipped => "skipped",
            LocalPrBranchActionKind::Blocked => "blocked",
        };
        info!(
            "local branch {} -> {} {}..{} ({})",
            action.branch, verb, old_tip, new_tip, action.reason
        );
    }
}

fn short_sha(sha: &str) -> &str {
    if sha.len() > 8 {
        &sha[..8]
    } else {
        sha
    }
}

#[cfg(test)]
mod tests {
    use super::{
        plan_local_pr_branch_drift, sync_local_pr_branches, targets_from_groups,
        LocalPrBranchActionKind, LocalPrBranchTarget,
    };
    use crate::config::LocalPrBranchSyncPolicy;
    use crate::execution::ExecutionMode;
    use crate::group_markers::GroupMarker;
    use crate::parsing::Group;
    use crate::test_support::{commit_file, git, init_repo, lock_cwd, DirGuard};
    use std::path::Path;

    fn rev_parse(repo: &Path, revision: &str) -> String {
        git(repo, ["rev-parse", revision].as_slice())
            .trim()
            .to_string()
    }

    fn branch_tip(repo: &Path, branch: &str) -> Option<String> {
        let out = std::process::Command::new("git")
            .current_dir(repo)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}^{{commit}}"),
            ])
            .output()
            .unwrap();
        if out.status.success() {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            None
        }
    }

    fn target(branch_name: &str, tip: &str) -> LocalPrBranchTarget {
        LocalPrBranchTarget {
            stable_handle: "pr:alpha".to_string(),
            branch_name: branch_name.to_string(),
            tip: tip.to_string(),
        }
    }

    #[test]
    fn targets_from_groups_manage_resolved_branch_names_for_both_marker_kinds() {
        let groups = vec![
            Group {
                marker: GroupMarker::BranchName("feature/login".to_string()),
                subjects: Vec::new(),
                commits: vec!["a1".to_string()],
                first_message: None,
                ignored_after: Vec::new(),
            },
            Group {
                marker: GroupMarker::PrLabel("beta".to_string()),
                subjects: Vec::new(),
                commits: vec!["b1".to_string()],
                first_message: None,
                ignored_after: Vec::new(),
            },
        ];

        let targets = targets_from_groups("dank-spr/", &groups).unwrap();

        assert_eq!(targets[0].stable_handle, "branch:feature/login");
        assert_eq!(targets[0].branch_name, "feature/login");
        assert_eq!(targets[1].stable_handle, "pr:beta");
        assert_eq!(targets[1].branch_name, "dank-spr/beta");
    }

    #[test]
    fn off_policy_leaves_missing_branch_unreported_and_absent() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let head = rev_parse(&repo, "HEAD");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::Off,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &head)],
        )
        .unwrap();

        assert!(actions.is_empty());
        assert!(branch_tip(&repo, "dank-spr/alpha").is_none());
    }

    #[test]
    fn update_existing_policy_does_not_create_missing_branch() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let head = rev_parse(&repo, "HEAD");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::UpdateExisting,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &head)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Skipped);
        assert!(branch_tip(&repo, "dank-spr/alpha").is_none());
    }

    #[test]
    fn create_or_update_policy_creates_missing_branch() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let head = rev_parse(&repo, "HEAD");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::CreateOrUpdate,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &head)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Created);
        assert_eq!(branch_tip(&repo, "dank-spr/alpha"), Some(head));
    }

    #[test]
    fn update_existing_policy_moves_existing_branch() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let old_tip = rev_parse(&repo, "HEAD");
        git(&repo, ["branch", "dank-spr/alpha", &old_tip].as_slice());
        let new_tip = commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::UpdateExisting,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &new_tip)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Updated);
        assert_eq!(actions[0].old_tip.as_deref(), Some(old_tip.as_str()));
        assert_eq!(branch_tip(&repo, "dank-spr/alpha"), Some(new_tip));
    }

    #[test]
    fn dry_run_reports_create_without_creating_branch() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let head = rev_parse(&repo, "HEAD");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::CreateOrUpdate,
            ExecutionMode::DryRun,
            &[target("dank-spr/alpha", &head)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Created);
        assert!(branch_tip(&repo, "dank-spr/alpha").is_none());
    }

    #[test]
    fn drift_planning_reports_only_reconcilable_or_blocked_branches() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let current_tip = rev_parse(&repo, "HEAD");
        git(
            &repo,
            ["branch", "dank-spr/current", &current_tip].as_slice(),
        );
        git(&repo, ["branch", "dank-spr/stale", &current_tip].as_slice());
        let target_tip = commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha");

        let actions = plan_local_pr_branch_drift(
            LocalPrBranchSyncPolicy::CreateOrUpdate,
            &[
                target("dank-spr/missing", &target_tip),
                target("dank-spr/current", &current_tip),
                target("dank-spr/stale", &target_tip),
            ],
        )
        .unwrap();

        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].branch, "dank-spr/missing");
        assert_eq!(actions[0].action, LocalPrBranchActionKind::Created);
        assert_eq!(actions[1].branch, "dank-spr/stale");
        assert_eq!(actions[1].action, LocalPrBranchActionKind::Updated);
    }

    #[test]
    fn checked_out_local_branch_is_reported_as_blocked_and_left_unchanged() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let old_tip = rev_parse(&repo, "HEAD");
        git(&repo, ["branch", "dank-spr/alpha", &old_tip].as_slice());
        let worktree_parent = tempfile::tempdir().unwrap();
        let worktree_path = worktree_parent.path().join("alpha-worktree");
        git(
            &repo,
            [
                "worktree",
                "add",
                worktree_path.to_str().unwrap(),
                "dank-spr/alpha",
            ]
            .as_slice(),
        );
        let new_tip = commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha");

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::UpdateExisting,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &new_tip)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Blocked);
        assert_eq!(branch_tip(&repo, "dank-spr/alpha"), Some(old_tip));
    }

    #[test]
    fn non_fast_forward_update_creates_backup_tag_at_old_branch_tip() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let target_tip = rev_parse(&repo, "HEAD");
        let old_tip = commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha");
        git(&repo, ["branch", "dank-spr/alpha", &old_tip].as_slice());

        let actions = sync_local_pr_branches(
            LocalPrBranchSyncPolicy::UpdateExisting,
            ExecutionMode::Apply,
            &[target("dank-spr/alpha", &target_tip)],
        )
        .unwrap();

        assert_eq!(actions[0].action, LocalPrBranchActionKind::Updated);
        let backup_tag = actions[0].backup_tag.as_deref().unwrap();
        assert_eq!(
            rev_parse(&repo, &format!("refs/tags/{backup_tag}")),
            old_tip
        );
        assert_eq!(branch_tip(&repo, "dank-spr/alpha"), Some(target_tip));
    }
}
