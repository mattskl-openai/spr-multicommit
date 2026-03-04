//! Shared git helpers for stack-rewriting commands.
//!
//! These helpers centralize the mechanics used by `restack`, `move`, and
//! `fix-pr`: naming temporary branches/worktrees, creating safety backups, and
//! resetting the current branch to a rebuilt tip.
//!
//! A subtle but important invariant is that backup tag names include the
//! short SHA of `HEAD`. When a rewrite command fails before `HEAD` changes, a
//! second attempt would otherwise try to create the same backup tag again and
//! fail with "tag already exists". To keep `--safe` re-runnable after a
//! failure, `create_backup_tag` force-updates the backup ref in place.
//!
//! Temporary worktrees and their branches follow the same naming scheme. A
//! failed rewrite can therefore leave behind a temp branch that will collide
//! on the next run. `create_temp_worktree` proactively removes any existing
//! temp worktree/branch with the same derived name before creating a new one,
//! and uses `git worktree add -B` as a final safeguard when cleanup is skipped
//! in dry-run mode.
//!
//! Branch-rewriting commands also share dirty-worktree handling. Depending on
//! config, they may preserve current behavior and discard tracked changes,
//! auto-stash local changes and restore them after the rewrite, or halt before
//! any destructive step.
//!
//! Backup tags are local-only and are intended as an escape hatch for a single
//! user; callers should not rely on them being immutable.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

use crate::config::DirtyWorktreePolicy;
use crate::git::{git_ro, git_rw, normalize_branch_name};
use crate::parsing::Group;

/// Returns the current branch name and the short SHA of `HEAD`.
///
/// This is primarily used to derive stable, human-readable names for backup
/// tags and temporary worktree branches. If `HEAD` is detached, the branch
/// component will be reported as `HEAD`, which can lead to less useful backup
/// names.
pub fn get_current_branch_and_short() -> Result<(String, String)> {
    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    Ok((cur_branch, short))
}

/// Creates or updates a local backup tag pointing at the current `HEAD`.
///
/// The backup name is derived from `(kind, cur_branch, short)` and is therefore
/// stable for a given `HEAD`. This stability is desirable for operator clarity,
/// but it means repeat runs at the same `HEAD` will collide. We force-update
/// the tag (`git tag -f`) so that `spr restack --safe` remains runnable after a
/// failed attempt that left the backup tag behind.
///
/// The existence check is only used to drive the log message; the tag is
/// always updated to point at `HEAD`.
pub fn create_backup_tag(dry: bool, kind: &str, cur_branch: &str, short: &str) -> Result<String> {
    let backup = format!("backup/{}/{}-{}", kind, cur_branch, short);
    let exists = git_ro(["tag", "--list", &backup].as_slice())?;
    if exists.trim().is_empty() {
        info!("Creating backup tag at HEAD: {}", backup);
    } else {
        info!("Backup tag exists; overwriting at HEAD: {}", backup);
    }
    // Use `-f` to make backup creation idempotent. When the name already
    // exists, we explicitly move it to the current HEAD.
    let _ = git_rw(dry, ["tag", "-f", &backup, "HEAD"].as_slice())?;
    Ok(backup)
}

/// Creates a temporary worktree/branch off `merge_base` for stack rewrites.
///
/// The temp branch and path names are derived from `(kind, short)` and are
/// therefore stable for a given `HEAD`. To keep rewrite commands re-runnable
/// after failures, we delete any existing temp worktree/branch with the same
/// derived name before creating the new worktree. We then use `-B` (reset or
/// create) rather than `-b` to avoid "branch already exists" failures when a
/// dry-run skipped the cleanup steps.
pub fn create_temp_worktree(
    dry: bool,
    kind: &str,
    merge_base: &str,
    short: &str,
) -> Result<(String, String)> {
    let tmp_branch = format!("spr/tmp-{}-{}", kind, short);
    let tmp_path = format!("/tmp/spr-{}-{}", kind, short);
    cleanup_existing_temp_state(dry, &tmp_path, &tmp_branch)?;
    info!(
        "Creating temp worktree {} on branch {}…",
        tmp_path, tmp_branch
    );
    let _ = git_rw(
        dry,
        [
            "worktree",
            "add",
            "-f",
            "-B",
            &tmp_branch,
            &tmp_path,
            merge_base,
        ]
        .as_slice(),
    )?;
    Ok((tmp_path, tmp_branch))
}

/// A single parsed entry from `git worktree list --porcelain`.
///
/// The `branch` value, when present, is normalized to a local branch name.
#[derive(Debug, Clone)]
struct WorktreeEntry {
    path: String,
    branch: Option<String>,
}

/// Lists worktrees in porcelain form and extracts their paths and branches.
///
/// We parse porcelain output to reliably determine whether a temp branch is
/// currently checked out elsewhere, which must be resolved before deleting the
/// branch.
fn list_worktrees() -> Result<Vec<WorktreeEntry>> {
    let out = git_ro(["worktree", "list", "--porcelain"].as_slice())?;
    let mut entries: Vec<WorktreeEntry> = Vec::new();
    let mut cur_path: Option<String> = None;
    let mut cur_branch: Option<String> = None;

    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            if let Some(path) = cur_path.take() {
                entries.push(WorktreeEntry {
                    path,
                    branch: cur_branch.take(),
                });
            }
            cur_path = Some(rest.trim().to_string());
            cur_branch = None;
            continue;
        }
        if let Some(rest) = line.strip_prefix("branch ") {
            if cur_path.is_some() {
                cur_branch = Some(normalize_branch_name(rest.trim()));
            }
        }
    }

    if let Some(path) = cur_path.take() {
        entries.push(WorktreeEntry {
            path,
            branch: cur_branch.take(),
        });
    }

    Ok(entries)
}

/// Returns true when a local branch with the given name exists.
fn branch_exists(branch: &str) -> Result<bool> {
    let out = git_ro(["branch", "--list", branch].as_slice())?;
    Ok(!out.trim().is_empty())
}

/// Removes any pre-existing temp worktree and branch for this derived name.
///
/// Cleanup is ordered to respect Git's constraints: any worktree that has the
/// temp branch checked out must be removed before the branch can be deleted.
/// We also remove a matching temp path that is not registered as a worktree,
/// which can happen after interrupted runs.
fn cleanup_existing_temp_state(dry: bool, tmp_path: &str, tmp_branch: &str) -> Result<()> {
    let entries = list_worktrees()?;
    let mut removed_paths: HashSet<String> = HashSet::new();

    // If the temp branch is checked out in any worktree, remove those first
    // so the branch can be deleted and recreated.
    for entry in entries
        .iter()
        .filter(|e| e.branch.as_deref() == Some(tmp_branch))
    {
        info!(
            "Removing existing temp worktree {} for branch {}…",
            entry.path, tmp_branch
        );
        let _ = git_rw(
            dry,
            ["worktree", "remove", "-f", entry.path.as_str()].as_slice(),
        )?;
        removed_paths.insert(entry.path.clone());
    }

    // Also remove a worktree registered at the exact temp path, even if the
    // branch name is missing (e.g., detached HEAD).
    if entries.iter().any(|e| e.path == tmp_path) && !removed_paths.contains(tmp_path) {
        info!("Removing existing temp worktree at {}…", tmp_path);
        let _ = git_rw(dry, ["worktree", "remove", "-f", tmp_path].as_slice())?;
    } else if Path::new(tmp_path).exists() {
        info!(
            "Temp path {} exists but is not registered as a worktree; removing it…",
            tmp_path
        );
        if !dry {
            fs::remove_dir_all(tmp_path)
                .with_context(|| format!("failed to remove existing temp path {}", tmp_path))?;
        }
    }

    if branch_exists(tmp_branch)? {
        info!("Deleting existing temp branch {}…", tmp_branch);
        let _ = git_rw(dry, ["branch", "-D", tmp_branch].as_slice())?;
    }

    Ok(())
}

fn cherry_pick_args<'a>(
    tmp_path: &'a str,
    empty_policy: CherryPickEmptyPolicy,
    tail_args: &[&'a str],
) -> Vec<&'a str> {
    let mut args = vec!["-C", tmp_path, "cherry-pick"];
    if empty_policy == CherryPickEmptyPolicy::KeepRedundantCommits {
        args.push("--empty=keep");
    }
    args.extend_from_slice(tail_args);
    args
}

pub fn cherry_pick_commit(
    dry: bool,
    tmp_path: &str,
    sha: &str,
    empty_policy: CherryPickEmptyPolicy,
) -> Result<()> {
    let args = cherry_pick_args(tmp_path, empty_policy, &[sha]);
    let _ = git_rw(dry, args.as_slice())?;
    Ok(())
}

pub fn cherry_pick_range(
    dry: bool,
    tmp_path: &str,
    first: &str,
    last: &str,
    empty_policy: CherryPickEmptyPolicy,
) -> Result<()> {
    let range = format!("{first}^..{last}");
    let args = cherry_pick_args(tmp_path, empty_policy, &[range.as_str()]);
    let _ = git_rw(dry, args.as_slice())?;
    Ok(())
}

pub fn tip_of_tmp(tmp_path: &str) -> Result<String> {
    Ok(git_ro(["-C", tmp_path, "rev-parse", "HEAD"].as_slice())?
        .trim()
        .to_string())
}

pub fn reset_current_branch_to(dry: bool, new_tip: &str) -> Result<()> {
    let _ = git_rw(dry, ["reset", "--hard", new_tip].as_slice())?;
    Ok(())
}

pub fn cleanup_temp_worktree(dry: bool, tmp_path: &str, tmp_branch: &str) -> Result<()> {
    let _ = git_rw(dry, ["worktree", "remove", "-f", tmp_path].as_slice())?;
    let _ = git_rw(dry, ["branch", "-D", tmp_branch].as_slice())?;
    Ok(())
}

/// Deferred dirty-worktree restoration that can survive a suspended rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DeferredDirtyWorktreeRestore {
    #[default]
    Noop,
    Stash {
        stash_commit: String,
    },
}

impl DeferredDirtyWorktreeRestore {
    pub fn restore_after_success(
        self,
        dry: bool,
        command_name: &str,
        worktree_root: &str,
    ) -> Result<()> {
        match self {
            Self::Noop => Ok(()),
            Self::Stash { stash_commit } => restore_stash_in_worktree(
                dry,
                command_name,
                worktree_root,
                &stash_commit,
                RewriteOutcome::Succeeded,
            ),
        }
    }

    pub fn discard_instruction_lines(&self, worktree_root: &str) -> Vec<String> {
        match self {
            Self::Noop => Vec::new(),
            Self::Stash { stash_commit } => vec![
                "  # optional: restore the deferred auto-stash before deleting the resume file"
                    .to_string(),
                format!(
                    "  git -C {} stash apply --index {}",
                    worktree_root, stash_commit
                ),
                "  # optional: drop the matching stash entry from `git stash list` after recovery"
                    .to_string(),
            ],
        }
    }
}

pub trait DirtyWorktreeOutcome {
    fn keeps_dirty_worktree_restore_deferred(&self) -> bool;
}

#[derive(Debug)]
enum DirtyWorktreeRestore {
    Noop,
    PlannedStash,
    Stashed { stash_commit: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewriteOutcome {
    Succeeded,
    Failed,
}

impl RewriteOutcome {
    fn verb(self) -> &'static str {
        if self == Self::Succeeded {
            "rewrote history"
        } else {
            "failed before finishing the rewrite"
        }
    }
}

fn worktree_status_lines() -> Result<Vec<String>> {
    let out = git_ro(["status", "--porcelain=v1"].as_slice())?;
    Ok(out.lines().map(|line| line.to_string()).collect::<Vec<_>>())
}

fn status_line_has_conflict(line: &str) -> bool {
    let bytes = line.as_bytes();
    if bytes.len() < 2 {
        false
    } else {
        let x = bytes[0] as char;
        let y = bytes[1] as char;
        x == 'U' || y == 'U' || (x == 'A' && y == 'A') || (x == 'D' && y == 'D')
    }
}

fn prepare_dirty_worktree(
    dry: bool,
    command_name: &str,
    policy: DirtyWorktreePolicy,
) -> Result<DirtyWorktreeRestore> {
    let status_lines = worktree_status_lines()?;
    if status_lines.is_empty() {
        Ok(DirtyWorktreeRestore::Noop)
    } else if status_lines
        .iter()
        .any(|line| status_line_has_conflict(line))
    {
        bail!(
            "{} cannot rewrite history with unresolved merge conflicts:\n{}\nResolve the conflicts first.",
            command_name,
            status_lines.join("\n")
        );
    } else if policy == DirtyWorktreePolicy::Discard {
        info!(
            "{} detected local changes and dirty_worktree=discard; proceeding without preserving tracked changes. Untracked files are left in place.",
            command_name
        );
        Ok(DirtyWorktreeRestore::Noop)
    } else if policy == DirtyWorktreePolicy::Halt {
        bail!(
            "{} rewrites the checked-out branch and dirty_worktree=halt found local changes:\n{}\nCommit, stash, or discard them first.",
            command_name,
            status_lines.join("\n")
        );
    } else if dry {
        info!(
            "DRY-RUN: would stash local changes before {} because dirty_worktree=stash",
            command_name
        );
        Ok(DirtyWorktreeRestore::PlannedStash)
    } else {
        let message = format!("spr auto-stash before {}", command_name);
        let _ = git_rw(
            false,
            [
                "stash",
                "push",
                "--include-untracked",
                "--message",
                &message,
            ]
            .as_slice(),
        )?;
        let stash_commit = git_ro(["rev-parse", "stash@{0}"].as_slice())?
            .trim()
            .to_string();
        let remaining = worktree_status_lines()?;
        if remaining.is_empty() {
            info!(
                "Stashed local changes before {} because dirty_worktree=stash",
                command_name
            );
            Ok(DirtyWorktreeRestore::Stashed { stash_commit })
        } else {
            bail!(
                "{} stashed local changes but the worktree is still dirty:\n{}",
                command_name,
                remaining.join("\n")
            );
        }
    }
}

impl DirtyWorktreeRestore {
    fn deferred_restore(&self) -> DeferredDirtyWorktreeRestore {
        match self {
            Self::Noop | Self::PlannedStash => DeferredDirtyWorktreeRestore::Noop,
            Self::Stashed { stash_commit } => DeferredDirtyWorktreeRestore::Stash {
                stash_commit: stash_commit.clone(),
            },
        }
    }

    fn restore(self, dry: bool, command_name: &str, outcome: RewriteOutcome) -> Result<()> {
        match self {
            Self::Noop => Ok(()),
            Self::PlannedStash => {
                if dry {
                    info!(
                        "DRY-RUN: would restore stashed local changes after {}",
                        command_name
                    );
                }
                Ok(())
            }
            Self::Stashed { stash_commit } => {
                restore_stash_in_worktree(dry, command_name, ".", &stash_commit, outcome)
            }
        }
    }
}

fn restore_stash_in_worktree(
    dry: bool,
    command_name: &str,
    worktree_root: &str,
    stash_commit: &str,
    outcome: RewriteOutcome,
) -> Result<()> {
    let _ = git_rw(
        false,
        [
            "-C",
            worktree_root,
            "stash",
            "apply",
            "--index",
            stash_commit,
        ]
        .as_slice(),
    )
    .with_context(|| {
        format!(
            "{} {} but failed to restore stashed changes from {}; the stash entry was kept for manual recovery",
            command_name,
            outcome.verb(),
            stash_commit
        )
    })?;
    match stash_ref_for_commit_in_worktree(worktree_root, stash_commit)? {
        Some(stash_ref) => {
            if let Err(err) = git_rw(
                false,
                ["-C", worktree_root, "stash", "drop", &stash_ref].as_slice(),
            ) {
                warn!(
                    "Restored stashed local changes after {}, but failed to drop {}: {}",
                    command_name, stash_ref, err
                );
            }
        }
        None => {
            warn!(
                "Restored stashed local changes after {}, but could not find a live stash ref for {}; leaving any matching stash entry in place",
                command_name, stash_commit
            );
        }
    }
    if dry {
        info!(
            "DRY-RUN: restored stashed local changes after {}",
            command_name
        );
    } else {
        info!("Restored stashed local changes after {}", command_name);
    }
    Ok(())
}

fn stash_ref_for_commit_in_worktree(
    worktree_root: &str,
    stash_commit: &str,
) -> Result<Option<String>> {
    let out = git_ro(["-C", worktree_root, "stash", "list", "--format=%H%x00%gd"].as_slice())?;
    Ok(out.lines().find_map(|line| {
        let (sha, stash_ref) = line.split_once('\0')?;
        if sha.trim() == stash_commit {
            Some(stash_ref.trim().to_string())
        } else {
            None
        }
    }))
}

/// Run a branch rewrite under the configured dirty-worktree policy.
///
/// This is the single source of truth for commands that rebuild the
/// checked-out branch and then replace it with a rewritten tip.
pub fn with_dirty_worktree_policy<T, F>(
    dry: bool,
    command_name: &str,
    policy: DirtyWorktreePolicy,
    rewrite: F,
) -> Result<T>
where
    T: DirtyWorktreeOutcome,
    F: FnOnce(DeferredDirtyWorktreeRestore) -> Result<T>,
{
    let restore = prepare_dirty_worktree(dry, command_name, policy)?;
    let deferred_restore = restore.deferred_restore();
    let rewrite_result = rewrite(deferred_restore);
    let restore_result = if rewrite_result
        .as_ref()
        .is_ok_and(DirtyWorktreeOutcome::keeps_dirty_worktree_restore_deferred)
    {
        Ok(())
    } else {
        let outcome = if rewrite_result.is_ok() {
            RewriteOutcome::Succeeded
        } else {
            RewriteOutcome::Failed
        };
        restore.restore(dry, command_name, outcome)
    };

    match (rewrite_result, restore_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(restore_err)) => Err(restore_err),
        (Err(rewrite_err), Ok(())) => Err(rewrite_err),
        (Err(rewrite_err), Err(restore_err)) => Err(rewrite_err).with_context(|| {
            format!(
                "also failed to restore local changes after {}: {restore_err:#}",
                command_name
            )
        }),
    }
}

/// A single cherry-pick operation used to rebuild stack history.
///
/// `Range` represents an inclusive `first^..last` cherry-pick over a contiguous
/// commit interval. Callers must supply commits that already reflect the
/// intended replay order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CherryPickEmptyPolicy {
    /// Stop when a replay becomes empty because earlier history already applied it.
    StopOnEmpty,
    /// Keep redundant replays as explicit empty commits.
    KeepRedundantCommits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CherryPickOp {
    /// Cherry-pick exactly one commit.
    Commit {
        sha: String,
        empty_policy: CherryPickEmptyPolicy,
    },
    /// Cherry-pick one contiguous inclusive range.
    Range {
        first: String,
        last: String,
        empty_policy: CherryPickEmptyPolicy,
    },
}

impl CherryPickOp {
    /// Builds the smallest cherry-pick operation that can replay `commits`.
    ///
    /// Returns `None` when the slice is empty.
    pub fn from_commits(commits: &[String]) -> Option<Self> {
        Self::from_commits_with_empty_policy(commits, CherryPickEmptyPolicy::StopOnEmpty)
    }

    /// Builds the smallest cherry-pick operation that can replay `commits`
    /// under a specific empty-commit policy.
    pub fn from_commits_with_empty_policy(
        commits: &[String],
        empty_policy: CherryPickEmptyPolicy,
    ) -> Option<Self> {
        if let (Some(first), Some(last)) = (commits.first(), commits.last()) {
            if commits.len() == 1 {
                Some(Self::Commit {
                    sha: first.clone(),
                    empty_policy,
                })
            } else {
                Some(Self::Range {
                    first: first.clone(),
                    last: last.clone(),
                    empty_policy,
                })
            }
        } else {
            None
        }
    }
}

/// Build expected (head, base) chain bottom→top from local groups.
///
/// This validates that the current stack does not derive case-colliding
/// synthetic branch names before returning any branch chain.
pub fn build_head_base_chain(
    base: &str,
    groups: &[Group],
    prefix: &str,
) -> Result<Vec<(String, String)>> {
    let mut expected: Vec<(String, String)> = vec![];
    let mut parent = base.to_string();
    for identity in crate::branch_names::group_branch_identities(groups, prefix)? {
        expected.push((identity.exact.clone(), parent.clone()));
        parent = identity.exact;
    }
    Ok(expected)
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_temp_worktree, create_backup_tag, create_temp_worktree,
        get_current_branch_and_short,
    };
    use crate::test_support::{lock_cwd, DirGuard};
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(repo: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("spawn git");
        assert!(
            out.status.success(),
            "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn init_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        fs::write(repo.join("file.txt"), "v1\n").expect("write file");
        git(repo, ["add", "."].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        dir
    }

    #[test]
    fn create_backup_tag_overwrites_existing() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let (cur_branch, short) =
            get_current_branch_and_short().expect("get current branch and short sha");

        let backup =
            create_backup_tag(false, "restack", &cur_branch, &short).expect("create backup");
        let backup_again =
            create_backup_tag(false, "restack", &cur_branch, &short).expect("overwrite backup");

        assert_eq!(backup, backup_again, "backup name should be stable");

        let head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        let backup_ref = format!("refs/tags/{}", backup);
        let backup_head = git(&repo, ["rev-parse", backup_ref.as_str()].as_slice());
        assert_eq!(head.trim(), backup_head.trim(), "backup should match HEAD");
        let branch_out = git(&repo, ["branch", "--list", backup.as_str()].as_slice());
        assert!(
            branch_out.trim().is_empty(),
            "backup should be a tag, not a branch"
        );
    }

    #[test]
    fn create_temp_worktree_replaces_existing_temp_branch() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let (_cur_branch, short) =
            get_current_branch_and_short().expect("get current branch and short sha");
        let merge_base = git(&repo, ["rev-parse", "HEAD"].as_slice());
        let merge_base = merge_base.trim().to_string();

        let (tmp_path, tmp_branch) = create_temp_worktree(false, "restack", &merge_base, &short)
            .expect("create initial temp worktree");

        // Simulate a failed prior run that removed the worktree but left the
        // temp branch behind.
        git(
            &repo,
            ["worktree", "remove", "-f", tmp_path.as_str()].as_slice(),
        );
        let branch_head = git(&repo, ["rev-parse", tmp_branch.as_str()].as_slice());
        assert_eq!(
            branch_head.trim(),
            merge_base,
            "temp branch should still exist after worktree removal"
        );

        let (tmp_path_2, tmp_branch_2) =
            create_temp_worktree(false, "restack", &merge_base, &short)
                .expect("recreate temp worktree after cleanup");
        assert_eq!(tmp_path, tmp_path_2, "temp path should be stable");
        assert_eq!(tmp_branch, tmp_branch_2, "temp branch should be stable");
        assert!(
            Path::new(&tmp_path_2).exists(),
            "temp worktree path should exist"
        );

        cleanup_temp_worktree(false, &tmp_path_2, &tmp_branch_2)
            .expect("cleanup recreated temp worktree");
    }
}
