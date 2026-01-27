//! Shared git helpers for stack-rewriting commands.
//!
//! These helpers centralize the mechanics used by `restack`, `move`, and
//! `fix-pr`: naming temporary branches/worktrees, creating safety backups, and
//! resetting the current branch to a rebuilt tip.
//!
//! A subtle but important invariant is that backup branch names include the
//! short SHA of `HEAD`. When a rewrite command fails before `HEAD` changes, a
//! second attempt would otherwise try to create the same backup branch again
//! and fail with "branch already exists". To keep `--safe` re-runnable after a
//! failure, `create_backup_branch` force-updates the backup ref in place.
//!
//! Temporary worktrees and their branches follow the same naming scheme. A
//! failed rewrite can therefore leave behind a temp branch that will collide
//! on the next run. `create_temp_worktree` proactively removes any existing
//! temp worktree/branch with the same derived name before creating a new one,
//! and uses `git worktree add -B` as a final safeguard when cleanup is skipped
//! in dry-run mode.
//!
//! Backup branches are local-only and are intended as an escape hatch for a
//! single user; callers should not rely on them being immutable.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tracing::info;

use crate::git::{git_ro, git_rw, normalize_branch_name};
use crate::parsing::Group;

/// Returns the current branch name and the short SHA of `HEAD`.
///
/// This is primarily used to derive stable, human-readable names for backup
/// branches and temporary worktree branches. If `HEAD` is detached, the branch
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

/// Creates or updates a local backup branch pointing at the current `HEAD`.
///
/// The backup name is derived from `(kind, cur_branch, short)` and is therefore
/// stable for a given `HEAD`. This stability is desirable for operator clarity,
/// but it means repeat runs at the same `HEAD` will collide. We force-update
/// the branch (`git branch -f`) so that `spr restack --safe` remains runnable
/// after a failed attempt that left the backup branch behind.
///
/// The existence check is only used to drive the log message; the branch is
/// always updated to point at `HEAD`.
pub fn create_backup_branch(
    dry: bool,
    kind: &str,
    cur_branch: &str,
    short: &str,
) -> Result<String> {
    let backup = format!("backup/{}/{}-{}", kind, cur_branch, short);
    let exists = git_ro(["branch", "--list", &backup].as_slice())?;
    if exists.trim().is_empty() {
        info!("Creating backup branch at HEAD: {}", backup);
    } else {
        info!("Backup branch exists; overwriting at HEAD: {}", backup);
    }
    // Use `-f` to make backup creation idempotent. When the name already
    // exists, we explicitly move it to the current HEAD.
    let _ = git_rw(dry, ["branch", "-f", &backup, "HEAD"].as_slice())?;
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

pub fn cherry_pick_commit(dry: bool, tmp_path: &str, sha: &str) -> Result<()> {
    let _ = git_rw(dry, ["-C", tmp_path, "cherry-pick", sha].as_slice())?;
    Ok(())
}

pub fn cherry_pick_range(dry: bool, tmp_path: &str, first: &str, last: &str) -> Result<()> {
    let range = format!("{}^..{}", first, last);
    let _ = git_rw(dry, ["-C", tmp_path, "cherry-pick", &range].as_slice())?;
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

/// Build expected (head, base) chain bottom→top from local groups
pub fn build_head_base_chain(base: &str, groups: &[Group], prefix: &str) -> Vec<(String, String)> {
    let mut expected: Vec<(String, String)> = vec![];
    let mut parent = base.to_string();
    for g in groups {
        let head = format!("{}{}", prefix, g.tag);
        expected.push((head.clone(), parent.clone()));
        parent = head;
    }
    expected
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_temp_worktree, create_backup_branch, create_temp_worktree,
        get_current_branch_and_short,
    };
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // Tests in this module mutate the process-wide current working directory.
    // Serialize them to avoid cross-test interference.
    static CWD_LOCK: Mutex<()> = Mutex::new(());

    struct DirGuard {
        original: PathBuf,
    }

    impl DirGuard {
        fn change_to(path: &Path) -> Self {
            let original = env::current_dir().expect("current dir available");
            env::set_current_dir(path).expect("set current dir to temp repo");
            Self { original }
        }
    }

    impl Drop for DirGuard {
        fn drop(&mut self) {
            env::set_current_dir(&self.original).expect("restore original current dir");
        }
    }

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
    fn create_backup_branch_overwrites_existing() {
        let _lock = CWD_LOCK.lock().expect("lock cwd");
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let (cur_branch, short) =
            get_current_branch_and_short().expect("get current branch and short sha");

        let backup =
            create_backup_branch(false, "restack", &cur_branch, &short).expect("create backup");
        let backup_again = create_backup_branch(false, "restack", &cur_branch, &short)
            .expect("overwrite backup");

        assert_eq!(backup, backup_again, "backup name should be stable");

        let head = git(&repo, ["rev-parse", "HEAD"].as_slice());
        let backup_head = git(&repo, ["rev-parse", backup.as_str()].as_slice());
        assert_eq!(head.trim(), backup_head.trim(), "backup should match HEAD");
    }

    #[test]
    fn create_temp_worktree_replaces_existing_temp_branch() {
        let _lock = CWD_LOCK.lock().expect("lock cwd");
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
