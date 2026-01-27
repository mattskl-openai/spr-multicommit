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
//! Backup branches are local-only and are intended as an escape hatch for a
//! single user; callers should not rely on them being immutable.

use anyhow::Result;
use tracing::info;

use crate::git::{git_ro, git_rw};
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

pub fn create_temp_worktree(
    dry: bool,
    kind: &str,
    merge_base: &str,
    short: &str,
) -> Result<(String, String)> {
    let tmp_branch = format!("spr/tmp-{}-{}", kind, short);
    let tmp_path = format!("/tmp/spr-{}-{}", kind, short);
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
            "-b",
            &tmp_branch,
            &tmp_path,
            merge_base,
        ]
        .as_slice(),
    )?;
    Ok((tmp_path, tmp_branch))
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
    use super::{create_backup_branch, get_current_branch_and_short};
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
}
