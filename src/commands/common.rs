use anyhow::Result;
use tracing::info;

use crate::git::{git_ro, git_rw};

pub fn get_current_branch_and_short() -> Result<(String, String)> {
    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    let short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
        .trim()
        .to_string();
    Ok((cur_branch, short))
}

pub fn create_backup_branch(
    dry: bool,
    kind: &str,
    cur_branch: &str,
    short: &str,
) -> Result<String> {
    let backup = format!("backup/{}/{}-{}", kind, cur_branch, short);
    info!("Creating backup branch at HEAD: {}", backup);
    let _ = git_rw(dry, ["branch", &backup, "HEAD"].as_slice())?;
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
