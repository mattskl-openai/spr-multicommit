//! Shared suspend/resume engine for local history rewrites.
//!
//! Rewrite-style commands build a temporary worktree, replay commit-level
//! cherry-picks there, and only move the original branch after the replay
//! succeeds. When a replay hits a cherry-pick conflict, this module can either
//! clean up immediately or persist a local resume file under the repository's
//! common Git directory so the operator can resolve the conflict and hand
//! control back to `spr`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::commands::common::{self, CherryPickEmptyPolicy, CherryPickOp};
use crate::git::{git_rev_list_range, git_ro, git_rw, repo_root};

const REWRITE_RESUME_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteConflictPolicy {
    Rollback,
    Suspend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteCommandOutcome {
    Completed,
    Suspended { resume_path: PathBuf },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RewriteCommandKind {
    Restack,
    Absorb,
    Move,
    FixPr,
}

impl RewriteCommandKind {
    fn resume_slug(self) -> &'static str {
        match self {
            Self::Restack => "restack",
            Self::Absorb => "absorb",
            Self::Move => "move",
            Self::FixPr => "fix-pr",
        }
    }

    fn command_name(self) -> &'static str {
        match self {
            Self::Restack => "spr restack",
            Self::Absorb => "spr absorb",
            Self::Move => "spr move",
            Self::FixPr => "spr fix-pr",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteReplayStep {
    pub source_sha: String,
    pub empty_policy: CherryPickEmptyPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteResumeState {
    pub schema_version: u32,
    pub command_kind: RewriteCommandKind,
    pub git_common_dir: String,
    pub original_worktree_root: String,
    pub original_branch: String,
    pub original_head: String,
    pub temp_branch: String,
    pub temp_worktree_path: String,
    pub backup_tag: Option<String>,
    pub paused_head: String,
    pub suspended_step_index: usize,
    pub steps: Vec<RewriteReplayStep>,
    pub post_success_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteSession {
    pub command_kind: RewriteCommandKind,
    pub conflict_policy: RewriteConflictPolicy,
    pub original_worktree_root: String,
    pub original_branch: String,
    pub original_head: String,
    pub temp_branch: String,
    pub temp_worktree_path: String,
    pub backup_tag: Option<String>,
    pub steps: Vec<RewriteReplayStep>,
    pub post_success_hint: Option<String>,
}

pub fn build_replay_steps(ops: &[CherryPickOp]) -> Result<Vec<RewriteReplayStep>> {
    let mut steps = Vec::new();
    for op in ops {
        match op {
            CherryPickOp::Commit { sha, empty_policy } => steps.push(RewriteReplayStep {
                source_sha: sha.clone(),
                empty_policy: *empty_policy,
            }),
            CherryPickOp::Range {
                first,
                last,
                empty_policy,
            } => {
                let commits = git_rev_list_range(&format!("{first}^"), last)?;
                steps.extend(commits.into_iter().map(|source_sha| RewriteReplayStep {
                    source_sha,
                    empty_policy: *empty_policy,
                }));
            }
        }
    }
    Ok(steps)
}

pub fn current_repo_root() -> Result<String> {
    repo_root()?.ok_or_else(|| anyhow!("`spr` must run from inside a git worktree"))
}

pub fn run_rewrite_session(dry: bool, session: RewriteSession) -> Result<RewriteCommandOutcome> {
    let git_common_dir = current_common_git_dir()?;
    let resume_path = default_resume_path(
        &git_common_dir,
        session.command_kind,
        &session.original_branch,
        &session.original_head,
    );
    let temp_worktree_path = session.temp_worktree_path.clone();
    remove_resume_file_if_exists(dry, &resume_path)?;
    let state = RewriteResumeState {
        schema_version: REWRITE_RESUME_SCHEMA_VERSION,
        command_kind: session.command_kind,
        git_common_dir: git_common_dir.display().to_string(),
        original_worktree_root: session.original_worktree_root,
        original_branch: session.original_branch,
        original_head: session.original_head,
        temp_branch: session.temp_branch,
        temp_worktree_path: session.temp_worktree_path,
        backup_tag: session.backup_tag,
        paused_head: head_at(&temp_worktree_path)?,
        suspended_step_index: 0,
        steps: session.steps,
        post_success_hint: session.post_success_hint,
    };
    continue_rewrite(dry, state, session.conflict_policy, &resume_path)
}

pub fn resume_rewrite(dry: bool, path: &Path) -> Result<RewriteCommandOutcome> {
    if dry {
        bail!("`spr resume` does not support `--dry-run`");
    }

    let resume_path = absolute_path(path)?;
    let mut state = read_resume_state(&resume_path)?;
    validate_resume_state_against_current_repo(&resume_path, &state)?;
    validate_temp_worktree_exists(&state)?;

    if state.suspended_step_index >= state.steps.len() {
        bail!(
            "resume file {} refers to step {} but only records {} replay steps",
            resume_path.display(),
            state.suspended_step_index,
            state.steps.len()
        );
    }

    if cherry_pick_head_exists(&state.temp_worktree_path) {
        if head_at(&state.temp_worktree_path)? != state.paused_head {
            bail!(
                "temp worktree {} no longer points at the paused HEAD {}; abort or discard this suspended rewrite",
                state.temp_worktree_path,
                state.paused_head
            );
        }
        ensure_no_unmerged_paths(&state.temp_worktree_path)?;
        let continue_result = git_rw(
            false,
            [
                "-C",
                state.temp_worktree_path.as_str(),
                "cherry-pick",
                "--continue",
            ]
            .as_slice(),
        )
        .with_context(|| {
            format!(
                "{} could not continue the staged conflict resolution in {}",
                state.command_kind.command_name(),
                state.temp_worktree_path
            )
        });
        if let Err(err) = continue_result {
            if cherry_pick_head_exists(&state.temp_worktree_path)
                && worktree_status_lines(&state.temp_worktree_path)?.is_empty()
            {
                info!(
                    "The paused cherry-pick in {} became empty after conflict resolution; skipping it.",
                    state.temp_worktree_path
                );
                let _ = git_rw(
                    false,
                    [
                        "-C",
                        state.temp_worktree_path.as_str(),
                        "cherry-pick",
                        "--skip",
                    ]
                    .as_slice(),
                )
                .with_context(|| {
                    format!(
                        "{} could not skip the now-empty cherry-pick in {}",
                        state.command_kind.command_name(),
                        state.temp_worktree_path
                    )
                })?;
            } else {
                return Err(err);
            }
        }
        state.suspended_step_index += 1;
    } else {
        let advanced = commits_since_paused_head(&state.temp_worktree_path, &state.paused_head)?;
        if advanced == 0 {
            bail!(
                "resume file {} expects an in-progress cherry-pick in {}, but `CHERRY_PICK_HEAD` is missing and HEAD did not advance from {}",
                resume_path.display(),
                state.temp_worktree_path,
                state.paused_head
            );
        } else if advanced == 1 {
            info!(
                "Detected one manual `git cherry-pick --continue` in {}; resuming remaining replay steps.",
                state.temp_worktree_path
            );
            state.suspended_step_index += 1;
        } else {
            bail!(
                "temp worktree {} advanced by {} commits beyond paused HEAD {}; only one accidental manual continue is supported",
                state.temp_worktree_path,
                advanced,
                state.paused_head
            );
        }
    }

    continue_rewrite(false, state, RewriteConflictPolicy::Suspend, &resume_path)
}

fn continue_rewrite(
    dry: bool,
    mut state: RewriteResumeState,
    conflict_policy: RewriteConflictPolicy,
    resume_path: &Path,
) -> Result<RewriteCommandOutcome> {
    let mut next_index = state.suspended_step_index;
    while next_index < state.steps.len() {
        let paused_head = head_at(&state.temp_worktree_path)?;
        let step = &state.steps[next_index];
        if let Err(err) = common::cherry_pick_commit(
            dry,
            &state.temp_worktree_path,
            &step.source_sha,
            step.empty_policy,
        ) {
            let conflict = cherry_pick_head_exists(&state.temp_worktree_path);
            if conflict && conflict_policy == RewriteConflictPolicy::Suspend {
                state.paused_head = paused_head;
                state.suspended_step_index = next_index;
                write_resume_state(resume_path, &state)?;
                emit_suspend_instructions(resume_path, &state);
                return Ok(RewriteCommandOutcome::Suspended {
                    resume_path: resume_path.to_path_buf(),
                });
            }

            if conflict {
                abort_cherry_pick_best_effort(dry, &state.temp_worktree_path);
            }
            cleanup_temp_state_best_effort(dry, &state.temp_worktree_path, &state.temp_branch);
            return Err(err).with_context(|| {
                format!(
                    "{} failed; temp rewrite state was cleaned up",
                    state.command_kind.command_name()
                )
            });
        }
        next_index += 1;
    }

    validate_original_worktree_target(&state)?;
    let new_tip = common::tip_of_tmp(&state.temp_worktree_path)?;
    info!(
        "Updating original branch {} in {} to new tip {}...",
        state.original_branch, state.original_worktree_root, new_tip
    );
    let _ = git_rw(
        dry,
        [
            "-C",
            state.original_worktree_root.as_str(),
            "reset",
            "--hard",
            &new_tip,
        ]
        .as_slice(),
    )?;
    cleanup_temp_state_best_effort(dry, &state.temp_worktree_path, &state.temp_branch);
    remove_resume_file_if_exists(dry, resume_path)?;
    if let Some(post_success_hint) = &state.post_success_hint {
        info!("{post_success_hint}");
    }
    Ok(RewriteCommandOutcome::Completed)
}

fn emit_suspend_instructions(resume_path: &Path, state: &RewriteResumeState) {
    for line in suspend_instruction_lines(resume_path, state) {
        info!("{line}");
    }
}

fn suspend_instruction_lines(resume_path: &Path, state: &RewriteResumeState) -> Vec<String> {
    let mut lines = vec![
        format!(
            "{} suspended due to a cherry-pick conflict.",
            state.command_kind.command_name()
        ),
        format!("Temp worktree: {}", state.temp_worktree_path),
        format!("Temp branch: {}", state.temp_branch),
        format!("Original branch: {}", state.original_branch),
        format!("Resume file: {}", resume_path.display()),
        "Resolve the conflict in the temp worktree, stage the resolution, and run:".to_string(),
        format!("  spr resume {}", resume_path.display()),
        "To discard this suspended rewrite and clean up its temp state:".to_string(),
        format!("  git -C {} cherry-pick --abort", state.temp_worktree_path),
        format!("  rm {}", resume_path.display()),
        format!("  git worktree remove -f {}", state.temp_worktree_path),
        format!("  git branch -D {}", state.temp_branch),
    ];
    if let Some(backup_tag) = &state.backup_tag {
        lines.extend([
            "  # optional: restore the backup tag onto the original worktree".to_string(),
            format!(
                "  git -C {} reset --hard refs/tags/{}",
                state.original_worktree_root, backup_tag
            ),
        ]);
    }
    lines
}

fn abort_cherry_pick_best_effort(dry: bool, tmp_path: &str) {
    if let Err(err) = git_rw(dry, ["-C", tmp_path, "cherry-pick", "--abort"].as_slice()) {
        warn!("Failed to abort cherry-pick in {}: {}", tmp_path, err);
    }
}

fn cleanup_temp_state_best_effort(dry: bool, tmp_path: &str, tmp_branch: &str) {
    if let Err(err) = common::cleanup_temp_worktree(dry, tmp_path, tmp_branch) {
        warn!(
            "Failed to clean up temp rewrite state ({} / {}): {}",
            tmp_path, tmp_branch, err
        );
    }
}

fn write_resume_state(path: &Path, state: &RewriteResumeState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create resume-state directory {}",
                parent.display()
            )
        })?;
    }
    let json = serde_json::to_string_pretty(state).context("failed to encode resume-state JSON")?;
    fs::write(path, json)
        .with_context(|| format!("failed to write resume-state file {}", path.display()))
}

fn read_resume_state(path: &Path) -> Result<RewriteResumeState> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read resume-state file {}", path.display()))?;
    let state: RewriteResumeState = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse resume-state file {}", path.display()))?;
    if state.schema_version != REWRITE_RESUME_SCHEMA_VERSION {
        bail!(
            "resume file {} uses schema version {}, but this `spr` expects {}",
            path.display(),
            state.schema_version,
            REWRITE_RESUME_SCHEMA_VERSION
        );
    }
    Ok(state)
}

fn validate_resume_state_against_current_repo(
    path: &Path,
    state: &RewriteResumeState,
) -> Result<()> {
    let current_common_dir = current_common_git_dir()?;
    let expected_common_dir = canonicalize_existing_path(Path::new(&state.git_common_dir))
        .with_context(|| {
            format!(
                "resume file {} points at missing git-common-dir {}",
                path.display(),
                state.git_common_dir
            )
        })?;
    if current_common_dir != expected_common_dir {
        bail!(
            "resume file {} belongs to git-common-dir {}, but the current repository uses {}",
            path.display(),
            expected_common_dir.display(),
            current_common_dir.display()
        );
    }

    let expected_prefix = expected_common_dir.join("spr").join("resume");
    let actual_parent = path.parent().ok_or_else(|| {
        anyhow!(
            "resume file path {} has no parent directory",
            path.display()
        )
    })?;
    let canonical_parent = canonicalize_existing_path(actual_parent)?;
    if canonical_parent != expected_prefix {
        bail!(
            "resume file {} is not located under the expected resume directory {}",
            path.display(),
            expected_prefix.display()
        );
    }

    Ok(())
}

fn validate_temp_worktree_exists(state: &RewriteResumeState) -> Result<()> {
    if Path::new(&state.temp_worktree_path).exists() {
        Ok(())
    } else {
        bail!(
            "temp worktree {} recorded in the resume file no longer exists",
            state.temp_worktree_path
        );
    }
}

fn validate_original_worktree_target(state: &RewriteResumeState) -> Result<()> {
    if !Path::new(&state.original_worktree_root).exists() {
        bail!(
            "original worktree root {} no longer exists",
            state.original_worktree_root
        );
    }

    let current_branch = branch_at(&state.original_worktree_root)?;
    if current_branch != state.original_branch {
        bail!(
            "original worktree {} is now on branch {} instead of recorded branch {}",
            state.original_worktree_root,
            current_branch,
            state.original_branch
        );
    }

    let current_head = head_at(&state.original_worktree_root)?;
    if current_head != state.original_head {
        bail!(
            "original branch {} moved from recorded tip {} to {}; resume refuses to reset it",
            state.original_branch,
            state.original_head,
            current_head
        );
    }

    Ok(())
}

fn current_common_git_dir() -> Result<PathBuf> {
    let raw = git_ro(["rev-parse", "--git-common-dir"].as_slice())?;
    canonicalize_existing_path(&absolute_path(Path::new(raw.trim()))?)
}

fn default_resume_path(
    git_common_dir: &Path,
    command_kind: RewriteCommandKind,
    original_branch: &str,
    original_head: &str,
) -> PathBuf {
    let sanitized_branch = sanitize_branch_for_filename(original_branch);
    let short = short_head(original_head);
    git_common_dir.join("spr").join("resume").join(format!(
        "{}-{}-{}.json",
        command_kind.resume_slug(),
        sanitized_branch,
        short
    ))
}

fn sanitize_branch_for_filename(branch: &str) -> String {
    let sanitized: String = branch
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    if sanitized.is_empty() {
        "HEAD".to_string()
    } else {
        sanitized
    }
}

fn short_head(head: &str) -> &str {
    let limit = std::cmp::min(7, head.len());
    &head[..limit]
}

fn remove_resume_file_if_exists(dry: bool, path: &Path) -> Result<()> {
    if dry {
        Ok(())
    } else if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("failed to remove stale resume file {}", path.display()))
    } else {
        Ok(())
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()
            .context("current directory is unavailable")?
            .join(path))
    }
}

fn canonicalize_existing_path(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path)
        .with_context(|| format!("failed to canonicalize path {}", path.display()))
}

fn branch_at(path: &str) -> Result<String> {
    Ok(
        git_ro(["-C", path, "rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
            .trim()
            .to_string(),
    )
}

fn head_at(path: &str) -> Result<String> {
    Ok(git_ro(["-C", path, "rev-parse", "HEAD"].as_slice())?
        .trim()
        .to_string())
}

fn cherry_pick_head_exists(tmp_path: &str) -> bool {
    Command::new("git")
        .args([
            "-C",
            tmp_path,
            "rev-parse",
            "-q",
            "--verify",
            "CHERRY_PICK_HEAD",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_no_unmerged_paths(tmp_path: &str) -> Result<()> {
    let conflict_lines = worktree_status_lines(tmp_path)?
        .into_iter()
        .filter(|line| status_line_has_conflict(line))
        .collect::<Vec<_>>();
    if conflict_lines.is_empty() {
        Ok(())
    } else {
        bail!(
            "temp worktree {} still has unresolved conflicts:\n{}\nResolve them and stage the result before running `spr resume`.",
            tmp_path,
            conflict_lines.join("\n")
        );
    }
}

fn worktree_status_lines(tmp_path: &str) -> Result<Vec<String>> {
    Ok(
        git_ro(["-C", tmp_path, "status", "--porcelain=v1"].as_slice())?
            .lines()
            .map(str::to_string)
            .collect(),
    )
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

fn commits_since_paused_head(tmp_path: &str, paused_head: &str) -> Result<usize> {
    let current_head = head_at(tmp_path)?;
    let is_ancestor = Command::new("git")
        .args([
            "-C",
            tmp_path,
            "merge-base",
            "--is-ancestor",
            paused_head,
            &current_head,
        ])
        .status()
        .with_context(|| {
            format!(
                "failed to compare paused HEAD {} with current HEAD {} in {}",
                paused_head, current_head, tmp_path
            )
        })?;
    if !is_ancestor.success() {
        bail!(
            "paused HEAD {} is not an ancestor of current HEAD {} in {}",
            paused_head,
            current_head,
            tmp_path
        );
    }
    let out = git_ro(
        [
            "-C",
            tmp_path,
            "rev-list",
            "--count",
            &format!("{paused_head}..{current_head}"),
        ]
        .as_slice(),
    )?;
    out.trim().parse::<usize>().with_context(|| {
        format!(
            "failed to parse commit count for paused HEAD {}",
            paused_head
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use tempfile::TempDir;

    use super::{
        build_replay_steps, default_resume_path, resume_rewrite, run_rewrite_session,
        sanitize_branch_for_filename, suspend_instruction_lines, RewriteCommandKind,
        RewriteCommandOutcome, RewriteConflictPolicy, RewriteResumeState, RewriteSession,
        REWRITE_RESUME_SCHEMA_VERSION,
    };
    use crate::commands::common::{CherryPickEmptyPolicy, CherryPickOp};
    use crate::test_support::{lock_cwd, DirGuard};

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

    fn init_conflict_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        fs::write(repo.join("story.txt"), "base\n").expect("write base file");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        dir
    }

    fn commit_file(repo: &Path, file: &str, contents: &str, message: &str) -> String {
        fs::write(repo.join(file), contents).expect("write file");
        git(repo, ["add", file].as_slice());
        git(repo, ["commit", "-m", message].as_slice());
        git(repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string()
    }

    fn current_common_git_dir(repo: &Path) -> PathBuf {
        let raw = git(repo, ["rev-parse", "--git-common-dir"].as_slice());
        let path = PathBuf::from(raw.trim());
        if path.is_absolute() {
            fs::canonicalize(path).expect("canonicalize git common dir")
        } else {
            fs::canonicalize(repo.join(path)).expect("canonicalize git common dir")
        }
    }

    fn resolve_keep_both(repo: &Path, contents: &str) {
        fs::write(repo.join("story.txt"), contents).expect("resolve conflict");
        git(repo, ["add", "story.txt"].as_slice());
    }

    fn suspended_session_repo() -> (TempDir, PathBuf, PathBuf) {
        let dir = init_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        let original_head = commit_file(&repo, "story.txt", "stack-change\n", "feat: stack change");

        git(&repo, ["checkout", "main"].as_slice());
        let base_head = commit_file(&repo, "story.txt", "base-updated\n", "feat: base update");
        git(&repo, ["checkout", "stack"].as_slice());
        let short = git(&repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();
        let (tmp_path, tmp_branch) =
            crate::commands::common::create_temp_worktree(false, "restack", &base_head, &short)
                .expect("create temp worktree");

        let session = RewriteSession {
            command_kind: RewriteCommandKind::Restack,
            conflict_policy: RewriteConflictPolicy::Suspend,
            original_worktree_root: repo.display().to_string(),
            original_branch: "stack".to_string(),
            original_head: original_head.clone(),
            temp_branch: tmp_branch.clone(),
            temp_worktree_path: tmp_path.clone(),
            backup_tag: None,
            steps: vec![super::RewriteReplayStep {
                source_sha: original_head.clone(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }],
            post_success_hint: None,
        };

        let outcome = run_rewrite_session(false, session).expect("run rewrite session");
        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended rewrite"),
            RewriteCommandOutcome::Suspended { resume_path } => resume_path,
        };

        (dir, repo, resume_path)
    }

    #[test]
    fn build_replay_steps_expands_ranges_to_single_commits() {
        let _lock = lock_cwd();
        let dir = init_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let first = commit_file(&repo, "alpha.txt", "alpha-1\n", "feat: alpha");
        let second = commit_file(&repo, "alpha.txt", "alpha-1\nalpha-2\n", "feat: alpha 2");
        let steps = build_replay_steps(&[
            CherryPickOp::Commit {
                sha: first.clone(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            },
            CherryPickOp::Range {
                first,
                last: second.clone(),
                empty_policy: CherryPickEmptyPolicy::KeepRedundantCommits,
            },
        ])
        .expect("flatten replay steps");

        assert_eq!(steps.len(), 3);
        assert_eq!(steps[1].source_sha, steps[0].source_sha);
        assert_eq!(steps[2].source_sha, second);
        assert_eq!(
            steps[2].empty_policy,
            CherryPickEmptyPolicy::KeepRedundantCommits
        );
    }

    #[test]
    fn sanitize_branch_for_filename_replaces_separators() {
        assert_eq!(sanitize_branch_for_filename("dank/main"), "dank_main");
    }

    #[test]
    fn resume_rewrite_completes_after_staged_resolution() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        resolve_keep_both(
            Path::new(&resume_state.temp_worktree_path),
            "base-updated\nstack-change\n",
        );

        let outcome = resume_rewrite(false, &resume_path).expect("resume rewrite");
        assert_eq!(outcome, RewriteCommandOutcome::Completed);
        assert!(
            !resume_path.exists(),
            "successful resume should delete the resume file"
        );
        assert_eq!(
            fs::read_to_string(repo.join("story.txt")).expect("read final file"),
            "base-updated\nstack-change\n"
        );
    }

    #[test]
    fn resume_rewrite_tolerates_one_manual_continue() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        resolve_keep_both(temp_repo, "base-updated\nstack-change\n");
        git(temp_repo, ["cherry-pick", "--continue"].as_slice());

        let outcome = resume_rewrite(false, &resume_path).expect("resume after manual continue");
        assert_eq!(outcome, RewriteCommandOutcome::Completed);
        assert!(
            !resume_path.exists(),
            "resume file should be removed after the resumed replay finishes"
        );
    }

    #[test]
    fn resume_rewrite_rejects_wrong_repository() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let wrong_repo = init_conflict_repo();
        let _guard = DirGuard::change_to(wrong_repo.path());
        let err = resume_rewrite(false, &resume_path).expect_err("wrong repo should fail");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("belongs to git-common-dir"),
            "unexpected error: {err_text}"
        );
        drop(repo);
    }

    #[test]
    fn resume_rewrite_rejects_unresolved_conflicts() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let err = resume_rewrite(false, &resume_path).expect_err("unresolved conflict should fail");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("still has unresolved conflicts"),
            "unexpected error: {err_text}"
        );
    }

    #[test]
    fn resume_rewrite_rejects_unknown_schema_version() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let mut state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        state.schema_version = REWRITE_RESUME_SCHEMA_VERSION + 1;
        fs::write(
            &resume_path,
            serde_json::to_string_pretty(&state).expect("encode modified state"),
        )
        .expect("overwrite resume state");

        let err = resume_rewrite(false, &resume_path).expect_err("unknown schema should fail");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("schema version"),
            "unexpected error: {err_text}"
        );
    }

    #[test]
    fn resume_rewrite_rejects_multiple_manual_commits() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        resolve_keep_both(temp_repo, "base-updated\nstack-change\n");
        git(temp_repo, ["cherry-pick", "--continue"].as_slice());
        commit_file(
            temp_repo,
            "extra.txt",
            "extra\n",
            "feat: unsupported manual edit",
        );

        let err = resume_rewrite(false, &resume_path).expect_err("extra manual commit should fail");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("only one accidental manual continue is supported"),
            "unexpected error: {err_text}"
        );
    }

    #[test]
    fn default_resume_path_uses_common_git_directory() {
        let dir = init_conflict_repo();
        let repo = dir.path().to_path_buf();
        let common_dir = current_common_git_dir(&repo);
        let path = default_resume_path(
            &common_dir,
            RewriteCommandKind::Restack,
            "dank/main",
            "2b613d8f7076641755b8137a320c8b9c881a5469",
        );

        assert_eq!(
            path,
            common_dir
                .join("spr")
                .join("resume")
                .join("restack-dank_main-2b613d8.json")
        );
    }

    #[test]
    fn suspend_instruction_lines_include_original_branch() {
        let state = RewriteResumeState {
            schema_version: REWRITE_RESUME_SCHEMA_VERSION,
            command_kind: RewriteCommandKind::Restack,
            git_common_dir: "/tmp/repo/.git".to_string(),
            original_worktree_root: "/tmp/repo".to_string(),
            original_branch: "stack".to_string(),
            original_head: "abcdef0123456789".to_string(),
            temp_branch: "spr/tmp-restack-abcdef0".to_string(),
            temp_worktree_path: "/tmp/spr-restack-abcdef0".to_string(),
            backup_tag: None,
            paused_head: "abcdef0123456789".to_string(),
            suspended_step_index: 0,
            steps: vec![],
            post_success_hint: None,
        };

        let lines =
            suspend_instruction_lines(Path::new("/tmp/repo/.git/spr/resume/state.json"), &state);

        assert!(
            lines.contains(&"Original branch: stack".to_string()),
            "expected original branch in suspend output: {lines:#?}"
        );
    }
}
