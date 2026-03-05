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

use crate::commands::common::{
    self, CherryPickEmptyPolicy, CherryPickOp, DeferredDirtyWorktreeRestore, DirtyWorktreeOutcome,
};
use crate::git::{git_common_dir, git_rev_list_range, git_ro, git_rw, repo_root};

const REWRITE_RESUME_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteConflictPolicy {
    Rollback,
    Suspend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteCommandOutcome {
    Completed,
    Suspended(Box<RewriteSuspendedState>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteSuspendedState {
    pub command_kind: RewriteCommandKind,
    pub original_worktree_root: String,
    pub original_branch: String,
    pub temp_branch: String,
    pub temp_worktree_path: String,
    pub resume_path: PathBuf,
    pub paused_source_sha: String,
    pub conflicted_paths: Vec<String>,
    pub post_success_hint: Option<String>,
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
    pub paused_step: RewriteReplayStep,
    pub remaining_operations: Vec<CherryPickOp>,
    #[serde(default)]
    pub deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore,
    pub post_success_hint: Option<String>,
    #[serde(default)]
    pub metadata_refresh_context: Option<crate::stack_metadata::RefreshMetadataContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteSession {
    pub command_kind: RewriteCommandKind,
    pub conflict_policy: RewriteConflictPolicy,
    pub original_worktree_root: String,
    pub original_branch: String,
    pub original_head: String,
    pub resume_path: PathBuf,
    pub temp_branch: String,
    pub temp_worktree_path: String,
    pub backup_tag: Option<String>,
    pub operations: Vec<CherryPickOp>,
    pub deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore,
    pub post_success_hint: Option<String>,
    pub metadata_refresh_context: Option<crate::stack_metadata::RefreshMetadataContext>,
}

impl DirtyWorktreeOutcome for RewriteCommandOutcome {
    fn keeps_dirty_worktree_restore_deferred(&self) -> bool {
        matches!(self, Self::Suspended { .. })
    }
}

pub fn current_repo_root() -> Result<String> {
    repo_root()?.ok_or_else(|| anyhow!("`spr` must run from inside a git worktree"))
}

pub fn prepare_resume_path_for_new_session(
    dry: bool,
    command_kind: RewriteCommandKind,
    original_branch: &str,
    original_head: &str,
) -> Result<PathBuf> {
    let git_common_dir = git_common_dir()?;
    let resume_path = default_resume_path(
        &git_common_dir,
        command_kind,
        original_branch,
        original_head,
    );
    if !resume_path.exists() {
        return Ok(resume_path);
    }

    let existing_state = read_resume_state(&resume_path).with_context(|| {
        format!(
            "existing resume file {} must be resumed, discarded, or repaired before starting a new {} session",
            resume_path.display(),
            command_kind.command_name()
        )
    })?;
    if Path::new(&existing_state.temp_worktree_path).exists() {
        bail!(
            "an active suspended {} session already exists at {}; run `spr resume {}` or discard that session first",
            command_kind.command_name(),
            resume_path.display(),
            resume_path.display()
        );
    }

    if dry {
        bail!(
            "stale resume file {} exists for {}, but `--dry-run` will not remove it; delete the file or rerun without `--dry-run`",
            resume_path.display(),
            command_kind.command_name()
        );
    }

    info!(
        "Removing stale resume file {} because its temp worktree {} no longer exists.",
        resume_path.display(),
        existing_state.temp_worktree_path
    );
    remove_resume_file_if_exists(false, &resume_path)?;
    Ok(resume_path)
}

pub fn run_rewrite_session(dry: bool, session: RewriteSession) -> Result<RewriteCommandOutcome> {
    let git_common_dir = git_common_dir()?;
    let state = RewriteResumeState {
        schema_version: REWRITE_RESUME_SCHEMA_VERSION,
        command_kind: session.command_kind,
        git_common_dir: git_common_dir.display().to_string(),
        original_worktree_root: session.original_worktree_root,
        original_branch: session.original_branch,
        original_head: session.original_head.clone(),
        temp_branch: session.temp_branch,
        temp_worktree_path: session.temp_worktree_path.clone(),
        backup_tag: session.backup_tag,
        paused_head: head_at(&session.temp_worktree_path)?,
        paused_step: RewriteReplayStep {
            source_sha: session.original_head,
            empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
        },
        remaining_operations: Vec::new(),
        deferred_dirty_worktree_restore: session.deferred_dirty_worktree_restore,
        post_success_hint: session.post_success_hint,
        metadata_refresh_context: session.metadata_refresh_context,
    };
    continue_rewrite_operations(
        dry,
        state,
        session.conflict_policy,
        &session.resume_path,
        &session.operations,
        false,
    )
}

pub fn resume_rewrite(dry: bool, path: &Path) -> Result<RewriteCommandOutcome> {
    if dry {
        bail!("`spr resume` does not support `--dry-run`");
    }

    let resume_path = absolute_path(path)?;
    let state = read_resume_state(&resume_path)?;
    validate_resume_state_against_current_repo(&resume_path, &state)?;
    validate_temp_worktree_exists(&state)?;

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
                let skip_result = git_rw(
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
                });
                skip_result?;
            } else {
                return Err(err);
            }
        }
    } else {
        let advanced_commits =
            advanced_commits_since_paused_head(&state.temp_worktree_path, &state.paused_head)?;
        if advanced_commits.is_empty() {
            bail!(
                "resume file {} expects an in-progress cherry-pick in {}, but `CHERRY_PICK_HEAD` is missing and HEAD did not advance from {}",
                resume_path.display(),
                state.temp_worktree_path,
                state.paused_head
            );
        } else {
            validate_manual_continue_commit(&state)?;
            info!(
                "Detected one manual `git cherry-pick --continue` in {}; resuming the remaining replay operations.",
                state.temp_worktree_path
            );
        }
    }

    let remaining_operations = state.remaining_operations.clone();
    continue_rewrite_operations(
        false,
        state,
        RewriteConflictPolicy::Suspend,
        &resume_path,
        &remaining_operations,
        true,
    )
}

fn continue_rewrite_operations(
    dry: bool,
    mut state: RewriteResumeState,
    conflict_policy: RewriteConflictPolicy,
    resume_path: &Path,
    operations: &[CherryPickOp],
    restore_dirty_worktree_on_success: bool,
) -> Result<RewriteCommandOutcome> {
    for (op_index, op) in operations.iter().enumerate() {
        if let Err(err) = run_cherry_pick_op(dry, &state.temp_worktree_path, op) {
            let conflict = cherry_pick_head_exists(&state.temp_worktree_path);
            if conflict && conflict_policy == RewriteConflictPolicy::Suspend {
                state.paused_head = head_at(&state.temp_worktree_path)?;
                let (paused_step, remaining_operations) = split_conflicted_operation(
                    &state.temp_worktree_path,
                    op,
                    &operations[op_index + 1..],
                )?;
                state.paused_step = paused_step;
                state.remaining_operations = remaining_operations;
                write_resume_state(resume_path, &state)?;
                emit_suspend_instructions(resume_path, &state);
                return Ok(RewriteCommandOutcome::Suspended(Box::new(
                    suspended_state_from_resume_state(resume_path, &state)?,
                )));
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
    }

    finish_rewrite(dry, state, resume_path, restore_dirty_worktree_on_success)
}

fn finish_rewrite(
    dry: bool,
    state: RewriteResumeState,
    resume_path: &Path,
    restore_dirty_worktree_on_success: bool,
) -> Result<RewriteCommandOutcome> {
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
    let metadata_refresh_result = if dry {
        Ok(())
    } else if let Some(metadata_refresh_context) = &state.metadata_refresh_context {
        crate::stack_metadata::refresh_metadata_for_branch(
            &state.original_worktree_root,
            &state.original_branch,
            metadata_refresh_context,
            Some(Path::new(&state.git_common_dir)),
        )
    } else {
        Ok(())
    };
    let restore_result = if restore_dirty_worktree_on_success {
        state
            .deferred_dirty_worktree_restore
            .clone()
            .restore_after_success(
                dry,
                state.command_kind.command_name(),
                &state.original_worktree_root,
            )
    } else {
        Ok(())
    };
    cleanup_temp_state_best_effort(dry, &state.temp_worktree_path, &state.temp_branch);
    remove_resume_file_if_exists(dry, resume_path)?;
    if let Some(post_success_hint) = &state.post_success_hint {
        info!("{post_success_hint}");
    }
    restore_result?;
    metadata_refresh_result?;
    Ok(RewriteCommandOutcome::Completed)
}

fn emit_suspend_instructions(resume_path: &Path, state: &RewriteResumeState) {
    for line in suspend_instruction_lines(resume_path, state) {
        info!("{line}");
    }
}

fn suspended_state_from_resume_state(
    resume_path: &Path,
    state: &RewriteResumeState,
) -> Result<RewriteSuspendedState> {
    let conflicted_paths =
        conflicted_paths_from_status_lines(&worktree_status_lines(&state.temp_worktree_path)?);
    Ok(RewriteSuspendedState {
        command_kind: state.command_kind,
        original_worktree_root: state.original_worktree_root.clone(),
        original_branch: state.original_branch.clone(),
        temp_branch: state.temp_branch.clone(),
        temp_worktree_path: state.temp_worktree_path.clone(),
        resume_path: resume_path.to_path_buf(),
        paused_source_sha: state.paused_step.source_sha.clone(),
        conflicted_paths,
        post_success_hint: state.post_success_hint.clone(),
    })
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
    ];
    lines.extend(
        state
            .deferred_dirty_worktree_restore
            .discard_instruction_lines(&state.original_worktree_root),
    );
    lines.extend([
        format!("  rm {}", resume_path.display()),
        format!("  git worktree remove -f {}", state.temp_worktree_path),
        format!("  git branch -D {}", state.temp_branch),
    ]);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitIdentity {
    parents: Vec<String>,
    message: String,
    author_name: String,
    author_email: String,
    author_date: String,
}

fn run_cherry_pick_op(dry: bool, tmp_path: &str, op: &CherryPickOp) -> Result<()> {
    match op {
        CherryPickOp::Commit { sha, empty_policy } => {
            common::cherry_pick_commit(dry, tmp_path, sha, *empty_policy)
        }
        CherryPickOp::Range {
            first,
            last,
            empty_policy,
        } => common::cherry_pick_range(dry, tmp_path, first, last, *empty_policy),
    }
}

fn split_conflicted_operation(
    tmp_path: &str,
    op: &CherryPickOp,
    later_operations: &[CherryPickOp],
) -> Result<(RewriteReplayStep, Vec<CherryPickOp>)> {
    match op {
        CherryPickOp::Commit { sha, empty_policy } => Ok((
            RewriteReplayStep {
                source_sha: sha.clone(),
                empty_policy: *empty_policy,
            },
            later_operations.to_vec(),
        )),
        CherryPickOp::Range {
            first,
            last,
            empty_policy,
        } => {
            let commits = git_rev_list_range(&format!("{first}^"), last)?;
            let paused_source_sha = cherry_pick_head_at(tmp_path)?;
            let paused_index = commits
                .iter()
                .position(|sha| sha == &paused_source_sha)
                .ok_or_else(|| {
                    anyhow!(
                        "paused cherry-pick commit {} was not found inside range {}^..{}",
                        paused_source_sha,
                        first,
                        last
                    )
                })?;
            trim_sequencer_todo_to_current_step(tmp_path)?;
            let paused_step = RewriteReplayStep {
                source_sha: paused_source_sha,
                empty_policy: *empty_policy,
            };
            let mut remaining_operations = Vec::new();
            if let Some(remaining_range) = CherryPickOp::from_commits_with_empty_policy(
                &commits[paused_index + 1..],
                *empty_policy,
            ) {
                remaining_operations.push(remaining_range);
            }
            remaining_operations.extend_from_slice(later_operations);
            Ok((paused_step, remaining_operations))
        }
    }
}

fn validate_manual_continue_commit(state: &RewriteResumeState) -> Result<()> {
    let advanced_commits =
        advanced_commits_since_paused_head(&state.temp_worktree_path, &state.paused_head)?;
    if advanced_commits.len() > 1 {
        bail!(
            "temp worktree {} advanced by {} commits beyond paused HEAD {}; only one accidental manual continue is supported",
            state.temp_worktree_path,
            advanced_commits.len(),
            state.paused_head
        );
    } else if let Some(actual_sha) = advanced_commits.first() {
        validate_replayed_commit_matches_step(
            &state.temp_worktree_path,
            actual_sha,
            &state.paused_step,
            &state.paused_head,
        )
    } else {
        bail!(
            "temp worktree {} did not advance any validated replay steps beyond paused HEAD {}",
            state.temp_worktree_path,
            state.paused_head
        )
    }
}

fn commit_identity(sha: &str) -> Result<CommitIdentity> {
    commit_identity_from_args(
        ["show", "-s", "--format=%P%x00%an%x00%ae%x00%aI%x00%B", sha].as_slice(),
    )
}

fn commit_identity_at(tmp_path: &str, sha: &str) -> Result<CommitIdentity> {
    commit_identity_from_args(
        [
            "-C",
            tmp_path,
            "show",
            "-s",
            "--format=%P%x00%an%x00%ae%x00%aI%x00%B",
            sha,
        ]
        .as_slice(),
    )
}

fn commit_identity_from_args(args: &[&str]) -> Result<CommitIdentity> {
    let raw = git_ro(args)?;
    let mut parts = raw.splitn(5, '\0');
    let parents_raw = parts
        .next()
        .ok_or_else(|| anyhow!("missing parent list in commit identity"))?;
    let author_name = parts
        .next()
        .ok_or_else(|| anyhow!("missing author name in commit identity"))?
        .to_string();
    let author_email = parts
        .next()
        .ok_or_else(|| anyhow!("missing author email in commit identity"))?
        .to_string();
    let author_date = parts
        .next()
        .ok_or_else(|| anyhow!("missing author date in commit identity"))?
        .to_string();
    let message = parts
        .next()
        .ok_or_else(|| anyhow!("missing commit message in commit identity"))?
        .to_string();
    let parents = parents_raw
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect();
    Ok(CommitIdentity {
        parents,
        message,
        author_name,
        author_email,
        author_date,
    })
}

fn validate_replayed_commit_matches_step(
    tmp_path: &str,
    actual_sha: &str,
    step: &RewriteReplayStep,
    expected_parent: &str,
) -> Result<()> {
    let actual = commit_identity_at(tmp_path, actual_sha)?;
    let expected = commit_identity(&step.source_sha)?;
    if actual.parents.len() != 1 {
        bail!(
            "temp worktree {} advanced to {}, but that commit has {} parents instead of the expected one-parent cherry-pick result for {}",
            tmp_path,
            actual_sha,
            actual.parents.len(),
            step.source_sha
        );
    } else if actual.parents[0] != expected_parent {
        bail!(
            "temp worktree {} advanced to {}, but its parent {} does not match the expected replay parent {} for {}",
            tmp_path,
            actual_sha,
            actual.parents[0],
            expected_parent,
            step.source_sha
        );
    } else if actual.message != expected.message {
        bail!(
            "temp worktree {} advanced to {}, but its commit message no longer matches the paused source {}",
            tmp_path,
            actual_sha,
            step.source_sha
        );
    } else if actual.author_name != expected.author_name
        || actual.author_email != expected.author_email
        || actual.author_date != expected.author_date
    {
        bail!(
            "temp worktree {} advanced to {}, but its author metadata no longer matches the paused source {}",
            tmp_path,
            actual_sha,
            step.source_sha
        );
    } else {
        Ok(())
    }
}

fn trim_sequencer_todo_to_current_step(tmp_path: &str) -> Result<()> {
    let todo_path = git_dir_at(tmp_path)?.join("sequencer").join("todo");
    let todo = fs::read_to_string(&todo_path)
        .with_context(|| format!("failed to read sequencer todo {}", todo_path.display()))?;
    let trimmed = trim_todo_to_first_action(&todo)?;
    fs::write(&todo_path, trimmed)
        .with_context(|| format!("failed to rewrite sequencer todo {}", todo_path.display()))
}

fn trim_todo_to_first_action(todo: &str) -> Result<String> {
    let mut kept_lines = Vec::new();
    let mut kept_action = false;
    for line in todo.lines() {
        if kept_action {
            continue;
        }
        kept_lines.push(line.to_string());
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            kept_action = true;
        }
    }
    if !kept_action {
        bail!("sequencer todo did not contain a cherry-pick action to keep");
    } else {
        let mut trimmed = kept_lines.join("\n");
        if todo.ends_with('\n') {
            trimmed.push('\n');
        }
        Ok(trimmed)
    }
}

fn git_dir_at(tmp_path: &str) -> Result<PathBuf> {
    let raw = git_ro(["-C", tmp_path, "rev-parse", "--git-dir"].as_slice())?;
    let path = PathBuf::from(raw.trim());
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(Path::new(tmp_path).join(path))
    }
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
    let current_common_dir = git_common_dir()?;
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

fn cherry_pick_head_at(tmp_path: &str) -> Result<String> {
    Ok(
        git_ro(["-C", tmp_path, "rev-parse", "--verify", "CHERRY_PICK_HEAD"].as_slice())?
            .trim()
            .to_string(),
    )
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

fn conflicted_paths_from_status_lines(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter(|line| status_line_has_conflict(line))
        .filter_map(|line| line.get(3..).map(str::trim))
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .collect()
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

fn advanced_commits_since_paused_head(tmp_path: &str, paused_head: &str) -> Result<Vec<String>> {
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
    Ok(git_ro(
        [
            "-C",
            tmp_path,
            "rev-list",
            "--reverse",
            &format!("{paused_head}..{current_head}"),
        ]
        .as_slice(),
    )?
    .lines()
    .map(str::trim)
    .filter(|line| !line.is_empty())
    .map(ToOwned::to_owned)
    .collect())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use tempfile::TempDir;

    use super::{
        conflicted_paths_from_status_lines, default_resume_path,
        prepare_resume_path_for_new_session, resume_rewrite, run_rewrite_session,
        sanitize_branch_for_filename, suspend_instruction_lines, RewriteCommandKind,
        RewriteCommandOutcome, RewriteConflictPolicy, RewriteReplayStep, RewriteResumeState,
        RewriteSession, REWRITE_RESUME_SCHEMA_VERSION,
    };
    use crate::commands::common::{
        CherryPickEmptyPolicy, CherryPickOp, DeferredDirtyWorktreeRestore,
    };
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

    fn git_dir(repo: &Path) -> PathBuf {
        let raw = git(repo, ["rev-parse", "--git-dir"].as_slice());
        let path = PathBuf::from(raw.trim());
        if path.is_absolute() {
            path
        } else {
            repo.join(path)
        }
    }

    fn sequencer_todo(repo: &Path) -> String {
        fs::read_to_string(git_dir(repo).join("sequencer").join("todo")).expect("read todo")
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
        let resume_path = prepare_resume_path_for_new_session(
            false,
            RewriteCommandKind::Restack,
            "stack",
            &original_head,
        )
        .expect("prepare resume path");

        let session = RewriteSession {
            command_kind: RewriteCommandKind::Restack,
            conflict_policy: RewriteConflictPolicy::Suspend,
            original_worktree_root: repo.display().to_string(),
            original_branch: "stack".to_string(),
            original_head: original_head.clone(),
            resume_path,
            temp_branch: tmp_branch.clone(),
            temp_worktree_path: tmp_path.clone(),
            backup_tag: None,
            operations: vec![CherryPickOp::Commit {
                sha: original_head.clone(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }],
            deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore::Noop,
            post_success_hint: None,
            metadata_refresh_context: None,
        };

        let outcome = run_rewrite_session(false, session).expect("run rewrite session");
        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended rewrite"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };

        (dir, repo, resume_path)
    }

    fn suspended_range_session_repo() -> (TempDir, PathBuf, PathBuf, String, String, String) {
        let dir = init_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        let first = commit_file(&repo, "story.txt", "stack-1\n", "feat: first");
        let second = commit_file(&repo, "extra.txt", "stack-2\n", "feat: second");
        let third = commit_file(&repo, "more.txt", "stack-3\n", "feat: third");
        let original_head = third.clone();

        git(&repo, ["checkout", "main"].as_slice());
        let base_head = commit_file(&repo, "story.txt", "base-updated\n", "feat: base update");
        git(&repo, ["checkout", "stack"].as_slice());
        let short = git(&repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();
        let (tmp_path, tmp_branch) =
            crate::commands::common::create_temp_worktree(false, "restack", &base_head, &short)
                .expect("create temp worktree");
        let resume_path = prepare_resume_path_for_new_session(
            false,
            RewriteCommandKind::Restack,
            "stack",
            &original_head,
        )
        .expect("prepare resume path");

        let outcome = run_rewrite_session(
            false,
            RewriteSession {
                command_kind: RewriteCommandKind::Restack,
                conflict_policy: RewriteConflictPolicy::Suspend,
                original_worktree_root: repo.display().to_string(),
                original_branch: "stack".to_string(),
                original_head,
                resume_path: resume_path.clone(),
                temp_branch: tmp_branch,
                temp_worktree_path: tmp_path,
                backup_tag: None,
                operations: vec![CherryPickOp::Range {
                    first: first.clone(),
                    last: third.clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                }],
                deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore::Noop,
                post_success_hint: None,
                metadata_refresh_context: None,
            },
        )
        .expect("run range rewrite session");

        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended rewrite"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };

        (dir, repo, resume_path, first, second, third)
    }

    fn suspended_repeated_range_conflict_repo() -> (
        TempDir,
        PathBuf,
        PathBuf,
        String,
        String,
        String,
        String,
        String,
    ) {
        let dir = init_conflict_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        let first = commit_file(&repo, "story.txt", "stack-1\n", "feat: first");
        let second = commit_file(&repo, "extra.txt", "stack-2\n", "feat: second");
        let third = commit_file(&repo, "notes.txt", "stack-notes\n", "feat: third");
        let fourth = commit_file(&repo, "tail-a.txt", "tail-a\n", "feat: fourth");
        let fifth = commit_file(&repo, "tail-b.txt", "tail-b\n", "feat: fifth");
        let original_head = fifth.clone();

        git(&repo, ["checkout", "main"].as_slice());
        fs::write(repo.join("story.txt"), "base-updated\n").expect("write base story");
        fs::write(repo.join("notes.txt"), "base-notes\n").expect("write base notes");
        git(&repo, ["add", "story.txt", "notes.txt"].as_slice());
        git(&repo, ["commit", "-m", "feat: base update"].as_slice());
        let base_head = git(&repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();
        git(&repo, ["checkout", "stack"].as_slice());
        let short = git(&repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();
        let (tmp_path, tmp_branch) =
            crate::commands::common::create_temp_worktree(false, "restack", &base_head, &short)
                .expect("create temp worktree");
        let resume_path = prepare_resume_path_for_new_session(
            false,
            RewriteCommandKind::Restack,
            "stack",
            &original_head,
        )
        .expect("prepare resume path");

        let outcome = run_rewrite_session(
            false,
            RewriteSession {
                command_kind: RewriteCommandKind::Restack,
                conflict_policy: RewriteConflictPolicy::Suspend,
                original_worktree_root: repo.display().to_string(),
                original_branch: "stack".to_string(),
                original_head,
                resume_path: resume_path.clone(),
                temp_branch: tmp_branch,
                temp_worktree_path: tmp_path,
                backup_tag: None,
                operations: vec![CherryPickOp::Range {
                    first: first.clone(),
                    last: fifth.clone(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                }],
                deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore::Noop,
                post_success_hint: None,
                metadata_refresh_context: None,
            },
        )
        .expect("run repeated range rewrite session");

        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended rewrite"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };

        (dir, repo, resume_path, first, second, third, fourth, fifth)
    }

    #[test]
    fn suspend_trims_range_todo_and_persists_range_suffix() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path, first, second, third) = suspended_range_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        let todo = sequencer_todo(temp_repo);

        assert!(
            todo.contains("feat: first")
                && !todo.contains("feat: second")
                && !todo.contains("feat: third"),
            "expected the suspended sequencer todo to keep only the current pick: {todo}"
        );
        assert_eq!(
            resume_state.paused_step,
            RewriteReplayStep {
                source_sha: first,
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }
        );
        assert_eq!(
            resume_state.remaining_operations,
            vec![CherryPickOp::Range {
                first: second,
                last: third,
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }]
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
        let (dir, repo, resume_path, _first, _second, _third) = suspended_range_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        resolve_keep_both(temp_repo, "base-updated\nstack-1\n");
        git(temp_repo, ["cherry-pick", "--continue"].as_slice());
        assert!(
            !temp_repo.join("extra.txt").exists() && !temp_repo.join("more.txt").exists(),
            "manual continue should finish only the paused commit before spr resume relaunches the suffix"
        );

        let outcome = resume_rewrite(false, &resume_path).expect("resume after manual continue");
        assert_eq!(outcome, RewriteCommandOutcome::Completed);
        assert!(
            !resume_path.exists(),
            "resume file should be removed after the resumed replay finishes"
        );
        assert_eq!(
            fs::read_to_string(repo.join("story.txt")).expect("read final story"),
            "base-updated\nstack-1\n"
        );
        assert_eq!(
            fs::read_to_string(repo.join("extra.txt")).expect("read final extra"),
            "stack-2\n"
        );
        assert_eq!(
            fs::read_to_string(repo.join("more.txt")).expect("read final more"),
            "stack-3\n"
        );
    }

    #[test]
    fn resume_rewrite_rejects_unrelated_single_manual_commit() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        git(temp_repo, ["cherry-pick", "--abort"].as_slice());
        commit_file(
            temp_repo,
            "manual.txt",
            "manual unrelated\n",
            "manual unrelated commit",
        );

        let err = resume_rewrite(false, &resume_path)
            .expect_err("unrelated one-commit manual history should fail");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("no longer matches the paused source")
                || err_text.contains("does not match the paused HEAD"),
            "unexpected error: {err_text}"
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
        let (dir, repo, resume_path, _first, _second, _third) = suspended_range_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let temp_repo = Path::new(&resume_state.temp_worktree_path);
        resolve_keep_both(temp_repo, "base-updated\nstack-1\n");
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
    fn second_conflict_resuspends_with_new_suffix_range() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path, _first, _second, third, fourth, fifth) =
            suspended_repeated_range_conflict_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let first_resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        resolve_keep_both(
            Path::new(&first_resume_state.temp_worktree_path),
            "base-updated\nstack-1\n",
        );

        let resumed = resume_rewrite(false, &resume_path).expect("resume into second conflict");
        let second_resume_path = match resumed {
            RewriteCommandOutcome::Completed => panic!("expected second suspension"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };
        let second_resume_state: RewriteResumeState = serde_json::from_str(
            &fs::read_to_string(&second_resume_path).expect("read second resume state"),
        )
        .expect("parse second resume state");
        let todo = sequencer_todo(Path::new(&second_resume_state.temp_worktree_path));

        assert!(
            todo.contains("feat: third")
                && !todo.contains("feat: fourth")
                && !todo.contains("feat: fifth"),
            "expected the second suspended todo to keep only the current conflicted pick: {todo}"
        );
        assert_eq!(second_resume_state.paused_step.source_sha, third);
        assert_eq!(
            second_resume_state.remaining_operations,
            vec![CherryPickOp::Range {
                first: fourth,
                last: fifth,
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            }]
        );
    }

    #[test]
    fn prepare_resume_path_for_new_session_rejects_active_session() {
        let _lock = lock_cwd();
        let (dir, repo, resume_path) = suspended_session_repo();
        let _keep_dir_alive = dir.path();
        let _guard = DirGuard::change_to(&repo);

        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        let err = prepare_resume_path_for_new_session(
            false,
            RewriteCommandKind::Restack,
            &resume_state.original_branch,
            &resume_state.original_head,
        )
        .expect_err("active suspended session should block replacement");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("active suspended")
                || err_text.contains("discard that session first"),
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
            paused_step: RewriteReplayStep {
                source_sha: "abcdef0123456789".to_string(),
                empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
            },
            remaining_operations: vec![],
            deferred_dirty_worktree_restore: DeferredDirtyWorktreeRestore::Noop,
            post_success_hint: None,
            metadata_refresh_context: None,
        };

        let lines =
            suspend_instruction_lines(Path::new("/tmp/repo/.git/spr/resume/state.json"), &state);

        assert!(
            lines.contains(&"Original branch: stack".to_string()),
            "expected original branch in suspend output: {lines:#?}"
        );
    }

    #[test]
    fn conflicted_paths_from_status_lines_filters_conflicts() {
        let lines = vec![
            "UU story.txt".to_string(),
            " M untouched.txt".to_string(),
            "AA both-added.txt".to_string(),
        ];

        assert_eq!(
            conflicted_paths_from_status_lines(&lines),
            vec!["story.txt".to_string(), "both-added.txt".to_string()]
        );
    }
}
