//! Machine-readable command results for resumable rewrite workflows.
//!
//! This module defines the `--json` contract used by rewrite-style commands
//! and `spr land` when a follow-on restack can suspend. The contract is
//! intentionally separate from the human-oriented tracing output: in JSON mode
//! callers should read stdout only and avoid scraping stderr.

use serde::Serialize;

use crate::commands::{RewriteCommandKind, RewriteSuspendedState};

pub const MACHINE_OUTPUT_SCHEMA_VERSION: u32 = 1;
pub const EXIT_SUCCESS: i32 = 0;
pub const EXIT_FAILURE: i32 = 1;
pub const EXIT_SUSPENDED: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MachineCommand {
    Restack,
    Absorb,
    Move,
    FixPr,
    Resume,
    Land,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MachineRewriteCommandKind {
    Restack,
    Absorb,
    Move,
    FixPr,
}

impl From<RewriteCommandKind> for MachineRewriteCommandKind {
    fn from(value: RewriteCommandKind) -> Self {
        match value {
            RewriteCommandKind::Restack => Self::Restack,
            RewriteCommandKind::Absorb => Self::Absorb,
            RewriteCommandKind::Move => Self::Move,
            RewriteCommandKind::FixPr => Self::FixPr,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MachineOutput {
    pub schema_version: u32,
    #[serde(flatten)]
    pub payload: MachinePayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MachineSuspendedPayload {
    pub command: MachineCommand,
    pub rewrite_command_kind: MachineRewriteCommandKind,
    pub original_worktree_root: String,
    pub original_branch: String,
    pub temp_branch: String,
    pub temp_worktree: String,
    pub resume_file: String,
    pub resume_argv: Vec<String>,
    pub paused_source_sha: String,
    pub conflicted_paths: Vec<String>,
    pub post_success_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum MachinePayload {
    Completed {
        command: MachineCommand,
    },
    Suspended {
        #[serde(flatten)]
        details: Box<MachineSuspendedPayload>,
    },
    Error {
        command: MachineCommand,
        message: String,
    },
}

impl MachineOutput {
    pub fn completed(command: MachineCommand) -> Self {
        Self {
            schema_version: MACHINE_OUTPUT_SCHEMA_VERSION,
            payload: MachinePayload::Completed { command },
        }
    }

    pub fn suspended(
        command: MachineCommand,
        suspended: RewriteSuspendedState,
        post_success_hint: Option<String>,
    ) -> Self {
        let resume_file = suspended.resume_path.display().to_string();
        let post_success_hint = if let Some(post_success_hint) = post_success_hint {
            Some(post_success_hint)
        } else {
            suspended.post_success_hint.clone()
        };
        Self {
            schema_version: MACHINE_OUTPUT_SCHEMA_VERSION,
            payload: MachinePayload::Suspended {
                details: Box::new(MachineSuspendedPayload {
                    command,
                    rewrite_command_kind: suspended.command_kind.into(),
                    original_worktree_root: suspended.original_worktree_root,
                    original_branch: suspended.original_branch,
                    temp_branch: suspended.temp_branch,
                    temp_worktree: suspended.temp_worktree_path,
                    resume_file: resume_file.clone(),
                    resume_argv: vec!["spr".to_string(), "resume".to_string(), resume_file],
                    paused_source_sha: suspended.paused_source_sha,
                    conflicted_paths: suspended.conflicted_paths,
                    post_success_hint,
                }),
            },
        }
    }

    pub fn error(command: MachineCommand, message: String) -> Self {
        Self {
            schema_version: MACHINE_OUTPUT_SCHEMA_VERSION,
            payload: MachinePayload::Error { command, message },
        }
    }

    pub fn exit_code(&self) -> i32 {
        if matches!(self.payload, MachinePayload::Suspended { .. }) {
            EXIT_SUSPENDED
        } else if matches!(self.payload, MachinePayload::Error { .. }) {
            EXIT_FAILURE
        } else {
            EXIT_SUCCESS
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        MachineCommand, MachineOutput, MachinePayload, MachineRewriteCommandKind,
        MachineSuspendedPayload,
    };
    use crate::commands::{RewriteCommandKind, RewriteSuspendedState};

    #[test]
    fn suspended_output_carries_resume_contract() {
        let output = MachineOutput::suspended(
            MachineCommand::Restack,
            RewriteSuspendedState {
                command_kind: RewriteCommandKind::Restack,
                original_worktree_root: "/tmp/repo".to_string(),
                original_branch: "stack".to_string(),
                temp_branch: "spr/tmp-restack-abc1234".to_string(),
                temp_worktree_path: "/tmp/spr-restack-abc1234".to_string(),
                resume_path: PathBuf::from("/tmp/repo/.git/spr/resume/restack-stack-abc1234.json"),
                paused_source_sha: "abc1234".to_string(),
                conflicted_paths: vec!["story.txt".to_string()],
                post_success_hint: None,
            },
            None,
        );

        match output.payload {
            MachinePayload::Suspended { details } => {
                let MachineSuspendedPayload {
                    command,
                    rewrite_command_kind,
                    resume_argv,
                    conflicted_paths,
                    ..
                } = *details;
                assert_eq!(command, MachineCommand::Restack);
                assert_eq!(rewrite_command_kind, MachineRewriteCommandKind::Restack);
                assert_eq!(
                    resume_argv,
                    vec![
                        "spr".to_string(),
                        "resume".to_string(),
                        "/tmp/repo/.git/spr/resume/restack-stack-abc1234.json".to_string()
                    ]
                );
                assert_eq!(conflicted_paths, vec!["story.txt".to_string()]);
            }
            other => panic!("unexpected machine payload: {:?}", other),
        }
    }
}
