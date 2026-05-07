//! Machine-readable output for read-only discovery commands.
//!
//! Rewrite-style commands use [`crate::machine_output`] because they need suspension-aware
//! lifecycle states. Read-only discovery commands need schema-versioned snapshots in canonical
//! stack order plus typed error results instead.

use serde::Serialize;

pub const READ_ONLY_OUTPUT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadOnlyCommand {
    Cli,
    ListPr,
    ListCommit,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteMetadataState {
    Complete,
    CiReviewStatusUnavailable,
    NotRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ReadOnlyContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
}

impl ReadOnlyContext {
    fn capture(base: Option<&str>, prefix: Option<&str>) -> Self {
        let repo_root = crate::git::repo_root().ok().flatten();
        let current_branch = if let Some(repo_root) = repo_root.as_deref() {
            crate::stack_metadata::current_branch_or_none(repo_root)
                .ok()
                .flatten()
        } else {
            None
        };
        Self {
            repo_root,
            current_branch,
            base_branch: base.map(str::to_string),
            prefix: prefix.map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadOnlyOutput {
    pub schema_version: u32,
    pub command: ReadOnlyCommand,
    #[serde(flatten)]
    context: ReadOnlyContext,
    #[serde(flatten)]
    pub payload: ReadOnlyPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ReadOnlyPayload {
    Error {
        remote_metadata_state: RemoteMetadataState,
        #[serde(flatten)]
        error: ReadOnlyError,
    },
    PrList {
        #[serde(flatten)]
        data: crate::commands::PrListData,
    },
    CommitList {
        #[serde(flatten)]
        data: crate::commands::CommitListData,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollisionGroup {
    pub stable_handle: String,
    pub head_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "error_kind", rename_all = "snake_case")]
pub enum ReadOnlyError {
    SyntheticBranchNameCollision {
        conflicting_groups: Vec<CollisionGroup>,
    },
    InvalidArguments {
        message: String,
    },
    Internal {
        message: String,
    },
}

impl ReadOnlyOutput {
    fn new(command: ReadOnlyCommand, context: ReadOnlyContext, payload: ReadOnlyPayload) -> Self {
        Self {
            schema_version: READ_ONLY_OUTPUT_SCHEMA_VERSION,
            command,
            context,
            payload,
        }
    }

    pub fn pr_list(
        command: ReadOnlyCommand,
        base: &str,
        prefix: &str,
        data: crate::commands::PrListData,
    ) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(Some(base), Some(prefix)),
            ReadOnlyPayload::PrList { data },
        )
    }

    pub fn commit_list(
        command: ReadOnlyCommand,
        base: &str,
        prefix: &str,
        data: crate::commands::CommitListData,
    ) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(Some(base), Some(prefix)),
            ReadOnlyPayload::CommitList { data },
        )
    }

    pub fn invalid_arguments(command: ReadOnlyCommand, message: String) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(None, None),
            ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::InvalidArguments { message },
            },
        )
    }

    pub fn synthetic_branch_name_collision(
        command: ReadOnlyCommand,
        base: &str,
        prefix: &str,
        collision: &crate::branch_names::SyntheticBranchNameCollision,
    ) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(Some(base), Some(prefix)),
            ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::SyntheticBranchNameCollision {
                    conflicting_groups: vec![
                        CollisionGroup {
                            stable_handle: collision.first.stable_handle.clone(),
                            head_branch: collision.first.head_branch.clone(),
                        },
                        CollisionGroup {
                            stable_handle: collision.second.stable_handle.clone(),
                            head_branch: collision.second.head_branch.clone(),
                        },
                    ],
                },
            },
        )
    }

    pub fn internal(command: ReadOnlyCommand, base: &str, prefix: &str, message: String) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(Some(base), Some(prefix)),
            ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::Internal { message },
            },
        )
    }

    pub fn internal_without_context(command: ReadOnlyCommand, message: String) -> Self {
        Self::new(
            command,
            ReadOnlyContext::capture(None, None),
            ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::Internal { message },
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CollisionGroup, ReadOnlyCommand, ReadOnlyError, ReadOnlyOutput, ReadOnlyPayload,
        READ_ONLY_OUTPUT_SCHEMA_VERSION,
    };
    use crate::commands::{
        CommitEntryData, CommitGroupData, CommitListData, PrGroupData, PrListData, RemotePrMetadata,
    };
    use crate::github::PrState;
    use crate::read_only_output::RemoteMetadataState;

    #[test]
    fn schema_version_is_stable() {
        assert_eq!(READ_ONLY_OUTPUT_SCHEMA_VERSION, 1);
    }

    #[test]
    fn command_serializes_in_kebab_case() {
        let json = serde_json::to_string(&ReadOnlyCommand::ListPr).unwrap();

        assert_eq!(json, "\"list-pr\"");
    }

    #[test]
    fn invalid_arguments_output_uses_typed_error_payload() {
        let output = ReadOnlyOutput::invalid_arguments(
            ReadOnlyCommand::Status,
            "unexpected flag".to_string(),
        );

        assert_eq!(output.schema_version, READ_ONLY_OUTPUT_SCHEMA_VERSION);
        assert_eq!(output.command, ReadOnlyCommand::Status);
        assert_eq!(
            output.payload,
            ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::InvalidArguments {
                    message: "unexpected flag".to_string(),
                },
            }
        );
    }

    #[test]
    fn pr_list_output_flattens_canonical_data() {
        let output = ReadOnlyOutput::pr_list(
            ReadOnlyCommand::ListPr,
            "main",
            "dank-spr/",
            PrListData {
                remote_metadata_state: RemoteMetadataState::Complete,
                groups: vec![PrGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    first_commit_sha: "aaaaaaaa1".to_string(),
                    commit_count: 1,
                    first_subject: "feat: alpha".to_string(),
                    remote: Some(RemotePrMetadata {
                        pr_number: 11,
                        url: "https://github.com/o/r/pull/11".to_string(),
                        base_branch: "main".to_string(),
                        state: PrState::Open,
                        ci_review_status: None,
                    }),
                }],
            },
        );

        let json = serde_json::to_value(&output).unwrap();

        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["command"], "list-pr");
        assert_eq!(json["base_branch"], "main");
        assert_eq!(json["prefix"], "dank-spr/");
        assert_eq!(json["result"], "pr_list");
        assert_eq!(json["remote_metadata_state"], "complete");
        assert_eq!(json["groups"][0]["local_pr_number"], 1);
    }

    #[test]
    fn commit_list_output_flattens_canonical_data() {
        let output = ReadOnlyOutput::commit_list(
            ReadOnlyCommand::ListCommit,
            "main",
            "dank-spr/",
            CommitListData {
                remote_metadata_state: RemoteMetadataState::CiReviewStatusUnavailable,
                groups: vec![CommitGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    remote: None,
                    commits: vec![CommitEntryData {
                        global_commit_index: 1,
                        sha: "aaaaaaaa1".to_string(),
                        subject: "feat: alpha".to_string(),
                    }],
                }],
            },
        );

        let json = serde_json::to_value(&output).unwrap();

        assert_eq!(json["command"], "list-commit");
        assert_eq!(json["result"], "commit_list");
        assert_eq!(
            json["remote_metadata_state"],
            "ci_review_status_unavailable"
        );
        assert_eq!(json["groups"][0]["commits"][0]["global_commit_index"], 1);
    }

    #[test]
    fn synthetic_branch_name_collision_payload_is_typed() {
        let output = ReadOnlyOutput {
            schema_version: READ_ONLY_OUTPUT_SCHEMA_VERSION,
            command: ReadOnlyCommand::ListPr,
            context: super::ReadOnlyContext::capture(Some("main"), Some("dank-spr/")),
            payload: ReadOnlyPayload::Error {
                remote_metadata_state: RemoteMetadataState::NotRequested,
                error: ReadOnlyError::SyntheticBranchNameCollision {
                    conflicting_groups: vec![
                        CollisionGroup {
                            stable_handle: "pr:alpha".to_string(),
                            head_branch: "dank-spr/alpha".to_string(),
                        },
                        CollisionGroup {
                            stable_handle: "pr:Alpha".to_string(),
                            head_branch: "dank-spr/Alpha".to_string(),
                        },
                    ],
                },
            },
        };

        let json = serde_json::to_value(&output).unwrap();

        assert_eq!(json["result"], "error");
        assert_eq!(json["error_kind"], "synthetic_branch_name_collision");
        assert_eq!(json["conflicting_groups"][0]["stable_handle"], "pr:alpha");
        assert_eq!(
            json["conflicting_groups"][1]["head_branch"],
            "dank-spr/Alpha"
        );
    }
}
