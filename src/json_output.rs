use serde::Serialize;
use std::ffi::{OsStr, OsString};

pub const JSON_OUTPUT_SCHEMA_VERSION: u32 = 1;
pub const EXIT_SUCCESS: i32 = 0;
pub const EXIT_FAILURE: i32 = 1;
pub const EXIT_SUSPENDED: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JsonCommand {
    Cli,
    Restack,
    Absorb,
    Move,
    FixPr,
    ResolveStack,
    Resume,
    Land,
    List,
    ListPr,
    ListCommit,
    Status,
    Update,
    Prep,
    RelinkPrs,
    Cleanup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollisionGroup {
    pub stable_handle: String,
    pub head_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    #[serde(flatten)]
    pub payload: ErrorPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ErrorPayload {
    Error {
        #[serde(flatten)]
        error: JsonError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "error_kind", rename_all = "snake_case")]
pub enum JsonError {
    InvalidArguments {
        message: String,
    },
    Internal {
        message: String,
    },
    SyntheticBranchNameCollision {
        conflicting_groups: Vec<CollisionGroup>,
    },
}

impl ErrorOutput {
    fn new(command: JsonCommand, error: JsonError) -> Self {
        Self {
            schema_version: JSON_OUTPUT_SCHEMA_VERSION,
            command,
            payload: ErrorPayload::Error { error },
        }
    }

    pub fn invalid_arguments(command: JsonCommand, message: String) -> Self {
        Self::new(command, JsonError::InvalidArguments { message })
    }

    pub fn internal(command: JsonCommand, message: String) -> Self {
        Self::new(command, JsonError::Internal { message })
    }

    pub fn synthetic_branch_name_collision(
        command: JsonCommand,
        collision: &crate::branch_names::SyntheticBranchNameCollision,
    ) -> Self {
        Self::new(
            command,
            JsonError::SyntheticBranchNameCollision {
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
        )
    }

    pub fn exit_code(&self) -> i32 {
        EXIT_FAILURE
    }
}

pub fn command_for_raw_args(args: &[OsString]) -> JsonCommand {
    let mut skip_value = false;
    let mut saw_list = false;
    for arg in args.iter().skip(1) {
        if skip_value {
            skip_value = false;
        } else if let Some(arg) = arg.to_str() {
            if arg == "--cd"
                || arg == "--base"
                || arg == "--prefix"
                || arg == "--until"
                || arg == "--exact"
                || arg == "-b"
            {
                skip_value = true;
            } else if saw_list && (arg == "pr" || arg == "p") {
                return JsonCommand::ListPr;
            } else if saw_list && (arg == "commit" || arg == "c") {
                return JsonCommand::ListCommit;
            } else if arg == "restack" {
                return JsonCommand::Restack;
            } else if arg == "absorb" {
                return JsonCommand::Absorb;
            } else if arg == "move" || arg == "mv" {
                return JsonCommand::Move;
            } else if arg == "fix-pr" || arg == "fix" {
                return JsonCommand::FixPr;
            } else if arg == "resolve-stack" {
                return JsonCommand::ResolveStack;
            } else if arg == "resume" {
                return JsonCommand::Resume;
            } else if arg == "land" {
                return JsonCommand::Land;
            } else if arg == "list" || arg == "ls" {
                saw_list = true;
            } else if arg == "status" || arg == "stat" {
                return JsonCommand::Status;
            } else if arg == "update" || arg == "u" {
                return JsonCommand::Update;
            } else if arg == "prep" {
                return JsonCommand::Prep;
            } else if arg == "relink-prs" {
                return JsonCommand::RelinkPrs;
            } else if arg == "cleanup" || arg == "clean" {
                return JsonCommand::Cleanup;
            } else if !arg.starts_with('-') {
                if saw_list {
                    return JsonCommand::List;
                }
                return JsonCommand::Cli;
            }
        }
    }
    if saw_list {
        JsonCommand::List
    } else {
        JsonCommand::Cli
    }
}

pub fn raw_args_request_json(args: &[OsString]) -> bool {
    args.iter()
        .skip(1)
        .any(|arg| arg.as_os_str() == OsStr::new("--json"))
}

#[cfg(test)]
mod tests {
    use super::{command_for_raw_args, raw_args_request_json, JsonCommand};
    use std::ffi::OsString;

    #[test]
    fn raw_args_detect_list_leaf_commands() {
        let pr_args = vec![
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("pr"),
            OsString::from("--json"),
        ];
        let commit_args = vec![
            OsString::from("spr"),
            OsString::from("ls"),
            OsString::from("c"),
            OsString::from("--json"),
        ];

        assert_eq!(command_for_raw_args(&pr_args), JsonCommand::ListPr);
        assert_eq!(command_for_raw_args(&commit_args), JsonCommand::ListCommit);
    }

    #[test]
    fn raw_args_detect_incomplete_list_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("--json"),
        ];

        assert_eq!(command_for_raw_args(&args), JsonCommand::List);
    }

    #[test]
    fn raw_args_detect_json_flag() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("status"),
            OsString::from("--json"),
        ];

        assert!(raw_args_request_json(&args));
    }
}
