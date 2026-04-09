use serde::Serialize;
use std::ffi::{OsStr, OsString};

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
    Status,
    Update,
    Prep,
    RelinkPrs,
    Cleanup,
}

pub fn command_for_raw_args(args: &[OsString]) -> JsonCommand {
    let mut skip_value = false;
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
                return JsonCommand::List;
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
                return JsonCommand::Cli;
            }
        }
    }
    JsonCommand::Cli
}

pub fn raw_args_request_json(args: &[OsString]) -> bool {
    args.iter()
        .skip(1)
        .any(|arg| arg.as_os_str() == OsStr::new("--json"))
}
