use anyhow::{anyhow, Result};
use clap::{Arg, Command, CommandFactory};
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
    Help,
    Version,
    Restack,
    AdoptPrefix,
    DropMergedPrefix,
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
    SyncLocalBranches,
    Update,
    Prep,
    RelinkPrs,
    Cleanup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of scanning raw argv for a global JSON output request before Clap parsing.
///
/// `clap_args` preserves argv order except for exact `--json` tokens before the first literal
/// `--`. Tokens after `--` are passthrough payload and remain untouched.
pub struct JsonArgScan {
    pub requested: bool,
    pub clap_args: Vec<OsString>,
}

/// Non-operational JSON result kinds for display-only CLI output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplayResult {
    Help,
    Version,
}

/// Structured JSON representation of Clap help for a resolved command path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelpOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: DisplayResult,
    pub data: HelpData,
}

/// Agent-oriented command metadata plus Clap's rendered human help text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelpData {
    pub command_path: Vec<String>,
    pub name: String,
    pub about: Option<String>,
    pub long_about: Option<String>,
    pub usage: String,
    pub aliases: Vec<String>,
    pub options: Vec<HelpOption>,
    pub positionals: Vec<HelpPositional>,
    pub subcommands: Vec<HelpSubcommand>,
    pub rendered_text: String,
}

/// Structured metadata for an option accepted by a command or inherited globally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelpOption {
    pub long: Option<String>,
    pub short: Option<char>,
    pub value_name: Option<String>,
    pub required: bool,
    pub global: bool,
    pub help: Option<String>,
    pub default_values: Vec<String>,
    pub possible_values: Vec<String>,
}

/// Structured metadata for a positional argument accepted by a command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelpPositional {
    pub name: String,
    pub value_name: Option<String>,
    pub required: bool,
    pub help: Option<String>,
    pub possible_values: Vec<String>,
}

/// Structured metadata for a visible subcommand of the resolved help target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelpSubcommand {
    pub name: String,
    pub aliases: Vec<String>,
    pub about: Option<String>,
    pub has_subcommands: bool,
}

/// Structured JSON representation of binary version output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VersionOutput {
    pub schema_version: u32,
    pub command: JsonCommand,
    pub result: DisplayResult,
    pub data: VersionData,
}

/// Package identity reported by JSON version output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VersionData {
    pub name: String,
    pub version: String,
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
        collision: &crate::branch_names::GroupBranchNameCollision,
    ) -> Self {
        Self::new(
            command,
            JsonError::SyntheticBranchNameCollision {
                conflicting_groups: vec![
                    CollisionGroup {
                        stable_handle: collision.first.selector.clone(),
                        head_branch: collision.first.head_branch.clone(),
                    },
                    CollisionGroup {
                        stable_handle: collision.second.selector.clone(),
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

impl HelpOutput {
    pub fn exit_code(&self) -> i32 {
        EXIT_SUCCESS
    }
}

impl VersionOutput {
    pub fn exit_code(&self) -> i32 {
        EXIT_SUCCESS
    }
}

/// Detect and remove global `--json` tokens before Clap handles display exits or parse errors.
pub fn scan_json_output_request(args: Vec<OsString>) -> JsonArgScan {
    struct ScanState {
        requested: bool,
        clap_args: Vec<OsString>,
        passthrough: bool,
    }

    let state = args.into_iter().fold(
        ScanState {
            requested: false,
            clap_args: Vec::new(),
            passthrough: false,
        },
        |state, arg| {
            let ScanState {
                requested,
                clap_args,
                passthrough,
            } = state;
            if passthrough {
                ScanState {
                    requested,
                    clap_args: clap_args.into_iter().chain(std::iter::once(arg)).collect(),
                    passthrough,
                }
            } else if arg.as_os_str() == OsStr::new("--json") {
                ScanState {
                    requested: true,
                    clap_args,
                    passthrough,
                }
            } else if arg.as_os_str() == OsStr::new("--") {
                ScanState {
                    requested,
                    clap_args: clap_args.into_iter().chain(std::iter::once(arg)).collect(),
                    passthrough: true,
                }
            } else {
                ScanState {
                    requested,
                    clap_args: clap_args.into_iter().chain(std::iter::once(arg)).collect(),
                    passthrough,
                }
            }
        },
    );

    JsonArgScan {
        requested: state.requested,
        clap_args: state.clap_args,
    }
}

pub fn command_for_raw_args(args: &[OsString]) -> JsonCommand {
    let mut skip_value = false;
    let mut saw_list = false;
    for arg in args.iter().skip(1) {
        if skip_value {
            skip_value = false;
        } else if let Some(arg) = arg.to_str() {
            if arg == "--" {
                return if saw_list {
                    JsonCommand::List
                } else {
                    JsonCommand::Cli
                };
            } else if arg == "--json" {
                continue;
            } else if arg == "--version" || arg == "-V" {
                return JsonCommand::Version;
            } else if arg == "--help" || arg == "-h" || arg == "help" {
                return JsonCommand::Help;
            } else if arg == "--cd"
                || arg == "--base"
                || arg == "--prefix"
                || arg == "--local-pr-branches"
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
            } else if arg == "adopt-prefix" {
                return JsonCommand::AdoptPrefix;
            } else if arg == "drop-merged-prefix" {
                return JsonCommand::DropMergedPrefix;
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
            } else if arg == "sync-local-branches" {
                return JsonCommand::SyncLocalBranches;
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

/// Build structured help output for either `spr help <path>` or `<path> --help`.
pub fn help_output_for_args(args: &[OsString]) -> Result<HelpOutput> {
    let scan = scan_json_output_request(args.to_vec());
    let tokens = help_command_tokens(&scan.clap_args);
    let root = crate::cli::Cli::command();
    let global_options = root
        .get_arguments()
        .filter(|arg| arg.is_global_set() && !arg.is_hide_set())
        .map(help_option)
        .collect();
    let (command_path, command) = resolve_command_path(root, &tokens)?;
    Ok(HelpOutput {
        schema_version: JSON_OUTPUT_SCHEMA_VERSION,
        command: JsonCommand::Help,
        result: DisplayResult::Help,
        data: help_data(command_path, command, global_options),
    })
}

/// Build structured version output from Cargo package metadata.
pub fn version_output() -> VersionOutput {
    VersionOutput {
        schema_version: JSON_OUTPUT_SCHEMA_VERSION,
        command: JsonCommand::Version,
        result: DisplayResult::Version,
        data: VersionData {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
    }
}

fn help_command_tokens(args: &[OsString]) -> Vec<String> {
    let mut command = crate::cli::Cli::command();
    let mut tokens = Vec::new();
    let mut skip_value = false;
    let mut help_subcommand = false;
    for arg in args.iter().skip(1) {
        if arg.as_os_str() == OsStr::new("--") {
            break;
        } else if skip_value {
            skip_value = false;
        } else if let Some(arg) = arg.to_str() {
            if help_subcommand {
                tokens.push(arg.to_string());
            } else if arg == "help" {
                tokens.clear();
                help_subcommand = true;
            } else if arg == "--help" || arg == "-h" {
                break;
            } else if let Some(subcommand) = matching_subcommand(&command, arg) {
                tokens.push(subcommand.get_name().to_string());
                command = subcommand.clone();
            } else if option_consumes_value(&command, arg) {
                skip_value = true;
            }
        }
    }
    tokens
}

fn resolve_command_path(root: Command, tokens: &[String]) -> Result<(Vec<String>, Command)> {
    tokens.iter().try_fold(
        (vec![root.get_name().to_string()], root),
        |(command_path, command), token| {
            if let Some(subcommand) = matching_subcommand(&command, token) {
                let next_command = subcommand.clone();
                let next_path = command_path
                    .into_iter()
                    .chain(std::iter::once(next_command.get_name().to_string()))
                    .collect();
                Ok((next_path, next_command))
            } else {
                Err(anyhow!("unknown help command path component `{token}`"))
            }
        },
    )
}

fn matching_subcommand<'a>(command: &'a Command, token: &str) -> Option<&'a Command> {
    command.get_subcommands().find(|subcommand| {
        subcommand.get_name() == token || subcommand.get_all_aliases().any(|alias| alias == token)
    })
}

fn option_consumes_value(command: &Command, token: &str) -> bool {
    if let Some(long) = token.strip_prefix("--") {
        if token.contains('=') {
            false
        } else {
            let name = long.split('=').next().unwrap_or(long);
            command
                .get_arguments()
                .find(|arg| arg.get_long() == Some(name))
                .map(arg_takes_value)
                .unwrap_or(false)
        }
    } else if let Some(short) = token.strip_prefix('-') {
        if short.chars().count() == 1 {
            let short = short.chars().next().unwrap();
            command
                .get_arguments()
                .find(|arg| arg.get_short() == Some(short))
                .map(arg_takes_value)
                .unwrap_or(false)
        } else {
            false
        }
    } else {
        false
    }
}

fn arg_takes_value(arg: &Arg) -> bool {
    arg.get_num_args().is_some() || arg.get_value_names().is_some()
}

fn help_data(
    command_path: Vec<String>,
    command: Command,
    inherited_options: Vec<HelpOption>,
) -> HelpData {
    let mut usage_command = command.clone();
    let mut help_command = command.clone();
    let options = if command_path.len() == 1 {
        command
            .get_arguments()
            .filter(|arg| !arg.is_positional() && !arg.is_hide_set())
            .map(help_option)
            .collect()
    } else {
        inherited_options
            .into_iter()
            .chain(
                command
                    .get_arguments()
                    .filter(|arg| !arg.is_positional() && !arg.is_hide_set())
                    .map(help_option),
            )
            .collect()
    };
    HelpData {
        command_path,
        name: command.get_name().to_string(),
        about: command.get_about().map(ToString::to_string),
        long_about: command.get_long_about().map(ToString::to_string),
        usage: usage_command.render_usage().to_string(),
        aliases: command.get_all_aliases().map(ToString::to_string).collect(),
        options,
        positionals: command
            .get_arguments()
            .filter(|arg| arg.is_positional() && !arg.is_hide_set())
            .map(help_positional)
            .collect(),
        subcommands: command
            .get_subcommands()
            .filter(|subcommand| !subcommand.is_hide_set())
            .map(help_subcommand)
            .collect(),
        rendered_text: help_command.render_help().to_string(),
    }
}

fn help_option(arg: &Arg) -> HelpOption {
    HelpOption {
        long: arg.get_long().map(ToString::to_string),
        short: arg.get_short(),
        value_name: value_name(arg),
        required: arg.is_required_set(),
        global: arg.is_global_set(),
        help: arg.get_help().map(ToString::to_string),
        default_values: default_values(arg),
        possible_values: possible_values(arg),
    }
}

fn help_positional(arg: &Arg) -> HelpPositional {
    HelpPositional {
        name: arg.get_id().to_string(),
        value_name: value_name(arg),
        required: arg.is_required_set(),
        help: arg.get_help().map(ToString::to_string),
        possible_values: possible_values(arg),
    }
}

fn help_subcommand(command: &Command) -> HelpSubcommand {
    HelpSubcommand {
        name: command.get_name().to_string(),
        aliases: command.get_all_aliases().map(ToString::to_string).collect(),
        about: command.get_about().map(ToString::to_string),
        has_subcommands: command.get_subcommands().next().is_some(),
    }
}

fn value_name(arg: &Arg) -> Option<String> {
    arg.get_value_names()
        .map(|names| names.iter().map(ToString::to_string).collect::<Vec<_>>())
        .filter(|names| !names.is_empty())
        .map(|names| names.join(" "))
}

fn default_values(arg: &Arg) -> Vec<String> {
    arg.get_default_values()
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect()
}

fn possible_values(arg: &Arg) -> Vec<String> {
    arg.get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        command_for_raw_args, help_output_for_args, scan_json_output_request, version_output,
        DisplayResult, JsonCommand,
    };
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
    fn raw_args_detect_drop_merged_prefix_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("drop-merged-prefix"),
            OsString::from("--json"),
        ];

        assert_eq!(command_for_raw_args(&args), JsonCommand::DropMergedPrefix);
    }

    #[test]
    fn raw_args_detect_adopt_prefix_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("adopt-prefix"),
            OsString::from("--json"),
        ];

        assert_eq!(command_for_raw_args(&args), JsonCommand::AdoptPrefix);
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

        assert!(scan_json_output_request(args).requested);
    }

    #[test]
    fn scan_json_output_request_removes_json_before_passthrough() {
        let scan = scan_json_output_request(vec![
            OsString::from("spr"),
            OsString::from("--json"),
            OsString::from("list"),
            OsString::from("commit"),
            OsString::from("--"),
            OsString::from("--json"),
        ]);

        assert!(scan.requested);
        assert_eq!(
            scan.clap_args,
            vec![
                OsString::from("spr"),
                OsString::from("list"),
                OsString::from("commit"),
                OsString::from("--"),
                OsString::from("--json"),
            ]
        );
    }

    #[test]
    fn scan_json_output_request_is_idempotent_before_passthrough() {
        let scan = scan_json_output_request(vec![
            OsString::from("spr"),
            OsString::from("--json"),
            OsString::from("--json"),
            OsString::from("status"),
        ]);

        assert!(scan.requested);
        assert_eq!(
            scan.clap_args,
            vec![OsString::from("spr"), OsString::from("status")]
        );
    }

    #[test]
    fn raw_args_do_not_detect_json_after_passthrough() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("status"),
            OsString::from("--"),
            OsString::from("--json"),
        ];

        assert!(!scan_json_output_request(args).requested);
    }

    #[test]
    fn command_detection_ignores_json_and_stops_at_passthrough() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("--json"),
            OsString::from("--"),
            OsString::from("status"),
        ];

        assert_eq!(command_for_raw_args(&args), JsonCommand::Cli);
    }

    #[test]
    fn help_output_resolves_help_subcommand_path() {
        let output = help_output_for_args(&[
            OsString::from("spr"),
            OsString::from("--json"),
            OsString::from("help"),
            OsString::from("list"),
            OsString::from("commit"),
        ])
        .unwrap();

        assert_eq!(output.command, JsonCommand::Help);
        assert_eq!(output.result, DisplayResult::Help);
        assert_eq!(output.data.command_path, ["spr", "list", "commit"]);
        assert_eq!(output.data.name, "commit");
        assert!(!output.data.rendered_text.is_empty());
    }

    #[test]
    fn help_output_resolves_trailing_help_path() {
        let output = help_output_for_args(&[
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("commit"),
            OsString::from("--help"),
            OsString::from("--json"),
        ])
        .unwrap();

        assert_eq!(output.data.command_path, ["spr", "list", "commit"]);
        assert!(!output.data.options.is_empty());
    }

    #[test]
    fn version_output_uses_package_metadata() {
        let output = version_output();

        assert_eq!(output.command, JsonCommand::Version);
        assert_eq!(output.result, DisplayResult::Version);
        assert_eq!(output.data.name, "spr");
        assert_eq!(output.data.version, env!("CARGO_PKG_VERSION"));
    }
}
