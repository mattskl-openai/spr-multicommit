use anyhow::{Context, Result};
use clap::{error::ErrorKind, Parser};
use std::ffi::OsString;
use std::path::Path;

mod branch_names;
mod cli;
mod commands;
mod config;
mod format;
mod git;
mod github;
mod json_command;
mod limit;
mod machine_output;
mod parsing;
mod pr_labels;
mod read_only_output;
mod selectors;
mod stack_metadata;
#[cfg(test)]
mod test_support;
mod update_output;

fn resolve_update_pr_limit(
    groups: &[crate::parsing::Group],
    to: Option<crate::selectors::GroupSelector>,
    n: Option<usize>,
    legacy_n: Option<usize>,
) -> Result<crate::limit::Limit> {
    let provided_limit_count =
        usize::from(to.is_some()) + usize::from(n.is_some()) + usize::from(legacy_n.is_some());
    if provided_limit_count > 1 {
        Err(anyhow::anyhow!(
            "`spr update pr` accepts only one limit selector: `--to <N|label|pr:<label>>`, `--n <N>`, or the positional `N`."
        ))
    } else if let Some(to) = to {
        let count = crate::selectors::resolve_group_ordinal(groups, &to)?;
        Ok(crate::limit::Limit::ByPr(count))
    } else if let Some(n) = n {
        if n == 0 {
            Err(anyhow::anyhow!("`spr update pr --n` must be 1 or greater."))
        } else {
            Ok(crate::limit::Limit::ByPr(n))
        }
    } else if let Some(n) = legacy_n {
        if n == 0 {
            Err(anyhow::anyhow!(
                "`spr update pr` positional limit must be 1 or greater."
            ))
        } else {
            Ok(crate::limit::Limit::ByPr(n))
        }
    } else {
        Err(anyhow::anyhow!(
            "`spr update pr` requires either `--to <N|label|pr:<label>>` or `--n <N>`."
        ))
    }
}

fn command_requires_gh(cmd: &crate::cli::Cmd) -> bool {
    match cmd {
        crate::cli::Cmd::Restack { .. }
        | crate::cli::Cmd::Absorb { .. }
        | crate::cli::Cmd::Resume { .. }
        | crate::cli::Cmd::FixPr { .. } => false,
        crate::cli::Cmd::ResolveStack { target, .. } => target
            .as_deref()
            .map(crate::commands::looks_like_pr_url)
            .unwrap_or(false),
        crate::cli::Cmd::Update { no_pr, .. } => !*no_pr,
        crate::cli::Cmd::List { .. }
        | crate::cli::Cmd::Status { .. }
        | crate::cli::Cmd::Prep {}
        | crate::cli::Cmd::Land { .. }
        | crate::cli::Cmd::RelinkPrs {}
        | crate::cli::Cmd::Cleanup {}
        | crate::cli::Cmd::Move { .. } => true,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputMode {
    Human,
    Json,
}

#[derive(Debug)]
enum CommandOutput {
    None,
    Machine(crate::machine_output::MachineOutput),
    ReadOnly(crate::read_only_output::ReadOnlyOutput),
    ResolveStack(crate::commands::ResolveStackOutput),
    Update(crate::update_output::UpdateOutput),
}

fn init_tools(needs_gh: bool) -> Result<()> {
    crate::git::ensure_tool("git")?;
    if needs_gh {
        crate::git::ensure_tool("gh")?;
    }
    Ok(())
}

fn set_dry_run_env(dry_run: bool, assume_existing_prs: bool) {
    if dry_run {
        std::env::set_var("SPR_DRY_RUN", "1");
        if assume_existing_prs {
            std::env::set_var("SPR_DRY_ASSUME_EXISTING", "1");
        }
    }
}

/// Change to the requested working directory before config discovery or repo-scoped commands.
fn apply_working_directory_override(path: Option<&Path>) -> Result<()> {
    if let Some(path) = path {
        std::env::set_current_dir(path)
            .with_context(|| format!("failed to change directory to {}", path.display()))?;
    }
    Ok(())
}

/// Resolve the base branch, branch prefix, and ignore tag with explicit precedence.
///
/// Base resolution follows: CLI `--base` → merged config `base` → discovery
/// via `origin/HEAD`. Unlike other defaults, base discovery is not optional:
/// if `origin/HEAD` cannot be resolved, this returns an error so the failure is
/// visible and actionable to the user.
fn resolve_base_prefix(
    cfg: &crate::config::Config,
    base: Option<String>,
    prefix: Option<String>,
) -> Result<(String, String, String)> {
    let base = match base {
        Some(base) => base,
        None => {
            if cfg.base.trim().is_empty() {
                crate::git::discover_origin_head_base()?
            } else {
                cfg.base.clone()
            }
        }
    };
    let prefix = crate::config::normalize_prefix(&prefix.unwrap_or_else(|| cfg.prefix.clone()))?;
    let mut ignore_tag = cfg.ignore_tag.clone();
    if ignore_tag.trim().is_empty() {
        ignore_tag = "ignore".to_string();
    }
    Ok((base, prefix, ignore_tag))
}

fn ensure_rewrite_completed(
    mode: OutputMode,
    command_name: &str,
    machine_command: crate::machine_output::MachineCommand,
    outcome: crate::commands::RewriteCommandOutcome,
) -> Result<crate::machine_output::MachineOutput> {
    if outcome == crate::commands::RewriteCommandOutcome::Completed {
        Ok(crate::machine_output::MachineOutput::completed(
            machine_command,
        ))
    } else if let crate::commands::RewriteCommandOutcome::Suspended(state) = outcome {
        if mode == OutputMode::Json {
            Ok(crate::machine_output::MachineOutput::suspended(
                machine_command,
                *state,
                None,
            ))
        } else {
            Err(anyhow::anyhow!(
                "{} suspended due to a cherry-pick conflict. Resolve the conflict in the temp worktree, stage the resolution, and run `spr resume {}`.",
                command_name,
                state.resume_path.display()
            ))
        }
    } else {
        Ok(crate::machine_output::MachineOutput::completed(
            machine_command,
        ))
    }
}

fn ensure_resume_completed(
    mode: OutputMode,
    outcome: crate::commands::RewriteCommandOutcome,
) -> Result<crate::machine_output::MachineOutput> {
    if outcome == crate::commands::RewriteCommandOutcome::Completed {
        Ok(crate::machine_output::MachineOutput::completed(
            crate::machine_output::MachineCommand::Resume,
        ))
    } else if let crate::commands::RewriteCommandOutcome::Suspended(state) = outcome {
        if mode == OutputMode::Json {
            Ok(crate::machine_output::MachineOutput::suspended(
                crate::machine_output::MachineCommand::Resume,
                *state,
                None,
            ))
        } else {
            Err(anyhow::anyhow!(
                "`spr resume` hit another cherry-pick conflict. Resolve the next conflict in the temp worktree, stage the resolution, and rerun `spr resume {}`.",
                state.resume_path.display()
            ))
        }
    } else {
        Ok(crate::machine_output::MachineOutput::completed(
            crate::machine_output::MachineCommand::Resume,
        ))
    }
}

fn read_only_pr_list_output(
    command: crate::read_only_output::ReadOnlyCommand,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> crate::read_only_output::ReadOnlyOutput {
    match crate::commands::collect_pr_list_data_for_json(base, prefix, ignore_tag) {
        Ok(data) => crate::read_only_output::ReadOnlyOutput::pr_list(command, base, prefix, data),
        Err(crate::commands::ReadOnlyQueryError::SyntheticBranchNameCollision(collision)) => {
            crate::read_only_output::ReadOnlyOutput::synthetic_branch_name_collision(
                command, base, prefix, &collision,
            )
        }
        Err(crate::commands::ReadOnlyQueryError::Internal(err)) => {
            crate::read_only_output::ReadOnlyOutput::internal(
                command,
                base,
                prefix,
                format!("{err:#}"),
            )
        }
    }
}

fn read_only_commit_list_output(
    command: crate::read_only_output::ReadOnlyCommand,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> crate::read_only_output::ReadOnlyOutput {
    match crate::commands::collect_commit_list_data_for_json(base, prefix, ignore_tag) {
        Ok(data) => {
            crate::read_only_output::ReadOnlyOutput::commit_list(command, base, prefix, data)
        }
        Err(crate::commands::ReadOnlyQueryError::SyntheticBranchNameCollision(collision)) => {
            crate::read_only_output::ReadOnlyOutput::synthetic_branch_name_collision(
                command, base, prefix, &collision,
            )
        }
        Err(crate::commands::ReadOnlyQueryError::Internal(err)) => {
            crate::read_only_output::ReadOnlyOutput::internal(
                command,
                base,
                prefix,
                format!("{err:#}"),
            )
        }
    }
}

fn run_cli(cli: crate::cli::Cli, output_mode: OutputMode) -> Result<CommandOutput> {
    apply_working_directory_override(cli.cd.as_deref())?;
    init_tools(command_requires_gh(&cli.cmd))?;
    if let crate::cli::Cmd::Resume { path, .. } = &cli.cmd {
        return Ok(CommandOutput::Machine(ensure_resume_completed(
            output_mode,
            crate::commands::resume_rewrite(cli.dry_run, path)?,
        )?));
    }

    let cfg = crate::config::load_config()?;
    let (base, prefix, ignore_tag) =
        resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone())?;
    let metadata_refresh_context = crate::stack_metadata::RefreshMetadataContext {
        base: base.clone(),
        prefix: prefix.clone(),
        ignore_tag: ignore_tag.clone(),
    };
    let pr_description_mode = cfg.pr_description_mode;
    let restack_conflict_policy = cfg.restack_conflict;
    let dirty_worktree_policy = cfg.dirty_worktree;
    let list_order = cfg.list_order;
    let branch_reuse_guard_days = cfg.branch_reuse_guard_days;
    match cli.cmd {
        crate::cli::Cmd::Update {
            from,
            no_pr,
            restack,
            assume_existing_prs,
            pr_description_mode: pr_description_mode_override,
            allow_branch_reuse,
            json: _,
            extent,
        } => {
            set_dry_run_env(cli.dry_run, assume_existing_prs);
            let pr_description_mode = pr_description_mode_override.unwrap_or(pr_description_mode);
            if restack {
                Err(anyhow::anyhow!(
                    "`spr update --restack` is deprecated. Use `spr restack --after N` instead."
                ))
            } else {
                let (_merge_base, leading_ignored, all_groups) =
                    crate::parsing::derive_groups_between_with_ignored(&base, &from, &ignore_tag)?;
                if all_groups.is_empty() {
                    return Err(anyhow::anyhow!(
                        "No pr:<tag> markers found between {} and {}. Use `spr restack --after N`.",
                        base,
                        from
                    ));
                }
                let (groups, skipped_handles) =
                    crate::parsing::split_groups_for_update(&leading_ignored, all_groups);
                crate::branch_names::group_branch_identities(&groups, &prefix)?;
                let (limit, resolved_extent) = if let Some(extent) = extent {
                    match extent {
                        crate::cli::Extent::Pr { to, n, legacy_n } => {
                            let limit = resolve_update_pr_limit(&groups, to, n, legacy_n)?;
                            let count = match limit {
                                crate::limit::Limit::ByPr(count) => count,
                                crate::limit::Limit::ByCommits(_) => unreachable!(),
                            };
                            (
                                Some(limit),
                                crate::update_output::ResolvedUpdateLimit::ByPr { count },
                            )
                        }
                        crate::cli::Extent::Commits { n } => (
                            Some(crate::limit::Limit::ByCommits(n)),
                            crate::update_output::ResolvedUpdateLimit::ByCommits { count: n },
                        ),
                    }
                } else {
                    (None, crate::update_output::ResolvedUpdateLimit::All)
                };
                if output_mode == OutputMode::Json {
                    let execution = crate::commands::build_from_groups_with_summary(
                        &base,
                        &prefix,
                        &skipped_handles,
                        no_pr,
                        cli.dry_run,
                        pr_description_mode,
                        limit,
                        groups,
                        list_order,
                        allow_branch_reuse,
                        branch_reuse_guard_days,
                    )?;
                    let summary = crate::update_output::UpdateSummaryData::from_execution(
                        crate::update_output::UpdateRepoContext {
                            base: base.clone(),
                            from,
                            prefix: prefix.clone(),
                            ignore_tag: ignore_tag.clone(),
                        },
                        crate::update_output::UpdateOptions {
                            dry_run: cli.dry_run,
                            no_pr,
                            pr_description_mode,
                        },
                        resolved_extent,
                        execution,
                    );
                    if !cli.dry_run {
                        crate::stack_metadata::refresh_metadata_for_current_checkout(
                            &metadata_refresh_context.base,
                            &metadata_refresh_context.prefix,
                            &metadata_refresh_context.ignore_tag,
                        )?;
                    }
                    Ok(CommandOutput::Update(
                        crate::update_output::UpdateOutput::summary(summary),
                    ))
                } else {
                    crate::commands::build_from_groups(
                        &base,
                        &prefix,
                        &skipped_handles,
                        no_pr,
                        cli.dry_run,
                        pr_description_mode,
                        limit,
                        groups,
                        list_order,
                        allow_branch_reuse,
                        branch_reuse_guard_days,
                    )?;
                    if !cli.dry_run {
                        crate::stack_metadata::refresh_metadata_for_current_checkout(
                            &metadata_refresh_context.base,
                            &metadata_refresh_context.prefix,
                            &metadata_refresh_context.ignore_tag,
                        )?;
                    }
                    Ok(CommandOutput::None)
                }
            }
        }
        crate::cli::Cmd::Restack {
            after,
            safe,
            json: _,
        } => {
            set_dry_run_env(cli.dry_run, false);
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_mode,
                "spr restack",
                crate::machine_output::MachineCommand::Restack,
                crate::commands::restack_after(
                    &metadata_refresh_context,
                    &after,
                    safe,
                    cli.dry_run,
                    restack_conflict_policy,
                    dirty_worktree_policy,
                )?,
            )?))
        }
        crate::cli::Cmd::Absorb {
            allow_replayed_duplicates,
            json: _,
        } => {
            set_dry_run_env(cli.dry_run, false);
            let options = crate::commands::AbsorbOptions {
                copied_later_stack_commit_policy: if allow_replayed_duplicates {
                    crate::commands::CopiedLaterStackCommitPolicy::AllowKeepNonSeedDuplicates
                } else {
                    crate::commands::CopiedLaterStackCommitPolicy::Block
                },
            };
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_mode,
                "spr absorb",
                crate::machine_output::MachineCommand::Absorb,
                crate::commands::absorb_branch_tails(
                    &base,
                    &prefix,
                    &ignore_tag,
                    cli.dry_run,
                    dirty_worktree_policy,
                    options,
                )?,
            )?))
        }
        crate::cli::Cmd::Prep {} => {
            set_dry_run_env(cli.dry_run, false);
            if cli.until.is_some() && cli.exact.is_some() {
                return Err(anyhow::anyhow!("--until conflicts with --exact"));
            }
            let selection = if let Some(n) = cli.until {
                if n == crate::selectors::InclusiveSelector::All {
                    crate::cli::PrepSelection::All
                } else {
                    crate::cli::PrepSelection::Until(n)
                }
            } else if let Some(i) = cli.exact {
                crate::cli::PrepSelection::Exact(i)
            } else {
                crate::cli::PrepSelection::All
            };
            crate::commands::prep_squash(
                &base,
                &prefix,
                &ignore_tag,
                pr_description_mode,
                list_order,
                selection,
                cli.dry_run,
            )?;
            Ok(CommandOutput::None)
        }
        crate::cli::Cmd::List { what } => {
            if output_mode == OutputMode::Json {
                match what {
                    crate::cli::ListWhat::Pr { .. } => {
                        Ok(CommandOutput::ReadOnly(read_only_pr_list_output(
                            crate::read_only_output::ReadOnlyCommand::ListPr,
                            &base,
                            &prefix,
                            &ignore_tag,
                        )))
                    }
                    crate::cli::ListWhat::Commit { .. } => {
                        Ok(CommandOutput::ReadOnly(read_only_commit_list_output(
                            crate::read_only_output::ReadOnlyCommand::ListCommit,
                            &base,
                            &prefix,
                            &ignore_tag,
                        )))
                    }
                }
            } else {
                match what {
                    crate::cli::ListWhat::Pr { .. } => {
                        crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?
                    }
                    crate::cli::ListWhat::Commit { .. } => crate::commands::list_commits_display(
                        &base,
                        &prefix,
                        &ignore_tag,
                        list_order,
                    )?,
                }
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::Status { json: _ } => {
            if output_mode == OutputMode::Json {
                Ok(CommandOutput::ReadOnly(read_only_pr_list_output(
                    crate::read_only_output::ReadOnlyCommand::Status,
                    &base,
                    &prefix,
                    &ignore_tag,
                )))
            } else {
                crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?;
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::ResolveStack { target, json: _ } => Ok(CommandOutput::ResolveStack(
            crate::commands::resolve_stack(target, &ignore_tag)?,
        )),
        crate::cli::Cmd::Resume { .. } => unreachable!("handled before config loading"),
        crate::cli::Cmd::Land {
            which,
            r#unsafe,
            no_restack,
            json: _,
        } => {
            set_dry_run_env(cli.dry_run, false);
            let mode = which.unwrap_or(match cfg.land.as_str() {
                "per-pr" | "perpr" | "per_pr" => crate::cli::LandCmd::PerPr,
                _ => crate::cli::LandCmd::Flatten,
            });
            let until = cli
                .until
                .unwrap_or(crate::selectors::InclusiveSelector::All);
            let landed_count = match mode {
                crate::cli::LandCmd::Flatten => crate::commands::land_flatten_until(
                    &base,
                    &prefix,
                    &ignore_tag,
                    &until,
                    cli.dry_run,
                    r#unsafe,
                )?,
                crate::cli::LandCmd::PerPr => crate::commands::land_per_pr_until(
                    &base,
                    &prefix,
                    &ignore_tag,
                    &until,
                    cli.dry_run,
                    r#unsafe,
                )?,
            };
            if !no_restack {
                // After landing the first N PRs, restack the remaining commits onto the latest base
                let outcome = crate::commands::restack_after_count(
                    &metadata_refresh_context,
                    landed_count,
                    false,
                    cli.dry_run,
                    restack_conflict_policy,
                    dirty_worktree_policy,
                )?;
                if let crate::commands::RewriteCommandOutcome::Suspended(state) = outcome {
                    let post_success_hint = Some(
                        "GitHub landing already succeeded; resolve the local restack conflict and run the printed `spr resume <path>` command instead of rerunning `spr land`."
                            .to_string(),
                    );
                    if output_mode == OutputMode::Json {
                        return Ok(CommandOutput::Machine(
                            crate::machine_output::MachineOutput::suspended(
                                crate::machine_output::MachineCommand::Land,
                                *state,
                                post_success_hint,
                            ),
                        ));
                    } else {
                        return Err(anyhow::anyhow!(
                            "GitHub landing already succeeded, but the follow-on restack suspended due to a cherry-pick conflict. Resolve the conflict in the temp worktree, stage the resolution, and run `spr resume {}` instead of rerunning `spr land`.",
                            state.resume_path.display()
                        ));
                    }
                }
            }
            Ok(CommandOutput::Machine(
                crate::machine_output::MachineOutput::completed(
                    crate::machine_output::MachineCommand::Land,
                ),
            ))
        }
        crate::cli::Cmd::RelinkPrs {} => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::relink_prs(&base, &prefix, &ignore_tag, cli.dry_run)?;
            Ok(CommandOutput::None)
        }
        crate::cli::Cmd::Cleanup {} => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::cleanup_remote_branches(&prefix, cli.dry_run)?;
            Ok(CommandOutput::None)
        }
        crate::cli::Cmd::FixPr {
            target,
            tail,
            safe,
            json: _,
        } => {
            set_dry_run_env(cli.dry_run, false);
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_mode,
                "spr fix-pr",
                crate::machine_output::MachineCommand::FixPr,
                crate::commands::fix_pr_tail(
                    &metadata_refresh_context,
                    &target,
                    tail,
                    safe,
                    cli.dry_run,
                    dirty_worktree_policy,
                )?,
            )?))
        }
        crate::cli::Cmd::Move {
            range,
            after,
            safe,
            json: _,
        } => {
            set_dry_run_env(cli.dry_run, false);
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_mode,
                "spr move",
                crate::machine_output::MachineCommand::Move,
                crate::commands::move_groups_after(
                    &base,
                    &prefix,
                    &ignore_tag,
                    &range,
                    &after,
                    crate::commands::MoveExecutionOptions {
                        safe,
                        dry: cli.dry_run,
                        dirty_worktree_policy,
                    },
                )?,
            )?))
        }
    }
}

fn init_logging(verbose: bool, output_mode: OutputMode) {
    if output_mode == OutputMode::Json {
        return;
    }
    if verbose {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_target(false)
            .with_level(false)
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_target(false)
            .with_level(false)
            .without_time()
            .compact()
            .init();
    }
    if verbose {
        std::env::set_var("SPR_VERBOSE", "1");
    }
}

fn raw_args_request_json(args: &[OsString]) -> bool {
    crate::json_command::raw_args_request_json(args)
}

fn read_only_command_for_cli(
    cmd: &crate::cli::Cmd,
) -> Option<crate::read_only_output::ReadOnlyCommand> {
    match cmd {
        crate::cli::Cmd::List {
            what: crate::cli::ListWhat::Pr { .. },
        } => Some(crate::read_only_output::ReadOnlyCommand::ListPr),
        crate::cli::Cmd::List {
            what: crate::cli::ListWhat::Commit { .. },
        } => Some(crate::read_only_output::ReadOnlyCommand::ListCommit),
        crate::cli::Cmd::Status { .. } => Some(crate::read_only_output::ReadOnlyCommand::Status),
        _ => None,
    }
}

fn read_only_command_for_raw_args(
    args: &[OsString],
) -> Option<crate::read_only_output::ReadOnlyCommand> {
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
                return Some(crate::read_only_output::ReadOnlyCommand::ListPr);
            } else if saw_list && (arg == "commit" || arg == "c") {
                return Some(crate::read_only_output::ReadOnlyCommand::ListCommit);
            } else if arg == "list" || arg == "ls" {
                saw_list = true;
            } else if arg == "status" || arg == "stat" {
                return Some(crate::read_only_output::ReadOnlyCommand::Status);
            } else if !arg.starts_with('-') {
                saw_list = false;
            }
        }
    }
    if saw_list {
        Some(crate::read_only_output::ReadOnlyCommand::Cli)
    } else {
        None
    }
}

fn machine_command_for_raw_args(args: &[OsString]) -> crate::machine_output::MachineCommand {
    crate::json_command::command_for_raw_args(args)
}

#[derive(Debug)]
enum JsonParseFailureOutput {
    Machine(crate::machine_output::MachineOutput),
    ReadOnly(crate::read_only_output::ReadOnlyOutput),
}

fn parse_failure_as_json_output(
    args: &[OsString],
    err: &clap::Error,
) -> Option<JsonParseFailureOutput> {
    let is_display_only = matches!(
        err.kind(),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
    );
    if raw_args_request_json(args) && !is_display_only {
        if let Some(command) = read_only_command_for_raw_args(args) {
            Some(JsonParseFailureOutput::ReadOnly(
                crate::read_only_output::ReadOnlyOutput::invalid_arguments(
                    command,
                    err.to_string(),
                ),
            ))
        } else {
            Some(JsonParseFailureOutput::Machine(
                crate::machine_output::MachineOutput::error(
                    machine_command_for_raw_args(args),
                    err.to_string(),
                ),
            ))
        }
    } else {
        None
    }
}

fn main() {
    let raw_args: Vec<OsString> = std::env::args_os().collect();
    let cli = match crate::cli::Cli::try_parse_from(raw_args.clone()) {
        Ok(cli) => cli,
        Err(err) => {
            if let Some(output) = parse_failure_as_json_output(&raw_args, &err) {
                match output {
                    JsonParseFailureOutput::Machine(output) => {
                        println!("{}", serde_json::to_string(&output).unwrap());
                        std::process::exit(crate::machine_output::EXIT_FAILURE);
                    }
                    JsonParseFailureOutput::ReadOnly(output) => {
                        println!("{}", serde_json::to_string(&output).unwrap());
                        std::process::exit(crate::machine_output::EXIT_FAILURE);
                    }
                }
            } else {
                err.exit();
            }
        }
    };
    let output_mode = if cli.cmd.json_mode() {
        OutputMode::Json
    } else {
        OutputMode::Human
    };
    init_logging(cli.verbose, output_mode);
    let rewrite_command = machine_command_for_cli(&cli.cmd);
    let read_only_command = read_only_command_for_cli(&cli.cmd);
    match run_cli(cli, output_mode) {
        Ok(output) => match output {
            CommandOutput::None => {}
            CommandOutput::Machine(output) => {
                if output_mode == OutputMode::Json {
                    println!("{}", serde_json::to_string(&output).unwrap());
                    std::process::exit(output.exit_code());
                }
            }
            CommandOutput::ReadOnly(output) => {
                if output_mode == OutputMode::Json {
                    println!("{}", serde_json::to_string(&output).unwrap());
                }
            }
            CommandOutput::Update(output) => {
                if output_mode == OutputMode::Json {
                    println!("{}", serde_json::to_string(&output).unwrap());
                    std::process::exit(output.exit_code());
                }
            }
            CommandOutput::ResolveStack(output) => {
                if output_mode == OutputMode::Json {
                    println!("{}", serde_json::to_string(&output).unwrap());
                } else {
                    println!("{}", output.render_human());
                }
            }
        },
        Err(err) => {
            if output_mode == OutputMode::Json {
                if let Some(command) = read_only_command {
                    let output = crate::read_only_output::ReadOnlyOutput::internal_without_context(
                        command,
                        format!("{err:#}"),
                    );
                    println!("{}", serde_json::to_string(&output).unwrap());
                } else {
                    let output = crate::machine_output::MachineOutput::error(
                        rewrite_command,
                        format!("{err:#}"),
                    );
                    println!("{}", serde_json::to_string(&output).unwrap());
                }
            } else {
                eprintln!("Error: {err:#}");
            }
            std::process::exit(crate::machine_output::EXIT_FAILURE);
        }
    }
}

fn machine_command_for_cli(cmd: &crate::cli::Cmd) -> crate::machine_output::MachineCommand {
    match cmd {
        crate::cli::Cmd::Restack { .. } => crate::machine_output::MachineCommand::Restack,
        crate::cli::Cmd::Absorb { .. } => crate::machine_output::MachineCommand::Absorb,
        crate::cli::Cmd::ResolveStack { .. } => crate::machine_output::MachineCommand::ResolveStack,
        crate::cli::Cmd::Resume { .. } => crate::machine_output::MachineCommand::Resume,
        crate::cli::Cmd::Land { .. } => crate::machine_output::MachineCommand::Land,
        crate::cli::Cmd::FixPr { .. } => crate::machine_output::MachineCommand::FixPr,
        crate::cli::Cmd::Move { .. } => crate::machine_output::MachineCommand::Move,
        crate::cli::Cmd::Update { .. } => crate::machine_output::MachineCommand::Update,
        crate::cli::Cmd::Prep {} => crate::machine_output::MachineCommand::Prep,
        crate::cli::Cmd::List { .. } => crate::machine_output::MachineCommand::List,
        crate::cli::Cmd::Status { .. } => crate::machine_output::MachineCommand::Status,
        crate::cli::Cmd::RelinkPrs {} => crate::machine_output::MachineCommand::RelinkPrs,
        crate::cli::Cmd::Cleanup {} => crate::machine_output::MachineCommand::Cleanup,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_working_directory_override, command_requires_gh, machine_command_for_raw_args,
        parse_failure_as_json_output, read_only_command_for_raw_args, resolve_update_pr_limit,
        run_cli, CommandOutput, JsonParseFailureOutput, OutputMode,
    };
    use crate::machine_output::{MachineCommand, MachinePayload};
    use crate::parsing::Group;
    use crate::read_only_output::{
        ReadOnlyCommand, ReadOnlyError, ReadOnlyPayload, RemoteMetadataState,
    };
    use crate::selectors::{GroupSelector, StableHandle};
    use crate::test_support::{
        commit_file, git, init_case_conflicting_stack_repo, init_repo as init_stack_repo, lock_cwd,
        write_file, DirGuard,
    };
    use crate::update_output::{ResolvedUpdateLimit, UpdatePayload};
    use clap::Parser;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::Command;
    use tempfile::TempDir;

    struct CurrentDirGuard {
        original: PathBuf,
    }

    impl CurrentDirGuard {
        fn capture() -> Self {
            Self {
                original: std::env::current_dir().unwrap(),
            }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.original).unwrap();
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: String) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(original) = &self.original {
                env::set_var(self.key, original);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn install_gh_wrapper(script_body: &str) -> (TempDir, EnvVarGuard) {
        let wrapper_dir = tempfile::tempdir().unwrap();
        let script_path = wrapper_dir.path().join("gh");
        fs::write(&script_path, script_body).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();
        let original_path = env::var("PATH").unwrap_or_default();
        let path_guard = EnvVarGuard::set(
            "PATH",
            format!("{}:{}", wrapper_dir.path().display(), original_path),
        );
        (wrapper_dir, path_guard)
    }

    fn group(tag: &str) -> Group {
        Group {
            tag: tag.to_string(),
            subjects: vec![format!("feat: {tag}")],
            commits: vec![format!("{tag}1")],
            first_message: Some(format!("feat: {tag} pr:{tag}")),
            ignored_after: Vec::new(),
        }
    }

    #[test]
    fn resolve_update_pr_limit_rejects_conflicting_selectors() {
        let groups = vec![group("alpha")];
        let result = resolve_update_pr_limit(
            &groups,
            Some(GroupSelector::Stable(StableHandle {
                tag: "alpha".to_string(),
            })),
            Some(1),
            None,
        );
        let err = match result {
            Ok(_) => panic!("expected conflicting selector inputs to fail"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("accepts only one limit selector"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_update_pr_limit_rejects_local_only_group_selector() {
        let groups = vec![group("alpha")];
        let result = resolve_update_pr_limit(
            &groups,
            Some(GroupSelector::Stable(StableHandle {
                tag: "beta".to_string(),
            })),
            None,
            None,
        );
        let err = match result {
            Ok(_) => panic!("expected local-only selector to fail"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("No outstanding PR group matches stable handle `pr:beta`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn absorb_is_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::Absorb {
            allow_replayed_duplicates: false,
            json: false,
        }));
    }

    #[test]
    fn resume_is_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::Resume {
            path: std::path::PathBuf::from(".git/spr/resume/example.json"),
            json: false,
        }));
    }

    #[test]
    fn status_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::Status {
            json: false
        }));
    }

    #[test]
    fn update_without_no_pr_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::Update {
            from: "HEAD".to_string(),
            no_pr: false,
            restack: false,
            assume_existing_prs: false,
            pr_description_mode: None,
            allow_branch_reuse: false,
            json: false,
            extent: None,
        }));
    }

    #[test]
    fn update_no_pr_stays_git_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::Update {
            from: "HEAD".to_string(),
            no_pr: true,
            restack: false,
            assume_existing_prs: false,
            pr_description_mode: None,
            allow_branch_reuse: false,
            json: true,
            extent: None,
        }));
    }

    #[test]
    fn resolve_stack_without_pr_url_stays_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::ResolveStack {
            target: Some("dank-spr/alpha".to_string()),
            json: false,
        }));
    }

    #[test]
    fn resolve_stack_pr_url_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::ResolveStack {
            target: Some("https://github.com/o/r/pull/17".to_string()),
            json: true,
        }));
    }

    fn init_repo() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        let status = Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git init failed");
        dir
    }

    fn init_update_stack_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).expect("create repo dir");
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(&repo, "README.md", "init\n");
        git(&repo, ["add", "README.md"].as_slice());
        git(&repo, ["commit", "-m", "init"].as_slice());

        let origin = dir.path().join("origin.git");
        git(
            &repo,
            ["init", "--bare", origin.to_str().unwrap()].as_slice(),
        );
        git(
            &repo,
            ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
        );
        git(&repo, ["push", "-u", "origin", "main"].as_slice());

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\n",
            "feat: alpha start pr:alpha",
        );
        commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\nalpha-2\n",
            "feat: alpha follow-up",
        );
        commit_file(&repo, "beta.txt", "beta-1\n", "feat: beta start pr:beta");
        dir
    }

    fn install_failing_gh_wrapper() -> (TempDir, EnvVarGuard) {
        install_gh_wrapper("#!/bin/sh\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n")
    }

    #[test]
    fn working_directory_override_changes_repo_context() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_repo();

        apply_working_directory_override(Some(repo.path())).unwrap();
        let expected_root = fs::canonicalize(repo.path()).unwrap();
        let actual_root = PathBuf::from(crate::git::repo_root().unwrap().unwrap());

        assert_eq!(fs::canonicalize(actual_root).unwrap(), expected_root);
    }

    #[test]
    fn machine_command_for_raw_args_skips_global_option_values() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("--cd"),
            OsString::from("/tmp/repo"),
            OsString::from("resume"),
            OsString::from("--json"),
            OsString::from("state.json"),
        ];

        assert_eq!(machine_command_for_raw_args(&args), MachineCommand::Resume);
    }

    #[test]
    fn machine_command_for_raw_args_detects_resolve_stack() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("resolve-stack"),
            OsString::from("--json"),
            OsString::from("dank-spr/alpha"),
        ];

        assert_eq!(
            machine_command_for_raw_args(&args),
            MachineCommand::ResolveStack
        );
    }

    #[test]
    fn machine_command_for_raw_args_detects_status() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("status"),
            OsString::from("--json"),
        ];

        assert_eq!(machine_command_for_raw_args(&args), MachineCommand::Status);
    }

    #[test]
    fn machine_command_for_raw_args_detects_list_alias() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("ls"),
            OsString::from("pr"),
            OsString::from("--json"),
        ];

        assert_eq!(machine_command_for_raw_args(&args), MachineCommand::List);
    }

    #[test]
    fn machine_command_for_raw_args_detects_update() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("update"),
            OsString::from("--json"),
        ];

        assert_eq!(machine_command_for_raw_args(&args), MachineCommand::Update);
    }

    #[test]
    fn read_only_command_for_raw_args_detects_list_status_commands() {
        let list_args = vec![
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("pr"),
            OsString::from("--json"),
        ];
        let status_args = vec![
            OsString::from("spr"),
            OsString::from("status"),
            OsString::from("--json"),
        ];

        assert_eq!(
            read_only_command_for_raw_args(&list_args),
            Some(ReadOnlyCommand::ListPr)
        );
        assert_eq!(
            read_only_command_for_raw_args(&status_args),
            Some(ReadOnlyCommand::Status)
        );
    }

    #[test]
    fn parse_failure_with_json_returns_machine_error_for_rewrite_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("restack"),
            OsString::from("--json"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output =
            parse_failure_as_json_output(&args, &err).expect("json parse failure should serialize");

        match output {
            JsonParseFailureOutput::Machine(output) => {
                match output.payload {
                    MachinePayload::Error {
                        command,
                        ref message,
                    } => {
                        assert_eq!(command, MachineCommand::Restack);
                        assert!(message.contains("--after"));
                    }
                    other => panic!("unexpected parse-failure payload: {:?}", other),
                }
                assert_eq!(output.exit_code(), crate::machine_output::EXIT_FAILURE);
            }
            other => panic!("unexpected parse-failure output: {:?}", other),
        }
    }

    #[test]
    fn parse_failure_with_json_returns_read_only_error_for_list_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("pr"),
            OsString::from("--json"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output =
            parse_failure_as_json_output(&args, &err).expect("json parse failure should serialize");

        match output {
            JsonParseFailureOutput::ReadOnly(output) => {
                assert_eq!(output.command, ReadOnlyCommand::ListPr);
                assert_eq!(output.schema_version, 1);
                match output.payload {
                    ReadOnlyPayload::Error {
                        remote_metadata_state,
                        error: ReadOnlyError::InvalidArguments { ref message },
                    } => {
                        assert_eq!(remote_metadata_state, RemoteMetadataState::NotRequested);
                        assert!(message.contains("--bogus"));
                    }
                    other => panic!("unexpected read-only payload: {:?}", other),
                }
            }
            other => panic!("unexpected parse-failure output: {:?}", other),
        }
    }

    #[test]
    fn parse_failure_with_update_json_uses_update_command_identity() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("update"),
            OsString::from("--json"),
            OsString::from("pr"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output =
            parse_failure_as_json_output(&args, &err).expect("json parse failure should serialize");

        match output {
            JsonParseFailureOutput::Machine(output) => match output.payload {
                MachinePayload::Error { command, .. } => {
                    assert_eq!(command, MachineCommand::Update);
                }
                other => panic!("unexpected parse-failure payload: {:?}", other),
            },
            other => panic!("unexpected parse-failure output: {:?}", other),
        }
    }

    #[test]
    fn parse_failure_without_json_stays_human() {
        let args = vec![OsString::from("spr"), OsString::from("restack")];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");

        assert!(parse_failure_as_json_output(&args, &err).is_none());
    }

    #[test]
    fn help_request_with_json_stays_human() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("restack"),
            OsString::from("--json"),
            OsString::from("--help"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected help output");

        assert!(parse_failure_as_json_output(&args, &err).is_none());
    }

    fn init_local_stack_repo() -> TempDir {
        let dir = init_stack_repo();
        let repo = dir.path();
        git(repo, ["checkout", "-b", "stack"].as_slice());
        git(
            repo,
            ["remote", "add", "origin", "git@github.com:o/r.git"].as_slice(),
        );
        commit_file(repo, "alpha.txt", "alpha-1\n", "feat: alpha start pr:alpha");
        commit_file(
            repo,
            "alpha.txt",
            "alpha-1\nalpha-2\n",
            "feat: alpha follow-up",
        );
        commit_file(repo, "beta.txt", "beta-1\n", "feat: beta start pr:beta");
        fs::write(
            repo.join(".spr_multicommit_cfg.yml"),
            "list_order: recent_on_top\n",
        )
        .unwrap();
        dir
    }

    fn list_json_gh_script(
        log_path: &std::path::Path,
        open_json_path: &std::path::Path,
        status_json_path: &std::path::Path,
    ) -> String {
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'gh test wrapper'\n  exit 0\nfi\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*)\n      cat \"{}\" ;;\n    *\"is:pr is:open head:dank-spr/alpha\"*)\n      echo '{{\"data\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}' ;;\n    *\"pullRequest(number: 17)\"*\"pullRequest(number: 18)\"*)\n      cat \"{}\" ;;\n    *)\n      echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  echo '[]'\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            open_json_path.display(),
            status_json_path.display()
        )
    }

    #[test]
    fn run_cli_list_pr_json_uses_canonical_group_order() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_local_stack_repo();
        let log_path = repo.path().join("gh.log");
        let open_json_path = repo.path().join("gh-open.json");
        let status_json_path = repo.path().join("gh-status.json");
        fs::write(
            &open_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "nodes": [{
                                "number": 17,
                                "headRefName": "dank-spr/alpha",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/17",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        },
                        "pr1": {
                            "nodes": [{
                                "number": 18,
                                "headRefName": "dank-spr/beta",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/18",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &status_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "reviewDecision": "APPROVED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [{ "state": "APPROVED" }] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "SUCCESS" }
                                    }
                                }]
                            }
                        },
                        "pr1": {
                            "reviewDecision": "REVIEW_REQUIRED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "PENDING" }
                                    }
                                }]
                            }
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let script = list_json_gh_script(&log_path, &open_json_path, &status_json_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "list",
            "pr",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => {
                assert_eq!(output.command, ReadOnlyCommand::ListPr);
                match output.payload {
                    ReadOnlyPayload::PrList { data } => {
                        assert_eq!(data.remote_metadata_state, RemoteMetadataState::Complete);
                        assert_eq!(data.groups.len(), 2);
                        assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                        assert_eq!(data.groups[0].local_pr_number, 1);
                        assert_eq!(data.groups[1].stable_handle, "pr:beta");
                        assert_eq!(
                            data.groups[1]
                                .remote
                                .as_ref()
                                .and_then(|remote| remote.ci_review_status.as_ref())
                                .map(|status| status.review_decision.as_str()),
                            Some("REVIEW_REQUIRED")
                        );
                        let json = serde_json::to_string(
                            &crate::read_only_output::ReadOnlyOutput::pr_list(
                                ReadOnlyCommand::ListPr,
                                "main",
                                "dank-spr/",
                                data,
                            ),
                        )
                        .unwrap();
                        assert!(!json.contains("CI status"));
                        assert!(!json.contains("review status"));
                    }
                    other => panic!("unexpected read-only payload: {:?}", other),
                }
            }
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn run_cli_status_json_matches_list_pr_payload() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_local_stack_repo();
        let log_path = repo.path().join("gh.log");
        let open_json_path = repo.path().join("gh-open.json");
        let status_json_path = repo.path().join("gh-status.json");
        fs::write(
            &open_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "nodes": [{
                                "number": 17,
                                "headRefName": "dank-spr/alpha",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/17",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        },
                        "pr1": {
                            "nodes": [{
                                "number": 18,
                                "headRefName": "dank-spr/beta",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/18",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &status_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "reviewDecision": "APPROVED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [{ "state": "APPROVED" }] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "SUCCESS" }
                                    }
                                }]
                            }
                        },
                        "pr1": {
                            "reviewDecision": "REVIEW_REQUIRED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "PENDING" }
                                    }
                                }]
                            }
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let script = list_json_gh_script(&log_path, &open_json_path, &status_json_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let list_cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "list",
            "pr",
            "--json",
        ])
        .unwrap();
        let status_cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "status",
            "--json",
        ])
        .unwrap();

        let list_output = run_cli(list_cli, OutputMode::Json).unwrap();
        let status_output = run_cli(status_cli, OutputMode::Json).unwrap();

        match (list_output, status_output) {
            (CommandOutput::ReadOnly(list_output), CommandOutput::ReadOnly(status_output)) => {
                assert_eq!(list_output.command, ReadOnlyCommand::ListPr);
                assert_eq!(status_output.command, ReadOnlyCommand::Status);
                assert_eq!(list_output.payload, status_output.payload);
            }
            other => panic!("unexpected command outputs: {:?}", other),
        }
    }

    #[test]
    fn run_cli_list_commit_json_preserves_canonical_indices() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_local_stack_repo();
        let log_path = repo.path().join("gh.log");
        let open_json_path = repo.path().join("gh-open.json");
        let status_json_path = repo.path().join("gh-status.json");
        fs::write(
            &open_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "nodes": [{
                                "number": 17,
                                "headRefName": "dank-spr/alpha",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/17",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        },
                        "pr1": {
                            "nodes": [{
                                "number": 18,
                                "headRefName": "dank-spr/beta",
                                "baseRefName": "main",
                                "state": "OPEN",
                                "mergedAt": serde_json::Value::Null,
                                "closedAt": serde_json::Value::Null,
                                "url": "https://github.com/o/r/pull/18",
                                "autoMergeRequest": serde_json::Value::Null
                            }]
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &status_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "reviewDecision": "APPROVED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [{ "state": "APPROVED" }] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "SUCCESS" }
                                    }
                                }]
                            }
                        },
                        "pr1": {
                            "reviewDecision": "REVIEW_REQUIRED",
                            "isDraft": false,
                            "reviewRequests": { "totalCount": 0 },
                            "reviews": { "nodes": [] },
                            "commits": {
                                "nodes": [{
                                    "commit": {
                                        "statusCheckRollup": { "state": "PENDING" }
                                    }
                                }]
                            }
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let script = list_json_gh_script(&log_path, &open_json_path, &status_json_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "list",
            "commit",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => match output.payload {
                ReadOnlyPayload::CommitList { data } => {
                    assert_eq!(data.remote_metadata_state, RemoteMetadataState::Complete);
                    assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                    assert_eq!(
                        data.groups[0]
                            .commits
                            .iter()
                            .map(|commit| commit.global_commit_index)
                            .collect::<Vec<_>>(),
                        vec![1, 2]
                    );
                    assert_eq!(data.groups[1].stable_handle, "pr:beta");
                    assert_eq!(data.groups[1].commits[0].global_commit_index, 3);
                }
                other => panic!("unexpected read-only payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn run_cli_list_pr_json_collision_error_is_typed() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_case_conflicting_stack_repo();
        let log_path = repo.path().join("gh.log");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'gh test wrapper'\n  exit 0\nfi\nprintf '%s\\n' \"$*\" >> \"{}\"\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display()
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "list",
            "pr",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => match output.payload {
                ReadOnlyPayload::Error {
                    remote_metadata_state,
                    error: ReadOnlyError::SyntheticBranchNameCollision { conflicting_groups },
                } => {
                    assert_eq!(remote_metadata_state, RemoteMetadataState::NotRequested);
                    assert_eq!(conflicting_groups[0].stable_handle, "pr:alpha");
                    assert_eq!(conflicting_groups[1].head_branch, "dank-spr/Alpha");
                    let gh_log = fs::read_to_string(log_path).unwrap_or_default();
                    assert!(gh_log.is_empty());
                }
                other => panic!("unexpected read-only payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_json_no_pr_summary_stays_git_only_and_reports_full_extent() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let (_wrapper_dir, _path_guard) = install_failing_gh_wrapper();
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--dry-run",
            "update",
            "--json",
            "--no-pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::Update(output) => match output.payload {
                UpdatePayload::Summary { data } => {
                    assert_eq!(data.repo.base, "main");
                    assert_eq!(data.repo.from, "HEAD");
                    assert_eq!(data.repo.prefix, "dank-spr/");
                    assert_eq!(data.repo.ignore_tag, "ignore");
                    assert!(data.options.dry_run);
                    assert!(data.options.no_pr);
                    assert_eq!(data.extent, ResolvedUpdateLimit::All);
                    assert!(data.warnings.is_empty());
                    assert!(data.skipped_groups.is_empty());
                    assert_eq!(data.groups.len(), 2);
                    assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                    assert_eq!(
                        data.groups[0].push_action,
                        crate::update_output::UpdatePushAction::CreateBranch
                    );
                    assert_eq!(
                        data.groups[0].pr_action,
                        crate::update_output::UpdatePrAction::NotRequested
                    );
                    assert_eq!(
                        data.groups[0].description_action,
                        crate::update_output::UpdateEditAction::NotRequested
                    );
                    assert_eq!(data.groups[1].stable_handle, "pr:beta");
                    assert_eq!(data.groups[1].base_ref, "dank-spr/alpha");
                }
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_json_pr_selector_reports_resolved_pr_extent() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let (_wrapper_dir, _path_guard) = install_failing_gh_wrapper();
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--dry-run",
            "update",
            "--json",
            "--no-pr",
            "pr",
            "--to",
            "beta",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::Update(output) => match output.payload {
                UpdatePayload::Summary { data } => {
                    assert_eq!(data.extent, ResolvedUpdateLimit::ByPr { count: 2 });
                    assert_eq!(data.groups.len(), 2);
                }
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_json_legacy_pr_limit_reports_resolved_pr_extent() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let (_wrapper_dir, _path_guard) = install_failing_gh_wrapper();
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--dry-run",
            "update",
            "--json",
            "--no-pr",
            "pr",
            "1",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::Update(output) => match output.payload {
                UpdatePayload::Summary { data } => {
                    assert_eq!(data.extent, ResolvedUpdateLimit::ByPr { count: 1 });
                    assert_eq!(data.groups.len(), 1);
                    assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                }
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_json_commit_extent_reports_resolved_commit_limit() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let (_wrapper_dir, _path_guard) = install_failing_gh_wrapper();
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--dry-run",
            "update",
            "--json",
            "--no-pr",
            "commits",
            "2",
        ])
        .unwrap();

        let output = run_cli(cli, OutputMode::Json).unwrap();

        match output {
            CommandOutput::Update(output) => match output.payload {
                UpdatePayload::Summary { data } => {
                    assert_eq!(data.extent, ResolvedUpdateLimit::ByCommits { count: 2 });
                    assert_eq!(data.groups.len(), 1);
                    assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                    assert_eq!(data.groups[0].target_sha.len(), 40);
                }
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }
}
