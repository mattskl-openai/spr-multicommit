use anyhow::{Context, Result};
use clap::{error::ErrorKind, Parser};
use std::ffi::{OsStr, OsString};
use std::path::Path;

mod branch_names;
mod cli;
mod commands;
mod config;
mod format;
mod git;
mod github;
mod limit;
mod machine_output;
mod parsing;
mod pr_labels;
mod selectors;
mod stack_metadata;
#[cfg(test)]
mod test_support;

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
        crate::cli::Cmd::Update { .. }
        | crate::cli::Cmd::Prep {}
        | crate::cli::Cmd::List { .. }
        | crate::cli::Cmd::Status {}
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

enum CommandOutput {
    None,
    Machine(crate::machine_output::MachineOutput),
    ResolveStack(crate::commands::ResolveStackOutput),
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
                let limit = if let Some(extent) = extent {
                    match extent {
                        crate::cli::Extent::Pr { to, n, legacy_n } => {
                            Some(resolve_update_pr_limit(&groups, to, n, legacy_n)?)
                        }
                        crate::cli::Extent::Commits { n } => {
                            Some(crate::limit::Limit::ByCommits(n))
                        }
                    }
                } else {
                    None
                };
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
            match what {
                crate::cli::ListWhat::Pr => {
                    crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?;
                }
                crate::cli::ListWhat::Commit => {
                    crate::commands::list_commits_display(&base, &prefix, &ignore_tag, list_order)?;
                }
            }
            Ok(CommandOutput::None)
        }
        crate::cli::Cmd::Status {} => {
            // alias for `spr list pr`
            crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?;
            Ok(CommandOutput::None)
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
    args.iter()
        .skip(1)
        .any(|arg| arg.as_os_str() == OsStr::new("--json"))
}

fn machine_command_for_raw_args(args: &[OsString]) -> crate::machine_output::MachineCommand {
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
                return crate::machine_output::MachineCommand::Restack;
            } else if arg == "absorb" {
                return crate::machine_output::MachineCommand::Absorb;
            } else if arg == "move" || arg == "mv" {
                return crate::machine_output::MachineCommand::Move;
            } else if arg == "fix-pr" || arg == "fix" {
                return crate::machine_output::MachineCommand::FixPr;
            } else if arg == "resolve-stack" {
                return crate::machine_output::MachineCommand::ResolveStack;
            } else if arg == "resume" {
                return crate::machine_output::MachineCommand::Resume;
            } else if arg == "land" {
                return crate::machine_output::MachineCommand::Land;
            } else if !arg.starts_with('-') {
                return crate::machine_output::MachineCommand::Cli;
            }
        }
    }
    crate::machine_output::MachineCommand::Cli
}

fn parse_failure_as_machine_output(
    args: &[OsString],
    err: &clap::Error,
) -> Option<crate::machine_output::MachineOutput> {
    let is_display_only = matches!(
        err.kind(),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
    );
    if raw_args_request_json(args) && !is_display_only {
        Some(crate::machine_output::MachineOutput::error(
            machine_command_for_raw_args(args),
            err.to_string(),
        ))
    } else {
        None
    }
}

fn main() {
    let raw_args: Vec<OsString> = std::env::args_os().collect();
    let cli = match crate::cli::Cli::try_parse_from(raw_args.clone()) {
        Ok(cli) => cli,
        Err(err) => {
            if let Some(output) = parse_failure_as_machine_output(&raw_args, &err) {
                println!("{}", serde_json::to_string(&output).unwrap());
                std::process::exit(crate::machine_output::EXIT_FAILURE);
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
    let command = machine_command_for_cli(&cli.cmd);
    match run_cli(cli, output_mode) {
        Ok(output) => match output {
            CommandOutput::None => {}
            CommandOutput::Machine(output) => {
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
                let output =
                    crate::machine_output::MachineOutput::error(command, format!("{err:#}"));
                println!("{}", serde_json::to_string(&output).unwrap());
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
        crate::cli::Cmd::Update { .. }
        | crate::cli::Cmd::Prep {}
        | crate::cli::Cmd::List { .. }
        | crate::cli::Cmd::Status {}
        | crate::cli::Cmd::RelinkPrs {}
        | crate::cli::Cmd::Cleanup {} => crate::machine_output::MachineCommand::Restack,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_working_directory_override, command_requires_gh, machine_command_for_raw_args,
        parse_failure_as_machine_output, resolve_update_pr_limit,
    };
    use crate::machine_output::{MachineCommand, MachinePayload};
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, StableHandle};
    use crate::test_support::lock_cwd;
    use clap::Parser;
    use std::ffi::OsString;
    use std::fs;
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
    fn status_still_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::Status {}));
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
    fn parse_failure_with_json_returns_machine_error() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("restack"),
            OsString::from("--json"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output = parse_failure_as_machine_output(&args, &err)
            .expect("json parse failure should serialize");

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

    #[test]
    fn parse_failure_without_json_stays_human() {
        let args = vec![OsString::from("spr"), OsString::from("restack")];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");

        assert!(parse_failure_as_machine_output(&args, &err).is_none());
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

        assert!(parse_failure_as_machine_output(&args, &err).is_none());
    }
}
