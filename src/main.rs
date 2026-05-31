use anyhow::{Context, Result};
use clap::{error::ErrorKind, Parser};
use serde::Serialize;
use std::ffi::OsString;
use std::path::Path;

use crate::execution::ExecutionMode;

mod absorb_output;
mod branch_names;
mod cli;
mod commands;
mod config;
mod execution;
mod format;
mod git;
mod github;
mod group_markers;
mod json_output;
mod limit;
mod local_pr_branches;
mod machine_output;
mod maintenance_output;
mod parsing;
mod pr_base_chain;
mod pr_labels;
mod read_only_output;
mod restack_output;
mod selectors;
mod stack_metadata;
mod summary_output;
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
        | crate::cli::Cmd::SyncLocalBranches
        | crate::cli::Cmd::FixPr { .. } => false,
        crate::cli::Cmd::ResolveStack { target } => target
            .as_deref()
            .map(crate::commands::looks_like_pr_url)
            .unwrap_or(false),
        crate::cli::Cmd::Update { no_pr, .. } => !*no_pr,
        crate::cli::Cmd::List { .. }
        | crate::cli::Cmd::Status
        | crate::cli::Cmd::Prep { .. }
        | crate::cli::Cmd::DropMergedPrefix { .. }
        | crate::cli::Cmd::Land { .. }
        | crate::cli::Cmd::RelinkPrs { .. }
        | crate::cli::Cmd::Cleanup { .. }
        | crate::cli::Cmd::Move { .. } => true,
    }
}

#[derive(Debug)]
enum CommandOutput {
    None,
    AbsorbQuery(crate::absorb_output::AbsorbChangedBranchesOutput),
    Machine(crate::machine_output::MachineOutput),
    ReadOnly(crate::read_only_output::ReadOnlyOutput),
    RestackPreview(crate::restack_output::RestackPreviewOutput),
    ResolveStack(crate::commands::ResolveStackOutput),
    Update(crate::update_output::UpdateOutput),
    Maintenance(Box<crate::maintenance_output::MaintenanceOutput>),
    Error(crate::json_output::ErrorOutput),
}

fn init_tools(needs_gh: bool) -> Result<()> {
    crate::git::ensure_tool("git")?;
    if needs_gh {
        crate::git::ensure_tool("gh")?;
    }
    Ok(())
}

fn set_dry_run_env(execution_mode: ExecutionMode, assume_existing_prs: bool) {
    if execution_mode == ExecutionMode::DryRun {
        std::env::set_var("SPR_DRY_RUN", "1");
        if assume_existing_prs {
            std::env::set_var("SPR_DRY_ASSUME_EXISTING", "1");
        }
    }
}

fn refresh_metadata_after_update(
    context: &crate::stack_metadata::RefreshMetadataContext,
) -> Result<()> {
    if !crate::stack_metadata::refresh_metadata_for_current_checkout_if_attached(
        &context.base,
        &context.prefix,
        &context.ignore_tag,
    )? {
        tracing::warn!(
            "Skipping stack metadata refresh because HEAD is detached. Rerun `spr update` after completing the active Git operation."
        );
    }
    Ok(())
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
    output_format: crate::cli::OutputFormat,
    command_name: &str,
    machine_command: crate::machine_output::MachineCommand,
    outcome: crate::commands::RewriteCommandOutcome,
    local_pr_branch_actions: Vec<crate::local_pr_branches::LocalPrBranchAction>,
) -> Result<crate::machine_output::MachineOutput> {
    if outcome == crate::commands::RewriteCommandOutcome::Completed {
        Ok(
            crate::machine_output::MachineOutput::completed_with_local_pr_branch_actions(
                machine_command,
                local_pr_branch_actions,
            ),
        )
    } else if let crate::commands::RewriteCommandOutcome::Suspended(state) = outcome {
        if output_format == crate::cli::OutputFormat::Json {
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
        Ok(
            crate::machine_output::MachineOutput::completed_with_local_pr_branch_actions(
                machine_command,
                local_pr_branch_actions,
            ),
        )
    }
}

fn ensure_resume_completed(
    output_format: crate::cli::OutputFormat,
    outcome: crate::commands::RewriteCommandOutcome,
    local_pr_branch_actions: Vec<crate::local_pr_branches::LocalPrBranchAction>,
) -> Result<crate::machine_output::MachineOutput> {
    if outcome == crate::commands::RewriteCommandOutcome::Completed {
        Ok(
            crate::machine_output::MachineOutput::completed_with_local_pr_branch_actions(
                crate::machine_output::MachineCommand::Resume,
                local_pr_branch_actions,
            ),
        )
    } else if let crate::commands::RewriteCommandOutcome::Suspended(state) = outcome {
        if output_format == crate::cli::OutputFormat::Json {
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
        Ok(
            crate::machine_output::MachineOutput::completed_with_local_pr_branch_actions(
                crate::machine_output::MachineCommand::Resume,
                local_pr_branch_actions,
            ),
        )
    }
}

fn sync_local_pr_branches_for_current_stack(
    policy: crate::config::LocalPrBranchSyncPolicy,
    execution_mode: ExecutionMode,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> Result<Vec<crate::local_pr_branches::LocalPrBranchAction>> {
    if policy == crate::config::LocalPrBranchSyncPolicy::Off {
        return Ok(Vec::new());
    }

    let (_merge_base, _leading_ignored, groups) =
        crate::parsing::derive_local_groups_with_ignored(base, ignore_tag)?;
    crate::branch_names::group_branch_identities(&groups, prefix)?;
    let targets = crate::local_pr_branches::targets_from_groups(prefix, &groups)?;
    crate::local_pr_branches::sync_local_pr_branches(policy, execution_mode, &targets)
}

fn sync_actions_after_completed_rewrite(
    outcome: &crate::commands::RewriteCommandOutcome,
    policy: crate::config::LocalPrBranchSyncPolicy,
    execution_mode: ExecutionMode,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> Result<Vec<crate::local_pr_branches::LocalPrBranchAction>> {
    if execution_mode == ExecutionMode::DryRun {
        return Ok(Vec::new());
    }
    if *outcome == crate::commands::RewriteCommandOutcome::Completed {
        sync_local_pr_branches_for_current_stack(policy, execution_mode, base, prefix, ignore_tag)
    } else {
        Ok(Vec::new())
    }
}

fn read_only_pr_list_output(
    command: crate::json_output::JsonCommand,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: crate::config::LocalPrBranchSyncPolicy,
) -> std::result::Result<crate::read_only_output::ReadOnlyOutput, crate::json_output::ErrorOutput> {
    match crate::commands::collect_pr_list_data_for_json(
        base,
        prefix,
        ignore_tag,
        local_pr_branch_policy,
    ) {
        Ok(data) => Ok(crate::read_only_output::pr_list(command, data)),
        Err(crate::commands::ReadOnlyQueryError::SyntheticBranchNameCollision(collision)) => Err(
            crate::json_output::ErrorOutput::synthetic_branch_name_collision(command, &collision),
        ),
        Err(crate::commands::ReadOnlyQueryError::Internal(err)) => Err(
            crate::json_output::ErrorOutput::internal(command, format!("{err:#}")),
        ),
    }
}

fn read_only_commit_list_output(
    command: crate::json_output::JsonCommand,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: crate::config::LocalPrBranchSyncPolicy,
) -> std::result::Result<crate::read_only_output::ReadOnlyOutput, crate::json_output::ErrorOutput> {
    match crate::commands::collect_commit_list_data_for_json(
        base,
        prefix,
        ignore_tag,
        local_pr_branch_policy,
    ) {
        Ok(data) => Ok(crate::read_only_output::commit_list(command, data)),
        Err(crate::commands::ReadOnlyQueryError::SyntheticBranchNameCollision(collision)) => Err(
            crate::json_output::ErrorOutput::synthetic_branch_name_collision(command, &collision),
        ),
        Err(crate::commands::ReadOnlyQueryError::Internal(err)) => Err(
            crate::json_output::ErrorOutput::internal(command, format!("{err:#}")),
        ),
    }
}

fn run_cli(cli: crate::cli::Cli, output_format: crate::cli::OutputFormat) -> Result<CommandOutput> {
    apply_working_directory_override(cli.cd.as_deref())?;
    init_tools(command_requires_gh(&cli.cmd))?;
    if let crate::cli::Cmd::Resume { path, .. } = &cli.cmd {
        let resume_path = if path.is_absolute() {
            path.clone()
        } else {
            std::env::current_dir()?.join(path)
        };
        let resume_context = crate::commands::resume_context(&resume_path)?;
        std::env::set_current_dir(&resume_context.original_worktree_root).with_context(|| {
            format!(
                "failed to change to original rewrite worktree {}",
                resume_context.original_worktree_root
            )
        })?;
        let explicit_local_pr_branch_policy = cli.local_pr_branches;
        let sync_context = if explicit_local_pr_branch_policy
            == Some(crate::config::LocalPrBranchSyncPolicy::Off)
        {
            None
        } else {
            match crate::config::load_config() {
                Ok(cfg) => {
                    let policy = explicit_local_pr_branch_policy.unwrap_or(cfg.local_pr_branches);
                    if policy == crate::config::LocalPrBranchSyncPolicy::Off {
                        None
                    } else {
                        match resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone()) {
                            Ok(context) => Some((policy, context)),
                            Err(err) if explicit_local_pr_branch_policy.is_none() => {
                                tracing::warn!(
                                    "Skipping local PR branch sync after resume because base and prefix could not be resolved: {err:#}"
                                );
                                None
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
                Err(err) if explicit_local_pr_branch_policy.is_none() => {
                    tracing::warn!(
                        "Skipping local PR branch sync after resume because config could not be loaded: {err:#}"
                    );
                    None
                }
                Err(err) => return Err(err),
            }
        };
        let outcome = crate::commands::resume_rewrite(&resume_path)?;
        let local_pr_branch_actions = if let (
            crate::commands::RewriteCommandOutcome::Completed,
            Some((policy, (base, prefix, ignore_tag))),
        ) = (&outcome, sync_context)
        {
            sync_local_pr_branches_for_current_stack(
                policy,
                ExecutionMode::Apply,
                &base,
                &prefix,
                &ignore_tag,
            )?
        } else {
            Vec::new()
        };
        return Ok(CommandOutput::Machine(ensure_resume_completed(
            output_format,
            outcome,
            local_pr_branch_actions,
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
    let local_pr_branch_policy = cli.local_pr_branches.unwrap_or(cfg.local_pr_branches);
    match cli.cmd {
        crate::cli::Cmd::Update {
            from,
            no_pr,
            restack,
            assume_existing_prs,
            pr_description_mode: pr_description_mode_override,
            allow_branch_reuse,
            dry_run,
            extent,
        } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, assume_existing_prs);
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
                if output_format == crate::cli::OutputFormat::Json {
                    let execution = crate::commands::build_from_groups_with_summary(
                        &base,
                        &prefix,
                        &skipped_handles,
                        no_pr,
                        execution_mode,
                        pr_description_mode,
                        limit,
                        groups,
                        list_order,
                        allow_branch_reuse,
                        branch_reuse_guard_days,
                        local_pr_branch_policy,
                    )?;
                    let summary = crate::update_output::UpdateSummaryData::from_execution(
                        crate::update_output::UpdateRepoContext {
                            base: base.clone(),
                            from,
                            prefix: prefix.clone(),
                            ignore_tag: ignore_tag.clone(),
                        },
                        crate::update_output::UpdateOptions {
                            dry_run: execution_mode == ExecutionMode::DryRun,
                            no_pr,
                            pr_description_mode,
                            local_pr_branches: local_pr_branch_policy,
                        },
                        resolved_extent,
                        execution,
                    );
                    if execution_mode == ExecutionMode::Apply {
                        refresh_metadata_after_update(&metadata_refresh_context)?;
                    }
                    Ok(CommandOutput::Update(crate::update_output::summary(
                        summary,
                    )))
                } else {
                    crate::commands::build_from_groups(
                        &base,
                        &prefix,
                        &skipped_handles,
                        no_pr,
                        execution_mode,
                        pr_description_mode,
                        limit,
                        groups,
                        list_order,
                        allow_branch_reuse,
                        branch_reuse_guard_days,
                        local_pr_branch_policy,
                    )?;
                    if execution_mode == ExecutionMode::Apply {
                        refresh_metadata_after_update(&metadata_refresh_context)?;
                    }
                    Ok(CommandOutput::None)
                }
            }
        }
        crate::cli::Cmd::Restack {
            after,
            safe,
            preview,
            dry_run,
        } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            if preview {
                Ok(CommandOutput::RestackPreview(
                    crate::restack_output::preview(crate::commands::preview_restack_after(
                        &metadata_refresh_context,
                        &after,
                        safe,
                    )?),
                ))
            } else {
                let outcome = crate::commands::restack_after(
                    &metadata_refresh_context,
                    &after,
                    safe,
                    execution_mode,
                    restack_conflict_policy,
                    dirty_worktree_policy,
                )?;
                let local_pr_branch_actions = sync_actions_after_completed_rewrite(
                    &outcome,
                    local_pr_branch_policy,
                    execution_mode,
                    &base,
                    &prefix,
                    &ignore_tag,
                )?;
                Ok(CommandOutput::Machine(ensure_rewrite_completed(
                    output_format,
                    "spr restack",
                    crate::machine_output::MachineCommand::Restack,
                    outcome,
                    local_pr_branch_actions,
                )?))
            }
        }
        crate::cli::Cmd::DropMergedPrefix { safe, dry_run } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let outcome = crate::commands::drop_merged_prefix(
                &metadata_refresh_context,
                safe,
                execution_mode,
                restack_conflict_policy,
                dirty_worktree_policy,
            )?;
            let local_pr_branch_actions = sync_actions_after_completed_rewrite(
                &outcome,
                local_pr_branch_policy,
                execution_mode,
                &base,
                &prefix,
                &ignore_tag,
            )?;
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_format,
                "spr drop-merged-prefix",
                crate::machine_output::MachineCommand::DropMergedPrefix,
                outcome,
                local_pr_branch_actions,
            )?))
        }
        crate::cli::Cmd::Absorb {
            from,
            allow_replayed_duplicates,
            query_changed_branches,
            dry_run,
        } => {
            let options = crate::commands::AbsorbOptions {
                copied_later_stack_commit_policy: if allow_replayed_duplicates {
                    crate::commands::CopiedLaterStackCommitPolicy::AllowKeepNonSeedDuplicates
                } else {
                    crate::commands::CopiedLaterStackCommitPolicy::Block
                },
            };
            if query_changed_branches {
                let changed_branches = crate::commands::query_absorb_changed_branches(
                    &base,
                    &prefix,
                    &ignore_tag,
                    from.as_ref(),
                    options,
                )?;
                return Ok(CommandOutput::AbsorbQuery(
                    crate::absorb_output::changed_branches(changed_branches),
                ));
            }
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let outcome = crate::commands::absorb_branch_tails(
                &base,
                &prefix,
                &ignore_tag,
                from.as_ref(),
                execution_mode,
                dirty_worktree_policy,
                options,
            )?;
            let local_pr_branch_actions = sync_actions_after_completed_rewrite(
                &outcome,
                local_pr_branch_policy,
                execution_mode,
                &base,
                &prefix,
                &ignore_tag,
            )?;
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_format,
                "spr absorb",
                crate::machine_output::MachineCommand::Absorb,
                outcome,
                local_pr_branch_actions,
            )?))
        }
        crate::cli::Cmd::Prep { from, dry_run } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let selection = match (cli.until, cli.exact, from) {
                (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => {
                    return Err(anyhow::anyhow!(
                        "--until, --exact, and --from are mutually exclusive"
                    ));
                }
                (Some(n), None, None) => {
                    if n == crate::selectors::InclusiveSelector::All {
                        crate::cli::PrepSelection::All
                    } else {
                        crate::cli::PrepSelection::Until(n)
                    }
                }
                (None, Some(i), None) => crate::cli::PrepSelection::Exact(i),
                (None, None, Some(selector)) => crate::cli::PrepSelection::From(selector),
                (None, None, None) => crate::cli::PrepSelection::All,
            };
            let summary = crate::commands::prep_squash(
                &base,
                &prefix,
                &ignore_tag,
                crate::commands::PrepExecutionOptions {
                    pr_description_mode,
                    list_order,
                    local_pr_branch_policy,
                    selection,
                    execution_mode,
                },
            )?;
            if output_format == crate::cli::OutputFormat::Json {
                Ok(CommandOutput::Maintenance(Box::new(
                    crate::maintenance_output::prep_summary(summary),
                )))
            } else {
                crate::commands::print_prep_summary(&summary);
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::List { what, .. } => {
            if output_format == crate::cli::OutputFormat::Json {
                match what {
                    crate::cli::ListWhat::Pr => match read_only_pr_list_output(
                        crate::json_output::JsonCommand::ListPr,
                        &base,
                        &prefix,
                        &ignore_tag,
                        local_pr_branch_policy,
                    ) {
                        Ok(output) => Ok(CommandOutput::ReadOnly(output)),
                        Err(output) => Ok(CommandOutput::Error(output)),
                    },
                    crate::cli::ListWhat::Commit => match read_only_commit_list_output(
                        crate::json_output::JsonCommand::ListCommit,
                        &base,
                        &prefix,
                        &ignore_tag,
                        local_pr_branch_policy,
                    ) {
                        Ok(output) => Ok(CommandOutput::ReadOnly(output)),
                        Err(output) => Ok(CommandOutput::Error(output)),
                    },
                }
            } else {
                match what {
                    crate::cli::ListWhat::Pr => crate::commands::list_prs_display(
                        &base,
                        &prefix,
                        &ignore_tag,
                        list_order,
                        local_pr_branch_policy,
                    )?,
                    crate::cli::ListWhat::Commit => crate::commands::list_commits_display(
                        &base,
                        &prefix,
                        &ignore_tag,
                        list_order,
                        local_pr_branch_policy,
                    )?,
                }
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::Status => {
            if output_format == crate::cli::OutputFormat::Json {
                match read_only_pr_list_output(
                    crate::json_output::JsonCommand::Status,
                    &base,
                    &prefix,
                    &ignore_tag,
                    local_pr_branch_policy,
                ) {
                    Ok(output) => Ok(CommandOutput::ReadOnly(output)),
                    Err(output) => Ok(CommandOutput::Error(output)),
                }
            } else {
                crate::commands::list_prs_display(
                    &base,
                    &prefix,
                    &ignore_tag,
                    list_order,
                    local_pr_branch_policy,
                )?;
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::SyncLocalBranches => {
            let local_pr_branch_actions = sync_local_pr_branches_for_current_stack(
                local_pr_branch_policy,
                ExecutionMode::Apply,
                &base,
                &prefix,
                &ignore_tag,
            )?;
            if output_format == crate::cli::OutputFormat::Json {
                Ok(CommandOutput::Maintenance(Box::new(
                    crate::maintenance_output::local_pr_branch_sync_summary(
                        crate::maintenance_output::LocalPrBranchSyncSummaryData {
                            repo: crate::maintenance_output::LocalPrBranchSyncRepoContext {
                                base,
                                prefix,
                                ignore_tag,
                            },
                            policy: local_pr_branch_policy,
                            local_pr_branch_actions,
                        },
                    ),
                )))
            } else {
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::ResolveStack { target } => Ok(CommandOutput::ResolveStack(
            crate::commands::resolve_stack(target, &ignore_tag)?,
        )),
        crate::cli::Cmd::Resume { .. } => unreachable!("handled before config loading"),
        crate::cli::Cmd::Land {
            which,
            r#unsafe,
            no_restack,
            dry_run,
        } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
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
                    execution_mode,
                    r#unsafe,
                )?,
                crate::cli::LandCmd::PerPr => crate::commands::land_per_pr_until(
                    &base,
                    &prefix,
                    &ignore_tag,
                    &until,
                    execution_mode,
                    r#unsafe,
                )?,
            };
            let local_pr_branch_actions = if !no_restack {
                // After landing the first N PRs, restack the remaining commits onto the latest base
                let outcome = crate::commands::restack_after_count(
                    &metadata_refresh_context,
                    landed_count,
                    false,
                    execution_mode,
                    restack_conflict_policy,
                    dirty_worktree_policy,
                )?;
                if let crate::commands::RewriteCommandOutcome::Suspended(state) = &outcome {
                    let post_success_hint = Some(
                        "GitHub landing already succeeded; resolve the local restack conflict and run the printed `spr resume <path>` command instead of rerunning `spr land`."
                            .to_string(),
                    );
                    if output_format == crate::cli::OutputFormat::Json {
                        return Ok(CommandOutput::Machine(
                            crate::machine_output::MachineOutput::suspended(
                                crate::machine_output::MachineCommand::Land,
                                (**state).clone(),
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
                sync_actions_after_completed_rewrite(
                    &outcome,
                    local_pr_branch_policy,
                    execution_mode,
                    &base,
                    &prefix,
                    &ignore_tag,
                )?
            } else {
                Vec::new()
            };
            Ok(CommandOutput::Machine(
                crate::machine_output::MachineOutput::completed_with_local_pr_branch_actions(
                    crate::machine_output::MachineCommand::Land,
                    local_pr_branch_actions,
                ),
            ))
        }
        crate::cli::Cmd::RelinkPrs { dry_run } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let summary = crate::commands::relink_prs(&base, &prefix, &ignore_tag, execution_mode)?;
            if output_format == crate::cli::OutputFormat::Json {
                Ok(CommandOutput::Maintenance(Box::new(
                    crate::maintenance_output::relink_prs_summary(summary),
                )))
            } else {
                crate::commands::print_relink_prs_summary(&summary);
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::Cleanup { dry_run } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let summary = crate::commands::cleanup_remote_branches(&prefix, execution_mode)?;
            if output_format == crate::cli::OutputFormat::Json {
                Ok(CommandOutput::Maintenance(Box::new(
                    crate::maintenance_output::cleanup_summary(summary),
                )))
            } else {
                crate::commands::print_cleanup_summary(&summary);
                Ok(CommandOutput::None)
            }
        }
        crate::cli::Cmd::FixPr {
            target,
            tail,
            safe,
            dry_run,
        } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let outcome = crate::commands::fix_pr_tail(
                &metadata_refresh_context,
                &target,
                tail,
                safe,
                execution_mode,
                dirty_worktree_policy,
            )?;
            let local_pr_branch_actions = sync_actions_after_completed_rewrite(
                &outcome,
                local_pr_branch_policy,
                execution_mode,
                &base,
                &prefix,
                &ignore_tag,
            )?;
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_format,
                "spr fix-pr",
                crate::machine_output::MachineCommand::FixPr,
                outcome,
                local_pr_branch_actions,
            )?))
        }
        crate::cli::Cmd::Move {
            range,
            after,
            safe,
            dry_run,
        } => {
            let execution_mode = ExecutionMode::from(dry_run);
            set_dry_run_env(execution_mode, false);
            let outcome = crate::commands::move_groups_after(
                &base,
                &prefix,
                &ignore_tag,
                &range,
                &after,
                crate::commands::MoveExecutionOptions {
                    safe,
                    execution_mode,
                    dirty_worktree_policy,
                },
            )?;
            let local_pr_branch_actions = sync_actions_after_completed_rewrite(
                &outcome,
                local_pr_branch_policy,
                execution_mode,
                &base,
                &prefix,
                &ignore_tag,
            )?;
            Ok(CommandOutput::Machine(ensure_rewrite_completed(
                output_format,
                "spr move",
                crate::machine_output::MachineCommand::Move,
                outcome,
                local_pr_branch_actions,
            )?))
        }
    }
}

fn init_logging(verbose: bool, output_format: crate::cli::OutputFormat) {
    if output_format == crate::cli::OutputFormat::Json {
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

fn json_command_for_raw_args(args: &[OsString]) -> crate::json_output::JsonCommand {
    crate::json_output::command_for_raw_args(args)
}

fn parse_failure_as_json_output(
    args: &[OsString],
    json_requested: bool,
    err: &clap::Error,
) -> Option<crate::json_output::ErrorOutput> {
    let is_display_only = matches!(
        err.kind(),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
    );
    if json_requested && !is_display_only {
        Some(crate::json_output::ErrorOutput::invalid_arguments(
            json_command_for_raw_args(args),
            err.to_string(),
        ))
    } else {
        None
    }
}

fn exit_with_json<T: Serialize>(output: &T, exit_code: i32) -> ! {
    println!("{}", serde_json::to_string(output).unwrap());
    std::process::exit(exit_code);
}

fn main() {
    let raw_args: Vec<OsString> = std::env::args_os().collect();
    let json_scan = crate::json_output::scan_json_output_request(raw_args.clone());
    let cli = match crate::cli::Cli::try_parse_from(json_scan.clap_args.clone()) {
        Ok(cli) => cli,
        Err(err) => {
            if json_scan.requested && err.kind() == ErrorKind::DisplayHelp {
                match crate::json_output::help_output_for_args(&raw_args) {
                    Ok(output) => exit_with_json(&output, output.exit_code()),
                    Err(err) => {
                        let output = crate::json_output::ErrorOutput::internal(
                            crate::json_output::JsonCommand::Help,
                            format!("{err:#}"),
                        );
                        exit_with_json(&output, output.exit_code());
                    }
                }
            } else if json_scan.requested && err.kind() == ErrorKind::DisplayVersion {
                let output = crate::json_output::version_output();
                exit_with_json(&output, output.exit_code());
            } else if let Some(output) =
                parse_failure_as_json_output(&raw_args, json_scan.requested, &err)
            {
                exit_with_json(&output, output.exit_code());
            } else {
                err.exit();
            }
        }
    };
    let output_format = if json_scan.requested {
        crate::cli::OutputFormat::Json
    } else {
        crate::cli::OutputFormat::Human
    };
    init_logging(cli.verbose, output_format);
    let command = json_command_for_cli(&cli.cmd);
    match run_cli(cli, output_format) {
        Ok(output) => match output {
            CommandOutput::None => {}
            CommandOutput::AbsorbQuery(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                } else {
                    println!("{}", output.render_human());
                }
            }
            CommandOutput::Machine(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                }
            }
            CommandOutput::ReadOnly(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                }
            }
            CommandOutput::RestackPreview(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                } else {
                    println!("{}", output.render_human());
                }
            }
            CommandOutput::Update(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                }
            }
            CommandOutput::Maintenance(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                }
            }
            CommandOutput::Error(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, output.exit_code());
                }
            }
            CommandOutput::ResolveStack(output) => {
                if output_format == crate::cli::OutputFormat::Json {
                    exit_with_json(&output, crate::json_output::EXIT_SUCCESS);
                } else {
                    println!("{}", output.render_human());
                }
            }
        },
        Err(err) => {
            if output_format == crate::cli::OutputFormat::Json {
                let output = crate::json_output::ErrorOutput::internal(command, format!("{err:#}"));
                exit_with_json(&output, output.exit_code());
            } else {
                eprintln!("Error: {err:#}");
                std::process::exit(crate::json_output::EXIT_FAILURE);
            }
        }
    }
}

fn json_command_for_cli(cmd: &crate::cli::Cmd) -> crate::json_output::JsonCommand {
    match cmd {
        crate::cli::Cmd::Restack { .. } => crate::machine_output::MachineCommand::Restack,
        crate::cli::Cmd::DropMergedPrefix { .. } => {
            crate::machine_output::MachineCommand::DropMergedPrefix
        }
        crate::cli::Cmd::Absorb { .. } => crate::machine_output::MachineCommand::Absorb,
        crate::cli::Cmd::ResolveStack { .. } => crate::machine_output::MachineCommand::ResolveStack,
        crate::cli::Cmd::Resume { .. } => crate::machine_output::MachineCommand::Resume,
        crate::cli::Cmd::Land { .. } => crate::machine_output::MachineCommand::Land,
        crate::cli::Cmd::FixPr { .. } => crate::machine_output::MachineCommand::FixPr,
        crate::cli::Cmd::Move { .. } => crate::machine_output::MachineCommand::Move,
        crate::cli::Cmd::Update { .. } => crate::machine_output::MachineCommand::Update,
        crate::cli::Cmd::Prep { .. } => crate::machine_output::MachineCommand::Prep,
        crate::cli::Cmd::List { what, .. } => match what {
            crate::cli::ListWhat::Pr => crate::machine_output::MachineCommand::ListPr,
            crate::cli::ListWhat::Commit => crate::machine_output::MachineCommand::ListCommit,
        },
        crate::cli::Cmd::Status => crate::machine_output::MachineCommand::Status,
        crate::cli::Cmd::SyncLocalBranches => {
            crate::machine_output::MachineCommand::SyncLocalBranches
        }
        crate::cli::Cmd::RelinkPrs { .. } => crate::machine_output::MachineCommand::RelinkPrs,
        crate::cli::Cmd::Cleanup { .. } => crate::machine_output::MachineCommand::Cleanup,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_working_directory_override, command_requires_gh, json_command_for_raw_args,
        parse_failure_as_json_output, resolve_update_pr_limit, run_cli, CommandOutput,
    };
    use crate::cli::{DryRunArgs, OutputFormat};
    use crate::json_output::{ErrorPayload, JsonCommand, JsonError, EXIT_FAILURE};
    use crate::maintenance_output::{
        MaintenancePayload, PrepNextChildAction, ResolvedPrepSelection,
    };
    use crate::parsing::{derive_local_groups_with_ignored, Group};
    use crate::read_only_output::ReadOnlyPayload;
    use crate::selectors::{ExplicitGroupSelector, GroupSelector};
    use crate::test_support::{
        commit_file, git, init_case_conflicting_stack_repo, init_repo as init_stack_repo, lock_cwd,
        write_file, DirGuard,
    };
    use crate::update_output::ResolvedUpdateLimit;
    use clap::Parser;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
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
            marker: crate::group_markers::GroupMarker::PrLabel(tag.to_string()),
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
            Some(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
                "alpha".to_string(),
            ))),
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
            Some(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
                "beta".to_string(),
            ))),
            None,
            None,
        );
        let err = match result {
            Ok(_) => panic!("expected local-only selector to fail"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("No outstanding PR group matches selector `pr:beta`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn absorb_is_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::Absorb {
            from: None,
            allow_replayed_duplicates: false,
            query_changed_branches: false,
            dry_run: DryRunArgs::default(),
        }));
    }

    #[test]
    fn drop_merged_prefix_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::DropMergedPrefix {
            safe: true,
            dry_run: DryRunArgs::default(),
        }));
    }

    #[test]
    fn resume_is_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::Resume {
            path: std::path::PathBuf::from(".git/spr/resume/example.json"),
        }));
    }

    #[test]
    fn status_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::Status));
    }

    #[test]
    fn sync_local_branches_stays_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::SyncLocalBranches));
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
            dry_run: DryRunArgs::default(),
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
            dry_run: DryRunArgs::default(),
            extent: None,
        }));
    }

    #[test]
    fn resolve_stack_without_pr_url_stays_local_only_for_tool_checks() {
        assert!(!command_requires_gh(&crate::cli::Cmd::ResolveStack {
            target: Some("dank-spr/alpha".to_string()),
        }));
    }

    #[test]
    fn resolve_stack_pr_url_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::ResolveStack {
            target: Some("https://github.com/o/r/pull/17".to_string()),
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

    fn local_branch_exists(repo: &Path, branch: &str) -> bool {
        Command::new("git")
            .current_dir(repo)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}^{{commit}}"),
            ])
            .status()
            .unwrap()
            .success()
    }

    fn rev_parse(repo: &Path, revision: &str) -> String {
        git(repo, ["rev-parse", revision].as_slice())
            .trim()
            .to_string()
    }

    fn prep_json_gh_script(log_path: &std::path::Path) -> String {
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'gh test wrapper'\n  exit 0\nfi\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*)\n      echo '{{\"data\":{{\"repository\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}}}' ;;\n    *\"is:pr is:open head:dank-spr/alpha\"*|*\"is:pr is:open head:dank-spr/beta\"*)\n      echo '{{\"data\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}' ;;\n    *)\n      echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
        )
    }

    fn stale_update_base_gh_script(
        log_path: &std::path::Path,
        open_prs_path: &std::path::Path,
        body_json_path: &std::path::Path,
    ) -> String {
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'gh test wrapper'\n  exit 0\nfi\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*) cat \"{}\" ;;\n    *\"is:pr is:open head:dank-spr/alpha\"*|*\"is:pr is:open head:dank-spr/beta\"*)\n      echo '{{\"data\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}' ;;\n    *\"pullRequest(number: 17)\"*\"pullRequest(number: 18)\"*) cat \"{}\" ;;\n    *\"mutation {{\"*) echo '{{\"data\":{{}}}}' ;;\n    *) echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            open_prs_path.display(),
            body_json_path.display(),
        )
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
    fn json_command_for_raw_args_skips_global_option_values() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("--cd"),
            OsString::from("/tmp/repo"),
            OsString::from("resume"),
            OsString::from("--json"),
            OsString::from("state.json"),
        ];

        assert_eq!(json_command_for_raw_args(&args), JsonCommand::Resume);
    }

    #[test]
    fn json_command_for_raw_args_detects_resolve_stack() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("resolve-stack"),
            OsString::from("--json"),
            OsString::from("dank-spr/alpha"),
        ];

        assert_eq!(json_command_for_raw_args(&args), JsonCommand::ResolveStack);
    }

    #[test]
    fn json_command_for_raw_args_detects_status() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("status"),
            OsString::from("--json"),
        ];

        assert_eq!(json_command_for_raw_args(&args), JsonCommand::Status);
    }

    #[test]
    fn json_command_for_raw_args_detects_sync_local_branches() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("sync-local-branches"),
            OsString::from("--json"),
        ];

        assert_eq!(
            json_command_for_raw_args(&args),
            JsonCommand::SyncLocalBranches
        );
    }

    #[test]
    fn json_command_for_raw_args_detects_list_alias() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("ls"),
            OsString::from("pr"),
            OsString::from("--json"),
        ];

        assert_eq!(json_command_for_raw_args(&args), JsonCommand::ListPr);
    }

    #[test]
    fn json_command_for_raw_args_detects_update() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("update"),
            OsString::from("--json"),
        ];

        assert_eq!(json_command_for_raw_args(&args), JsonCommand::Update);
    }

    #[test]
    fn json_command_for_raw_args_detects_drop_merged_prefix() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("drop-merged-prefix"),
            OsString::from("--json"),
        ];

        assert_eq!(
            json_command_for_raw_args(&args),
            JsonCommand::DropMergedPrefix
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
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::Restack);
        match output.payload {
            ErrorPayload::Error {
                error: JsonError::InvalidArguments { ref message },
            } => {
                assert!(message.contains("--after"));
            }
            other => panic!("unexpected parse-failure payload: {:?}", other),
        }
        assert_eq!(output.exit_code(), EXIT_FAILURE);
    }

    #[test]
    fn parse_failure_with_json_returns_read_only_error_for_list_command() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("list"),
            OsString::from("--json"),
            OsString::from("pr"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::ListPr);
        assert_eq!(output.schema_version, 1);
        match output.payload {
            ErrorPayload::Error {
                error: JsonError::InvalidArguments { ref message },
            } => {
                assert!(message.contains("--bogus"));
            }
            other => panic!("unexpected parse-failure payload: {:?}", other),
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
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::Update);
    }

    #[test]
    fn parse_failure_with_prep_json_uses_prep_command_identity() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("prep"),
            OsString::from("--json"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::Prep);
    }

    #[test]
    fn parse_failure_with_relink_prs_json_uses_relink_prs_command_identity() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("relink-prs"),
            OsString::from("--json"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::RelinkPrs);
    }

    #[test]
    fn parse_failure_with_cleanup_json_uses_cleanup_command_identity() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("cleanup"),
            OsString::from("--json"),
            OsString::from("--bogus"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");
        let output = parse_failure_as_json_output(&args, true, &err)
            .expect("json parse failure should serialize");

        assert_eq!(output.command, JsonCommand::Cleanup);
    }

    #[test]
    fn parse_failure_without_json_stays_human() {
        let args = vec![OsString::from("spr"), OsString::from("restack")];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected parse error");

        assert!(parse_failure_as_json_output(&args, false, &err).is_none());
    }

    #[test]
    fn help_request_with_json_is_not_parse_failure() {
        let args = vec![
            OsString::from("spr"),
            OsString::from("restack"),
            OsString::from("--json"),
            OsString::from("--help"),
        ];
        let err = crate::cli::Cli::try_parse_from(args.clone()).expect_err("expected help output");

        assert!(parse_failure_as_json_output(&args, true, &err).is_none());
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
    fn run_cli_prep_json_returns_maintenance_summary() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let origin_url = format!("file://{}", dir.path().join("origin.git").display());
        git(
            &repo,
            ["remote", "set-url", "origin", origin_url.as_str()].as_slice(),
        );
        let log_path = repo.join("gh.log");
        let script = prep_json_gh_script(&log_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--until",
            "1",
            "prep",
            "--dry-run",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Maintenance(output) => {
                assert_eq!(output.command, JsonCommand::Prep);
                match output.data {
                    MaintenancePayload::Prep { data } => {
                        assert_eq!(
                            data.selection,
                            ResolvedPrepSelection::Until {
                                selector: "LPR #1".to_string(),
                                last_local_pr_number: 1,
                            }
                        );
                        assert_eq!(data.selected_groups.len(), 1);
                        assert_eq!(data.selected_groups[0].stable_handle, "pr:alpha");
                        assert_eq!(
                            data.next_child.as_ref().map(|child| child.action),
                            Some(PrepNextChildAction::MissingOpenPr)
                        );
                        let update = data
                            .update
                            .expect("prep should include nested update summary");
                        assert_eq!(update.extent, ResolvedUpdateLimit::ByPr { count: 1 });
                        assert_eq!(update.groups.len(), 1);
                        assert_eq!(update.groups[0].stable_handle, "pr:alpha");
                    }
                    other => panic!("unexpected maintenance payload: {:?}", other),
                }
            }
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn run_cli_prep_from_json_returns_suffix_summary() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let origin_url = format!("file://{}", dir.path().join("origin.git").display());
        git(
            &repo,
            ["remote", "set-url", "origin", origin_url.as_str()].as_slice(),
        );
        let log_path = repo.join("gh.log");
        let script = prep_json_gh_script(&log_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "prep",
            "--from",
            "pr:beta",
            "--dry-run",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Maintenance(output) => match output.data {
                MaintenancePayload::Prep { data } => {
                    assert_eq!(
                        data.selection,
                        ResolvedPrepSelection::From {
                            selector: "pr:beta".to_string(),
                            first_local_pr_number: 2,
                        }
                    );
                    assert_eq!(data.selected_groups.len(), 1);
                    assert_eq!(data.selected_groups[0].stable_handle, "pr:beta");
                    assert!(data.next_child.is_none());
                    let update = data
                        .update
                        .expect("prep should include nested update summary");
                    assert_eq!(update.extent, ResolvedUpdateLimit::All);
                    assert_eq!(update.groups.len(), 2);
                    assert_eq!(update.groups[0].stable_handle, "pr:alpha");
                    assert_eq!(update.groups[1].stable_handle, "pr:beta");
                }
                other => panic!("unexpected maintenance payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn run_cli_prep_rejects_mixed_selectors() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let cases = [
            vec![
                "spr", "--base", "main", "--until", "1", "--exact", "1", "prep",
            ],
            vec![
                "spr", "--base", "main", "--until", "1", "prep", "--from", "pr:beta",
            ],
            vec![
                "spr", "--base", "main", "--exact", "1", "prep", "--from", "pr:beta",
            ],
        ];

        for case in cases {
            let cli = crate::cli::Cli::try_parse_from(case).unwrap();
            let err = run_cli(cli, OutputFormat::Human).unwrap_err();

            assert_eq!(
                err.to_string(),
                "--until, --exact, and --from are mutually exclusive"
            );
        }
    }

    #[test]
    fn run_cli_prep_json_propagates_local_branch_sync_policy() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let origin_url = format!("file://{}", dir.path().join("origin.git").display());
        git(
            &repo,
            ["remote", "set-url", "origin", origin_url.as_str()].as_slice(),
        );
        let log_path = repo.join("gh.log");
        let script = prep_json_gh_script(&log_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--local-pr-branches",
            "create-or-update",
            "--until",
            "1",
            "prep",
            "--dry-run",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Maintenance(output) => match output.data {
                MaintenancePayload::Prep { data } => {
                    let update = data
                        .update
                        .expect("prep should include nested update summary");
                    assert_eq!(
                        update.options.local_pr_branches,
                        crate::config::LocalPrBranchSyncPolicy::CreateOrUpdate
                    );
                    assert_eq!(update.local_pr_branch_actions.len(), 1);
                    assert_eq!(
                        update.local_pr_branch_actions[0].action,
                        crate::local_pr_branches::LocalPrBranchActionKind::Created
                    );
                }
                other => panic!("unexpected maintenance payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn run_cli_restack_preview_json_returns_preview_payload() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "restack",
            "--after",
            "pr:alpha",
            "--preview",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::RestackPreview(output) => {
                assert_eq!(
                    output.result,
                    crate::restack_output::RestackPreviewResult::Preview
                );
                assert_eq!(output.data.current_branch, "stack");
                assert_eq!(output.data.after_selector, "pr:alpha");
                assert_eq!(output.data.dropped_groups[0].stable_handle, "pr:alpha");
                assert_eq!(output.data.remaining_groups[0].stable_handle, "pr:beta");
                assert!(output
                    .data
                    .not_validated
                    .contains(&"remote freshness".to_string()));
            }
            other => panic!("unexpected output: {:?}", other),
        }
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
            "--json",
            "pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => {
                assert_eq!(output.command, JsonCommand::ListPr);
                match output.data {
                    ReadOnlyPayload::PrList { data } => {
                        assert_eq!(data.groups.len(), 2);
                        assert_eq!(data.groups[0].stable_handle, "pr:alpha");
                        assert_eq!(data.groups[0].local_pr_number, 1);
                        assert_eq!(data.groups[1].stable_handle, "pr:beta");
                        assert!(matches!(
                            &data.groups[1].remote.state,
                            crate::commands::RemotePrState::RemoteWithCiReview {
                                ci_review_status,
                                ..
                            } if ci_review_status.review_decision
                                == crate::github::PrReviewDecision::ReviewRequired
                        ));
                        let json = serde_json::to_string(&crate::read_only_output::pr_list(
                            JsonCommand::ListPr,
                            data.clone(),
                        ))
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
    fn run_cli_list_pr_json_reports_local_branch_drift_without_mutating_refs() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_local_stack_repo();
        let alpha_tip = rev_parse(repo.path(), "HEAD~2");
        git(
            repo.path(),
            ["branch", "dank-spr/alpha", &alpha_tip].as_slice(),
        );
        let log_path = repo.path().join("gh.log");
        let open_json_path = repo.path().join("gh-open.json");
        let status_json_path = repo.path().join("gh-status.json");
        fs::write(
            &open_json_path,
            "{\"data\":{\"repository\":{\"pr0\":{\"nodes\":[]},\"pr1\":{\"nodes\":[]}}}}",
        )
        .unwrap();
        fs::write(&status_json_path, "{\"data\":{\"repository\":{}}}").unwrap();
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
            "--local-pr-branches",
            "create-or-update",
            "list",
            "--json",
            "pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => match output.data {
                ReadOnlyPayload::PrList { data } => {
                    assert_eq!(data.local_pr_branch_drift.len(), 2);
                    assert_eq!(data.local_pr_branch_drift[0].branch, "dank-spr/alpha");
                    assert_eq!(
                        data.local_pr_branch_drift[0].action,
                        crate::local_pr_branches::LocalPrBranchActionKind::Updated
                    );
                    assert_eq!(data.local_pr_branch_drift[1].branch, "dank-spr/beta");
                    assert_eq!(
                        data.local_pr_branch_drift[1].action,
                        crate::local_pr_branches::LocalPrBranchActionKind::Created
                    );
                }
                other => panic!("unexpected read-only payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }

        assert_eq!(rev_parse(repo.path(), "dank-spr/alpha"), alpha_tip);
        assert!(!local_branch_exists(repo.path(), "dank-spr/beta"));
    }

    #[test]
    fn run_cli_sync_local_branches_json_reconciles_current_stack() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let repo = init_local_stack_repo();
        let alpha_tip = rev_parse(repo.path(), "HEAD~2");
        git(
            repo.path(),
            ["branch", "dank-spr/alpha", &alpha_tip].as_slice(),
        );
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.path().to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--local-pr-branches",
            "create-or-update",
            "sync-local-branches",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Maintenance(output) => {
                assert_eq!(output.command, JsonCommand::SyncLocalBranches);
                match output.data {
                    MaintenancePayload::LocalPrBranchSync { data } => {
                        assert_eq!(
                            data.policy,
                            crate::config::LocalPrBranchSyncPolicy::CreateOrUpdate
                        );
                        assert_eq!(data.local_pr_branch_actions.len(), 2);
                        assert_eq!(
                            data.local_pr_branch_actions[0].action,
                            crate::local_pr_branches::LocalPrBranchActionKind::Updated
                        );
                        assert_eq!(
                            data.local_pr_branch_actions[1].action,
                            crate::local_pr_branches::LocalPrBranchActionKind::Created
                        );
                    }
                    other => panic!("unexpected maintenance payload: {:?}", other),
                }
            }
            other => panic!("unexpected command output: {:?}", other),
        }

        assert_eq!(
            rev_parse(repo.path(), "dank-spr/alpha"),
            rev_parse(repo.path(), "HEAD~1")
        );
        assert_eq!(
            rev_parse(repo.path(), "dank-spr/beta"),
            rev_parse(repo.path(), "HEAD")
        );
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
            "--json",
            "pr",
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

        let list_output = run_cli(list_cli, OutputFormat::Json).unwrap();
        let status_output = run_cli(status_cli, OutputFormat::Json).unwrap();

        match (list_output, status_output) {
            (CommandOutput::ReadOnly(list_output), CommandOutput::ReadOnly(status_output)) => {
                assert_eq!(list_output.command, JsonCommand::ListPr);
                assert_eq!(status_output.command, JsonCommand::Status);
                assert_eq!(list_output.data, status_output.data);
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
            "--json",
            "commit",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::ReadOnly(output) => match output.data {
                ReadOnlyPayload::CommitList { data } => {
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
            "--json",
            "pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Error(output) => match output.payload {
                ErrorPayload::Error {
                    error: JsonError::SyntheticBranchNameCollision { conflicting_groups },
                } => {
                    assert_eq!(output.command, JsonCommand::ListPr);
                    assert_eq!(conflicting_groups[0].stable_handle, "pr:alpha");
                    assert_eq!(conflicting_groups[1].head_branch, "dank-spr/Alpha");
                    let gh_log = fs::read_to_string(log_path).unwrap_or_default();
                    assert!(gh_log.is_empty());
                }
                other => panic!("unexpected error payload: {:?}", other),
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
            "update",
            "--dry-run",
            "--json",
            "--no-pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Update(output) => {
                let data = output.data;
                assert_eq!(data.repo.base, "main");
                assert_eq!(data.repo.from, "HEAD");
                assert_eq!(data.repo.prefix, "dank-spr/");
                assert_eq!(data.repo.ignore_tag, "ignore");
                assert!(data.options.dry_run);
                assert!(data.options.no_pr);
                assert_eq!(
                    data.options.local_pr_branches,
                    crate::config::LocalPrBranchSyncPolicy::Off
                );
                assert_eq!(data.extent, ResolvedUpdateLimit::All);
                assert!(data.warnings.is_empty());
                assert!(data.skipped_groups.is_empty());
                assert!(data.local_pr_branch_actions.is_empty());
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
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_json_no_pr_reports_opted_in_local_branch_sync_actions() {
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
            "--local-pr-branches",
            "create-or-update",
            "update",
            "--dry-run",
            "--json",
            "--no-pr",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Update(output) => {
                let data = output.data;
                assert_eq!(
                    data.options.local_pr_branches,
                    crate::config::LocalPrBranchSyncPolicy::CreateOrUpdate
                );
                assert_eq!(data.local_pr_branch_actions.len(), 2);
                assert_eq!(data.local_pr_branch_actions[0].stable_handle, "pr:alpha");
                assert_eq!(data.local_pr_branch_actions[0].branch, "dank-spr/alpha");
                assert_eq!(
                    data.local_pr_branch_actions[0].action,
                    crate::local_pr_branches::LocalPrBranchActionKind::Created
                );
                assert_eq!(data.local_pr_branch_actions[0].old_tip, None);
                assert_eq!(
                    data.local_pr_branch_actions[0].new_tip,
                    data.groups[0].target_sha
                );
                assert_eq!(data.local_pr_branch_actions[1].stable_handle, "pr:beta");
                assert_eq!(data.local_pr_branch_actions[1].branch, "dank-spr/beta");
                assert_eq!(
                    data.local_pr_branch_actions[1].action,
                    crate::local_pr_branches::LocalPrBranchActionKind::Created
                );
                assert!(!local_branch_exists(&repo, "dank-spr/alpha"));
                assert!(!local_branch_exists(&repo, "dank-spr/beta"));
            }
            other => panic!("unexpected command output: {:?}", other),
        }
    }

    #[test]
    fn update_rejects_base_edits_that_do_not_converge_after_apply() {
        let _lock = lock_cwd();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let _home_guard = EnvVarGuard::set("HOME", dir.path().display().to_string());
        let origin_url = format!("file://{}", dir.path().join("origin.git").display());
        git(
            &repo,
            ["remote", "set-url", "origin", origin_url.as_str()].as_slice(),
        );
        let log_path = repo.join("gh.log");
        let open_prs_path = repo.join("gh-open.json");
        let body_json_path = repo.join("gh-bodies.json");
        fs::write(
            &open_prs_path,
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
            &body_json_path,
            serde_json::to_string(&serde_json::json!({
                "data": {
                    "repository": {
                        "pr0": {
                            "id": "PR_17",
                            "body": "alpha body"
                        },
                        "pr1": {
                            "id": "PR_18",
                            "body": "beta body"
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let script = stale_update_base_gh_script(&log_path, &open_prs_path, &body_json_path);
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "update",
        ])
        .unwrap();

        let err = run_cli(cli, OutputFormat::Human).unwrap_err();

        assert_eq!(
            err.to_string(),
            "GitHub PR base chain did not converge after update: dank-spr/beta: main -> dank-spr/alpha"
        );
        let log = fs::read_to_string(log_path).unwrap();
        assert!(log.contains("baseRefName:\"dank-spr/alpha\""), "{log}");
    }

    #[test]
    fn absorb_with_local_pr_branch_sync_updates_rewritten_local_branch_tip() {
        let _lock = lock_cwd();
        let _restore = CurrentDirGuard::capture();
        let dir = init_update_stack_repo();
        let repo = dir.path().join("repo");
        let alpha_tip = rev_parse(&repo, "HEAD~1");
        let beta_tip = rev_parse(&repo, "HEAD");
        git(&repo, ["branch", "dank-spr/alpha", &alpha_tip].as_slice());
        git(&repo, ["branch", "dank-spr/beta", &beta_tip].as_slice());
        git(&repo, ["checkout", "dank-spr/alpha"].as_slice());
        commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo, ["checkout", "stack"].as_slice());
        let stale_beta_branch_tip = rev_parse(&repo, "dank-spr/beta");
        let cli = crate::cli::Cli::try_parse_from([
            "spr",
            "--cd",
            repo.to_str().unwrap(),
            "--base",
            "main",
            "--prefix",
            "dank-spr/",
            "--local-pr-branches",
            "create-or-update",
            "absorb",
            "--json",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Machine(output) => match output.payload {
                crate::machine_output::MachinePayload::Completed {
                    local_pr_branch_actions,
                } => {
                    assert_eq!(local_pr_branch_actions.len(), 2);
                    assert_eq!(local_pr_branch_actions[0].stable_handle, "pr:alpha");
                    assert!(matches!(
                        local_pr_branch_actions[0].action,
                        crate::local_pr_branches::LocalPrBranchActionKind::Updated
                            | crate::local_pr_branches::LocalPrBranchActionKind::Skipped
                    ));
                    assert_eq!(local_pr_branch_actions[1].stable_handle, "pr:beta");
                    assert_eq!(
                        local_pr_branch_actions[1].action,
                        crate::local_pr_branches::LocalPrBranchActionKind::Updated
                    );
                }
                other => panic!("unexpected machine payload: {:?}", other),
            },
            other => panic!("unexpected command output: {:?}", other),
        }

        let _guard = DirGuard::change_to(&repo);
        let (_merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored("main", "ignore").unwrap();
        assert!(leading_ignored.is_empty());
        let rewritten_alpha_tip = groups[0].commits.last().unwrap();
        let rewritten_beta_tip = groups[1].commits.last().unwrap();
        assert_ne!(
            rewritten_beta_tip, &stale_beta_branch_tip,
            "absorb should replay beta above the absorbed alpha tail"
        );
        assert_eq!(
            rev_parse(&repo, "dank-spr/alpha"),
            *rewritten_alpha_tip,
            "local alpha branch should move to the rewritten alpha group tip"
        );
        assert_eq!(
            rev_parse(&repo, "dank-spr/beta"),
            *rewritten_beta_tip,
            "local beta branch should move to the rewritten beta group tip"
        );
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
            "update",
            "--dry-run",
            "--json",
            "--no-pr",
            "pr",
            "--to",
            "beta",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Update(output) => {
                assert_eq!(output.data.extent, ResolvedUpdateLimit::ByPr { count: 2 });
                assert_eq!(output.data.groups.len(), 2);
            }
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
            "update",
            "--dry-run",
            "--json",
            "--no-pr",
            "pr",
            "1",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Update(output) => {
                assert_eq!(output.data.extent, ResolvedUpdateLimit::ByPr { count: 1 });
                assert_eq!(output.data.groups.len(), 1);
                assert_eq!(output.data.groups[0].stable_handle, "pr:alpha");
            }
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
            "update",
            "--dry-run",
            "--json",
            "--no-pr",
            "commits",
            "2",
        ])
        .unwrap();

        let output = run_cli(cli, OutputFormat::Json).unwrap();

        match output {
            CommandOutput::Update(output) => {
                assert_eq!(
                    output.data.extent,
                    ResolvedUpdateLimit::ByCommits { count: 2 }
                );
                assert_eq!(output.data.groups.len(), 1);
                assert_eq!(output.data.groups[0].stable_handle, "pr:alpha");
                assert_eq!(output.data.groups[0].target_sha.len(), 40);
            }
            other => panic!("unexpected command output: {:?}", other),
        }
    }
}
