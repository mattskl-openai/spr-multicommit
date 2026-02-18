use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;
mod config;
mod format;
mod git;
mod github;
mod limit;
mod parsing;

fn init_tools() -> Result<()> {
    crate::git::ensure_tool("git")?;
    crate::git::ensure_tool("gh")?;
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
    let mut prefix = prefix.unwrap_or_else(|| cfg.prefix.clone());
    // normalize: strip trailing '/' then ensure exactly one trailing '/'
    prefix = prefix.trim_end_matches('/').to_string();
    prefix.push('/');
    let mut ignore_tag = cfg.ignore_tag.clone();
    if ignore_tag.trim().is_empty() {
        ignore_tag = "ignore".to_string();
    }
    Ok((base, prefix, ignore_tag))
}

fn main() -> Result<()> {
    let cli = crate::cli::Cli::parse();
    if cli.verbose {
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
    if cli.verbose {
        std::env::set_var("SPR_VERBOSE", "1");
    }
    init_tools()?;
    let cfg = crate::config::load_config()?;
    let (base, prefix, ignore_tag) =
        resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone())?;
    let pr_description_mode = cfg.pr_description_mode;
    let restack_conflict_policy = cfg.restack_conflict;
    let list_order = cfg.list_order;
    match cli.cmd {
        crate::cli::Cmd::Update {
            from,
            no_pr,
            restack,
            assume_existing_prs,
            pr_description_mode: pr_description_mode_override,
            extent,
        } => {
            set_dry_run_env(cli.dry_run, assume_existing_prs);
            let limit = extent.map(|e| match e {
                crate::cli::Extent::Pr { n } => crate::limit::Limit::ByPr(n),
                crate::cli::Extent::Commits { n } => crate::limit::Limit::ByCommits(n),
            });
            let pr_description_mode = pr_description_mode_override.unwrap_or(pr_description_mode);
            if restack {
                return Err(anyhow::anyhow!(
                    "`spr update --restack` is deprecated. Use `spr restack --after N` instead."
                ));
            } else {
                let (_merge_base, groups) =
                    crate::parsing::derive_groups_between(&base, &from, &ignore_tag)?;
                if groups.is_empty() {
                    return Err(anyhow::anyhow!(
                        "No pr:<tag> markers found between {} and {}. Use `spr restack --after N`.",
                        base,
                        from
                    ));
                }
                crate::commands::build_from_groups(
                    &base,
                    &prefix,
                    no_pr,
                    cli.dry_run,
                    pr_description_mode,
                    limit,
                    groups,
                    list_order,
                )?;
            }
        }
        crate::cli::Cmd::Restack { after, safe } => {
            set_dry_run_env(cli.dry_run, false);
            let after_num: usize = match after.to_lowercase().as_str() {
                "bottom" => 0,
                "top" | "last" | "all" => usize::MAX,
                s => s.parse::<usize>().map_err(|_| {
                    anyhow::anyhow!(
                        "Invalid value for --after: {} (expected number or bottom|top|last)",
                        s
                    )
                })?,
            };
            crate::commands::restack_after(
                &base,
                &ignore_tag,
                after_num,
                safe,
                cli.dry_run,
                restack_conflict_policy,
            )?;
        }
        crate::cli::Cmd::Prep {} => {
            set_dry_run_env(cli.dry_run, false);
            if cli.until.is_some() && cli.exact.is_some() {
                return Err(anyhow::anyhow!("--until conflicts with --exact"));
            }
            let selection = if let Some(n) = cli.until {
                if n == 0 {
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
        }
        crate::cli::Cmd::List { what } => match what {
            crate::cli::ListWhat::Pr => {
                crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?
            }
            crate::cli::ListWhat::Commit => {
                crate::commands::list_commits_display(&base, &prefix, &ignore_tag, list_order)?
            }
        },
        crate::cli::Cmd::Status {} => {
            // alias for `spr list pr`
            crate::commands::list_prs_display(&base, &prefix, &ignore_tag, list_order)?
        }
        crate::cli::Cmd::Land {
            which,
            r#unsafe,
            no_restack,
        } => {
            set_dry_run_env(cli.dry_run, false);
            let mode = which.unwrap_or(match cfg.land.as_str() {
                "per-pr" | "perpr" | "per_pr" => crate::cli::LandCmd::PerPr,
                _ => crate::cli::LandCmd::Flatten,
            });
            let until = cli.until.unwrap_or(0);
            match mode {
                crate::cli::LandCmd::Flatten => crate::commands::land_flatten_until(
                    &base,
                    &prefix,
                    &ignore_tag,
                    until,
                    cli.dry_run,
                    r#unsafe,
                )?,
                crate::cli::LandCmd::PerPr => crate::commands::land_per_pr_until(
                    &base,
                    &prefix,
                    &ignore_tag,
                    until,
                    cli.dry_run,
                    r#unsafe,
                )?,
            }
            if !no_restack {
                // After landing the first N PRs, restack the remaining commits onto the latest base
                crate::commands::restack_after(
                    &base,
                    &ignore_tag,
                    until,
                    false,
                    cli.dry_run,
                    restack_conflict_policy,
                )?;
            }
        }
        crate::cli::Cmd::RelinkPrs {} => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::relink_prs(&base, &prefix, &ignore_tag, cli.dry_run)?;
        }
        crate::cli::Cmd::Cleanup {} => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::cleanup_remote_branches(&prefix, cli.dry_run)?;
        }
        crate::cli::Cmd::FixPr { n, tail, safe } => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::fix_pr_tail(&base, &ignore_tag, n, tail, safe, cli.dry_run)?;
        }
        crate::cli::Cmd::Move { range, after, safe } => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::move_groups_after(
                &base,
                &ignore_tag,
                &range,
                &after,
                safe,
                cli.dry_run,
            )?;
        }
    }
    Ok(())
}
