use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;
mod config;
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

fn resolve_base_prefix(
    cfg: &crate::config::FileConfig,
    base: Option<String>,
    prefix: Option<String>,
) -> (String, String) {
    let base = base
        .or(cfg.base.clone())
        .unwrap_or_else(|| "origin/oai-main".to_string());
    let user = std::env::var("USER").unwrap_or_else(|_| "".to_string());
    let mut prefix = prefix
        .or(cfg.prefix.clone())
        .unwrap_or_else(|| format!("{}-spr", user));
    // normalize: strip trailing '/' then ensure exactly one trailing '/'
    prefix = prefix.trim_end_matches('/').to_string();
    prefix.push('/');
    (base, prefix)
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
    match cli.cmd {
        crate::cli::Cmd::Update {
            from,
            no_pr,
            restack,
            assume_existing_prs,
            update_pr_body,
            extent,
        } => {
            set_dry_run_env(cli.dry_run, assume_existing_prs);
            let (base, prefix) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            let limit = extent.map(|e| match e {
                crate::cli::Extent::Pr { n } => crate::limit::Limit::ByPr(n),
                crate::cli::Extent::Commits { n } => crate::limit::Limit::ByCommits(n),
            });
            if restack {
                return Err(anyhow::anyhow!(
                    "`spr update --restack` is deprecated. Use `spr restack --after N` instead."
                ));
            } else if crate::parsing::has_tagged_commits(&base, &from)? {
                crate::commands::build_from_tags(
                    &base,
                    &from,
                    &prefix,
                    no_pr,
                    cli.dry_run,
                    update_pr_body,
                    limit,
                )?;
            } else {
                return Err(anyhow::anyhow!(
                    "No pr:<tag> markers found between {} and {}. Use `spr restack --after N`.",
                    base,
                    from
                ));
            }
        }
        crate::cli::Cmd::Restack { after, safe } => {
            set_dry_run_env(cli.dry_run, false);
            let (base, _) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            crate::commands::restack_after(&base, after, safe, cli.dry_run)?;
        }
        crate::cli::Cmd::Prep {} => {
            set_dry_run_env(cli.dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
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
            crate::commands::prep_squash(&base, &prefix, selection, cli.dry_run)?;
        }
        crate::cli::Cmd::List { what } => {
            let (base, prefix) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            match what {
                crate::cli::ListWhat::Pr => crate::commands::list_prs_display(&base, &prefix)?,
                crate::cli::ListWhat::Commit => {
                    crate::commands::list_commits_display(&base, &prefix)?
                }
            }
        }
        crate::cli::Cmd::Land { which } => {
            set_dry_run_env(cli.dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            let mode = which
                .or(match cfg.land.as_deref() {
                    Some("per-pr") | Some("perpr") | Some("per_pr") => {
                        Some(crate::cli::LandCmd::PerPr)
                    }
                    _ => Some(crate::cli::LandCmd::Flatten),
                })
                .unwrap_or(crate::cli::LandCmd::Flatten);
            let until = cli.until.unwrap_or(0);
            match mode {
                crate::cli::LandCmd::Flatten => {
                    crate::commands::land_flatten_until(&base, &prefix, until, cli.dry_run)?
                }
                crate::cli::LandCmd::PerPr => {
                    crate::commands::land_per_pr_until(&base, &prefix, until, cli.dry_run)?
                }
            }
        }
        crate::cli::Cmd::Relink {} => {
            set_dry_run_env(cli.dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            crate::commands::relink_stack(&base, &prefix, cli.dry_run)?;
        }
        crate::cli::Cmd::Move { range, after, safe } => {
            set_dry_run_env(cli.dry_run, false);
            let (base, _) = resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone());
            crate::commands::move_groups_after(&base, &range, &after, safe, cli.dry_run)?;
        }
    }
    Ok(())
}
