use anyhow::Result;
use clap::Parser;
use tracing::info;

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
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_target(false)
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
            base,
            prefix,
            from,
            no_pr,
            restack,
            dry_run,
            assume_existing_prs,
            update_pr_body,
            extent,
        } => {
            set_dry_run_env(dry_run, assume_existing_prs);
            let (base, prefix) = resolve_base_prefix(&cfg, base, prefix);
            let limit = extent.map(|e| match e {
                crate::cli::Extent::Pr { n } => crate::limit::Limit::ByPr(n),
                crate::cli::Extent::Commits { n } => crate::limit::Limit::ByCommits(n),
            });
            if restack {
                crate::commands::restack_existing(&base, &prefix, no_pr, dry_run, limit)?;
            } else if crate::parsing::has_tagged_commits(&base, &from)? {
                crate::commands::build_from_tags(
                    &base,
                    &from,
                    &prefix,
                    no_pr,
                    dry_run,
                    update_pr_body,
                    limit,
                )?;
            } else {
                info!(
                    "No pr:<tag> markers found between {} and {}. Falling back to --restack.",
                    base, from
                );
                crate::commands::restack_existing(&base, &prefix, no_pr, dry_run, limit)?;
            }
        }
        crate::cli::Cmd::Prep {
            base,
            prefix,
            until,
            exact,
            dry_run,
        } => {
            set_dry_run_env(dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, base, prefix);
            let selection = if let Some(n) = until {
                if n == 0 {
                    crate::cli::PrepSelection::All
                } else {
                    crate::cli::PrepSelection::Until(n)
                }
            } else if let Some(i) = exact {
                crate::cli::PrepSelection::Exact(i)
            } else {
                crate::cli::PrepSelection::All
            };
            crate::commands::prep_squash(&base, &prefix, selection, dry_run)?;
        }
        crate::cli::Cmd::List { what, base, prefix } => {
            let (base, prefix) = resolve_base_prefix(&cfg, base, prefix);
            match what {
                crate::cli::ListWhat::Pr => crate::commands::list_prs_display(&base, &prefix)?,
            }
        }
        crate::cli::Cmd::Land {
            base,
            prefix,
            until,
            dry_run,
        } => {
            set_dry_run_env(dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, base, prefix);
            crate::commands::land_prs_until(&base, &prefix, until, dry_run)?;
        }
        crate::cli::Cmd::FixChain {
            base,
            prefix,
            dry_run,
        } => {
            set_dry_run_env(dry_run, false);
            let (base, prefix) = resolve_base_prefix(&cfg, base, prefix);
            crate::commands::fix_chain(&base, &prefix, dry_run)?;
        }
    }
    Ok(())
}
