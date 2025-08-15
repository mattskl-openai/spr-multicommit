use anyhow::Result;
use clap::Parser;
use tracing::info;

mod cli;
mod commands;
mod git;
mod github;
mod limit;
mod parsing;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let cli = crate::cli::Cli::parse();
    if cli.verbose {
        std::env::set_var("SPR_VERBOSE", "1");
    }
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
            crate::git::ensure_tool("git")?;
            crate::git::ensure_tool("gh")?;
            if dry_run {
                std::env::set_var("SPR_DRY_RUN", "1");
            }
            if dry_run && assume_existing_prs {
                std::env::set_var("SPR_DRY_ASSUME_EXISTING", "1");
            }
            let limit = extent.map(|e| match e {
                crate::cli::Extent::Pr { n } => crate::limit::Limit::ByPr(n),
                crate::cli::Extent::Commits { n } => crate::limit::Limit::ByCommits(n),
            });
            if restack {
                crate::commands::restack_existing(&base, &prefix, no_pr, dry_run, limit)?;
            } else if crate::parsing::has_tagged_commits(&base, &from)? {
                crate::commands::build_from_tags(&base, &from, &prefix, no_pr, dry_run, update_pr_body, limit)?;
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
            crate::git::ensure_tool("git")?;
            crate::git::ensure_tool("gh")?;
            if dry_run {
                std::env::set_var("SPR_DRY_RUN", "1");
            }
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
            crate::git::ensure_tool("git")?;
            crate::git::ensure_tool("gh")?;
            match what {
                crate::cli::ListWhat::Pr => crate::commands::list_prs_display(&base, &prefix)?,
            }
        }
        crate::cli::Cmd::Merge {
            base,
            prefix,
            until,
            dry_run,
        } => {
            crate::git::ensure_tool("git")?;
            crate::git::ensure_tool("gh")?;
            crate::commands::merge_prs_until(&base, &prefix, until, dry_run)?;
        }
        crate::cli::Cmd::FixChain {
            base,
            prefix,
            dry_run,
        } => {
            crate::git::ensure_tool("git")?;
            crate::git::ensure_tool("gh")?;
            crate::commands::fix_chain(&base, &prefix, dry_run)?;
        }
    }
    Ok(())
}
