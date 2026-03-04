use anyhow::{Context, Result};
use clap::Parser;
use std::path::Path;

mod cli;
mod commands;
mod config;
mod format;
mod git;
mod github;
mod limit;
mod parsing;
mod pr_labels;
mod selectors;
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
        | crate::cli::Cmd::FixPr { .. } => false,
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
    apply_working_directory_override(cli.cd.as_deref())?;
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
    init_tools(command_requires_gh(&cli.cmd))?;
    let cfg = crate::config::load_config()?;
    let (base, prefix, ignore_tag) =
        resolve_base_prefix(&cfg, cli.base.clone(), cli.prefix.clone())?;
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
                return Err(anyhow::anyhow!(
                    "`spr update --restack` is deprecated. Use `spr restack --after N` instead."
                ));
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
            }
        }
        crate::cli::Cmd::Restack { after, safe } => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::restack_after(
                &base,
                &ignore_tag,
                &after,
                safe,
                cli.dry_run,
                restack_conflict_policy,
                dirty_worktree_policy,
            )?;
        }
        crate::cli::Cmd::Absorb {
            allow_replayed_duplicates,
        } => {
            set_dry_run_env(cli.dry_run, false);
            let options = crate::commands::AbsorbOptions {
                copied_later_stack_commit_policy: if allow_replayed_duplicates {
                    crate::commands::CopiedLaterStackCommitPolicy::AllowKeepNonSeedDuplicates
                } else {
                    crate::commands::CopiedLaterStackCommitPolicy::Block
                },
            };
            crate::commands::absorb_branch_tails(
                &base,
                &prefix,
                &ignore_tag,
                cli.dry_run,
                dirty_worktree_policy,
                options,
            )?;
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
                crate::commands::restack_after_count(
                    &base,
                    &ignore_tag,
                    landed_count,
                    false,
                    cli.dry_run,
                    restack_conflict_policy,
                    dirty_worktree_policy,
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
        crate::cli::Cmd::FixPr { target, tail, safe } => {
            set_dry_run_env(cli.dry_run, false);
            crate::commands::fix_pr_tail(
                &base,
                &ignore_tag,
                &target,
                tail,
                safe,
                cli.dry_run,
                dirty_worktree_policy,
            )?;
        }
        crate::cli::Cmd::Move { range, after, safe } => {
            set_dry_run_env(cli.dry_run, false);
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
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_working_directory_override, command_requires_gh, resolve_update_pr_limit};
    use crate::parsing::Group;
    use crate::selectors::{GroupSelector, StableHandle};
    use crate::test_support::lock_cwd;
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
        }));
    }

    #[test]
    fn status_still_requires_github_cli() {
        assert!(command_requires_gh(&crate::cli::Cmd::Status {}));
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
}
