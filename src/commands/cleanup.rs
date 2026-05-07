use anyhow::Result;
use tracing::info;

use crate::execution::ExecutionMode;
use crate::git::{git_rw, list_remote_branches_with_prefix};
use crate::github::list_open_pr_heads;
use crate::maintenance_output::{
    CleanupAction, CleanupDecisionData, CleanupRepoContext, CleanupSummaryData, MaintenanceOptions,
};

fn render_cleanup_action(action: CleanupAction) -> &'static str {
    match action {
        CleanupAction::Delete => "delete",
        CleanupAction::DryRunDelete => "would delete",
        CleanupAction::SkipOpenPr => "skip open pr",
    }
}

pub fn print_cleanup_summary(summary: &CleanupSummaryData) {
    if summary.remote_candidates.is_empty() {
        info!(
            "No remote branches found with prefix {}",
            summary.repo.prefix
        );
    } else {
        for decision in &summary.decisions {
            info!(
                "{} ({})",
                decision.branch,
                render_cleanup_action(decision.action)
            );
        }
    }
}

/// Delete remote branches that start with the configured prefix and have only closed PRs (or no PRs)
pub fn cleanup_remote_branches(
    prefix: &str,
    execution_mode: ExecutionMode,
) -> Result<CleanupSummaryData> {
    let dry_run = execution_mode == ExecutionMode::DryRun;
    let mut branches = list_remote_branches_with_prefix(prefix)?;
    branches.sort();
    if branches.is_empty() {
        return Ok(CleanupSummaryData {
            repo: CleanupRepoContext {
                prefix: prefix.to_string(),
            },
            options: MaintenanceOptions { dry_run },
            remote_candidates: branches,
            open_pr_heads: Vec::new(),
            decisions: Vec::new(),
            delete_batch: Vec::new(),
        });
    }
    let mut open_heads: Vec<String> = list_open_pr_heads()?.into_iter().collect();
    open_heads.sort();

    let decisions: Vec<CleanupDecisionData> = branches
        .iter()
        .map(|branch| CleanupDecisionData {
            branch: branch.clone(),
            action: if open_heads.contains(branch) {
                CleanupAction::SkipOpenPr
            } else if dry_run {
                CleanupAction::DryRunDelete
            } else {
                CleanupAction::Delete
            },
        })
        .collect();
    let delete_batch: Vec<String> = decisions
        .iter()
        .filter(|decision| {
            matches!(
                decision.action,
                CleanupAction::Delete | CleanupAction::DryRunDelete
            )
        })
        .map(|decision| decision.branch.clone())
        .collect();

    if !delete_batch.is_empty() {
        let mut owned_args: Vec<String> = vec!["push".into(), "origin".into(), "--delete".into()];
        owned_args.extend(delete_batch.iter().cloned());
        let args: Vec<&str> = owned_args.iter().map(String::as_str).collect();
        let _ = git_rw(execution_mode, &args)?;
    }

    Ok(CleanupSummaryData {
        repo: CleanupRepoContext {
            prefix: prefix.to_string(),
        },
        options: MaintenanceOptions { dry_run },
        remote_candidates: branches,
        open_pr_heads: open_heads,
        decisions,
        delete_batch,
    })
}

#[cfg(test)]
mod tests {
    use super::cleanup_remote_branches;
    use crate::execution::ExecutionMode;
    use crate::maintenance_output::CleanupAction;
    use crate::test_support::{commit_file, git, lock_cwd, write_file, DirGuard};
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::TempDir;

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

    fn init_cleanup_repo() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).unwrap();
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

        git(&repo, ["checkout", "-b", "skilltest/alpha"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha");
        git(
            &repo,
            ["push", "-u", "origin", "skilltest/alpha"].as_slice(),
        );

        git(&repo, ["checkout", "main"].as_slice());
        git(&repo, ["checkout", "-b", "skilltest/beta"].as_slice());
        commit_file(&repo, "beta.txt", "beta\n", "feat: beta");
        git(&repo, ["push", "-u", "origin", "skilltest/beta"].as_slice());

        git(&repo, ["checkout", "main"].as_slice());
        dir
    }

    fn log_contents(path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn cleanup_remote_branches_reports_sorted_skip_and_delete_sets() {
        let _lock = lock_cwd();
        let dir = init_cleanup_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  echo '[{{\"headRefName\":\"skilltest/alpha\"}}]'\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display()
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let summary = cleanup_remote_branches("skilltest/", ExecutionMode::DryRun).unwrap();

        assert_eq!(
            summary.remote_candidates,
            vec!["skilltest/alpha".to_string(), "skilltest/beta".to_string()]
        );
        assert_eq!(summary.open_pr_heads, vec!["skilltest/alpha".to_string()]);
        assert_eq!(summary.decisions.len(), 2);
        assert_eq!(summary.decisions[0].action, CleanupAction::SkipOpenPr);
        assert_eq!(summary.decisions[1].action, CleanupAction::DryRunDelete);
        assert_eq!(summary.delete_batch, vec!["skilltest/beta".to_string()]);
        let log = log_contents(&log_path);
        assert!(log.contains("pr list --state open --limit 200 --json headRefName"));
    }

    #[test]
    fn cleanup_remote_branches_returns_empty_summary_without_gh_lookup() {
        let _lock = lock_cwd();
        let dir = init_cleanup_repo();
        let repo = dir.path().join("repo");
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display()
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let summary = cleanup_remote_branches("missing/", ExecutionMode::DryRun).unwrap();

        assert!(summary.remote_candidates.is_empty());
        assert!(summary.open_pr_heads.is_empty());
        assert!(summary.decisions.is_empty());
        assert!(summary.delete_batch.is_empty());
        assert!(log_contents(&log_path).is_empty());
    }
}
