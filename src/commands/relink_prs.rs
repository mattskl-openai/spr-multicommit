use anyhow::{bail, Result};
use std::collections::HashMap;
use tracing::{info, warn};

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::commands::common;
use crate::git::{gh_rw, normalize_branch_name, sanitize_gh_base_ref};
use crate::github::list_open_prs_for_heads;
use crate::parsing::derive_local_groups;

pub fn relink_prs(base: &str, prefix: &str, ignore_tag: &str, dry: bool) -> Result<()> {
    let base_n = normalize_branch_name(base);
    // Build local expected stack from base..HEAD
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No local groups found; nothing to fix.");
        return Ok(());
    }
    let branch_identities = group_branch_identities(&groups, prefix)?;

    // Existing PRs map by head
    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_prs_for_heads(&heads)?;
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Expected connectivity bottom-up
    let expected = common::build_head_base_chain(&base_n, &groups, prefix)?;

    // Apply base edits where needed
    for (head, want_base) in expected {
        if let Some(pr) = prs_by_head
            .get(&canonical_branch_conflict_key(&head))
            .copied()
        {
            if pr.base != want_base {
                info!(
                    "Updating base of {} (#{}) from {} to {}",
                    head, pr.number, pr.base, want_base
                );
                gh_rw(
                    dry,
                    [
                        "pr",
                        "edit",
                        &format!("#{}", pr.number),
                        "--base",
                        &sanitize_gh_base_ref(&want_base),
                    ]
                    .as_slice(),
                )?;
            } else {
                info!("{} (#{}) already basing on {}", head, pr.number, want_base);
            }
        } else {
            warn!("No open PR found for {}; skipping", head);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::relink_prs;
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
            if let Some(value) = &self.original {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn init_stack_repo() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(repo, "story.txt", "base\n");
        git(repo, ["add", "story.txt"].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "alpha\n", "feat: alpha pr:alpha");
        commit_file(repo, "beta.txt", "beta\n", "feat: beta pr:beta");
        dir
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

    fn log_contents(path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn relink_prs_updates_bases_for_exact_open_heads() {
        let _lock = lock_cwd();
        let dir = init_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  echo '[{{\"number\":17,\"headRefName\":\"skilltest/alpha\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/17\",\"autoMergeRequest\":null}},{{\"number\":22,\"headRefName\":\"skilltest/beta\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/22\",\"autoMergeRequest\":null}}]'\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"edit\" ]; then\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        relink_prs("main", "skilltest/", "ignore", false).unwrap();

        let log = log_contents(&log_path);
        assert!(log.contains("pr list --state open --search head:skilltest/"));
        assert!(log.contains("pr edit #22 --base skilltest/alpha"));
        assert!(!log.contains("pr edit #17 --base"));
    }

    #[test]
    fn relink_prs_rejects_case_variant_open_head_before_edit() {
        let _lock = lock_cwd();
        let dir = init_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  echo '[{{\"number\":17,\"headRefName\":\"skilltest/Alpha\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/17\",\"autoMergeRequest\":null}}]'\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"edit\" ]; then\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let err = relink_prs("main", "skilltest/", "ignore", false).unwrap_err();

        assert!(err
            .to_string()
            .contains("Exact headRefName matches are required here"));
        let log = log_contents(&log_path);
        assert!(log.contains("pr list --state open --search head:skilltest/"));
        assert!(!log.contains("pr edit"));
    }
}
