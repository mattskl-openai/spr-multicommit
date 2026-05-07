use anyhow::Result;
use std::collections::HashMap;
use tracing::info;

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::commands::common;
use crate::git::{gh_rw, normalize_branch_name, sanitize_gh_base_ref};
use crate::github::list_open_prs_for_heads;
use crate::maintenance_output::{
    MaintenanceOptions, MaintenanceRepoContext, RelinkExpectedBaseData, RelinkPrAction,
    RelinkPrDecisionData, RelinkPrsSummaryData,
};
use crate::parsing::derive_local_groups;

fn render_relink_action(action: RelinkPrAction) -> &'static str {
    match action {
        RelinkPrAction::AlreadyCorrect => "already correct",
        RelinkPrAction::Edited => "edited",
        RelinkPrAction::DryRunEdit => "would edit",
        RelinkPrAction::MissingOpenPr => "missing open pr",
    }
}

pub fn print_relink_prs_summary(summary: &RelinkPrsSummaryData) {
    if summary.decisions.is_empty() {
        info!("No local groups found; nothing to fix.");
    } else {
        for decision in &summary.decisions {
            info!(
                "{} -> {} ({})",
                decision.head_branch,
                decision.expected_base_ref,
                render_relink_action(decision.action)
            );
        }
    }
}

pub fn relink_prs(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    dry: bool,
) -> Result<RelinkPrsSummaryData> {
    let normalized_base = normalize_branch_name(base);
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        return Ok(RelinkPrsSummaryData {
            repo: MaintenanceRepoContext {
                base: normalized_base,
                prefix: prefix.to_string(),
            },
            options: MaintenanceOptions { dry_run: dry },
            expected_chain: Vec::new(),
            decisions: Vec::new(),
        });
    }
    let branch_identities = group_branch_identities(&groups, prefix)?;

    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_prs_for_heads(&heads)?;
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();
    let expected = common::build_head_base_chain(&normalized_base, &groups, prefix)?;
    let expected_chain: Vec<RelinkExpectedBaseData> = expected
        .iter()
        .enumerate()
        .map(
            |(group_idx, (head_branch, expected_base_ref))| RelinkExpectedBaseData {
                local_pr_number: group_idx + 1,
                stable_handle: common::stable_handle_text(&groups[group_idx].tag),
                head_branch: head_branch.clone(),
                expected_base_ref: expected_base_ref.clone(),
            },
        )
        .collect();

    let mut decisions: Vec<RelinkPrDecisionData> = Vec::with_capacity(expected_chain.len());
    for expected in &expected_chain {
        if let Some(pr) = prs_by_head
            .get(&canonical_branch_conflict_key(&expected.head_branch))
            .copied()
        {
            let action = if pr.base == expected.expected_base_ref {
                RelinkPrAction::AlreadyCorrect
            } else {
                gh_rw(
                    dry,
                    [
                        "pr",
                        "edit",
                        &format!("#{}", pr.number),
                        "--base",
                        &sanitize_gh_base_ref(&expected.expected_base_ref),
                    ]
                    .as_slice(),
                )?;
                if dry {
                    RelinkPrAction::DryRunEdit
                } else {
                    RelinkPrAction::Edited
                }
            };
            decisions.push(RelinkPrDecisionData {
                local_pr_number: expected.local_pr_number,
                stable_handle: expected.stable_handle.clone(),
                head_branch: expected.head_branch.clone(),
                expected_base_ref: expected.expected_base_ref.clone(),
                current_base_ref: Some(pr.base.clone()),
                remote_pr_number: Some(pr.number),
                action,
            });
        } else {
            decisions.push(RelinkPrDecisionData {
                local_pr_number: expected.local_pr_number,
                stable_handle: expected.stable_handle.clone(),
                head_branch: expected.head_branch.clone(),
                expected_base_ref: expected.expected_base_ref.clone(),
                current_base_ref: None,
                remote_pr_number: None,
                action: RelinkPrAction::MissingOpenPr,
            });
        }
    }

    Ok(RelinkPrsSummaryData {
        repo: MaintenanceRepoContext {
            base: normalized_base,
            prefix: prefix.to_string(),
        },
        options: MaintenanceOptions { dry_run: dry },
        expected_chain,
        decisions,
    })
}

#[cfg(test)]
mod tests {
    use super::relink_prs;
    use crate::maintenance_output::RelinkPrAction;
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
        git(
            repo,
            ["remote", "add", "origin", "https://github.com/o/r.git"].as_slice(),
        );
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
        let exact_open_path = repo.join("exact-open.json");
        let search_open_path = repo.join("search-open.json");
        fs::write(
            &exact_open_path,
            "{\"data\":{\"repository\":{\"pr0\":{\"nodes\":[{\"number\":17,\"headRefName\":\"skilltest/alpha\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/17\",\"autoMergeRequest\":null}]},\"pr1\":{\"nodes\":[{\"number\":22,\"headRefName\":\"skilltest/beta\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/22\",\"autoMergeRequest\":null}]}}}}",
        )
        .unwrap();
        fs::write(
            &search_open_path,
            "{\"data\":{\"pr0\":{\"nodes\":[]},\"pr1\":{\"nodes\":[]}}}",
        )
        .unwrap();
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*) cat \"{}\" ;;\n    *\"is:pr is:open head:skilltest/alpha\"*) cat \"{}\" ;;\n    *) echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"edit\" ]; then\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            exact_open_path.display(),
            search_open_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let summary = relink_prs("main", "skilltest/", "ignore", false).unwrap();

        assert_eq!(summary.decisions[0].action, RelinkPrAction::AlreadyCorrect);
        assert_eq!(summary.decisions[1].action, RelinkPrAction::Edited);
        let log = log_contents(&log_path);
        assert!(log.contains("api graphql"));
        assert!(log.contains("pr edit #22 --base skilltest/alpha"));
    }

    #[test]
    fn relink_prs_rejects_case_variant_open_head_before_edit() {
        let _lock = lock_cwd();
        let dir = init_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*) echo '{{\"data\":{{\"repository\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}}}' ;;\n    *\"is:pr is:open head:skilltest/alpha\"*) echo '{{\"data\":{{\"pr0\":{{\"nodes\":[{{\"number\":17,\"headRefName\":\"skilltest/Alpha\",\"baseRefName\":\"main\",\"state\":\"OPEN\",\"mergedAt\":null,\"closedAt\":null,\"url\":\"https://github.com/o/r/pull/17\",\"autoMergeRequest\":null}}]}},\"pr1\":{{\"nodes\":[]}}}}}}' ;;\n    *) echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"edit\" ]; then\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let err = relink_prs("main", "skilltest/", "ignore", false).unwrap_err();

        assert!(err
            .to_string()
            .contains("Exact headRefName matches are required here"));
        let log = log_contents(&log_path);
        assert!(log.contains("api graphql"));
        assert!(!log.contains("pr edit"));
    }

    #[test]
    fn relink_prs_reports_missing_open_pr_for_all_local_heads() {
        let _lock = lock_cwd();
        let dir = init_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let log_path = repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  echo '{{\"data\":{{\"repository\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}},\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}'\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"edit\" ]; then\n  echo \"unexpected gh invocation: $*\" >&2\n  exit 1\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let summary = relink_prs("main", "skilltest/", "ignore", false).unwrap();

        assert_eq!(summary.expected_chain.len(), 2);
        assert_eq!(summary.decisions.len(), 2);
        assert!(summary
            .decisions
            .iter()
            .all(|decision| decision.action == RelinkPrAction::MissingOpenPr));
        let log = log_contents(&log_path);
        assert!(log.contains("api graphql"));
        assert!(!log.contains("pr edit"));
    }
}
