//! Drop already-merged bottom PR groups from the local stack.
//!
//! This command is post-merge local maintenance. It detects the contiguous bottom
//! prefix whose GitHub PRs are already merged, verifies those GitHub merge commits
//! are reachable from the configured base, and then rewrites only the checked-out
//! local stack. It never lands, closes, retargets, comments on, or pushes PRs.

use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use tracing::{info, warn};

use crate::branch_names::group_branch_identities;
use crate::commands::common::{self, DirtyWorktreeOutcome, NativeRebaseOutcome};
use crate::commands::restack_after_count;
use crate::commands::rewrite_resume::RewriteCommandOutcome;
use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
use crate::git::{git_is_ancestor, git_rw};
use crate::github::{
    fetch_merged_pr_merge_commit_oids, list_open_or_merged_prs_for_heads, PrInfoWithState, PrState,
};
use crate::parsing::{derive_local_groups_with_ignored, Group};
use crate::stack_metadata::RefreshMetadataContext;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MergedPrefixCandidate {
    tag: String,
    head: String,
    pr_number: u64,
    local_tip: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropMergedRewriteStrategy {
    FastRebase,
    FastResetAll,
    RestackFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DropMergedPrefixPlan {
    dropped: Vec<MergedPrefixCandidate>,
    boundary_commit: String,
    merge_commit_oids: Vec<String>,
    remaining_group_count: usize,
    strategy: DropMergedRewriteStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FastLocalRewriteOutcome {
    Completed,
    FallbackRequired,
}

impl DirtyWorktreeOutcome for FastLocalRewriteOutcome {
    fn keeps_dirty_worktree_restore_deferred(&self) -> bool {
        false
    }
}

fn select_bottom_merged_prefix(
    groups: &[Group],
    heads: &[String],
    remote_prs: &[PrInfoWithState],
) -> Result<Vec<MergedPrefixCandidate>> {
    if groups.len() != heads.len() {
        bail!(
            "internal error: {} local groups but {} synthetic branch heads",
            groups.len(),
            heads.len()
        );
    }
    let remote_prs_by_head: HashMap<&str, &PrInfoWithState> =
        remote_prs.iter().map(|pr| (pr.head.as_str(), pr)).collect();
    let mut selected = Vec::new();
    for (group, head) in groups.iter().zip(heads) {
        let remote_pr = remote_prs_by_head.get(head.as_str()).copied();
        if let Some(remote_pr) = remote_pr {
            if remote_pr.state == PrState::Merged {
                let local_tip = group.commits.last().cloned().ok_or_else(|| {
                    anyhow!("local PR group pr:{} has no local commits", group.tag)
                })?;
                selected.push(MergedPrefixCandidate {
                    tag: group.tag.clone(),
                    head: head.clone(),
                    pr_number: remote_pr.number,
                    local_tip,
                });
            } else {
                break;
            }
        } else {
            break;
        }
    }
    Ok(selected)
}

fn build_drop_merged_prefix_plan(
    leading_ignored: &[String],
    groups: &[Group],
    selected: &[MergedPrefixCandidate],
    merge_commit_oids_by_pr_number: &HashMap<u64, String>,
) -> Result<DropMergedPrefixPlan> {
    let boundary_commit = selected
        .last()
        .map(|candidate| candidate.local_tip.clone())
        .ok_or_else(|| anyhow!("No bottom merged PR groups found."))?;
    let merge_commit_oids = selected
        .iter()
        .map(|candidate| {
            merge_commit_oids_by_pr_number
                .get(&candidate.pr_number)
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "Merged GitHub PR #{} has no merge commit OID",
                        candidate.pr_number
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let drop_count = selected.len();
    let remaining_group_count = groups.len().saturating_sub(drop_count);
    let dropped_prefix_has_ignored_work = !leading_ignored.is_empty()
        || groups
            .iter()
            .take(drop_count)
            .any(|group| !group.ignored_after.is_empty());
    let strategy = if dropped_prefix_has_ignored_work {
        DropMergedRewriteStrategy::RestackFallback
    } else if remaining_group_count == 0 {
        DropMergedRewriteStrategy::FastResetAll
    } else {
        DropMergedRewriteStrategy::FastRebase
    };
    Ok(DropMergedPrefixPlan {
        dropped: selected.to_vec(),
        boundary_commit,
        merge_commit_oids,
        remaining_group_count,
        strategy,
    })
}

fn verify_merge_commits_are_in_base(plan: &DropMergedPrefixPlan, base: &str) -> Result<()> {
    for (candidate, merge_commit_oid) in plan.dropped.iter().zip(&plan.merge_commit_oids) {
        if !git_is_ancestor(merge_commit_oid, base)? {
            bail!(
                "Merged GitHub PR #{} ({}) has merge commit {}, but that commit is not an ancestor of SPR base {}. Fetch/update the configured base before dropping local groups.",
                candidate.pr_number,
                candidate.head,
                merge_commit_oid,
                base
            );
        }
    }
    Ok(())
}

fn log_drop_plan(plan: &DropMergedPrefixPlan, dry: bool, base: &str) {
    let handles = plan
        .dropped
        .iter()
        .map(|candidate| format!("pr:{} (#{})", candidate.tag, candidate.pr_number))
        .collect::<Vec<_>>()
        .join(", ");
    let mode = if dry {
        "DRY-RUN: would drop"
    } else {
        "Dropping"
    };
    info!(
        "{} bottom merged PR prefix: {} onto {} (remaining PR groups: {}, strategy: {:?})",
        mode, handles, base, plan.remaining_group_count, plan.strategy
    );
}

fn run_native_rebase(
    dry: bool,
    base: &str,
    boundary_commit: &str,
    cur_branch: &str,
) -> Result<FastLocalRewriteOutcome> {
    let args = ["rebase", "--onto", base, boundary_commit, cur_branch];
    match common::run_native_rebase_with_abort(dry, args.as_slice(), "native drop-merged-prefix")? {
        NativeRebaseOutcome::Completed => Ok(FastLocalRewriteOutcome::Completed),
        NativeRebaseOutcome::Aborted => {
            warn!(
                "Native drop-merged-prefix rebase failed and was aborted; falling back to existing restack replay"
            );
            Ok(FastLocalRewriteOutcome::FallbackRequired)
        }
    }
}

fn execute_fast_local_rewrite(
    metadata_context: &RefreshMetadataContext,
    plan: &DropMergedPrefixPlan,
    safe: bool,
    dry: bool,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<FastLocalRewriteOutcome> {
    common::with_dirty_worktree_policy(
        dry,
        "spr drop-merged-prefix",
        dirty_worktree_policy,
        |_deferred_dirty_worktree_restore| {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            if safe {
                let _ = common::create_backup_tag(dry, "drop-merged-prefix", &cur_branch, &short)?;
            }
            let outcome = match plan.strategy {
                DropMergedRewriteStrategy::FastResetAll => {
                    common::reset_current_branch_to(dry, &metadata_context.base)?;
                    FastLocalRewriteOutcome::Completed
                }
                DropMergedRewriteStrategy::FastRebase => run_native_rebase(
                    dry,
                    &metadata_context.base,
                    &plan.boundary_commit,
                    &cur_branch,
                )?,
                DropMergedRewriteStrategy::RestackFallback => {
                    unreachable!("restack fallback is executed outside the fast-rewrite path")
                }
            };
            if outcome == FastLocalRewriteOutcome::Completed && !dry {
                crate::stack_metadata::refresh_metadata_for_current_checkout(
                    &metadata_context.base,
                    &metadata_context.prefix,
                    &metadata_context.ignore_tag,
                )?;
            }
            Ok(outcome)
        },
    )
}

pub fn drop_merged_prefix(
    metadata_context: &RefreshMetadataContext,
    safe: bool,
    dry: bool,
    restack_conflict_policy: RestackConflictPolicy,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    git_rw(dry, ["fetch", "origin"].as_slice())?;

    let (_merge_base, leading_ignored, groups) =
        derive_local_groups_with_ignored(&metadata_context.base, &metadata_context.ignore_tag)?;
    if groups.is_empty() {
        bail!("No local PR groups found; nothing to drop.");
    }
    let heads = group_branch_identities(&groups, &metadata_context.prefix)?
        .into_iter()
        .map(|identity| identity.exact)
        .collect::<Vec<_>>();
    let remote_prs = list_open_or_merged_prs_for_heads(&heads)?;
    let selected = select_bottom_merged_prefix(&groups, &heads, &remote_prs)?;
    if selected.is_empty() {
        bail!("No bottom merged PR groups found.");
    }
    let pr_numbers = selected
        .iter()
        .map(|candidate| candidate.pr_number)
        .collect::<Vec<_>>();
    let merge_commit_oids_by_pr_number = fetch_merged_pr_merge_commit_oids(&pr_numbers)?;
    let plan = build_drop_merged_prefix_plan(
        &leading_ignored,
        &groups,
        &selected,
        &merge_commit_oids_by_pr_number,
    )?;
    verify_merge_commits_are_in_base(&plan, &metadata_context.base)?;
    log_drop_plan(&plan, dry, &metadata_context.base);

    let fast_outcome = if plan.strategy == DropMergedRewriteStrategy::RestackFallback {
        FastLocalRewriteOutcome::FallbackRequired
    } else {
        execute_fast_local_rewrite(metadata_context, &plan, safe, dry, dirty_worktree_policy)?
    };
    if fast_outcome == FastLocalRewriteOutcome::FallbackRequired {
        restack_after_count(
            metadata_context,
            plan.dropped.len(),
            safe,
            dry,
            restack_conflict_policy,
            dirty_worktree_policy,
        )
    } else {
        info!(
            "Dropped {} merged PR group(s). Run `spr update` to publish remaining PR branch updates.",
            plan.dropped.len()
        );
        Ok(RewriteCommandOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_drop_merged_prefix_plan, drop_merged_prefix, select_bottom_merged_prefix,
        verify_merge_commits_are_in_base, DropMergedRewriteStrategy, MergedPrefixCandidate,
    };
    use crate::commands::RewriteCommandOutcome;
    use crate::config::{DirtyWorktreePolicy, RestackConflictPolicy};
    use crate::github::{PrInfoWithState, PrState};
    use crate::parsing::Group;
    use crate::stack_metadata::RefreshMetadataContext;
    use crate::test_support::{commit_file, git, lock_cwd, write_file, DirGuard};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::TempDir;

    fn group(tag: &str, commits: &[&str]) -> Group {
        Group {
            tag: tag.to_string(),
            subjects: vec![format!("feat: {tag}")],
            commits: commits.iter().map(|commit| (*commit).to_string()).collect(),
            first_message: Some(format!("feat: {tag} pr:{tag}")),
            ignored_after: Vec::new(),
        }
    }

    fn remote_pr(number: u64, head: &str, state: PrState) -> PrInfoWithState {
        PrInfoWithState {
            number,
            head: head.to_string(),
            base: "main".to_string(),
            state,
            url: format!("https://github.com/o/r/pull/{number}"),
        }
    }

    fn selected_candidate(
        tag: &str,
        head: &str,
        pr_number: u64,
        local_tip: &str,
    ) -> MergedPrefixCandidate {
        MergedPrefixCandidate {
            tag: tag.to_string(),
            head: head.to_string(),
            pr_number,
            local_tip: local_tip.to_string(),
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

    fn install_gh_wrapper(
        exact_open_json: &Path,
        exact_merged_json: &Path,
        merged_number_json: &Path,
    ) -> (TempDir, EnvVarGuard) {
        let wrapper_dir = tempfile::tempdir().unwrap();
        let script_path = wrapper_dir.path().join("gh");
        fs::write(
            &script_path,
            format!(
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'gh test wrapper'\n  exit 0\nfi\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"pullRequest(number: 10)\"*\"mergeCommit\"*) cat \"{}\" ;;\n    *\"states:[OPEN]\"*) cat \"{}\" ;;\n    *\"states:[MERGED]\"*) cat \"{}\" ;;\n    *\"is:pr is:open head:\"*) echo '{{\"data\":{{\"pr0\":{{\"nodes\":[]}},\"pr1\":{{\"nodes\":[]}}}}}}' ;;\n    *) echo '{{\"data\":{{}}}}' ;;\n  esac\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  echo '[]'\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
                merged_number_json.display(),
                exact_open_json.display(),
                exact_merged_json.display(),
            ),
        )
        .unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();
        let path_guard = EnvVarGuard::set(
            "PATH",
            format!(
                "{}:{}",
                wrapper_dir.path().display(),
                env::var("PATH").unwrap_or_default()
            ),
        );
        (wrapper_dir, path_guard)
    }

    #[test]
    fn select_bottom_merged_prefix_keeps_only_contiguous_merged_bottom() {
        let groups = vec![
            group("alpha", &["a1", "a2"]),
            group("beta", &["b1"]),
            group("gamma", &["g1"]),
        ];
        let heads = vec![
            "test-spr/alpha".to_string(),
            "test-spr/beta".to_string(),
            "test-spr/gamma".to_string(),
        ];
        let selected = select_bottom_merged_prefix(
            &groups,
            &heads,
            &[
                remote_pr(10, "test-spr/alpha", PrState::Merged),
                remote_pr(11, "test-spr/beta", PrState::Merged),
                remote_pr(12, "test-spr/gamma", PrState::Open),
            ],
        )
        .unwrap();

        assert_eq!(
            selected,
            vec![
                selected_candidate("alpha", "test-spr/alpha", 10, "a2"),
                selected_candidate("beta", "test-spr/beta", 11, "b1"),
            ]
        );
    }

    #[test]
    fn select_bottom_merged_prefix_returns_empty_when_bottom_pr_is_open() {
        let groups = vec![group("alpha", &["a1"]), group("beta", &["b1"])];
        let heads = vec!["test-spr/alpha".to_string(), "test-spr/beta".to_string()];
        let selected = select_bottom_merged_prefix(
            &groups,
            &heads,
            &[
                remote_pr(10, "test-spr/alpha", PrState::Open),
                remote_pr(11, "test-spr/beta", PrState::Merged),
            ],
        )
        .unwrap();

        assert!(selected.is_empty());
    }

    #[test]
    fn build_plan_requires_merge_commit_oid_for_each_selected_pr() {
        let groups = vec![group("alpha", &["a1"]), group("beta", &["b1"])];
        let selected = vec![selected_candidate("alpha", "test-spr/alpha", 10, "a1")];
        let err =
            build_drop_merged_prefix_plan(&[], &groups, &selected, &HashMap::new()).unwrap_err();

        assert!(err
            .to_string()
            .contains("Merged GitHub PR #10 has no merge commit OID"));
    }

    #[test]
    fn build_plan_chooses_restack_fallback_when_dropped_prefix_has_ignored_commit() {
        let mut alpha = group("alpha", &["a1"]);
        alpha.ignored_after = vec!["i1".to_string()];
        let groups = vec![alpha, group("beta", &["b1"])];
        let selected = vec![selected_candidate("alpha", "test-spr/alpha", 10, "a1")];
        let plan = build_drop_merged_prefix_plan(
            &[],
            &groups,
            &selected,
            &HashMap::from([(10, "merge-alpha".to_string())]),
        )
        .unwrap();

        assert_eq!(plan.boundary_commit, "a1");
        assert_eq!(plan.merge_commit_oids, vec!["merge-alpha".to_string()]);
        assert_eq!(plan.remaining_group_count, 1);
        assert_eq!(plan.strategy, DropMergedRewriteStrategy::RestackFallback);
    }

    #[test]
    fn verify_merge_commits_are_in_base_rejects_oid_missing_from_base() {
        let _lock = lock_cwd();
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _guard = DirGuard::change_to(repo);
        let base = commit_file(repo, "base.txt", "base\n", "feat: base update");
        git(repo, ["checkout", "-b", "side", "HEAD^"].as_slice());
        let unrelated = commit_file(repo, "side.txt", "side\n", "feat: unrelated side");
        git(repo, ["checkout", "main"].as_slice());
        let selected = vec![selected_candidate("alpha", "test-spr/alpha", 10, "a1")];
        let plan = build_drop_merged_prefix_plan(
            &[],
            &[group("alpha", &["a1"])],
            &selected,
            &HashMap::from([(10, unrelated.clone())]),
        )
        .unwrap();

        let err = verify_merge_commits_are_in_base(&plan, &base).unwrap_err();

        assert!(err.to_string().contains("GitHub PR #10"));
        assert!(err.to_string().contains("is not an ancestor of SPR base"));
    }

    #[test]
    fn drop_merged_prefix_fast_rebases_remaining_stack_onto_fetched_base() {
        let _lock = lock_cwd();
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let origin = dir.path().join("origin.git");
        fs::create_dir(&repo).unwrap();
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        git(
            &repo,
            ["init", "--bare", origin.to_str().unwrap()].as_slice(),
        );
        git(
            &repo,
            [
                "remote",
                "add",
                "origin",
                "git@github.com:example/spr-drop-test.git",
            ]
            .as_slice(),
        );
        git(
            &repo,
            [
                "config",
                &format!(
                    "url.{}.insteadOf",
                    origin.to_str().unwrap().trim_end_matches('/')
                ),
                "git@github.com:example/spr-drop-test.git",
            ]
            .as_slice(),
        );
        write_file(&repo, "README.md", "init\n");
        git(&repo, ["add", "README.md"].as_slice());
        git(&repo, ["commit", "-m", "init"].as_slice());
        git(&repo, ["push", "-u", "origin", "main"].as_slice());

        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha\n", "feat: alpha pr:alpha");
        commit_file(&repo, "beta.txt", "beta\n", "feat: beta pr:beta");
        let original_stack_tip = git(&repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();
        let original_short = git(&repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();

        git(&repo, ["checkout", "main"].as_slice());
        let merge_alpha = commit_file(&repo, "alpha.txt", "alpha\n", "squash merge PR #10 alpha");
        git(&repo, ["push", "origin", "main"].as_slice());
        git(&repo, ["checkout", "stack"].as_slice());

        let data_dir = tempfile::tempdir().unwrap();
        let exact_open = data_dir.path().join("exact-open.json");
        let exact_merged = data_dir.path().join("exact-merged.json");
        let merged_number = data_dir.path().join("merged-number.json");
        fs::write(
            &exact_open,
            r#"{"data":{"repository":{"pr0":{"nodes":[]},"pr1":{"nodes":[{"number":11,"headRefName":"test-spr/beta","baseRefName":"test-spr/alpha","state":"OPEN","mergedAt":null,"closedAt":null,"url":"https://github.com/example/spr-drop-test/pull/11","autoMergeRequest":null}]}}}}"#,
        )
        .unwrap();
        fs::write(
            &exact_merged,
            r#"{"data":{"repository":{"pr0":{"nodes":[{"number":10,"headRefName":"test-spr/alpha","baseRefName":"main","state":"MERGED","mergedAt":"2026-04-09T00:00:00Z","closedAt":"2026-04-09T00:00:00Z","url":"https://github.com/example/spr-drop-test/pull/10","autoMergeRequest":null}]}}}}"#,
        )
        .unwrap();
        fs::write(
            &merged_number,
            format!(
                r#"{{"data":{{"repository":{{"pr0":{{"number":10,"state":"MERGED","mergeCommit":{{"oid":"{}"}}}}}}}}}}"#,
                merge_alpha
            ),
        )
        .unwrap();
        let (_wrapper, _path_guard) =
            install_gh_wrapper(&exact_open, &exact_merged, &merged_number);
        let _guard = DirGuard::change_to(&repo);

        let outcome = drop_merged_prefix(
            &RefreshMetadataContext {
                base: "origin/main".to_string(),
                prefix: "test-spr/".to_string(),
                ignore_tag: "ignore".to_string(),
            },
            true,
            false,
            RestackConflictPolicy::Halt,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap();

        assert_eq!(outcome, RewriteCommandOutcome::Completed);
        assert_eq!(
            git(
                &repo,
                ["log", "--format=%s", "--reverse", "origin/main..HEAD"].as_slice()
            )
            .lines()
            .collect::<Vec<_>>(),
            vec!["feat: beta pr:beta"]
        );
        assert_eq!(
            fs::read_to_string(repo.join("alpha.txt")).unwrap(),
            "alpha\n"
        );
        assert_eq!(fs::read_to_string(repo.join("beta.txt")).unwrap(), "beta\n");
        assert_eq!(
            git(&repo, ["merge-base", "origin/main", "HEAD"].as_slice())
                .trim()
                .to_string(),
            git(&repo, ["rev-parse", "origin/main"].as_slice())
                .trim()
                .to_string()
        );
        assert_eq!(
            git(
                &repo,
                [
                    "rev-parse",
                    &format!("backup/drop-merged-prefix/stack-{original_short}")
                ]
                .as_slice()
            )
            .trim(),
            original_stack_tip
        );
    }
}
