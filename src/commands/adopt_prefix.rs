//! Adopt rewritten lower-stack history as the new prefix of a live stack.

use anyhow::{anyhow, bail, Result};
use tracing::info;

use crate::adopt_prefix_output::{AdoptPrefixPreviewData, AdoptPrefixPreviewGroup};
use crate::commands::common::{self, CherryPickOp};
use crate::commands::owning_stack;
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteDestinationKind,
    RewriteSession,
};
use crate::config::DirtyWorktreePolicy;
use crate::execution::ExecutionMode;
use crate::git::{git_rev_parse, git_ro, git_rw};
use crate::parsing::{
    derive_groups_between_with_ignored, derive_local_groups_with_leading_commits,
    split_groups_for_update, Group, ParsedGroups,
};
use crate::stack_metadata::{
    build_snapshot_from_groups, preflight_snapshot_update, RefreshMetadataContext, StackId,
};

#[derive(Debug, Clone)]
pub struct AdoptPrefixPlan {
    pub candidate_head: String,
    pub candidate_groups: Vec<Group>,
    pub owning_stack_id: StackId,
    pub owning_stack_branch: String,
    pub old_stack_head: String,
    pub merge_base: String,
    pub replaced_old_boundary: String,
    pub replay_suffix_groups: Vec<Group>,
    pub operations: Vec<CherryPickOp>,
    pub publishable_before: Vec<String>,
    pub publishable_after: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptPrefixOutcome {
    pub rewrite_outcome: RewriteCommandOutcome,
    pub owning_stack_branch: String,
}

impl AdoptPrefixPlan {
    fn preview_data(&self, safe_requested: bool) -> AdoptPrefixPreviewData {
        AdoptPrefixPreviewData {
            candidate_head: self.candidate_head.clone(),
            candidate_groups: self.candidate_groups.iter().map(group_preview).collect(),
            owning_stack_id: self.owning_stack_id.0.clone(),
            owning_stack_branch: self.owning_stack_branch.clone(),
            old_stack_head: self.old_stack_head.clone(),
            merge_base: self.merge_base.clone(),
            replaced_old_boundary: self.replaced_old_boundary.clone(),
            replay_suffix_groups: self
                .replay_suffix_groups
                .iter()
                .map(group_preview)
                .collect(),
            replay_operation_count: self.operations.len(),
            publishable_before: self.publishable_before.clone(),
            publishable_after: self.publishable_after.clone(),
            would_create_backup_tag: safe_requested,
            would_create_temp_worktree: !self.operations.is_empty(),
            would_move_stack_branch: self.old_stack_head != self.candidate_head
                || !self.operations.is_empty(),
            would_refresh_stack_metadata: true,
            not_validated: [
                "cherry-pick conflicts",
                "tests",
                "pre-push hooks",
                "GitHub mergeability",
                "spr update result",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        }
    }
}

fn group_preview(group: &Group) -> AdoptPrefixPreviewGroup {
    AdoptPrefixPreviewGroup {
        stable_handle: group.selector_text(),
        commit_count: group.commits.len(),
        ignored_after_count: group.ignored_after.len(),
    }
}

fn publishable_selector_sequence(leading_ignored: &[String], groups: &[Group]) -> Vec<String> {
    let (publishable, _skipped_handles) = split_groups_for_update(leading_ignored, groups.to_vec());
    owning_stack::selector_sequence(&publishable)
}

fn ensure_exact_bottom_sequence(
    candidate_selectors: &[String],
    old_groups: &[Group],
) -> Result<()> {
    let old_prefix = owning_stack::selector_sequence(
        old_groups
            .get(..candidate_selectors.len())
            .unwrap_or(old_groups),
    );
    if candidate_selectors.len() > old_groups.len() || old_prefix != candidate_selectors {
        bail!(
            "candidate selector sequence [{}] is not an exact bottom sequence of live stack [{}]",
            candidate_selectors.join(", "),
            owning_stack::selector_sequence(old_groups).join(", ")
        );
    }
    Ok(())
}

fn build_replay_suffix(
    candidate_groups: &[Group],
    old_groups: &[Group],
) -> Result<(String, Vec<Group>, Vec<CherryPickOp>)> {
    let matched_count = candidate_groups.len();
    let old_boundary_group = old_groups
        .get(matched_count.saturating_sub(1))
        .ok_or_else(|| anyhow!("candidate prefix has no matching old boundary group"))?;
    let candidate_trailing_ignored = candidate_groups
        .last()
        .map(|group| !group.ignored_after.is_empty())
        .unwrap_or(false);
    let replaced_old_boundary = if candidate_trailing_ignored {
        old_boundary_group
            .ignored_after
            .last()
            .cloned()
            .unwrap_or_else(|| {
                old_boundary_group
                    .commits
                    .last()
                    .cloned()
                    .expect("groups have at least one commit")
            })
    } else {
        old_boundary_group
            .commits
            .last()
            .cloned()
            .expect("groups have at least one commit")
    };

    let replay_boundary_ignored = if candidate_trailing_ignored {
        Vec::new()
    } else {
        old_boundary_group.ignored_after.clone()
    };
    let replay_suffix_groups = old_groups[matched_count..].to_vec();
    let mut operations = Vec::new();
    if let Some(operation) = CherryPickOp::from_commits(&replay_boundary_ignored) {
        operations.push(operation);
    }
    for group in &replay_suffix_groups {
        if let Some(operation) = CherryPickOp::from_commits(&group.commits) {
            operations.push(operation);
        }
        if let Some(operation) = CherryPickOp::from_commits(&group.ignored_after) {
            operations.push(operation);
        }
    }
    Ok((replaced_old_boundary, replay_suffix_groups, operations))
}

fn build_post_adoption_groups(
    candidate_groups: &[Group],
    old_groups: &[Group],
) -> Result<Vec<Group>> {
    let mut groups = candidate_groups.to_vec();
    let matched_count = groups.len();
    let old_boundary_group = old_groups
        .get(matched_count.saturating_sub(1))
        .ok_or_else(|| anyhow!("candidate prefix has no matching old boundary group"))?;
    if let Some(last_group) = groups.last_mut() {
        if last_group.ignored_after.is_empty() {
            last_group.ignored_after = old_boundary_group.ignored_after.clone();
        }
    }
    groups.extend_from_slice(&old_groups[matched_count..]);
    Ok(groups)
}

fn build_adopt_prefix_plan(metadata_context: &RefreshMetadataContext) -> Result<AdoptPrefixPlan> {
    let (
        candidate_merge_base,
        ParsedGroups {
            leading_ungrouped: candidate_leading_ungrouped,
            leading_ignored: candidate_leading_ignored,
            groups: candidate_groups,
        },
    ) = derive_local_groups_with_leading_commits(
        &metadata_context.base,
        &metadata_context.ignore_tag,
    )?;
    if !candidate_leading_ungrouped.is_empty() {
        bail!(
            "candidate history contains ungrouped commits before the first explicit selector; every adopted commit must belong to an explicit selector group"
        );
    }
    let candidate_selectors = owning_stack::selector_sequence(&candidate_groups);
    let resolved_stack = owning_stack::load_recorded_owning_stack_for_candidate_groups(
        metadata_context,
        &candidate_groups,
    )?
    .ok_or_else(|| anyhow!("no stack metadata recorded for this repository"))?;
    let owning_stack_id = resolved_stack.stack_id;
    let owning_stack_branch = resolved_stack.stack_branch;
    let metadata = resolved_stack.metadata;
    let branch_ref = format!("refs/heads/{owning_stack_branch}");
    let old_stack_head = git_rev_parse(&branch_ref)?;
    let (old_merge_base, old_leading_ignored, old_groups) = derive_groups_between_with_ignored(
        &metadata_context.base,
        &branch_ref,
        &metadata_context.ignore_tag,
    )?;
    if candidate_merge_base != old_merge_base {
        bail!(
            "candidate history uses merge base {}, but live stack {} uses {}; use `spr restack` for base changes before adoption",
            candidate_merge_base,
            owning_stack_branch,
            old_merge_base
        );
    }
    ensure_exact_bottom_sequence(&candidate_selectors, &old_groups)?;
    let publishable_before = publishable_selector_sequence(&old_leading_ignored, &old_groups);
    let post_adoption_groups = build_post_adoption_groups(&candidate_groups, &old_groups)?;
    let publishable_after =
        publishable_selector_sequence(&candidate_leading_ignored, &post_adoption_groups);
    if publishable_before != publishable_after {
        bail!(
            "adopting candidate prefix would change publishable selector sequence from [{}] to [{}]",
            publishable_before.join(", "),
            publishable_after.join(", ")
        );
    }
    let candidate_head = git_rev_parse("HEAD")?;
    let post_adoption_snapshot = build_snapshot_from_groups(
        &owning_stack_branch,
        &candidate_head,
        &metadata_context.base,
        &metadata_context.prefix,
        &post_adoption_groups,
    )?;
    preflight_snapshot_update(&metadata, &post_adoption_snapshot)?;
    let (replaced_old_boundary, replay_suffix_groups, operations) =
        build_replay_suffix(&candidate_groups, &old_groups)?;
    Ok(AdoptPrefixPlan {
        candidate_head,
        candidate_groups,
        owning_stack_id,
        owning_stack_branch,
        old_stack_head,
        merge_base: old_merge_base,
        replaced_old_boundary,
        replay_suffix_groups,
        operations,
        publishable_before,
        publishable_after,
    })
}

pub fn preview_adopt_prefix(
    metadata_context: &RefreshMetadataContext,
    safe_requested: bool,
) -> Result<AdoptPrefixPreviewData> {
    Ok(build_adopt_prefix_plan(metadata_context)?.preview_data(safe_requested))
}

fn move_branch_ref_to_candidate(
    plan: &AdoptPrefixPlan,
    metadata_context: &RefreshMetadataContext,
    safe: bool,
    execution_mode: ExecutionMode,
) -> Result<()> {
    owning_stack::ensure_branch_not_checked_out(&plan.owning_stack_branch)?;
    let old_short = git_ro(["rev-parse", "--short", &plan.old_stack_head].as_slice())?
        .trim()
        .to_string();
    if safe {
        let _ = common::create_backup_tag_at(
            execution_mode,
            "adopt-prefix",
            &plan.owning_stack_branch,
            &old_short,
            &plan.old_stack_head,
        )?;
    }
    let branch_ref = format!("refs/heads/{}", plan.owning_stack_branch);
    let _ = git_rw(
        execution_mode,
        [
            "update-ref",
            branch_ref.as_str(),
            plan.candidate_head.as_str(),
            plan.old_stack_head.as_str(),
        ]
        .as_slice(),
    )?;
    if execution_mode == ExecutionMode::Apply {
        crate::stack_metadata::refresh_metadata_for_branch(
            &rewrite_resume::current_repo_root()?,
            &plan.owning_stack_branch,
            metadata_context,
            None,
        )?;
    }
    Ok(())
}

pub fn adopt_prefix(
    metadata_context: &RefreshMetadataContext,
    safe: bool,
    execution_mode: ExecutionMode,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<AdoptPrefixOutcome> {
    let plan = build_adopt_prefix_plan(metadata_context)?;
    owning_stack::ensure_branch_not_checked_out(&plan.owning_stack_branch)?;
    let rewrite_outcome = common::with_dirty_worktree_policy(
        execution_mode,
        "spr adopt-prefix",
        dirty_worktree_policy,
        |deferred_dirty_worktree_restore| {
            if plan.operations.is_empty() {
                move_branch_ref_to_candidate(&plan, metadata_context, safe, execution_mode)?;
                info!(
                    "Adopted {} group(s) onto {} without replaying a suffix.",
                    plan.candidate_groups.len(),
                    plan.owning_stack_branch
                );
                Ok(RewriteCommandOutcome::Completed)
            } else {
                let candidate_short = git_ro(["rev-parse", "--short", "HEAD"].as_slice())?
                    .trim()
                    .to_string();
                let old_short = git_ro(["rev-parse", "--short", &plan.old_stack_head].as_slice())?
                    .trim()
                    .to_string();
                let backup_tag = if safe {
                    Some(common::create_backup_tag_at(
                        execution_mode,
                        "adopt-prefix",
                        &plan.owning_stack_branch,
                        &old_short,
                        &plan.old_stack_head,
                    )?)
                } else {
                    None
                };
                let resume_path = rewrite_resume::prepare_resume_path_for_new_session(
                    execution_mode,
                    RewriteCommandKind::AdoptPrefix,
                    &plan.owning_stack_branch,
                    &plan.old_stack_head,
                )?;
                let (tmp_path, tmp_branch) = common::create_temp_worktree(
                    execution_mode,
                    "adopt-prefix",
                    &plan.candidate_head,
                    &candidate_short,
                )?;
                let outcome = rewrite_resume::run_rewrite_session(
                    execution_mode,
                    RewriteSession {
                        command_kind: RewriteCommandKind::AdoptPrefix,
                        conflict_policy: RewriteConflictPolicy::Suspend,
                        original_worktree_root: rewrite_resume::current_repo_root()?,
                        original_branch: plan.owning_stack_branch.clone(),
                        original_head: plan.old_stack_head.clone(),
                        destination_kind: RewriteDestinationKind::UncheckedOutBranchRef,
                        resume_path,
                        temp_branch: tmp_branch,
                        temp_worktree_path: tmp_path,
                        backup_tag,
                        operations: plan.operations.clone(),
                        deferred_dirty_worktree_restore,
                        post_success_hint: Some(
                            "No GitHub changes were made. Run `spr update` after inspecting the rewritten stack."
                                .to_string(),
                        ),
                        metadata_refresh_context: Some(metadata_context.clone()),
                    },
                )?;
                if outcome == RewriteCommandOutcome::Completed {
                    info!(
                        "Adopted {} group(s) onto {} and replayed the remaining raw stack history.",
                        plan.candidate_groups.len(),
                        plan.owning_stack_branch
                    );
                }
                Ok(outcome)
            }
        },
    )?;
    Ok(AdoptPrefixOutcome {
        rewrite_outcome,
        owning_stack_branch: plan.owning_stack_branch,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        adopt_prefix, build_post_adoption_groups, build_replay_suffix,
        ensure_exact_bottom_sequence, preview_adopt_prefix, publishable_selector_sequence,
    };
    use crate::commands::RewriteCommandOutcome;
    use crate::config::DirtyWorktreePolicy;
    use crate::execution::ExecutionMode;
    use crate::group_markers::GroupMarker;
    use crate::parsing::Group;
    use crate::stack_metadata::{refresh_metadata_for_branch, RefreshMetadataContext};
    use crate::test_support::{commit_file, git, lock_cwd, DirGuard};
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn group(tag: &str, ignored_after: &[&str]) -> Group {
        Group {
            marker: GroupMarker::PrLabel(tag.to_string()),
            subjects: vec![format!("feat: {tag}")],
            commits: vec![format!("{tag}1")],
            first_message: Some(format!("feat: {tag}\n\npr:{tag}")),
            ignored_after: ignored_after.iter().map(|sha| (*sha).to_string()).collect(),
        }
    }

    fn metadata_context() -> RefreshMetadataContext {
        RefreshMetadataContext {
            base: "main".to_string(),
            prefix: "dank-spr/".to_string(),
            ignore_tag: "ignore".to_string(),
        }
    }

    struct AdoptPrefixRepo {
        _dir: TempDir,
        repo: PathBuf,
    }

    impl AdoptPrefixRepo {
        fn init() -> Self {
            let dir = crate::test_support::init_repo();
            let repo = dir.path().to_path_buf();
            git(&repo, ["checkout", "-b", "stack"].as_slice());
            commit_file(
                &repo,
                "alpha.txt",
                "old-alpha-1\n",
                "feat: alpha\n\npr:alpha",
            );
            commit_file(
                &repo,
                "alpha.txt",
                "old-alpha-1\nold-alpha-2\n",
                "feat: alpha follow-up",
            );
            commit_file(&repo, "beta.txt", "beta\n", "feat: beta\n\npr:beta");
            commit_file(&repo, "gamma.txt", "gamma\n", "feat: gamma\n\npr:gamma");
            refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
                .unwrap();
            git(&repo, ["checkout", "-b", "candidate", "main"].as_slice());
            commit_file(
                &repo,
                "alpha.txt",
                "new-alpha-1\n",
                "feat: alpha rewritten\n\npr:alpha",
            );
            commit_file(
                &repo,
                "alpha.txt",
                "new-alpha-1\nnew-alpha-2\n",
                "feat: alpha rewritten follow-up",
            );
            Self { _dir: dir, repo }
        }

        fn with_cwd<T>(&self, f: impl FnOnce() -> T) -> T {
            let _lock = lock_cwd();
            let _guard = DirGuard::change_to(&self.repo);
            f()
        }
    }

    fn read_preview_side_effect_snapshot(repo: &Path) -> String {
        [
            git(repo, ["rev-parse", "HEAD"].as_slice()),
            git(repo, ["branch", "--format=%(refname:short)"].as_slice()),
            git(repo, ["tag", "--list"].as_slice()),
            git(repo, ["worktree", "list", "--porcelain"].as_slice()),
            fs::read_dir(repo.join(".git/spr/resume"))
                .map(|entries| entries.count().to_string())
                .unwrap_or_else(|_| "<no-resume-dir>".to_string()),
        ]
        .join("\n---\n")
    }

    #[test]
    fn ensure_exact_bottom_sequence_rejects_reordered_candidate() {
        let old = vec![group("alpha", &[]), group("beta", &[]), group("gamma", &[])];

        let err =
            ensure_exact_bottom_sequence(&["pr:alpha".to_string(), "pr:gamma".to_string()], &old)
                .unwrap_err();

        assert!(err.to_string().contains("not an exact bottom sequence"));
    }

    #[test]
    fn build_replay_suffix_skips_replaced_trailing_ignored_block() {
        let candidate = vec![group("alpha", &["candidate-ignore"])];
        let old = vec![
            group("alpha", &["old-ignore"]),
            group("beta", &[]),
            group("gamma", &[]),
        ];

        let (boundary, replay_groups, operations) = build_replay_suffix(&candidate, &old).unwrap();

        assert_eq!(boundary, "old-ignore");
        assert_eq!(replay_groups.len(), 2);
        assert_eq!(operations.len(), 2);
    }

    #[test]
    fn build_post_adoption_groups_preserves_old_ignore_when_candidate_ends_at_group_tip() {
        let candidate = vec![group("alpha", &[])];
        let old = vec![group("alpha", &["old-ignore"]), group("beta", &[])];

        let groups = build_post_adoption_groups(&candidate, &old).unwrap();

        assert_eq!(groups[0].ignored_after, vec!["old-ignore".to_string()]);
        assert_eq!(
            publishable_selector_sequence(&[], &groups),
            vec!["pr:alpha".to_string()]
        );
    }

    #[test]
    fn adopt_prefix_rebuilds_descendants_on_rewritten_candidate_prefix() {
        let repo = AdoptPrefixRepo::init();

        repo.with_cwd(|| {
            let candidate_head = git(&repo.repo, ["rev-parse", "HEAD"].as_slice())
                .trim()
                .to_string();
            let outcome = adopt_prefix(
                &metadata_context(),
                false,
                ExecutionMode::Apply,
                DirtyWorktreePolicy::Halt,
            )
            .unwrap();

            assert_eq!(outcome.rewrite_outcome, RewriteCommandOutcome::Completed);
            assert_eq!(outcome.owning_stack_branch, "stack");
            assert_eq!(
                git(&repo.repo, ["rev-parse", "HEAD"].as_slice()).trim(),
                candidate_head
            );
            assert_eq!(
                git(
                    &repo.repo,
                    ["log", "--format=%s", "--reverse", "main..stack"].as_slice()
                )
                .lines()
                .collect::<Vec<_>>(),
                vec![
                    "feat: alpha rewritten",
                    "feat: alpha rewritten follow-up",
                    "feat: beta",
                    "feat: gamma",
                ]
            );
            assert_eq!(
                fs::read_to_string(repo.repo.join("alpha.txt")).unwrap(),
                "new-alpha-1\nnew-alpha-2\n"
            );
        });
    }

    #[test]
    fn preview_reports_plan_and_leaves_git_state_unchanged() {
        let repo = AdoptPrefixRepo::init();

        repo.with_cwd(|| {
            let before = read_preview_side_effect_snapshot(&repo.repo);
            let preview = preview_adopt_prefix(&metadata_context(), true).unwrap();

            assert_eq!(preview.owning_stack_branch, "stack");
            assert_eq!(preview.candidate_groups[0].stable_handle, "pr:alpha");
            assert_eq!(preview.replay_suffix_groups.len(), 2);
            assert!(preview.would_create_backup_tag);
            assert!(preview.would_create_temp_worktree);
            assert_eq!(read_preview_side_effect_snapshot(&repo.repo), before);
        });
    }

    #[test]
    fn preview_allows_owning_stack_branch_checked_out_elsewhere() {
        let repo = AdoptPrefixRepo::init();

        repo.with_cwd(|| {
            let stack_worktree = repo.repo.join("stack-worktree");
            git(
                &repo.repo,
                ["worktree", "add", stack_worktree.to_str().unwrap(), "stack"].as_slice(),
            );

            let preview = preview_adopt_prefix(&metadata_context(), false).unwrap();

            assert_eq!(preview.owning_stack_branch, "stack");
        });
    }

    #[test]
    fn adopt_prefix_accepts_detached_candidate_head() {
        let repo = AdoptPrefixRepo::init();

        repo.with_cwd(|| {
            git(&repo.repo, ["checkout", "--detach", "HEAD"].as_slice());

            let outcome = adopt_prefix(
                &metadata_context(),
                false,
                ExecutionMode::Apply,
                DirtyWorktreePolicy::Halt,
            )
            .unwrap();

            assert_eq!(outcome.rewrite_outcome, RewriteCommandOutcome::Completed);
            assert_eq!(
                git(&repo.repo, ["rev-parse", "--abbrev-ref", "HEAD"].as_slice()).trim(),
                "HEAD"
            );
        });
    }

    #[test]
    fn adopt_prefix_rejects_candidate_with_ungrouped_leading_commits() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "old-alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(repo, "beta.txt", "beta\n", "feat: beta\n\npr:beta");
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
            .unwrap();
        let stack_before = git(repo, ["rev-parse", "stack"].as_slice());
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(repo, "scratch.txt", "scratch\n", "wip: local scratch");
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );

        let err = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("ungrouped commits before the first explicit selector"));
        assert_eq!(git(repo, ["rev-parse", "stack"].as_slice()), stack_before);
    }

    #[test]
    fn adopt_prefix_preflights_post_adoption_snapshot_before_moving_stack_branch() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        let initial_context = RefreshMetadataContext {
            base: "main".to_string(),
            prefix: "x/".to_string(),
            ignore_tag: "ignore".to_string(),
        };
        let adoption_context = RefreshMetadataContext {
            base: "main".to_string(),
            prefix: "other/".to_string(),
            ignore_tag: "ignore".to_string(),
        };
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "old-alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(
            repo,
            "other-alpha.txt",
            "other-alpha\n",
            "feat: other alpha\n\nbranch:other/alpha",
        );
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &initial_context, None)
            .unwrap();
        let stack_before = git(repo, ["rev-parse", "stack"].as_slice());
        let metadata_path = repo.join(".git/spr/stack_metadata_v1.json");
        let metadata_before = fs::read_to_string(&metadata_path).unwrap();
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );

        let err = adopt_prefix(
            &adoption_context,
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap_err();

        assert!(err.to_string().contains("derive conflicting branch names"));
        assert_eq!(git(repo, ["rev-parse", "stack"].as_slice()), stack_before);
        assert_eq!(fs::read_to_string(metadata_path).unwrap(), metadata_before);
    }

    #[test]
    fn adopt_prefix_uses_candidate_trailing_ignore_instead_of_replaying_old_ignore() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "old-alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(
            repo,
            "old-ignore.txt",
            "old-ignore\n",
            "local old ignore\n\npr:ignore",
        );
        commit_file(repo, "beta.txt", "beta\n", "feat: beta\n\npr:beta");
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
            .unwrap();
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );
        commit_file(
            repo,
            "candidate-ignore.txt",
            "candidate-ignore\n",
            "local candidate ignore\n\npr:ignore",
        );

        let outcome = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap();

        assert_eq!(outcome.rewrite_outcome, RewriteCommandOutcome::Completed);
        assert_eq!(
            git(
                repo,
                ["log", "--format=%s", "--reverse", "main..stack"].as_slice()
            )
            .lines()
            .collect::<Vec<_>>(),
            vec![
                "feat: alpha rewritten",
                "local candidate ignore",
                "feat: beta",
            ]
        );
    }

    #[test]
    fn adopt_prefix_accepts_existing_local_only_groups_after_ignored_boundary() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "old-alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(
            repo,
            "old-ignore.txt",
            "old-ignore\n",
            "local old ignore\n\npr:ignore",
        );
        commit_file(repo, "beta.txt", "old-beta\n", "feat: beta\n\npr:beta");
        commit_file(repo, "gamma.txt", "gamma\n", "feat: gamma\n\npr:gamma");
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
            .unwrap();
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );
        commit_file(
            repo,
            "candidate-ignore.txt",
            "candidate-ignore\n",
            "local candidate ignore\n\npr:ignore",
        );
        commit_file(
            repo,
            "beta.txt",
            "new-beta\n",
            "feat: beta rewritten\n\npr:beta",
        );

        let outcome = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap();

        assert_eq!(outcome.rewrite_outcome, RewriteCommandOutcome::Completed);
        assert_eq!(
            git(
                repo,
                ["log", "--format=%s", "--reverse", "main..stack"].as_slice()
            )
            .lines()
            .collect::<Vec<_>>(),
            vec![
                "feat: alpha rewritten",
                "local candidate ignore",
                "feat: beta rewritten",
                "feat: gamma",
            ]
        );
    }

    #[test]
    fn adopt_prefix_accepts_entirely_local_only_stack_when_publishability_is_unchanged() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(
            repo,
            "old-ignore.txt",
            "old-ignore\n",
            "local old ignore\n\npr:ignore",
        );
        commit_file(repo, "alpha.txt", "old-alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(repo, "beta.txt", "beta\n", "feat: beta\n\npr:beta");
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
            .unwrap();
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "candidate-ignore.txt",
            "candidate-ignore\n",
            "local candidate ignore\n\npr:ignore",
        );
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );

        let outcome = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap();

        assert_eq!(outcome.rewrite_outcome, RewriteCommandOutcome::Completed);
        assert_eq!(
            git(
                repo,
                ["log", "--format=%s", "--reverse", "main..stack"].as_slice()
            )
            .lines()
            .collect::<Vec<_>>(),
            vec![
                "local candidate ignore",
                "feat: alpha rewritten",
                "feat: beta",
            ]
        );
    }

    #[test]
    fn adopt_prefix_rejects_raw_prefix_that_matches_multiple_verified_stacks() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        for stack_branch in ["stack-a", "stack-b"] {
            git(repo, ["checkout", "-b", stack_branch, "main"].as_slice());
            commit_file(
                repo,
                &format!("{stack_branch}-ignore.txt"),
                "ignore\n",
                "local ignore\n\npr:ignore",
            );
            commit_file(
                repo,
                &format!("{stack_branch}-alpha.txt"),
                "alpha\n",
                "feat: alpha\n\npr:alpha",
            );
            refresh_metadata_for_branch(
                repo.to_str().unwrap(),
                stack_branch,
                &metadata_context(),
                None,
            )
            .unwrap();
        }
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "candidate-ignore.txt",
            "candidate-ignore\n",
            "local candidate ignore\n\npr:ignore",
        );
        commit_file(
            repo,
            "alpha.txt",
            "new-alpha\n",
            "feat: alpha rewritten\n\npr:alpha",
        );

        let err = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("matches more than one verified stack history"));
    }

    #[test]
    fn adopt_prefix_rejects_candidate_that_changes_publishable_sequence() {
        let dir = crate::test_support::init_repo();
        let repo = dir.path();
        let _lock = lock_cwd();
        let _guard = DirGuard::change_to(repo);
        git(repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(repo, "alpha.txt", "alpha\n", "feat: alpha\n\npr:alpha");
        commit_file(repo, "beta.txt", "beta\n", "feat: beta\n\npr:beta");
        commit_file(
            repo,
            "old-ignore.txt",
            "ignore\n",
            "local ignore\n\npr:ignore",
        );
        commit_file(repo, "gamma.txt", "gamma\n", "feat: gamma\n\npr:gamma");
        refresh_metadata_for_branch(repo.to_str().unwrap(), "stack", &metadata_context(), None)
            .unwrap();
        git(repo, ["checkout", "-b", "candidate", "main"].as_slice());
        commit_file(
            repo,
            "alpha.txt",
            "alpha rewritten\n",
            "feat: alpha rewritten\n\npr:alpha",
        );
        commit_file(
            repo,
            "candidate-ignore.txt",
            "candidate ignore\n",
            "local candidate ignore\n\npr:ignore",
        );

        let err = adopt_prefix(
            &metadata_context(),
            false,
            ExecutionMode::Apply,
            DirtyWorktreePolicy::Halt,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("would change publishable selector sequence"));
    }

    #[test]
    fn adopt_prefix_rejects_owning_stack_branch_checked_out_elsewhere() {
        let repo = AdoptPrefixRepo::init();

        repo.with_cwd(|| {
            let stack_worktree = repo.repo.join("stack-worktree");
            git(
                &repo.repo,
                ["worktree", "add", stack_worktree.to_str().unwrap(), "stack"].as_slice(),
            );

            let err = adopt_prefix(
                &metadata_context(),
                false,
                ExecutionMode::Apply,
                DirtyWorktreePolicy::Halt,
            )
            .unwrap_err();

            assert!(err.to_string().contains("checked out in worktree"));
        });
    }
}
