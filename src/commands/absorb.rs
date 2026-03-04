//! Absorb append-only local per-PR branch tails back into the current stack.
//!
//! `spr absorb` inspects the current local stack, looks for exact local source
//! branches named `prefix + tag`, and classifies each one as absorbable,
//! skippable, or blocking. When one or more groups have append-only local
//! branch tails, the command rebuilds the checked-out stack branch from its
//! current merge-base and inserts those tails immediately after their owning
//! group's real commits and before any trailing ignored block. Rewritten local
//! per-PR branches that are patch-equivalent to the canonical stack prefix are
//! treated as unchanged instead of as divergence.

use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use tracing::info;

use crate::commands::common::{self, CherryPickEmptyPolicy, CherryPickOp};
use crate::commands::rewrite_resume::{
    self, RewriteCommandKind, RewriteCommandOutcome, RewriteConflictPolicy, RewriteSession,
};
use crate::config::DirtyWorktreePolicy;
use crate::git::{
    git_commit_message, git_commit_parent_count, git_is_ancestor, git_local_branch_tip,
    git_merge_base, git_patch_ids_for_commits, git_rev_list_range, git_rev_parse,
};
use crate::parsing::{derive_local_groups_with_ignored, Group};

/// Rebuilds the current stack branch by folding append-only local per-PR branch
/// tails back into their owning PR groups.
///
/// The exact source branch rule is intentional: only the local branch named
/// `prefix + tag` is considered for each group. Missing branches and branches
/// that are unchanged or behind the stack are no-op states. Divergence, source
/// branches that incorporated later stack commits, merge commits, and embedded
/// `pr:<tag>` markers in absorbed tails are blocking errors. A local branch
/// whose canonical stack prefix was rewritten to patch-equivalent commits is
/// still acceptable: absorb matches that rewritten-equivalent prefix and then
/// classifies only the unmatched tail. By default absorb also refuses to
/// absorb a tail commit whose patch is already owned by a later stack commit
/// that would still be replayed after this group's insertion point.
pub fn absorb_branch_tails(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    dry: bool,
    dirty_worktree_policy: DirtyWorktreePolicy,
    options: AbsorbOptions,
) -> Result<RewriteCommandOutcome> {
    let (_stack_head, plan) = build_plan_for_current_stack(base, prefix, ignore_tag, options)?;
    let validation = validate_absorb_plan(&plan);
    if !matches!(validation, AbsorbPlanValidation::NoGroups) {
        emit_absorb_summary(&plan);
    }
    match validation {
        AbsorbPlanValidation::NoGroups => {
            info!("No local PR groups found; nothing to absorb.");
            Ok(RewriteCommandOutcome::Completed)
        }
        AbsorbPlanValidation::Blocked { lines } => Err(anyhow!(
            "Refusing to absorb branch tails until blocking branches are fixed:\n{}",
            lines.join("\n")
        )),
        AbsorbPlanValidation::NoAbsorbableGroups => {
            info!("No absorbable branch tails found; nothing to rewrite.");
            Ok(RewriteCommandOutcome::Completed)
        }
        AbsorbPlanValidation::ReadyToRewrite => {
            execute_absorb_plan(&plan, dry, dirty_worktree_policy)
        }
    }
}

/// CLI-configurable behavior switches for `spr absorb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsorbOptions {
    /// Controls how absorb handles copied later stack commits in source-branch tails.
    pub copied_later_stack_commit_policy: CopiedLaterStackCommitPolicy,
}

impl Default for AbsorbOptions {
    fn default() -> Self {
        Self {
            copied_later_stack_commit_policy: CopiedLaterStackCommitPolicy::Block,
        }
    }
}

/// Policy for copied later stack commits discovered in an absorb tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopiedLaterStackCommitPolicy {
    /// Treat copied later stack commits as blocking invalid input.
    Block,
    /// Absorb copied later non-seed commits and keep their later replay.
    AllowKeepNonSeedDuplicates,
}

#[derive(Debug, Clone)]
struct RewritePlan {
    merge_base: String,
    leading_ignored: Vec<String>,
    groups: Vec<GroupRewritePlan>,
    operations: Vec<CherryPickOp>,
}

impl RewritePlan {
    fn has_blockers(&self) -> bool {
        self.groups.iter().any(|group| {
            matches!(
                group.source.classification,
                AbsorbClassification::Blocked(_)
            )
        })
    }

    fn has_absorbable_groups(&self) -> bool {
        self.groups.iter().any(|group| group.source.is_absorbable())
    }

    fn blocking_lines(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter_map(|group| {
                if matches!(
                    group.source.classification,
                    AbsorbClassification::Blocked(_)
                ) {
                    Some(group.source.summary_line())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Pure validation result for an absorb rewrite plan before any local rewrite
/// side effects are attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AbsorbPlanValidation {
    NoGroups,
    NoAbsorbableGroups,
    ReadyToRewrite,
    Blocked { lines: Vec<String> },
}

#[derive(Debug, Clone)]
struct GroupRewritePlan {
    group: Group,
    source: SourceBranchRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StackPrefix {
    commits: Vec<String>,
    group_start_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LaterReplayedCommitKind {
    GroupSeed,
    GroupFollowUp,
    IgnoredAfter,
}

impl LaterReplayedCommitKind {
    fn can_keep_duplicate_on_override(&self) -> bool {
        !matches!(self, Self::GroupSeed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaterReplayedCommit {
    tag: String,
    sha: String,
    kind: LaterReplayedCommitKind,
}

type LaterOwnedPatchMap = HashMap<String, Vec<LaterReplayedCommit>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceBranchRecord {
    tag: String,
    branch_name: String,
    group_tip: String,
    source_tip: Option<String>,
    classification: AbsorbClassification,
}

impl SourceBranchRecord {
    fn summary_line(&self) -> String {
        match &self.classification {
            AbsorbClassification::Skip(reason) => {
                format!(
                    "pr:{} <- {} : skip ({})",
                    self.tag, self.branch_name, reason
                )
            }
            AbsorbClassification::Absorbable(tail) => {
                let kept_text = if tail.kept_later_duplicate_replays.is_empty() {
                    String::new()
                } else {
                    format!(
                        " (override keeps {})",
                        commit_count_text(tail.kept_later_duplicate_replays.len())
                    )
                };
                format!(
                    "pr:{} <- {} : absorb {}{}",
                    self.tag,
                    self.branch_name,
                    commit_count_text(tail.commits.len()),
                    kept_text
                )
            }
            AbsorbClassification::Blocked(reason) => {
                format!(
                    "pr:{} <- {} : block ({})",
                    self.tag, self.branch_name, reason
                )
            }
        }
    }

    fn absorbed_tail(&self) -> &[String] {
        self.classification.absorbed_commits()
    }

    fn is_absorbable(&self) -> bool {
        self.classification.is_absorbable()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AbsorbClassification {
    Skip(AbsorbSkipReason),
    Absorbable(AbsorbTail),
    Blocked(AbsorbBlocker),
}

impl AbsorbClassification {
    fn absorbed_commits(&self) -> &[String] {
        if let Self::Absorbable(tail) = self {
            tail.commits.as_slice()
        } else {
            &[]
        }
    }

    fn is_absorbable(&self) -> bool {
        matches!(self, Self::Absorbable(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AbsorbTail {
    commits: Vec<String>,
    kept_later_duplicate_replays: Vec<LaterReplayedCommit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AbsorbSkipReason {
    MissingBranch,
    Unchanged,
    BranchBehindStack,
}

impl std::fmt::Display for AbsorbSkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBranch => write!(f, "missing local branch"),
            Self::Unchanged => write!(f, "branch unchanged"),
            Self::BranchBehindStack => write!(f, "branch behind stack"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AbsorbBlocker {
    Diverged,
    MergeBaseMismatch {
        merge_base: String,
    },
    TailHasMergeCommit {
        commit: String,
    },
    TailHasStackMarker {
        commit: String,
    },
    TailOwnedByLaterReplayedCommit {
        commit: String,
        owner_tag: String,
        owner_commit: String,
    },
    TailOwnedByLaterGroupSeed {
        commit: String,
        owner_tag: String,
        owner_commit: String,
    },
}

impl std::fmt::Display for AbsorbBlocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Diverged => write!(f, "branch diverged from stack group tip"),
            Self::MergeBaseMismatch { merge_base } => write!(
                f,
                "branch incorporated later stack commits (merge-base with HEAD is {})",
                short_sha(merge_base)
            ),
            Self::TailHasMergeCommit { commit } => write!(
                f,
                "absorbed tail contains a merge commit ({})",
                short_sha(commit)
            ),
            Self::TailHasStackMarker { commit } => write!(
                f,
                "absorbed tail contains a stack marker ({})",
                short_sha(commit)
            ),
            Self::TailOwnedByLaterReplayedCommit {
                commit,
                owner_tag,
                owner_commit,
            } => write!(
                f,
                "absorbed tail commit {} duplicates later replayed pr:{} commit {}",
                short_sha(commit),
                owner_tag,
                short_sha(owner_commit)
            ),
            Self::TailOwnedByLaterGroupSeed {
                commit,
                owner_tag,
                owner_commit,
            } => write!(
                f,
                "absorbed tail commit {} duplicates later pr:{} seed commit {}; --allow-replayed-duplicates only applies to later non-seed commits, so this still blocks",
                short_sha(commit),
                owner_tag,
                short_sha(owner_commit)
            ),
        }
    }
}

fn build_plan_for_current_stack(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    options: AbsorbOptions,
) -> Result<(String, RewritePlan)> {
    let (merge_base, leading_ignored, groups) = derive_local_groups_with_ignored(base, ignore_tag)?;
    if groups.is_empty() {
        Ok((
            git_rev_parse("HEAD")?,
            RewritePlan {
                merge_base,
                leading_ignored,
                groups: Vec::new(),
                operations: Vec::new(),
            },
        ))
    } else {
        let stack_head = git_rev_parse("HEAD")?;
        let later_owned_patch_maps = build_later_owned_patch_maps(&groups)?;
        let group_plans = groups
            .into_iter()
            .zip(later_owned_patch_maps)
            .map(|(group, later_owned_patches)| {
                build_group_rewrite_plan(
                    group,
                    prefix,
                    &merge_base,
                    &stack_head,
                    &later_owned_patches,
                    options,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let operations = build_rewrite_operations(&leading_ignored, &group_plans);
        Ok((
            stack_head,
            RewritePlan {
                merge_base,
                leading_ignored,
                groups: group_plans,
                operations,
            },
        ))
    }
}

fn build_group_rewrite_plan(
    group: Group,
    prefix: &str,
    stack_merge_base: &str,
    stack_head: &str,
    later_owned_patches: &LaterOwnedPatchMap,
    options: AbsorbOptions,
) -> Result<GroupRewritePlan> {
    let source = classify_source_branch(
        &group,
        prefix,
        stack_merge_base,
        stack_head,
        later_owned_patches,
        options,
    )?;
    Ok(GroupRewritePlan { group, source })
}

/// Classifies whether a rewrite plan is empty, blocked, a no-op, or ready to
/// execute without touching local git state.
fn validate_absorb_plan(plan: &RewritePlan) -> AbsorbPlanValidation {
    if plan.groups.is_empty() {
        AbsorbPlanValidation::NoGroups
    } else if plan.has_blockers() {
        AbsorbPlanValidation::Blocked {
            lines: plan.blocking_lines(),
        }
    } else if !plan.has_absorbable_groups() {
        AbsorbPlanValidation::NoAbsorbableGroups
    } else {
        AbsorbPlanValidation::ReadyToRewrite
    }
}

fn classify_source_branch(
    group: &Group,
    prefix: &str,
    stack_merge_base: &str,
    stack_head: &str,
    later_owned_patches: &LaterOwnedPatchMap,
    options: AbsorbOptions,
) -> Result<SourceBranchRecord> {
    let branch_name = format!("{}{}", prefix, group.tag);
    let stack_prefix = build_stack_prefix(group, stack_merge_base)?;
    let group_tip = stack_prefix
        .commits
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("Group {} has no commits", group.tag))?;
    let source_tip = git_local_branch_tip(&branch_name)?;
    let classification = if let Some(source_tip) = source_tip.as_deref() {
        if source_tip == group_tip {
            AbsorbClassification::Skip(AbsorbSkipReason::Unchanged)
        } else if git_is_ancestor(source_tip, &group_tip)? {
            AbsorbClassification::Skip(AbsorbSkipReason::BranchBehindStack)
        } else if git_is_ancestor(&group_tip, source_tip)? {
            let merge_base = git_merge_base(source_tip, stack_head)?;
            if merge_base != group_tip {
                AbsorbClassification::Blocked(AbsorbBlocker::MergeBaseMismatch { merge_base })
            } else {
                let tail = git_rev_list_range(&group_tip, source_tip)?;
                classify_absorbed_tail(&tail, later_owned_patches, options)?
            }
        } else {
            classify_rewritten_equivalent_source_branch(
                &stack_prefix,
                source_tip,
                stack_merge_base,
                later_owned_patches,
                options,
            )?
        }
    } else {
        AbsorbClassification::Skip(AbsorbSkipReason::MissingBranch)
    };
    Ok(SourceBranchRecord {
        tag: group.tag.clone(),
        branch_name,
        group_tip,
        source_tip,
        classification,
    })
}

fn build_stack_prefix(group: &Group, stack_merge_base: &str) -> Result<StackPrefix> {
    let group_tip = group
        .commits
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("Group {} has no commits", group.tag))?;
    let commits = git_rev_list_range(stack_merge_base, &group_tip)?;
    let group_start_index = commits
        .len()
        .checked_sub(group.commits.len())
        .ok_or_else(|| anyhow!("Group {} stack prefix is shorter than the group", group.tag))?;
    Ok(StackPrefix {
        commits,
        group_start_index,
    })
}

fn classify_rewritten_equivalent_source_branch(
    stack_prefix: &StackPrefix,
    source_tip: &str,
    stack_merge_base: &str,
    later_owned_patches: &LaterOwnedPatchMap,
    options: AbsorbOptions,
) -> Result<AbsorbClassification> {
    let source_commits = git_rev_list_range(stack_merge_base, source_tip)?;
    let patch_ids = git_patch_ids_for_commits(
        &stack_prefix
            .commits
            .iter()
            .chain(source_commits.iter())
            .cloned()
            .collect::<Vec<_>>(),
    )?;
    let stack_patch_ids = patch_id_sequence(&stack_prefix.commits, &patch_ids)?;
    let source_patch_ids = patch_id_sequence(&source_commits, &patch_ids)?;
    let matched = common_patch_prefix_len(&stack_patch_ids, &source_patch_ids);
    if source_commits.len() == matched {
        if matched < stack_prefix.group_start_index {
            Ok(AbsorbClassification::Blocked(AbsorbBlocker::Diverged))
        } else if stack_prefix.commits.len() == matched {
            Ok(AbsorbClassification::Skip(AbsorbSkipReason::Unchanged))
        } else {
            Ok(AbsorbClassification::Skip(
                AbsorbSkipReason::BranchBehindStack,
            ))
        }
    } else if matched != stack_prefix.commits.len() {
        Ok(AbsorbClassification::Blocked(AbsorbBlocker::Diverged))
    } else {
        let tail = source_commits.into_iter().skip(matched).collect::<Vec<_>>();
        classify_absorbed_tail(&tail, later_owned_patches, options)
    }
}

fn patch_id_sequence(
    commits: &[String],
    patch_ids: &HashMap<String, String>,
) -> Result<Vec<String>> {
    commits
        .iter()
        .map(|sha| {
            patch_ids
                .get(sha)
                .cloned()
                .ok_or_else(|| anyhow!("missing patch id for {}", sha))
        })
        .collect()
}

fn common_patch_prefix_len(left: &[String], right: &[String]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left_patch, right_patch)| left_patch == right_patch)
        .count()
}

fn classify_absorbed_tail(
    tail: &[String],
    later_owned_patches: &LaterOwnedPatchMap,
    options: AbsorbOptions,
) -> Result<AbsorbClassification> {
    for sha in tail {
        let parent_count = git_commit_parent_count(sha)?;
        if parent_count > 1 {
            return Ok(AbsorbClassification::Blocked(
                AbsorbBlocker::TailHasMergeCommit {
                    commit: sha.clone(),
                },
            ));
        }
        let message = git_commit_message(sha)?;
        if crate::pr_labels::candidate_marker_regex().is_match(&message) {
            return Ok(AbsorbClassification::Blocked(
                AbsorbBlocker::TailHasStackMarker {
                    commit: sha.clone(),
                },
            ));
        }
    }
    let tail_patch_ids = git_patch_ids_for_commits(tail)?;
    let mut kept_later_replay_shas = HashSet::new();
    let mut kept_later_duplicate_replays = Vec::new();
    for sha in tail {
        let Some(patch_id) = tail_patch_ids.get(sha) else {
            continue;
        };
        if let Some(owners) = later_owned_patches.get(patch_id) {
            match options.copied_later_stack_commit_policy {
                CopiedLaterStackCommitPolicy::Block => {
                    let owner = owners.first().expect("owners must not be empty");
                    return Ok(AbsorbClassification::Blocked(
                        AbsorbBlocker::TailOwnedByLaterReplayedCommit {
                            commit: sha.clone(),
                            owner_tag: owner.tag.clone(),
                            owner_commit: owner.sha.clone(),
                        },
                    ));
                }
                CopiedLaterStackCommitPolicy::AllowKeepNonSeedDuplicates => {
                    if let Some(owner) = owners
                        .iter()
                        .find(|owner| !owner.kind.can_keep_duplicate_on_override())
                    {
                        return Ok(AbsorbClassification::Blocked(
                            AbsorbBlocker::TailOwnedByLaterGroupSeed {
                                commit: sha.clone(),
                                owner_tag: owner.tag.clone(),
                                owner_commit: owner.sha.clone(),
                            },
                        ));
                    }
                    for owner in owners {
                        if kept_later_replay_shas.insert(owner.sha.clone()) {
                            kept_later_duplicate_replays.push(owner.clone());
                        }
                    }
                }
            }
        }
    }
    Ok(AbsorbClassification::Absorbable(AbsorbTail {
        commits: tail.to_vec(),
        kept_later_duplicate_replays,
    }))
}

fn build_later_owned_patch_maps(groups: &[Group]) -> Result<Vec<LaterOwnedPatchMap>> {
    let replayed_commits = groups
        .iter()
        .flat_map(|group| group.commits.iter().chain(group.ignored_after.iter()))
        .cloned()
        .collect::<Vec<_>>();
    let patch_ids_by_commit = git_patch_ids_for_commits(&replayed_commits)?;
    let mut later_owned_patches = HashMap::new();
    let mut later_owned_patch_maps = Vec::with_capacity(groups.len());

    for group in groups.iter().rev() {
        record_later_replayed_commits(
            &mut later_owned_patches,
            &group.ignored_after,
            &group.tag,
            &patch_ids_by_commit,
            LaterReplayedCommitKind::IgnoredAfter,
        );
        later_owned_patch_maps.push(later_owned_patches.clone());
        record_later_replayed_commits(
            &mut later_owned_patches,
            &group.commits,
            &group.tag,
            &patch_ids_by_commit,
            LaterReplayedCommitKind::GroupFollowUp,
        );
    }

    later_owned_patch_maps.reverse();
    Ok(later_owned_patch_maps)
}

fn record_later_replayed_commits(
    later_owned_patches: &mut LaterOwnedPatchMap,
    commits: &[String],
    tag: &str,
    patch_ids_by_commit: &HashMap<String, String>,
    default_kind: LaterReplayedCommitKind,
) {
    for (index, sha) in commits.iter().enumerate().rev() {
        if let Some(patch_id) = patch_ids_by_commit.get(sha) {
            let kind =
                if matches!(default_kind, LaterReplayedCommitKind::GroupFollowUp) && index == 0 {
                    LaterReplayedCommitKind::GroupSeed
                } else {
                    default_kind.clone()
                };
            later_owned_patches
                .entry(patch_id.clone())
                .or_default()
                .push(LaterReplayedCommit {
                    tag: tag.to_string(),
                    sha: sha.clone(),
                    kind,
                });
        }
    }
}

fn build_rewrite_operations(
    leading_ignored: &[String],
    group_plans: &[GroupRewritePlan],
) -> Vec<CherryPickOp> {
    let kept_later_replay_shas = kept_later_replay_shas(group_plans);
    let mut ops = build_replay_ops_for_commits(leading_ignored, &HashSet::new());
    for group_plan in group_plans {
        ops.extend(build_replay_ops_for_commits(
            &group_plan.group.commits,
            &kept_later_replay_shas,
        ));
        ops.extend(build_replay_ops_for_commits(
            group_plan.source.absorbed_tail(),
            &HashSet::new(),
        ));
        ops.extend(build_replay_ops_for_commits(
            &group_plan.group.ignored_after,
            &kept_later_replay_shas,
        ));
    }
    ops
}

fn kept_later_replay_shas(group_plans: &[GroupRewritePlan]) -> HashSet<String> {
    group_plans
        .iter()
        .flat_map(|group_plan| match &group_plan.source.classification {
            AbsorbClassification::Absorbable(tail) => tail
                .kept_later_duplicate_replays
                .iter()
                .map(|commit| commit.sha.clone())
                .collect::<Vec<_>>(),
            AbsorbClassification::Skip(_) | AbsorbClassification::Blocked(_) => Vec::new(),
        })
        .collect()
}

fn build_replay_ops_for_commits(
    commits: &[String],
    kept_later_replay_shas: &HashSet<String>,
) -> Vec<CherryPickOp> {
    let mut ops = Vec::new();
    let mut segment_start: Option<usize> = None;
    let mut segment_policy = CherryPickEmptyPolicy::StopOnEmpty;

    for (index, sha) in commits.iter().enumerate() {
        let next_policy = if kept_later_replay_shas.contains(sha) {
            CherryPickEmptyPolicy::KeepRedundantCommits
        } else {
            CherryPickEmptyPolicy::StopOnEmpty
        };
        if let Some(start) = segment_start {
            if next_policy != segment_policy {
                ops.extend(CherryPickOp::from_commits_with_empty_policy(
                    &commits[start..index],
                    segment_policy,
                ));
                segment_start = Some(index);
                segment_policy = next_policy;
            }
        } else {
            segment_start = Some(index);
            segment_policy = next_policy;
        }
    }

    if let Some(start) = segment_start {
        ops.extend(CherryPickOp::from_commits_with_empty_policy(
            &commits[start..],
            segment_policy,
        ));
    }

    ops
}

/// Executes a validated absorb rewrite plan by rebuilding the current stack in
/// a temporary worktree and resetting the checked-out branch to the new tip.
fn execute_absorb_plan(
    plan: &RewritePlan,
    dry: bool,
    dirty_worktree_policy: DirtyWorktreePolicy,
) -> Result<RewriteCommandOutcome> {
    emit_rewrite_plan(plan);
    common::with_dirty_worktree_policy(dry, "spr absorb", dirty_worktree_policy, || {
        if dry {
            info!("Dry run complete. No local git state or GitHub state was changed.");
            info!("Run `spr absorb` without `--dry-run` to apply the rewrite.");
            info!("After inspecting the rewritten stack, run `spr update`.");
            Ok(RewriteCommandOutcome::Completed)
        } else {
            let (cur_branch, short) = common::get_current_branch_and_short()?;
            let original_head = git_rev_parse("HEAD")?;
            let original_worktree_root = rewrite_resume::current_repo_root()?;
            let backup_tag = common::create_backup_tag(dry, "absorb", &cur_branch, &short)?;
            let (tmp_path, tmp_branch) =
                common::create_temp_worktree(dry, "absorb", &plan.merge_base, &short)?;
            let steps = rewrite_resume::build_replay_steps(&plan.operations)?;
            rewrite_resume::run_rewrite_session(
                dry,
                RewriteSession {
                    command_kind: RewriteCommandKind::Absorb,
                    conflict_policy: RewriteConflictPolicy::Suspend,
                    original_worktree_root,
                    original_branch: cur_branch,
                    original_head,
                    temp_branch: tmp_branch,
                    temp_worktree_path: tmp_path,
                    backup_tag: Some(backup_tag),
                    steps,
                    post_success_hint: Some(
                        "No GitHub changes were made. Run `spr update` after inspecting the rewritten stack."
                            .to_string(),
                    ),
                },
            )
        }
    })
}

fn emit_absorb_summary(plan: &RewritePlan) {
    info!("Absorb plan:");
    for group in &plan.groups {
        info!("{}", group.source.summary_line());
    }
}

fn emit_rewrite_plan(plan: &RewritePlan) {
    info!("Rewrite plan:");
    if !plan.leading_ignored.is_empty() {
        info!(
            "  leading ignored: replay {}",
            commit_count_text(plan.leading_ignored.len())
        );
    }
    for group in &plan.groups {
        let absorbed = group.source.absorbed_tail().len();
        let kept_replays = match &group.source.classification {
            AbsorbClassification::Absorbable(tail) => tail.kept_later_duplicate_replays.len(),
            AbsorbClassification::Skip(_) | AbsorbClassification::Blocked(_) => 0,
        };
        let absorbed_text = if absorbed == 0 {
            String::new()
        } else {
            format!(" + {}", commit_count_text(absorbed))
        };
        let ignored_text = if group.group.ignored_after.is_empty() {
            String::new()
        } else {
            format!(
                " + {} ignored",
                commit_count_text(group.group.ignored_after.len())
            )
        };
        let kept_text = if kept_replays == 0 {
            String::new()
        } else {
            format!(" + keep {}", commit_count_text(kept_replays))
        };
        info!(
            "  pr:{}: replay {}{}{}{}",
            group.group.tag,
            commit_count_text(group.group.commits.len()),
            absorbed_text,
            ignored_text,
            kept_text
        );
    }
}

fn commit_count_text(count: usize) -> String {
    if count == 1 {
        "1 commit".to_string()
    } else {
        format!("{} commits", count)
    }
}

fn short_sha(sha: &str) -> &str {
    if sha.len() > 8 {
        &sha[..8]
    } else {
        sha
    }
}

#[cfg(test)]
mod tests {
    use super::{
        absorb_branch_tails, build_group_rewrite_plan, build_later_owned_patch_maps,
        build_plan_for_current_stack, build_rewrite_operations, validate_absorb_plan,
        AbsorbBlocker, AbsorbClassification, AbsorbOptions, AbsorbPlanValidation, AbsorbSkipReason,
        AbsorbTail, CopiedLaterStackCommitPolicy, GroupRewritePlan, LaterReplayedCommit,
        LaterReplayedCommitKind, RewritePlan, SourceBranchRecord,
    };
    use crate::commands::common::{CherryPickEmptyPolicy, CherryPickOp};
    use crate::commands::rewrite_resume::{resume_rewrite, RewriteResumeState};
    use crate::commands::RewriteCommandOutcome;
    use crate::config::DirtyWorktreePolicy;
    use crate::parsing::{derive_local_groups_with_ignored, Group};
    use crate::test_support::{lock_cwd, DirGuard};
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;
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

    struct StackRepo {
        dir: TempDir,
        repo: PathBuf,
        stack_branch: String,
        base_branch: String,
        prefix: String,
        ignore_tag: String,
        alpha_tip: String,
        beta_tip: Option<String>,
        gamma_tip: Option<String>,
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("spawn git");
        assert!(
            out.status.success(),
            "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn init_repo() -> TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let repo = dir.path();
        git(repo, ["init", "-b", "main"].as_slice());
        git(repo, ["config", "user.email", "spr@example.com"].as_slice());
        git(repo, ["config", "user.name", "SPR Tests"].as_slice());
        write_file(repo, "README.md", "init\n");
        git(repo, ["add", "."].as_slice());
        git(repo, ["commit", "-m", "init"].as_slice());
        dir
    }

    fn write_file(repo: &Path, file: &str, contents: &str) {
        fs::write(repo.join(file), contents).expect("write file");
    }

    fn commit_file(repo: &Path, file: &str, contents: &str, message: &str) -> String {
        write_file(repo, file, contents);
        git(repo, ["add", file].as_slice());
        git(repo, ["commit", "-m", message].as_slice());
        rev_parse(repo, "HEAD")
    }

    fn rev_parse(repo: &Path, revision: &str) -> String {
        git(repo, ["rev-parse", revision].as_slice())
            .trim()
            .to_string()
    }

    fn current_branch(repo: &Path) -> String {
        git(repo, ["rev-parse", "--abbrev-ref", "HEAD"].as_slice())
            .trim()
            .to_string()
    }

    fn current_path() -> String {
        env::var("PATH").expect("PATH is set")
    }

    fn find_git_binary() -> String {
        let out = Command::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .expect("find git binary");
        assert!(
            out.status.success(),
            "failed to locate git binary\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn install_cleanup_failure_git_wrapper(tmp_path: &str) -> TempDir {
        let dir = tempfile::tempdir().expect("create wrapper dir");
        let wrapper_path = dir.path().join("git");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"worktree\" ] && [ \"$2\" = \"remove\" ] && [ \"$3\" = \"-f\" ] && [ \"$4\" = \"{tmp_path}\" ]; then\n  echo \"simulated absorb cleanup failure\" >&2\n  exit 1\nfi\nexec \"{}\" \"$@\"\n",
            find_git_binary()
        );
        fs::write(&wrapper_path, script).expect("write git wrapper");
        let mut permissions = fs::metadata(&wrapper_path)
            .expect("read git wrapper metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&wrapper_path, permissions).expect("chmod git wrapper");
        dir
    }

    fn tags_with_pattern(repo: &Path, pattern: &str) -> Vec<String> {
        git(repo, ["tag", "--list", pattern].as_slice())
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    fn branches_with_pattern(repo: &Path, pattern: &str) -> Vec<String> {
        git(repo, ["branch", "--list", pattern].as_slice())
            .lines()
            .map(str::trim)
            .map(|line| line.strip_prefix("* ").unwrap_or(line))
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    fn commit_range(repo: &Path, from_exclusive: &str, to_inclusive: &str) -> Vec<String> {
        let range = format!("{from_exclusive}..{to_inclusive}");
        git(repo, ["rev-list", "--reverse", &range].as_slice())
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    fn rewrite_stack_branch_equivalently(repo: &StackRepo) {
        let commits = commit_range(&repo.repo, &repo.base_branch, &repo.stack_branch);
        git(
            &repo.repo,
            ["checkout", "-b", "rewrite-copy", &repo.base_branch].as_slice(),
        );
        for commit in &commits {
            git(
                &repo.repo,
                [
                    "-c",
                    "user.name=Rewrite Stack",
                    "-c",
                    "user.email=rewrite@example.com",
                    "cherry-pick",
                    commit,
                ]
                .as_slice(),
            );
        }
        git(
            &repo.repo,
            ["branch", "-f", &repo.stack_branch, "HEAD"].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());
        git(&repo.repo, ["branch", "-D", "rewrite-copy"].as_slice());
    }

    fn setup_single_group_stack(create_alpha_branch: bool) -> StackRepo {
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let base_branch = "main".to_string();
        let stack_branch = "stack".to_string();
        let prefix = "dank-spr/".to_string();
        let ignore_tag = "ignore".to_string();

        git(&repo, ["checkout", "-b", &stack_branch].as_slice());
        let _alpha_seed = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\n",
            "feat: alpha seed\n\npr:alpha",
        );
        let alpha_tip = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\nalpha-2\n",
            "feat: alpha follow-up",
        );
        if create_alpha_branch {
            git(&repo, ["branch", "dank-spr/alpha", &alpha_tip].as_slice());
        }

        StackRepo {
            dir,
            repo,
            stack_branch,
            base_branch,
            prefix,
            ignore_tag,
            alpha_tip,
            beta_tip: None,
            gamma_tip: None,
        }
    }

    fn setup_three_group_stack() -> StackRepo {
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let base_branch = "main".to_string();
        let stack_branch = "stack".to_string();
        let prefix = "dank-spr/".to_string();
        let ignore_tag = "ignore".to_string();

        git(&repo, ["checkout", "-b", &stack_branch].as_slice());
        let _alpha_seed = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\n",
            "feat: alpha seed\n\npr:alpha",
        );
        let alpha_tip = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\nalpha-2\n",
            "feat: alpha follow-up",
        );
        let _ignore_seed = commit_file(
            &repo,
            "scratch.txt",
            "scratch-1\n",
            "chore: local experiments\n\npr:ignore",
        );
        let _ignore_follow = commit_file(
            &repo,
            "scratch.txt",
            "scratch-1\nscratch-2\n",
            "wip: scratch cleanup",
        );
        let beta_tip = commit_file(&repo, "beta.txt", "beta-1\n", "feat: beta seed\n\npr:beta");
        let gamma_tip = commit_file(
            &repo,
            "gamma.txt",
            "gamma-1\n",
            "feat: gamma seed\n\npr:gamma",
        );

        git(&repo, ["branch", "dank-spr/alpha", &alpha_tip].as_slice());
        git(&repo, ["branch", "dank-spr/beta", &beta_tip].as_slice());
        git(&repo, ["branch", "dank-spr/gamma", &gamma_tip].as_slice());

        StackRepo {
            dir,
            repo,
            stack_branch,
            base_branch,
            prefix,
            ignore_tag,
            alpha_tip,
            beta_tip: Some(beta_tip),
            gamma_tip: Some(gamma_tip),
        }
    }

    fn setup_three_group_stack_with_beta_follow_up() -> StackRepo {
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let base_branch = "main".to_string();
        let stack_branch = "stack".to_string();
        let prefix = "dank-spr/".to_string();
        let ignore_tag = "ignore".to_string();

        git(&repo, ["checkout", "-b", &stack_branch].as_slice());
        let _alpha_seed = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\n",
            "feat: alpha seed\n\npr:alpha",
        );
        let alpha_tip = commit_file(
            &repo,
            "alpha.txt",
            "alpha-1\nalpha-2\n",
            "feat: alpha follow-up",
        );
        let _ignore_seed = commit_file(
            &repo,
            "scratch.txt",
            "scratch-1\n",
            "chore: local experiments\n\npr:ignore",
        );
        let _ignore_follow = commit_file(
            &repo,
            "scratch.txt",
            "scratch-1\nscratch-2\n",
            "wip: scratch cleanup",
        );
        let _beta_seed = commit_file(&repo, "beta.txt", "beta-1\n", "feat: beta seed\n\npr:beta");
        let beta_tip = commit_file(
            &repo,
            "beta-extra.txt",
            "beta-extra-1\n",
            "feat: beta follow-up",
        );
        let gamma_tip = commit_file(
            &repo,
            "gamma.txt",
            "gamma-1\n",
            "feat: gamma seed\n\npr:gamma",
        );

        git(&repo, ["branch", "dank-spr/alpha", &alpha_tip].as_slice());
        git(&repo, ["branch", "dank-spr/beta", &beta_tip].as_slice());
        git(&repo, ["branch", "dank-spr/gamma", &gamma_tip].as_slice());

        StackRepo {
            dir,
            repo,
            stack_branch,
            base_branch,
            prefix,
            ignore_tag,
            alpha_tip,
            beta_tip: Some(beta_tip),
            gamma_tip: Some(gamma_tip),
        }
    }

    fn setup_absorb_conflict_stack() -> StackRepo {
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let base_branch = "main".to_string();
        let stack_branch = "stack".to_string();
        let prefix = "dank-spr/".to_string();
        let ignore_tag = "ignore".to_string();

        git(&repo, ["checkout", "-b", &stack_branch].as_slice());
        let alpha_tip = commit_file(
            &repo,
            "story.txt",
            "alpha-1\n",
            "feat: alpha seed\n\npr:alpha",
        );
        let beta_tip = commit_file(&repo, "story.txt", "beta-1\n", "feat: beta seed\n\npr:beta");

        git(&repo, ["branch", "dank-spr/alpha", &alpha_tip].as_slice());
        git(&repo, ["branch", "dank-spr/beta", &beta_tip].as_slice());

        StackRepo {
            dir,
            repo,
            stack_branch,
            base_branch,
            prefix,
            ignore_tag,
            alpha_tip,
            beta_tip: Some(beta_tip),
            gamma_tip: None,
        }
    }

    fn append_commit_to_branch(
        repo: &Path,
        branch: &str,
        file: &str,
        contents: &str,
        message: &str,
    ) -> String {
        git(repo, ["checkout", branch].as_slice());
        commit_file(repo, file, contents, message)
    }

    fn default_absorb_options() -> AbsorbOptions {
        AbsorbOptions::default()
    }

    fn absorb_override_options() -> AbsorbOptions {
        AbsorbOptions {
            copied_later_stack_commit_policy:
                CopiedLaterStackCommitPolicy::AllowKeepNonSeedDuplicates,
        }
    }

    fn classify_alpha(repo: &StackRepo, options: AbsorbOptions) -> AbsorbClassification {
        let _guard = DirGuard::change_to(&repo.repo);
        let (merge_base, _leading_ignored, groups) =
            derive_local_groups_with_ignored(&repo.base_branch, &repo.ignore_tag).unwrap();
        let later_owned_patch_maps = build_later_owned_patch_maps(&groups).unwrap();
        let plan = build_group_rewrite_plan(
            groups[0].clone(),
            &repo.prefix,
            &merge_base,
            &rev_parse(&repo.repo, "HEAD"),
            &later_owned_patch_maps[0],
            options,
        )
        .unwrap();
        plan.source.classification
    }

    #[test]
    fn classify_missing_source_branch_as_skip() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(false);
        let _keep_dir_alive = repo.dir.path();

        assert_eq!(
            classify_alpha(&repo, default_absorb_options()),
            AbsorbClassification::Skip(AbsorbSkipReason::MissingBranch)
        );
    }

    #[test]
    fn classify_unchanged_source_branch_as_skip() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();

        assert_eq!(
            classify_alpha(&repo, default_absorb_options()),
            AbsorbClassification::Skip(AbsorbSkipReason::Unchanged)
        );
    }

    #[test]
    fn classify_branch_behind_stack_as_skip() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());
        let _new_alpha_tip = commit_file(
            &repo.repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-3\n",
            "feat: alpha on stack",
        );

        assert_eq!(
            classify_alpha(&repo, default_absorb_options()),
            AbsorbClassification::Skip(AbsorbSkipReason::BranchBehindStack)
        );
    }

    #[test]
    fn classify_patch_equivalent_rewritten_source_branch_as_skip() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        rewrite_stack_branch_equivalently(&repo);

        assert_eq!(
            classify_alpha(&repo, default_absorb_options()),
            AbsorbClassification::Skip(AbsorbSkipReason::Unchanged)
        );
    }

    #[test]
    fn classify_patch_equivalent_rewritten_source_branch_with_tail_as_absorbable() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        rewrite_stack_branch_equivalently(&repo);
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        let tail_sha = commit_file(
            &repo.repo,
            "alpha-branch.txt",
            "alpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, default_absorb_options()) {
            AbsorbClassification::Absorbable(tail) => {
                assert_eq!(tail.commits, vec![tail_sha]);
            }
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    #[test]
    fn classify_diverged_source_branch_as_blocking() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());
        let _new_alpha_tip = commit_file(
            &repo.repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-stack\n",
            "feat: alpha on stack",
        );
        let _branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha on branch",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        assert_eq!(
            classify_alpha(&repo, default_absorb_options()),
            AbsorbClassification::Blocked(AbsorbBlocker::Diverged)
        );
    }

    #[test]
    fn classify_branch_with_later_stack_commits_as_blocking() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        git(
            &repo.repo,
            ["merge", "--ff-only", &repo.stack_branch].as_slice(),
        );
        let _branch_tip = commit_file(
            &repo.repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha after stack ff",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, default_absorb_options()) {
            AbsorbClassification::Blocked(AbsorbBlocker::MergeBaseMismatch { .. }) => {}
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    #[test]
    fn classify_tail_with_later_replayed_stack_patch_as_blocking() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack_with_beta_follow_up();
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        git(
            &repo.repo,
            ["cherry-pick", repo.beta_tip.as_ref().unwrap()].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, default_absorb_options()) {
            AbsorbClassification::Blocked(AbsorbBlocker::TailOwnedByLaterReplayedCommit {
                owner_tag,
                owner_commit,
                ..
            }) => {
                assert_eq!(owner_tag, "beta");
                assert_eq!(owner_commit, repo.beta_tip.unwrap());
            }
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    #[test]
    fn classify_tail_with_later_replayed_stack_patch_is_absorbable_with_override() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack_with_beta_follow_up();
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        git(
            &repo.repo,
            ["cherry-pick", repo.beta_tip.as_ref().unwrap()].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, absorb_override_options()) {
            AbsorbClassification::Absorbable(tail) => {
                assert_eq!(tail.commits.len(), 1);
                assert_eq!(tail.kept_later_duplicate_replays.len(), 1);
                assert_eq!(tail.kept_later_duplicate_replays[0].tag, "beta");
                assert_eq!(
                    tail.kept_later_duplicate_replays[0].sha,
                    repo.beta_tip.unwrap()
                );
            }
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    #[test]
    fn classify_tail_with_merge_commit_as_blocking() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        git(
            &repo.repo,
            ["checkout", "-b", "side", &repo.alpha_tip].as_slice(),
        );
        let _side_tip = commit_file(&repo.repo, "side.txt", "side-1\n", "feat: side change");
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        let _alpha_tail = commit_file(
            &repo.repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(
            &repo.repo,
            ["merge", "--no-ff", "side", "-m", "merge side"].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, default_absorb_options()) {
            AbsorbClassification::Blocked(AbsorbBlocker::TailHasMergeCommit { .. }) => {}
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    #[test]
    fn classify_tail_with_stack_marker_as_blocking() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        let _branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: branch marker\n\npr:beta",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        match classify_alpha(&repo, default_absorb_options()) {
            AbsorbClassification::Blocked(AbsorbBlocker::TailHasStackMarker { .. }) => {}
            other => panic!("unexpected classification: {:?}", other),
        }
    }

    fn synthetic_group_plan(
        tag: &str,
        commits: &[&str],
        absorbed_tail: &[&str],
        ignored_after: &[&str],
    ) -> GroupRewritePlan {
        GroupRewritePlan {
            group: Group {
                tag: tag.to_string(),
                subjects: Vec::new(),
                commits: commits.iter().map(|sha| sha.to_string()).collect(),
                first_message: None,
                ignored_after: ignored_after.iter().map(|sha| sha.to_string()).collect(),
            },
            source: SourceBranchRecord {
                tag: tag.to_string(),
                branch_name: format!("dank-spr/{tag}"),
                group_tip: commits.last().unwrap().to_string(),
                source_tip: Some(
                    absorbed_tail
                        .last()
                        .copied()
                        .unwrap_or_else(|| commits.last().unwrap())
                        .to_string(),
                ),
                classification: if absorbed_tail.is_empty() {
                    AbsorbClassification::Skip(AbsorbSkipReason::Unchanged)
                } else {
                    AbsorbClassification::Absorbable(AbsorbTail {
                        commits: absorbed_tail.iter().map(|sha| sha.to_string()).collect(),
                        kept_later_duplicate_replays: Vec::new(),
                    })
                },
            },
        }
    }

    #[test]
    fn validate_plan_reports_blocked_groups_without_git_side_effects() {
        let blocked_group = synthetic_group_plan("alpha", &["alpha-1"], &[], &[]);
        let plan = RewritePlan {
            merge_base: "base".to_string(),
            leading_ignored: Vec::new(),
            groups: vec![GroupRewritePlan {
                source: SourceBranchRecord {
                    classification: AbsorbClassification::Blocked(AbsorbBlocker::Diverged),
                    ..blocked_group.source
                },
                ..blocked_group
            }],
            operations: Vec::new(),
        };

        assert_eq!(
            validate_absorb_plan(&plan),
            AbsorbPlanValidation::Blocked {
                lines: vec![
                    "pr:alpha <- dank-spr/alpha : block (branch diverged from stack group tip)"
                        .to_string()
                ]
            }
        );
    }

    #[test]
    fn build_rewrite_operations_replays_all_segments_in_order() {
        let alpha = synthetic_group_plan("alpha", &["a1", "a2"], &["a3"], &["i1", "i2"]);
        let beta = synthetic_group_plan("beta", &["b1"], &[], &[]);

        assert_eq!(
            build_rewrite_operations(&["l1".to_string(), "l2".to_string()], &[alpha, beta]),
            vec![
                CherryPickOp::Range {
                    first: "l1".to_string(),
                    last: "l2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Range {
                    first: "a1".to_string(),
                    last: "a2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "a3".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Range {
                    first: "i1".to_string(),
                    last: "i2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "b1".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
            ]
        );
    }

    #[test]
    fn build_rewrite_operations_keeps_later_duplicate_replays_with_empty_keep_policy() {
        let mut alpha = synthetic_group_plan("alpha", &["a1", "a2"], &["x1"], &[]);
        if let AbsorbClassification::Absorbable(tail) = &mut alpha.source.classification {
            tail.kept_later_duplicate_replays = vec![LaterReplayedCommit {
                tag: "beta".to_string(),
                sha: "b2".to_string(),
                kind: LaterReplayedCommitKind::GroupFollowUp,
            }];
        }
        let beta = synthetic_group_plan("beta", &["b1", "b2", "b3"], &[], &[]);

        assert_eq!(
            build_rewrite_operations(&[], &[alpha, beta]),
            vec![
                CherryPickOp::Range {
                    first: "a1".to_string(),
                    last: "a2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "x1".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "b1".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
                CherryPickOp::Commit {
                    sha: "b2".to_string(),
                    empty_policy: CherryPickEmptyPolicy::KeepRedundantCommits,
                },
                CherryPickOp::Commit {
                    sha: "b3".to_string(),
                    empty_policy: CherryPickEmptyPolicy::StopOnEmpty,
                },
            ]
        );
    }

    #[test]
    fn absorb_blocked_plan_leaves_head_and_backup_tags_unchanged() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());
        let _new_alpha_tip = commit_file(
            &repo.repo,
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-stack\n",
            "feat: alpha on stack",
        );
        let original_head = rev_parse(&repo.repo, "HEAD");
        let _branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha on branch",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        let result = {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
        };

        assert!(result.is_err(), "blocked absorb should fail");
        assert_eq!(
            rev_parse(&repo.repo, "HEAD"),
            original_head,
            "blocked absorb must not rewrite HEAD"
        );
        assert!(
            tags_with_pattern(&repo.repo, "backup/absorb/*").is_empty(),
            "blocked absorb should not create a backup tag"
        );
    }

    #[test]
    fn absorb_skips_patch_equivalent_rewritten_branch_without_rewrite() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        rewrite_stack_branch_equivalently(&repo);
        let original_head = rev_parse(&repo.repo, "HEAD");

        let result = {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
            .unwrap()
        };

        assert_eq!(result, RewriteCommandOutcome::Completed);
        assert_eq!(
            rev_parse(&repo.repo, "HEAD"),
            original_head,
            "patch-equivalent stale branches should not rewrite HEAD"
        );
        assert!(
            tags_with_pattern(&repo.repo, "backup/absorb/*").is_empty(),
            "no-op absorb should not create a backup tag"
        );
    }

    #[test]
    fn absorb_rejects_tail_owned_by_later_stack_commit_without_rewrite() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack_with_beta_follow_up();
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        git(
            &repo.repo,
            ["cherry-pick", repo.beta_tip.as_ref().unwrap()].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        let result = {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
        };

        assert!(
            result.is_err(),
            "copied later stack commit should block absorb"
        );
        assert_eq!(
            rev_parse(&repo.repo, "HEAD"),
            original_head,
            "blocked absorb must not rewrite HEAD"
        );
        assert!(
            tags_with_pattern(&repo.repo, "backup/absorb/*").is_empty(),
            "blocked absorb should not create a backup tag"
        );
    }

    #[test]
    fn absorb_override_keeps_later_duplicate_replay_and_rewrites_stack() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack_with_beta_follow_up();
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");
        git(&repo.repo, ["checkout", "dank-spr/alpha"].as_slice());
        git(
            &repo.repo,
            ["cherry-pick", repo.beta_tip.as_ref().unwrap()].as_slice(),
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                absorb_override_options(),
            )
            .unwrap();
        }

        let rewritten_head = rev_parse(&repo.repo, "HEAD");
        assert_ne!(
            rewritten_head, original_head,
            "override absorb should rewrite HEAD"
        );
        let backup_tags = tags_with_pattern(&repo.repo, "backup/absorb/*");
        assert_eq!(
            backup_tags.len(),
            1,
            "override absorb should create one backup tag"
        );

        let _guard = DirGuard::change_to(&repo.repo);
        let (_merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored(&repo.base_branch, &repo.ignore_tag).unwrap();
        assert!(leading_ignored.is_empty());
        assert_eq!(
            groups
                .iter()
                .map(|group| group.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        assert_eq!(
            groups[0].commits.len(),
            3,
            "alpha should absorb the copied beta follow-up"
        );
        assert_eq!(
            groups[0].subjects.last().unwrap(),
            "feat: beta follow-up",
            "alpha should gain the copied later follow-up"
        );
        assert_eq!(
            groups[1].commits.len(),
            2,
            "beta should keep the later duplicate replay under the override"
        );
        assert_eq!(
            groups[1].subjects,
            vec![
                "feat: beta seed".to_string(),
                "feat: beta follow-up".to_string()
            ],
            "beta should retain both its seed and follow-up commits"
        );
    }

    #[test]
    fn absorb_rewrites_multiple_groups_and_preserves_ignored_blocks() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");
        let original_merge_base = git(
            &repo.repo,
            ["merge-base", &repo.base_branch, "HEAD"].as_slice(),
        )
        .trim()
        .to_string();
        let _alpha_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        let _alpha_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\nalpha-branch-2\n",
            "feat: alpha branch tail 2",
        );
        let _gamma_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/gamma",
            "gamma.txt",
            "gamma-1\ngamma-branch\n",
            "feat: gamma branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
            .unwrap();
        }

        let rewritten_head = rev_parse(&repo.repo, "HEAD");
        assert_ne!(rewritten_head, original_head, "absorb should rewrite HEAD");
        let backup_tags = tags_with_pattern(&repo.repo, "backup/absorb/*");
        assert_eq!(backup_tags.len(), 1, "absorb should create one backup tag");
        let backup_target = rev_parse(&repo.repo, &format!("refs/tags/{}", backup_tags[0]));
        assert_eq!(
            backup_target, original_head,
            "backup tag should point at the pre-absorb HEAD"
        );

        let rewritten_merge_base = git(
            &repo.repo,
            ["merge-base", &repo.base_branch, "HEAD"].as_slice(),
        )
        .trim()
        .to_string();
        assert_eq!(
            rewritten_merge_base, original_merge_base,
            "absorb should preserve the current stack/base merge-base"
        );

        let _guard = DirGuard::change_to(&repo.repo);
        let (_merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored(&repo.base_branch, &repo.ignore_tag).unwrap();
        assert!(
            leading_ignored.is_empty(),
            "stack should not gain leading ignored commits"
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        assert_eq!(
            groups[0].commits.len(),
            4,
            "alpha should include both absorbed branch-tail commits"
        );
        assert_eq!(
            groups[0].subjects[2..].to_vec(),
            vec![
                "feat: alpha branch tail".to_string(),
                "feat: alpha branch tail 2".to_string(),
            ],
            "alpha should keep the absorbed tail subjects in order"
        );
        assert_eq!(
            groups[0].ignored_after.len(),
            2,
            "alpha ignored block should remain attached after the absorbed commits"
        );
        assert_eq!(groups[1].commits.len(), 1, "beta should remain unchanged");
        assert_eq!(
            groups[2].commits.len(),
            2,
            "gamma should include the absorbed branch tail"
        );
        assert_eq!(
            groups[2].subjects.last().unwrap(),
            "feat: gamma branch tail"
        );
    }

    #[test]
    fn absorb_succeeds_even_if_post_reset_cleanup_fails() {
        let _lock = lock_cwd();
        let repo = setup_single_group_stack(true);
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");
        let _branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        let short = git(&repo.repo, ["rev-parse", "--short", "HEAD"].as_slice())
            .trim()
            .to_string();
        let tmp_path = format!("/tmp/spr-absorb-{}", short);
        let tmp_branch = format!("spr/tmp-absorb-{}", short);
        let wrapper_dir = install_cleanup_failure_git_wrapper(&tmp_path);

        {
            let wrapped_path = format!("{}:{}", wrapper_dir.path().display(), current_path());
            let _path_guard = EnvVarGuard::set("PATH", wrapped_path);
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
            .expect("cleanup failure after reset should not fail absorb");
        }

        let rewritten_head = rev_parse(&repo.repo, "HEAD");
        assert_ne!(
            rewritten_head, original_head,
            "absorb should still rewrite HEAD when cleanup fails afterward"
        );
        assert!(
            Path::new(&tmp_path).exists(),
            "the temp worktree should remain when cleanup is forced to fail"
        );

        git(
            &repo.repo,
            ["worktree", "remove", "-f", &tmp_path].as_slice(),
        );
        git(&repo.repo, ["branch", "-D", &tmp_branch].as_slice());
    }

    #[test]
    fn absorb_is_noop_when_no_groups_are_absorbable() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");

        {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
            .unwrap();
        }

        assert_eq!(rev_parse(&repo.repo, "HEAD"), original_head);
        assert!(
            tags_with_pattern(&repo.repo, "backup/absorb/*").is_empty(),
            "no-op absorb should not create a backup tag"
        );
    }

    #[test]
    fn absorb_dry_run_leaves_head_unchanged() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();
        let original_head = rev_parse(&repo.repo, "HEAD");
        let _alpha_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                true,
                crate::config::DirtyWorktreePolicy::Discard,
                default_absorb_options(),
            )
            .unwrap();
        }

        assert_eq!(
            rev_parse(&repo.repo, "HEAD"),
            original_head,
            "dry-run absorb must not change HEAD"
        );
        assert!(
            tags_with_pattern(&repo.repo, "backup/absorb/*").is_empty(),
            "dry-run absorb should not create a backup tag"
        );
        assert!(
            branches_with_pattern(&repo.repo, "spr/tmp-absorb-*").is_empty(),
            "dry-run absorb should not leave temp absorb branches behind"
        );
    }

    #[test]
    fn absorb_suspends_and_resumes_conflict() {
        let _lock = lock_cwd();
        let repo = setup_absorb_conflict_stack();
        let _keep_dir_alive = repo.dir.path();
        let _alpha_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "story.txt",
            "alpha-1\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        let outcome = {
            let _guard = DirGuard::change_to(&repo.repo);
            absorb_branch_tails(
                &repo.base_branch,
                &repo.prefix,
                &repo.ignore_tag,
                false,
                DirtyWorktreePolicy::Halt,
                default_absorb_options(),
            )
            .expect("absorb should suspend")
        };
        let resume_path = match outcome {
            RewriteCommandOutcome::Completed => panic!("expected suspended absorb"),
            RewriteCommandOutcome::Suspended(state) => state.resume_path.clone(),
        };
        let resume_state: RewriteResumeState =
            serde_json::from_str(&fs::read_to_string(&resume_path).expect("read resume state"))
                .expect("parse resume state");
        fs::write(
            Path::new(&resume_state.temp_worktree_path).join("story.txt"),
            "alpha-1\nalpha-branch\nbeta-1\n",
        )
        .expect("resolve absorb conflict");
        git(
            Path::new(&resume_state.temp_worktree_path),
            ["add", "story.txt"].as_slice(),
        );

        {
            let _guard = DirGuard::change_to(&repo.repo);
            let resumed = resume_rewrite(false, &resume_path).expect("resume absorb");
            assert_eq!(resumed, RewriteCommandOutcome::Completed);
        }

        assert!(
            !resume_path.exists(),
            "successful absorb resume should delete the resume file"
        );
        assert_eq!(
            fs::read_to_string(repo.repo.join("story.txt")).expect("read final file"),
            "alpha-1\nalpha-branch\nbeta-1\n"
        );
        let _guard = DirGuard::change_to(&repo.repo);
        let (_merge_base, _leading_ignored, groups) =
            derive_local_groups_with_ignored(&repo.base_branch, &repo.ignore_tag).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].commits.len(),
            2,
            "alpha should absorb its branch tail"
        );
        assert_eq!(groups[1].commits.len(), 1, "beta should remain one group");
    }

    #[test]
    fn build_plan_records_absorbable_groups_for_rewrite() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();
        let _alpha_branch_tip = append_commit_to_branch(
            &repo.repo,
            "dank-spr/alpha",
            "alpha.txt",
            "alpha-1\nalpha-2\nalpha-branch\n",
            "feat: alpha branch tail",
        );
        git(&repo.repo, ["checkout", &repo.stack_branch].as_slice());

        let _guard = DirGuard::change_to(&repo.repo);
        let (_stack_head, plan) = build_plan_for_current_stack(
            &repo.base_branch,
            &repo.prefix,
            &repo.ignore_tag,
            default_absorb_options(),
        )
        .unwrap();
        assert!(plan.has_absorbable_groups());
        assert!(!plan.has_blockers());
        assert!(
            plan.operations.len() >= 4,
            "rewrite plan should include leading replay plus group operations"
        );
    }

    #[test]
    fn setup_helpers_keep_expected_branches() {
        let _lock = lock_cwd();
        let repo = setup_three_group_stack();
        let _keep_dir_alive = repo.dir.path();

        assert_eq!(current_branch(&repo.repo), repo.stack_branch);
        assert_eq!(rev_parse(&repo.repo, "dank-spr/alpha"), repo.alpha_tip);
        assert_eq!(
            rev_parse(&repo.repo, "dank-spr/beta"),
            repo.beta_tip.unwrap()
        );
        assert_eq!(
            rev_parse(&repo.repo, "dank-spr/gamma"),
            repo.gamma_tip.unwrap()
        );
    }
}
