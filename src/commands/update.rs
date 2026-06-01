use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, Duration as TimeDuration, OffsetDateTime};
use tracing::{info, warn};

use crate::branch_names::{
    canonical_branch_conflict_key, group_branch_identities, CanonicalBranchConflictKey,
};
use crate::commands::common;
use crate::config::{ListOrder, LocalPrBranchSyncPolicy, PrDescriptionMode};
use crate::execution::ExecutionMode;
use crate::git::{get_remote_branches_sha, gh_rw, git_is_ancestor, git_rw, sanitize_gh_base_ref};
use crate::github::{
    convert_pull_requests_to_draft, fetch_pr_bodies_graphql, fetch_pr_stage_info_graphql,
    get_repo_owner_name, graphql_escape, is_resource_limit_error,
    list_recent_terminal_prs_for_heads, mark_pull_requests_ready_for_review, upsert_pr_cached,
    PrStageInfo, TerminalPrState,
};
use crate::limit::{apply_limit_groups, Limit};
use crate::parsing::Group;
use crate::pr_base_chain::{
    build_desired_pr_base_chain, plan_base_reconciliation, verify_base_edits_converged,
    BaseReconciliationAction, BaseReconciliationDecision, ObservedPrBaseChain,
};
use crate::update_output::{
    SkippedUpdateGroupData, UpdateEditAction, UpdateExecutionData, UpdateGroupData, UpdatePrAction,
    UpdatePushAction, UpdateSkippedReason,
};
use crate::validation::ValidationDescriptor;

#[cfg(test)]
use crate::parsing::{derive_groups_between_with_ignored, split_groups_for_update};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdatePushValidation {
    Legacy,
    Required(Vec<ValidationDescriptor>),
    Skip,
}

impl UpdatePushValidation {
    fn skips_push_hooks(&self) -> bool {
        matches!(self, Self::Required(_) | Self::Skip)
    }
}

/// Replace the existing spr stack block with `new_block`, or append it if missing.
///
/// The stack block is delimited by `<!-- spr-stack:start -->` and
/// `<!-- spr-stack:end -->`. If the markers are absent, the block is appended
/// with a blank line separator (or becomes the whole body when empty).
fn update_stack_block(body: &str, new_block: &str) -> String {
    let start = "<!-- spr-stack:start -->";
    let end = "<!-- spr-stack:end -->";
    if let (Some(s), Some(e)) = (body.find(start), body.find(end)) {
        if e >= s {
            let e = e + end.len();
            let mut out = String::new();
            out.push_str(&body[..s]);
            out.push_str(new_block);
            out.push_str(&body[e..]);
            return out;
        }
    }
    if body.trim().is_empty() {
        new_block.to_string()
    } else {
        format!("{}\n\n{}", body, new_block)
    }
}

/// Parse a GitHub GraphQL RFC3339 timestamp string.
fn parse_github_timestamp_rfc3339(s: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339)
        .with_context(|| format!("Failed to parse GitHub RFC3339 timestamp: {}", s))
}

/// Compute precise elapsed time since a terminal PR event.
///
/// Callers should use this typed duration for both comparisons and display conversion so the
/// same source value drives the guard decision and the error message.
fn recent_pr_age(terminal_at: OffsetDateTime, now: OffsetDateTime) -> TimeDuration {
    now - terminal_at
}

/// Return the typed duration threshold used by the branch reuse guard.
fn branch_reuse_guard_window(guard_days: u32) -> TimeDuration {
    TimeDuration::days(i64::from(guard_days))
}

/// Return true when a terminal PR age falls within the configured guard window.
///
/// The comparison uses precise typed durations and does not round/truncate before comparing.
fn recent_pr_age_blocks_recreation(age: TimeDuration, guard_window: TimeDuration) -> bool {
    age <= guard_window
}

/// Convert a duration to fractional days for user-facing error messages.
///
/// This is display-only and intentionally preserves sub-day precision so the error text matches
/// the same non-rounded comparison used by `merged_pr_age_blocks_recreation`.
fn duration_days_precise(duration: TimeDuration) -> f64 {
    duration.as_seconds_f64() / 86_400.0
}

/// Return the user-facing verb for a terminal PR event.
fn terminal_pr_action(state: TerminalPrState) -> &'static str {
    if state == TerminalPrState::Merged {
        "merged"
    } else {
        "closed"
    }
}

fn head_key(head: &str) -> CanonicalBranchConflictKey {
    canonical_branch_conflict_key(head)
}

fn heads_without_open_prs(
    heads: &[String],
    prs_by_head: &HashMap<CanonicalBranchConflictKey, u64>,
) -> Vec<String> {
    heads
        .iter()
        .filter(|head| !prs_by_head.contains_key(&head_key(head)))
        .cloned()
        .collect()
}

fn pr_number_for_head(
    prs_by_head: &HashMap<CanonicalBranchConflictKey, u64>,
    head: &str,
) -> Option<u64> {
    prs_by_head.get(&head_key(head)).copied()
}

/// Fail `spr update` early when branch-name reuse matches a recently closed or merged PR.
///
/// The guard only runs when PR creation is enabled, the CLI override is not set, and the
/// threshold is non-zero.
///
/// 1. Open PRs are resolved first, and this guard only examines heads that do not already have an
///    open PR.
/// 2. For each remaining head, GitHub GraphQL `search(query: ...)` is used with
///    `is:pr is:closed head:<full-head> closed:>=<date> sort:closed-desc`.
/// 3. That `head:` search is case-insensitive for the branch spellings `spr` cares about, so any
///    recent terminal PR on the same head identity is a candidate block.
/// 4. The returned `mergedAt` or `closedAt` timestamp is then parsed and compared precisely in
///    Rust because GitHub's `closed:` search qualifier is date-based, not full-RFC3339.
///
/// Querying all heads here would duplicate the open-PR lookup and could misreport a branch that
/// already has an exact open PR as a reuse conflict against its own history.
///
/// # Errors
///
/// Returns an error when the terminal-PR lookup fails, when GitHub timestamps cannot be parsed,
/// or when a recent closed or merged PR is found within the configured threshold.
fn enforce_branch_reuse_guard(
    no_pr: bool,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
    heads: &[String],
    prs_by_head: &HashMap<CanonicalBranchConflictKey, u64>,
) -> Result<()> {
    if no_pr || allow_branch_reuse || branch_reuse_guard_days == 0 {
        Ok(())
    } else {
        let heads_without_open_prs = heads_without_open_prs(heads, prs_by_head);
        if heads_without_open_prs.is_empty() {
            Ok(())
        } else {
            let now = OffsetDateTime::now_utc();
            let guard_window = branch_reuse_guard_window(branch_reuse_guard_days);
            let terminal_prs =
                list_recent_terminal_prs_for_heads(&heads_without_open_prs, now - guard_window)?;
            for terminal_pr in terminal_prs {
                let terminal_at = parse_github_timestamp_rfc3339(&terminal_pr.terminal_at)
                    .with_context(|| {
                        format!(
                            "Failed to parse terminal timestamp for PR #{} ({})",
                            terminal_pr.number, terminal_pr.url
                        )
                    })?;
                let age = recent_pr_age(terminal_at, now);
                if recent_pr_age_blocks_recreation(age, guard_window) {
                    let age_days = duration_days_precise(age);
                    let action = terminal_pr_action(terminal_pr.state);
                    return Err(anyhow!(
                        "Refusing to recreate a PR for branch {} because PR #{} ({}) on that branch was {} {:.3} day(s) ago, within the configured guard window (branch_reuse_guard_days={}). You probably meant spr restack. If branch-name reuse is intentional, rerun with --allow-branch-reuse.",
                        terminal_pr.head,
                        terminal_pr.number,
                        terminal_pr.url,
                        action,
                        age_days,
                        branch_reuse_guard_days
                    ));
                }
            }
            Ok(())
        }
    }
}

// GitHub does not publish a safe alias count for batched mutations. Base edits are small and retry
// by bisection on RESOURCE_LIMITS_EXCEEDED; body edits remain one-per-request because body size is
// user-controlled.
const MAX_BASE_UPDATES_PER_MUTATION: usize = 50;
const MAX_BASE_MUTATION_CHARS: usize = 20_000;
const MAX_BODY_UPDATES_PER_MUTATION: usize = 1;
const MAX_BODY_MUTATION_CHARS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushKind {
    Skip,
    FastForward,
    Force,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedPush {
    branch: String,
    target_sha: String,
    remote_exists: bool,
    kind: PushKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DraftProtectedBaseTransition {
    group_idx: usize,
    remote_pr_number: u64,
    head_branch: String,
    target_head_sha: String,
    current_base_ref: String,
    desired_base_ref: String,
}

fn draft_protected_base_transitions(
    base_reconciliation: &[BaseReconciliationDecision],
    planned_pushes: &[PlannedPush],
) -> Result<Vec<DraftProtectedBaseTransition>> {
    base_reconciliation
        .iter()
        .filter(|decision| decision.action == BaseReconciliationAction::NeedsEdit)
        .map(|decision| {
            let planned_push = planned_pushes
                .iter()
                .find(|planned_push| planned_push.branch == decision.desired.head_branch)
                .ok_or_else(|| {
                    anyhow!(
                        "Missing planned branch publication for {}",
                        decision.desired.head_branch
                    )
                })?;
            if planned_push.kind == PushKind::Skip {
                Ok(None)
            } else {
                let current_base_ref = decision.current_base_ref.clone().ok_or_else(|| {
                    anyhow!(
                        "Missing current GitHub base ref for {}",
                        decision.desired.head_branch
                    )
                })?;
                Ok(decision
                    .remote_pr_number
                    .map(|remote_pr_number| DraftProtectedBaseTransition {
                        group_idx: decision.desired.local_pr_number - 1,
                        remote_pr_number,
                        head_branch: decision.desired.head_branch.clone(),
                        target_head_sha: planned_push.target_sha.clone(),
                        current_base_ref: sanitize_gh_base_ref(&current_base_ref),
                        desired_base_ref: decision.desired.expected_base_ref.clone(),
                    }))
            }
        })
        .collect::<Result<Vec<_>>>()
        .map(|transitions| transitions.into_iter().flatten().collect())
}

fn base_ref_sha_after_branch_publication(
    transition: &DraftProtectedBaseTransition,
    planned_pushes: &[PlannedPush],
    remote_map: &HashMap<String, String>,
) -> Result<String> {
    if let Some(planned_base_push) = planned_pushes
        .iter()
        .find(|planned_push| planned_push.branch == transition.current_base_ref)
    {
        Ok(planned_base_push.target_sha.clone())
    } else {
        remote_map
            .get(&transition.current_base_ref)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "Missing remote branch SHA for current GitHub base {} on PR #{} ({})",
                    transition.current_base_ref,
                    transition.remote_pr_number,
                    transition.head_branch
                )
            })
    }
}

fn ancestry_collapse_risk_transitions(
    transitions: &[DraftProtectedBaseTransition],
    planned_pushes: &[PlannedPush],
    remote_map: &HashMap<String, String>,
) -> Result<Vec<DraftProtectedBaseTransition>> {
    transitions
        .iter()
        .map(|transition| {
            let future_old_base_sha =
                base_ref_sha_after_branch_publication(transition, planned_pushes, remote_map)?;
            Ok(
                git_is_ancestor(&transition.target_head_sha, &future_old_base_sha)?
                    .then(|| transition.clone()),
            )
        })
        .collect::<Result<Vec<_>>>()
        .map(|transitions| transitions.into_iter().flatten().collect())
}

fn pr_stage_info_for_transition<'a>(
    stage_info_by_number: &'a HashMap<u64, PrStageInfo>,
    transition: &DraftProtectedBaseTransition,
) -> Result<&'a PrStageInfo> {
    stage_info_by_number
        .get(&transition.remote_pr_number)
        .ok_or_else(|| {
            anyhow!(
                "Missing GitHub draft-stage metadata for PR #{} ({})",
                transition.remote_pr_number,
                transition.head_branch
            )
        })
}

fn ready_pull_request_ids_requiring_temporary_draft(
    transitions: &[DraftProtectedBaseTransition],
    stage_info_by_number: &HashMap<u64, PrStageInfo>,
) -> Result<Vec<String>> {
    transitions
        .iter()
        .map(|transition| {
            let stage_info = pr_stage_info_for_transition(stage_info_by_number, transition)?;
            Ok((!stage_info.is_draft).then(|| stage_info.id.clone()))
        })
        .collect::<Result<Vec<_>>>()
        .map(|pull_request_ids| pull_request_ids.into_iter().flatten().collect())
}

fn draft_protected_base_update_inputs(
    transitions: &[DraftProtectedBaseTransition],
    stage_info_by_number: &HashMap<u64, PrStageInfo>,
) -> Result<Vec<String>> {
    transitions
        .iter()
        .map(|transition| {
            let stage_info = pr_stage_info_for_transition(stage_info_by_number, transition)?;
            let desired_base_ref = sanitize_gh_base_ref(&transition.desired_base_ref);
            let fields = [
                format!("pullRequestId:\"{}\"", stage_info.id),
                format!("baseRefName:\"{}\"", graphql_escape(&desired_base_ref)),
            ];
            Ok(fields.join(", "))
        })
        .collect()
}

impl UpdatePushAction {
    fn from_planned_push(planned_push: &PlannedPush) -> Self {
        if planned_push.kind == PushKind::Skip {
            Self::Unchanged
        } else if !planned_push.remote_exists {
            Self::CreateBranch
        } else if planned_push.kind == PushKind::FastForward {
            Self::FastForwardBranch
        } else {
            Self::ForcePushBranch
        }
    }
}

fn mutation_len_for_inputs(update_inputs: &[String]) -> usize {
    let mut current_len = "mutation {".len() + 1;
    for (i, input) in update_inputs.iter().enumerate() {
        let alias = format!("m{}: ", i);
        let frag = format!(
            "updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
            input
        );
        current_len += alias.len() + frag.len();
    }
    current_len + 1
}

fn chunk_update_inputs(
    update_inputs: &[String],
    max_ops: usize,
    max_chars: usize,
) -> Vec<Vec<String>> {
    let mut chunks: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_len = "mutation {".len() + 1;
    for input in update_inputs {
        let alias = format!("m{}: ", current.len());
        let frag = format!(
            "updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
            input
        );
        let next_len = current_len + alias.len() + frag.len();
        if !current.is_empty() && (current.len() + 1 > max_ops || next_len > max_chars) {
            chunks.push(current);
            current = Vec::new();
            current_len = "mutation {".len() + 1;
        }
        let alias = format!("m{}: ", current.len());
        let frag = format!(
            "updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
            input
        );
        current_len += alias.len() + frag.len();
        current.push(input.clone());
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn should_use_single_update_mutation(
    update_inputs: &[String],
    max_ops: usize,
    max_chars: usize,
    prefer_single: bool,
) -> bool {
    prefer_single
        && update_inputs.len() <= max_ops
        && mutation_len_for_inputs(update_inputs) <= max_chars
}

fn run_update_chunk(execution_mode: ExecutionMode, update_inputs: &[String]) -> Result<()> {
    if update_inputs.is_empty() {
        return Ok(());
    }
    let mut m = String::from("mutation {");
    for (i, input) in update_inputs.iter().enumerate() {
        m.push_str(&format!(
            "m{}: updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
            i, input
        ));
    }
    m.push('}');
    gh_rw(
        execution_mode,
        ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
    )?;
    Ok(())
}

fn run_update_chunk_with_retry(
    execution_mode: ExecutionMode,
    update_inputs: &[String],
    progress_bar: Option<&ProgressBar>,
) -> Result<()> {
    if update_inputs.is_empty() {
        return Ok(());
    }
    match run_update_chunk(execution_mode, update_inputs) {
        Ok(()) => {
            if let Some(progress_bar) = progress_bar {
                progress_bar.inc(update_inputs.len() as u64);
            }
            Ok(())
        }
        Err(e) if is_resource_limit_error(&e) && update_inputs.len() > 1 => {
            info!(
                "Resource limits for this query exceeded; retrying with smaller chunks ({} updates)",
                update_inputs.len()
            );
            let mid = update_inputs.len() / 2;
            let (left, right) = update_inputs.split_at(mid);
            run_update_chunk_with_retry(execution_mode, left, progress_bar)?;
            run_update_chunk_with_retry(execution_mode, right, progress_bar)?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn run_update_mutations(
    execution_mode: ExecutionMode,
    update_inputs: Vec<String>,
    label: &str,
    max_ops: usize,
    max_chars: usize,
    prefer_single: bool,
    render_progress: bool,
) -> Result<()> {
    if update_inputs.is_empty() {
        return Ok(());
    }
    let total_updates = update_inputs.len();
    let progress_bar = if render_progress {
        let progress_bar = ProgressBar::new(total_updates as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(&format!("{{spinner}} {} {{pos}}/{{len}} PR(s)…", label))
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        progress_bar.enable_steady_tick(Duration::from_millis(120));
        Some(progress_bar)
    } else {
        None
    };
    let chunks =
        if should_use_single_update_mutation(&update_inputs, max_ops, max_chars, prefer_single) {
            vec![update_inputs]
        } else {
            chunk_update_inputs(&update_inputs, max_ops, max_chars)
        };
    for chunk in chunks {
        if let Err(e) = run_update_chunk_with_retry(execution_mode, &chunk, progress_bar.as_ref()) {
            if let Some(progress_bar) = &progress_bar {
                progress_bar.finish_and_clear();
            }
            return Err(e);
        }
    }
    if let Some(progress_bar) = &progress_bar {
        progress_bar.finish_and_clear();
    }
    Ok(())
}

pub(crate) fn ignored_boundary_warning(skipped_handles: &[String]) -> String {
    format!(
        "Skipping PR groups above the ignored block. GitHub PRs above an ignored block include the ignored commits, which defeats the point of `pr:ignore`. These groups stay local-only: {}",
        skipped_handles.join(", ")
    )
}

fn skipped_group_data(skipped_handles: &[String]) -> Vec<SkippedUpdateGroupData> {
    skipped_handles
        .iter()
        .map(|stable_handle| SkippedUpdateGroupData {
            stable_handle: stable_handle.clone(),
            reason: UpdateSkippedReason::IgnoredBoundary,
        })
        .collect()
}

fn update_warnings(skipped_handles: &[String]) -> Vec<String> {
    if skipped_handles.is_empty() {
        Vec::new()
    } else {
        vec![ignored_boundary_warning(skipped_handles)]
    }
}

fn empty_update_execution(skipped_handles: &[String]) -> UpdateExecutionData {
    UpdateExecutionData {
        warnings: update_warnings(skipped_handles),
        skipped_groups: skipped_group_data(skipped_handles),
        groups: Vec::new(),
        local_pr_branch_actions: Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_from_groups_internal(
    base: &str,
    prefix: &str,
    skipped_handles: &[String],
    no_pr: bool,
    execution_mode: ExecutionMode,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    mut groups: Vec<Group>,
    list_order: ListOrder,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
    push_validation: UpdatePushValidation,
    render_progress: bool,
) -> Result<UpdateExecutionData> {
    let dry_run = execution_mode == ExecutionMode::DryRun;
    if groups.is_empty() {
        if skipped_handles.is_empty() {
            info!("No groups discovered; nothing to do.");
        } else {
            warn!("{}", ignored_boundary_warning(skipped_handles));
            info!("No pushable groups remain after applying the ignored-block rule.");
        }
        return Ok(empty_update_execution(skipped_handles));
    }

    groups = apply_limit_groups(groups, limit)?;
    if !skipped_handles.is_empty() {
        warn!("{}", ignored_boundary_warning(skipped_handles));
    }
    if groups.is_empty() {
        if skipped_handles.is_empty() {
            info!("No groups selected; nothing to do.");
        } else {
            info!("No pushable groups remain after applying the ignored-block rule.");
        }
        return Ok(empty_update_execution(skipped_handles));
    }
    let total_groups = groups.len();
    let branch_identities = group_branch_identities(&groups, prefix)?;
    let desired_chain = build_desired_pr_base_chain(base, &groups, prefix)?;
    let desired_base_by_head: HashMap<String, String> = desired_chain
        .iter()
        .map(|desired| {
            (
                desired.head_branch.clone(),
                desired.expected_base_ref.clone(),
            )
        })
        .collect();

    info!("Preparing {} group(s)…", groups.len());

    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let mut observed_pr_bases = if no_pr {
        ObservedPrBaseChain::default()
    } else {
        ObservedPrBaseChain::observe_for_heads(&heads)?
    };
    let mut prs_by_head = observed_pr_bases.pr_numbers_by_head();
    enforce_branch_reuse_guard(
        no_pr,
        allow_branch_reuse,
        branch_reuse_guard_days,
        &heads,
        &prs_by_head,
    )?;

    let initial_base_reconciliation = if no_pr {
        Vec::new()
    } else {
        plan_base_reconciliation(&desired_chain, &observed_pr_bases)
    };
    let mut branch_names = heads.clone();
    let base_ref_for_remote = sanitize_gh_base_ref(base);
    if !branch_names.contains(&base_ref_for_remote) {
        branch_names.push(base_ref_for_remote);
    }
    for current_base_ref in initial_base_reconciliation
        .iter()
        .filter_map(|decision| decision.current_base_ref.as_deref())
        .map(sanitize_gh_base_ref)
    {
        if !branch_names.contains(&current_base_ref) {
            branch_names.push(current_base_ref);
        }
    }
    let remote_map = get_remote_branches_sha(&branch_names)?;

    let display_indices = list_order.display_indices(groups.len());
    for (display_idx, group_idx) in display_indices.iter().enumerate() {
        let branch = branch_identities[*group_idx].exact.clone();
        info!(
            "({}/{}) Rebuilding branch {}",
            display_idx + 1,
            total_groups,
            branch
        );
    }

    let mut planned: Vec<PlannedPush> = Vec::with_capacity(groups.len());
    for (group, identity) in groups.iter().zip(branch_identities.iter()) {
        let branch = identity.exact.clone();
        let remote_head = remote_map.get(&branch).cloned();
        let target_sha = group
            .commits
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("Group {} has no commits", group.selector_text()))?;
        let kind = if remote_head.as_deref() == Some(target_sha.as_str()) {
            PushKind::Skip
        } else if let Some(ref remote_sha) = remote_head {
            if git_is_ancestor(remote_sha, &target_sha)? {
                PushKind::FastForward
            } else {
                PushKind::Force
            }
        } else {
            PushKind::FastForward
        };
        planned.push(PlannedPush {
            branch,
            target_sha,
            remote_exists: remote_head.is_some(),
            kind,
        });
    }

    let draft_protected_transitions = if no_pr {
        Vec::new()
    } else {
        draft_protected_base_transitions(&initial_base_reconciliation, &planned)?
    };
    let prepublish_base_transitions =
        ancestry_collapse_risk_transitions(&draft_protected_transitions, &planned, &remote_map)?;
    let ancestry_collapse_risk_pr_numbers = prepublish_base_transitions
        .iter()
        .map(|transition| transition.remote_pr_number)
        .collect::<HashSet<_>>();
    let ancestry_collapse_risk_head_branches = prepublish_base_transitions
        .iter()
        .map(|transition| transition.head_branch.clone())
        .collect::<Vec<_>>();
    let draft_protected_head_branches = draft_protected_transitions
        .iter()
        .map(|transition| transition.head_branch.clone())
        .collect::<Vec<_>>();
    let temporarily_drafted_pull_request_ids = if draft_protected_transitions.is_empty() {
        Vec::new()
    } else {
        let protected_pr_numbers = draft_protected_transitions
            .iter()
            .map(|transition| transition.remote_pr_number)
            .collect::<Vec<_>>();
        let stage_info_by_number = fetch_pr_stage_info_graphql(&protected_pr_numbers)?;
        let ready_pull_request_ids = ready_pull_request_ids_requiring_temporary_draft(
            &draft_protected_transitions,
            &stage_info_by_number,
        )?;
        info!(
            "Guarding {} PR base/head transition(s) before branch publication",
            draft_protected_transitions.len()
        );
        convert_pull_requests_to_draft(&ready_pull_request_ids, execution_mode)?;
        let protected_base_updates = draft_protected_base_update_inputs(
            &prepublish_base_transitions,
            &stage_info_by_number,
        )?;
        run_update_mutations(
            execution_mode,
            protected_base_updates,
            "Protecting PR bases before branch publication",
            MAX_BASE_UPDATES_PER_MUTATION,
            MAX_BASE_MUTATION_CHARS,
            true,
            render_progress,
        )?;
        if execution_mode == ExecutionMode::Apply {
            let refreshed_pr_bases = ObservedPrBaseChain::observe_for_heads(&heads)?;
            let refreshed_decisions = plan_base_reconciliation(&desired_chain, &refreshed_pr_bases);
            verify_base_edits_converged(
                &ancestry_collapse_risk_head_branches,
                &refreshed_decisions,
            )?;
        }
        ready_pull_request_ids
    };

    let ff_refspecs: Vec<String> = planned
        .iter()
        .filter(|planned_push| planned_push.kind == PushKind::FastForward)
        .map(|planned_push| {
            format!(
                "{}:refs/heads/{}",
                planned_push.target_sha, planned_push.branch
            )
        })
        .collect();
    let force_refspecs: Vec<String> = planned
        .iter()
        .filter(|planned_push| planned_push.kind == PushKind::Force)
        .map(|planned_push| {
            format!(
                "{}:refs/heads/{}",
                planned_push.target_sha, planned_push.branch
            )
        })
        .collect();
    if (!ff_refspecs.is_empty() || !force_refspecs.is_empty())
        && execution_mode == ExecutionMode::Apply
    {
        match &push_validation {
            UpdatePushValidation::Legacy => {}
            UpdatePushValidation::Required(descriptors) => {
                let receipt_paths = crate::validation::require_matching_receipts(descriptors)?;
                info!(
                    "Using {} per-PR validation receipt(s); skipping push-time hooks",
                    receipt_paths.len()
                );
            }
            UpdatePushValidation::Skip => {
                warn!(
                    "Skipping validation receipt enforcement and Git pre-push hooks because --skip-validation was requested"
                );
            }
        }
    }
    let push_skips_hooks = push_validation.skips_push_hooks();
    if !ff_refspecs.is_empty() {
        let mut argv: Vec<String> = vec!["push".into()];
        if push_skips_hooks {
            argv.push("--no-verify".into());
        }
        argv.push("origin".into());
        argv.extend(ff_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|item| item.as_str()).collect();
        if render_progress {
            let progress_bar = ProgressBar::new_spinner();
            progress_bar.set_style(
                ProgressStyle::with_template("{spinner} Pushing {pos} branch(es) (-ff)…")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            progress_bar.set_position(ff_refspecs.len() as u64);
            progress_bar.enable_steady_tick(Duration::from_millis(120));
            let result = git_rw(execution_mode, &args);
            progress_bar.finish_and_clear();
            result?;
        } else {
            git_rw(execution_mode, &args)?;
        }
    }

    if !force_refspecs.is_empty() {
        let force_leases: Vec<String> = planned
            .iter()
            .filter(|planned_push| planned_push.kind == PushKind::Force)
            .filter_map(|planned_push| {
                remote_map.get(&planned_push.branch).map(|sha| {
                    format!(
                        "--force-with-lease=refs/heads/{}:{}",
                        planned_push.branch, sha
                    )
                })
            })
            .collect();
        let mut argv: Vec<String> = vec!["push".into()];
        if push_skips_hooks {
            argv.push("--no-verify".into());
        }
        argv.push("origin".into());
        if force_leases.is_empty() {
            argv.push("--force-with-lease".into());
        } else {
            argv.extend(force_leases);
        }
        argv.extend(force_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|item| item.as_str()).collect();
        if render_progress {
            let progress_bar = ProgressBar::new_spinner();
            progress_bar.set_style(
                ProgressStyle::with_template(
                    "{spinner} Pushing {pos} branch(es) (-force-with-lease)…",
                )
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            progress_bar.set_position(force_refspecs.len() as u64);
            progress_bar.enable_steady_tick(Duration::from_millis(120));
            let result = git_rw(execution_mode, &args);
            progress_bar.finish_and_clear();
            result?;
        } else {
            git_rw(execution_mode, &args)?;
        }
    }

    let mut pr_numbers_by_group: Vec<Option<u64>> = vec![None; groups.len()];
    let mut pr_actions_by_group: Vec<UpdatePrAction> =
        vec![UpdatePrAction::NotRequested; groups.len()];
    let mut base_actions_by_group: Vec<UpdateEditAction> = vec![
        if no_pr {
            UpdateEditAction::NotRequested
        } else {
            UpdateEditAction::Unchanged
        };
        groups.len()
    ];
    for transition in &draft_protected_transitions {
        base_actions_by_group[transition.group_idx] = UpdateEditAction::Updated;
    }
    let mut description_actions_by_group: Vec<UpdateEditAction> = vec![
        if no_pr {
            UpdateEditAction::NotRequested
        } else {
            UpdateEditAction::Unchanged
        };
        groups.len()
    ];
    let mut created_without_number: HashSet<usize> = HashSet::new();
    let mut parent_branch = base.to_string();
    for (group_idx, (group, identity)) in groups.iter().zip(branch_identities.iter()).enumerate() {
        let branch = identity.exact.clone();
        if !no_pr {
            let was_known = prs_by_head.contains_key(&identity.conflict_key);
            if dry_run && !was_known {
                pr_actions_by_group[group_idx] = UpdatePrAction::Created;
                created_without_number.insert(group_idx);
            } else {
                let number = upsert_pr_cached(
                    &branch,
                    &sanitize_gh_base_ref(&parent_branch),
                    &group.pr_title()?,
                    &group.pr_body()?,
                    execution_mode,
                    &mut prs_by_head,
                )?;
                pr_numbers_by_group[group_idx] = Some(number);
                pr_actions_by_group[group_idx] = if was_known {
                    UpdatePrAction::Existing
                } else {
                    UpdatePrAction::Created
                };
            }
        }
        parent_branch = branch;
    }

    if !no_pr && !dry_run {
        observed_pr_bases = ObservedPrBaseChain::observe_for_heads(&heads)?;
        prs_by_head.extend(observed_pr_bases.pr_numbers_by_head());
    }

    if !no_pr {
        let numbers_full: Vec<u64> = pr_numbers_by_group.iter().flatten().copied().collect();
        let mut desired_stack_by_number: HashMap<u64, String> = HashMap::new();
        let mut base_body_by_number: HashMap<u64, String> = HashMap::new();
        let mut desired_base_by_number: HashMap<u64, String> = HashMap::new();
        let numbers_rev: Vec<u64> = numbers_full.iter().cloned().rev().collect();
        for (group_idx, identity) in branch_identities.iter().enumerate() {
            if let Some(number) = pr_numbers_by_group[group_idx] {
                let want_base_ref = desired_base_by_head
                    .get(&identity.exact)
                    .cloned()
                    .ok_or_else(|| anyhow!("Missing desired base ref for {}", identity.exact))?;
                desired_base_by_number.insert(number, want_base_ref);
                let group = &groups[group_idx];
                let base_body = group.pr_body_base()?;
                base_body_by_number.insert(number, base_body);
                let mut lines = String::new();
                for pr_number in &numbers_rev {
                    let marker = if *pr_number == number {
                        "➡"
                    } else {
                        crate::format::EM_SPACE
                    };
                    lines.push_str(&format!("- {} #{}\n", marker, pr_number));
                }
                let stack_block = format!(
                    "<!-- spr-stack:start -->\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n<!-- spr-stack:end -->",
                    lines.trim_end(),
                );
                desired_stack_by_number.insert(number, stack_block);
            }
        }

        let mut fetch_set: HashSet<u64> = numbers_full.iter().cloned().collect();
        for &number in desired_stack_by_number.keys() {
            fetch_set.insert(number);
        }
        for &number in desired_base_by_number.keys() {
            fetch_set.insert(number);
        }
        let fetch_list: Vec<u64> = fetch_set.into_iter().collect();
        let bodies_by_number = if fetch_list.is_empty() {
            HashMap::new()
        } else {
            fetch_pr_bodies_graphql(&fetch_list)?
        };
        let group_index_by_number: HashMap<u64, usize> = pr_numbers_by_group
            .iter()
            .enumerate()
            .filter_map(|(group_idx, maybe_number)| maybe_number.map(|number| (number, group_idx)))
            .collect();
        let mut body_updates: Vec<String> = Vec::new();
        let mut base_updates: Vec<String> = Vec::new();
        if dry_run && !created_without_number.is_empty() {
            for group_idx in group_index_by_number.values().copied() {
                description_actions_by_group[group_idx] = UpdateEditAction::Updated;
            }
        } else {
            for (&number, stack_block) in &desired_stack_by_number {
                if let Some(info) = bodies_by_number.get(&number) {
                    let desired_body = if pr_description_mode == PrDescriptionMode::Overwrite {
                        if let Some(base_body) = base_body_by_number.get(&number) {
                            if base_body.trim().is_empty() {
                                stack_block.clone()
                            } else {
                                format!("{}\n\n{}", base_body, stack_block)
                            }
                        } else {
                            continue;
                        }
                    } else {
                        update_stack_block(&info.body, stack_block)
                    };
                    if desired_body != info.body {
                        if let Some(&group_idx) = group_index_by_number.get(&number) {
                            description_actions_by_group[group_idx] = UpdateEditAction::Updated;
                        }
                        let fields = [
                            format!("pullRequestId:\"{}\"", info.id),
                            format!("body:\"{}\"", graphql_escape(&desired_body)),
                        ];
                        body_updates.push(fields.join(", "));
                    }
                }
            }
        }
        let base_reconciliation = plan_base_reconciliation(&desired_chain, &observed_pr_bases);
        let edited_head_branches = base_reconciliation
            .iter()
            .filter(|decision| decision.action == BaseReconciliationAction::NeedsEdit)
            .map(|decision| decision.desired.head_branch.clone())
            .collect::<Vec<_>>();
        let base_update_numbers = base_reconciliation
            .into_iter()
            .filter_map(|decision| {
                (decision.action == BaseReconciliationAction::NeedsEdit)
                    .then_some(decision.remote_pr_number)
                    .flatten()
            })
            .filter(|number| !ancestry_collapse_risk_pr_numbers.contains(number))
            .collect::<HashSet<_>>();
        for (&number, want_base) in &desired_base_by_number {
            if let Some(info) = bodies_by_number.get(&number) {
                let desired_base_ref = sanitize_gh_base_ref(want_base);
                let needs_update = base_update_numbers.contains(&number);
                if needs_update {
                    if let Some(&group_idx) = group_index_by_number.get(&number) {
                        base_actions_by_group[group_idx] = UpdateEditAction::Updated;
                    }
                    let fields = [
                        format!("pullRequestId:\"{}\"", info.id),
                        format!("baseRefName:\"{}\"", graphql_escape(&desired_base_ref)),
                    ];
                    base_updates.push(fields.join(", "));
                }
            }
        }
        for group_idx in created_without_number {
            description_actions_by_group[group_idx] = UpdateEditAction::Updated;
        }
        let should_verify_base_updates = !edited_head_branches.is_empty();
        if !base_updates.is_empty() || !body_updates.is_empty() {
            if !base_updates.is_empty() {
                run_update_mutations(
                    execution_mode,
                    base_updates,
                    "Updating PR bases",
                    MAX_BASE_UPDATES_PER_MUTATION,
                    MAX_BASE_MUTATION_CHARS,
                    true,
                    render_progress,
                )?;
            }
            if !body_updates.is_empty() {
                run_update_mutations(
                    execution_mode,
                    body_updates,
                    "Updating PR descriptions",
                    MAX_BODY_UPDATES_PER_MUTATION,
                    MAX_BODY_MUTATION_CHARS,
                    false,
                    render_progress,
                )?;
            }
            if should_verify_base_updates && execution_mode == ExecutionMode::Apply {
                let refreshed_pr_bases = ObservedPrBaseChain::observe_for_heads(&heads)?;
                let refreshed_decisions =
                    plan_base_reconciliation(&desired_chain, &refreshed_pr_bases);
                verify_base_edits_converged(&edited_head_branches, &refreshed_decisions)?;
            }
        } else {
            info!("All PR descriptions/base refs up-to-date; no edits needed");
        }
        if !draft_protected_head_branches.is_empty() && execution_mode == ExecutionMode::Apply {
            let refreshed_pr_bases = ObservedPrBaseChain::observe_for_heads(&heads)?;
            let refreshed_decisions = plan_base_reconciliation(&desired_chain, &refreshed_pr_bases);
            verify_base_edits_converged(&draft_protected_head_branches, &refreshed_decisions)?;
        }
        mark_pull_requests_ready_for_review(&temporarily_drafted_pull_request_ids, execution_mode)?;
    }

    if !no_pr {
        let mut ordered: Vec<(u64, String)> = vec![];
        let display_indices = list_order.display_indices(groups.len());
        for group_idx in display_indices {
            let group = &groups[group_idx];
            let head_branch = &branch_identities[group_idx].exact;
            if let Some(number) = pr_number_for_head(&prs_by_head, head_branch) {
                let title = group.pr_title().unwrap_or_else(|_| String::new());
                ordered.push((number, title));
            }
        }
        if !ordered.is_empty() {
            if let Ok((owner, name)) = get_repo_owner_name() {
                info!("PRs:");
                for (number, title) in ordered {
                    let url = format!("https://github.com/{}/{}/pull/{}", owner, name, number);
                    info!("  {} - {}", url, title);
                }
            }
        }
    }

    let remote_url_prefix = get_repo_owner_name()
        .ok()
        .map(|(owner, name)| format!("https://github.com/{owner}/{name}/pull/"));
    let local_pr_targets = planned
        .iter()
        .enumerate()
        .map(
            |(group_idx, planned_push)| crate::local_pr_branches::LocalPrBranchTarget {
                stable_handle: common::group_selector_text(&groups[group_idx]),
                branch_name: planned_push.branch.clone(),
                tip: planned_push.target_sha.clone(),
            },
        )
        .collect::<Vec<_>>();
    let local_pr_branch_actions = crate::local_pr_branches::sync_local_pr_branches(
        local_pr_branch_policy,
        execution_mode,
        &local_pr_targets,
    )?;

    let groups = groups
        .iter()
        .zip(branch_identities.iter())
        .zip(planned.iter())
        .enumerate()
        .map(
            |(group_idx, ((group, identity), planned_push))| UpdateGroupData {
                local_pr_number: group_idx + 1,
                stable_handle: common::group_selector_text(group),
                head_branch: identity.exact.clone(),
                base_ref: desired_base_by_head
                    .get(&identity.exact)
                    .cloned()
                    .unwrap_or_else(|| base.to_string()),
                title: group.pr_title().unwrap_or_else(|_| String::new()),
                target_sha: planned_push.target_sha.clone(),
                push_action: UpdatePushAction::from_planned_push(planned_push),
                pr_action: pr_actions_by_group[group_idx],
                base_ref_action: base_actions_by_group[group_idx],
                description_action: description_actions_by_group[group_idx],
                remote_pr_number: pr_numbers_by_group[group_idx],
                remote_pr_url: match (remote_url_prefix.as_ref(), pr_numbers_by_group[group_idx]) {
                    (Some(prefix), Some(number)) => Some(format!("{prefix}{number}")),
                    _ => None,
                },
            },
        )
        .collect();
    Ok(UpdateExecutionData {
        warnings: update_warnings(skipped_handles),
        skipped_groups: skipped_group_data(skipped_handles),
        groups,
        local_pr_branch_actions,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_from_groups_with_summary_with_validation(
    base: &str,
    prefix: &str,
    skipped_handles: &[String],
    no_pr: bool,
    execution_mode: ExecutionMode,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    groups: Vec<Group>,
    list_order: ListOrder,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
    push_validation: UpdatePushValidation,
) -> Result<UpdateExecutionData> {
    build_from_groups_internal(
        base,
        prefix,
        skipped_handles,
        no_pr,
        execution_mode,
        pr_description_mode,
        limit,
        groups,
        list_order,
        allow_branch_reuse,
        branch_reuse_guard_days,
        local_pr_branch_policy,
        push_validation,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub fn build_from_groups(
    base: &str,
    prefix: &str,
    skipped_handles: &[String],
    no_pr: bool,
    execution_mode: ExecutionMode,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    groups: Vec<Group>,
    list_order: ListOrder,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> Result<()> {
    build_from_groups_with_validation(
        base,
        prefix,
        skipped_handles,
        no_pr,
        execution_mode,
        pr_description_mode,
        limit,
        groups,
        list_order,
        allow_branch_reuse,
        branch_reuse_guard_days,
        local_pr_branch_policy,
        UpdatePushValidation::Legacy,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_from_groups_with_validation(
    base: &str,
    prefix: &str,
    skipped_handles: &[String],
    no_pr: bool,
    execution_mode: ExecutionMode,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    groups: Vec<Group>,
    list_order: ListOrder,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
    push_validation: UpdatePushValidation,
) -> Result<()> {
    build_from_groups_internal(
        base,
        prefix,
        skipped_handles,
        no_pr,
        execution_mode,
        pr_description_mode,
        limit,
        groups,
        list_order,
        allow_branch_reuse,
        branch_reuse_guard_days,
        local_pr_branch_policy,
        push_validation,
        true,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub fn build_from_tags(
    base: &str,
    from: &str,
    prefix: &str,
    ignore_tag: &str,
    no_pr: bool,
    execution_mode: ExecutionMode,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    list_order: ListOrder,
) -> Result<()> {
    let (_merge_base, leading_ignored, groups): (String, Vec<String>, Vec<Group>) =
        derive_groups_between_with_ignored(base, from, ignore_tag)?;
    let (groups, skipped_handles) = split_groups_for_update(&leading_ignored, groups);
    group_branch_identities(&groups, prefix)?;
    build_from_groups(
        base,
        prefix,
        &skipped_handles,
        no_pr,
        execution_mode,
        pr_description_mode,
        limit,
        groups,
        list_order,
        true,
        0,
        LocalPrBranchSyncPolicy::Off,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ancestry_collapse_risk_transitions, branch_reuse_guard_window, build_from_groups,
        build_from_tags, draft_protected_base_transitions, head_key, heads_without_open_prs,
        ignored_boundary_warning, parse_github_timestamp_rfc3339, pr_number_for_head,
        ready_pull_request_ids_requiring_temporary_draft, recent_pr_age,
        recent_pr_age_blocks_recreation, should_use_single_update_mutation, terminal_pr_action,
        DraftProtectedBaseTransition, PlannedPush, PushKind,
    };
    use crate::branch_names::group_branch_identities;
    use crate::config::{ListOrder, LocalPrBranchSyncPolicy, PrDescriptionMode};
    use crate::execution::ExecutionMode;
    use crate::github::{PrStageInfo, TerminalPrState};
    use crate::parsing::{split_groups_for_update, Group};
    use crate::pr_base_chain::{
        BaseReconciliationAction, BaseReconciliationDecision, DesiredPrBase,
    };
    use crate::test_support::{
        commit_file, init_case_conflicting_stack_repo, init_repo, lock_cwd, DirGuard,
    };
    use std::collections::HashMap;
    use time::{Duration as TimeDuration, OffsetDateTime};

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap()
    }

    #[test]
    fn ignored_boundary_warning_explains_skipped_groups() {
        let warning = ignored_boundary_warning(&["pr:beta".to_string(), "pr:gamma".to_string()]);

        assert!(warning.contains("GitHub PRs above an ignored block include the ignored commits"));
        assert!(warning.contains("pr:beta, pr:gamma"));
    }

    #[test]
    fn preferred_single_update_mutation_still_respects_max_operations() {
        let update_inputs = vec!["a".to_string(), "b".to_string()];

        assert!(!should_use_single_update_mutation(
            &update_inputs,
            1,
            usize::MAX,
            true
        ));
    }

    #[test]
    // Verifies: recent terminal PRs well inside the threshold block PR recreation.
    // Catches: regressions that invert the recent-age comparison for ordinary cases.
    fn recent_pr_age_blocks_when_recent() {
        let now = fixed_now();
        let merged_at = now - TimeDuration::days(10);
        let age = recent_pr_age(merged_at, now);
        let guard_window = branch_reuse_guard_window(180);
        assert!(recent_pr_age_blocks_recreation(age, guard_window));
    }

    #[test]
    // Verifies: older terminal PRs outside the threshold do not block PR recreation.
    // Catches: regressions that use a too-large threshold or reverse the allow path.
    fn recent_pr_age_allows_when_old() {
        let now = fixed_now();
        let merged_at = now - TimeDuration::days(181);
        let age = recent_pr_age(merged_at, now);
        let guard_window = branch_reuse_guard_window(180);
        assert!(!recent_pr_age_blocks_recreation(age, guard_window));
    }

    #[test]
    // Verifies: a zero-day threshold allows historical terminal PRs with any positive elapsed age.
    // Catches: regressions where the disabled threshold still blocks normal historical closures.
    fn recent_pr_age_zero_threshold_allows_positive_age() {
        let now = fixed_now();
        let merged_at = now - TimeDuration::seconds(1);
        let age = recent_pr_age(merged_at, now);
        let guard_window = branch_reuse_guard_window(0);
        assert!(!recent_pr_age_blocks_recreation(age, guard_window));
    }

    #[test]
    // Verifies: the threshold comparison blocks values just under the day cutoff without rounding.
    // Catches: regressions that round fractional days before comparing to the integer threshold.
    fn recent_pr_age_just_under_threshold_blocks_without_rounding() {
        let now = fixed_now();
        let merged_at = now - TimeDuration::days(180) + TimeDuration::minutes(1);
        let age = recent_pr_age(merged_at, now);
        let guard_window = branch_reuse_guard_window(180);
        assert!(recent_pr_age_blocks_recreation(age, guard_window));
    }

    #[test]
    // Verifies: the threshold comparison allows values just over the day cutoff without rounding.
    // Catches: regressions that floor or ceil fractional-day ages before threshold comparison.
    fn recent_pr_age_just_over_threshold_allows_without_rounding() {
        let now = fixed_now();
        let merged_at = now - TimeDuration::days(180) - TimeDuration::minutes(1);
        let age = recent_pr_age(merged_at, now);
        let guard_window = branch_reuse_guard_window(180);
        assert!(!recent_pr_age_blocks_recreation(age, guard_window));
    }

    #[test]
    fn heads_without_open_prs_treats_case_only_open_head_as_present() {
        let mut prs_by_head = HashMap::new();
        prs_by_head.insert(head_key("dank-spr/Alpha"), 17);

        let missing = heads_without_open_prs(&["dank-spr/alpha".to_string()], &prs_by_head);

        assert!(missing.is_empty());
        assert_eq!(pr_number_for_head(&prs_by_head, "dank-spr/alpha"), Some(17));
    }

    #[test]
    // Verifies: the guard error wording distinguishes merged and closed terminal PR events.
    // Catches: regressions where closed PR blocks still claim the branch was merged.
    fn terminal_pr_action_describes_terminal_state() {
        assert_eq!(terminal_pr_action(TerminalPrState::Merged), "merged");
        assert_eq!(terminal_pr_action(TerminalPrState::Closed), "closed");
    }

    fn desired_base(head_branch: &str) -> DesiredPrBase {
        DesiredPrBase {
            local_pr_number: 1,
            stable_handle: "pr:gamma".to_string(),
            head_branch: head_branch.to_string(),
            expected_base_ref: "main".to_string(),
        }
    }

    fn planned_push(head_branch: &str, kind: PushKind) -> PlannedPush {
        PlannedPush {
            branch: head_branch.to_string(),
            target_sha: "next".to_string(),
            remote_exists: true,
            kind,
        }
    }

    #[test]
    fn draft_protection_detects_base_retarget_plus_head_move() {
        let head_branch = "dank-spr/gamma";
        let decisions = vec![BaseReconciliationDecision {
            desired: desired_base(head_branch),
            current_base_ref: Some("dank-spr/beta".to_string()),
            remote_pr_number: Some(3),
            action: BaseReconciliationAction::NeedsEdit,
        }];

        let transitions = draft_protected_base_transitions(
            &decisions,
            &[planned_push(head_branch, PushKind::Force)],
        )
        .unwrap();

        assert_eq!(
            transitions,
            vec![DraftProtectedBaseTransition {
                group_idx: 0,
                remote_pr_number: 3,
                head_branch: head_branch.to_string(),
                target_head_sha: "next".to_string(),
                current_base_ref: "dank-spr/beta".to_string(),
                desired_base_ref: "main".to_string(),
            }]
        );
    }

    #[test]
    fn draft_protection_ignores_base_retargets_without_head_moves() {
        let head_branch = "dank-spr/gamma";
        let decisions = vec![BaseReconciliationDecision {
            desired: desired_base(head_branch),
            current_base_ref: Some("dank-spr/beta".to_string()),
            remote_pr_number: Some(3),
            action: BaseReconciliationAction::NeedsEdit,
        }];

        let transitions = draft_protected_base_transitions(
            &decisions,
            &[planned_push(head_branch, PushKind::Skip)],
        )
        .unwrap();

        assert!(transitions.is_empty());
    }

    #[test]
    fn ready_pull_request_ids_only_restore_prs_that_spr_temporarily_drafts() {
        let transitions = vec![
            DraftProtectedBaseTransition {
                group_idx: 0,
                remote_pr_number: 3,
                head_branch: "dank-spr/gamma".to_string(),
                target_head_sha: "gamma-next".to_string(),
                current_base_ref: "dank-spr/beta".to_string(),
                desired_base_ref: "main".to_string(),
            },
            DraftProtectedBaseTransition {
                group_idx: 1,
                remote_pr_number: 4,
                head_branch: "dank-spr/delta".to_string(),
                target_head_sha: "delta-next".to_string(),
                current_base_ref: "dank-spr/gamma".to_string(),
                desired_base_ref: "dank-spr/gamma".to_string(),
            },
        ];
        let stage_info_by_number = HashMap::from([
            (
                3,
                PrStageInfo {
                    id: "ready-pr".to_string(),
                    is_draft: false,
                },
            ),
            (
                4,
                PrStageInfo {
                    id: "already-draft-pr".to_string(),
                    is_draft: true,
                },
            ),
        ]);

        assert_eq!(
            ready_pull_request_ids_requiring_temporary_draft(&transitions, &stage_info_by_number)
                .unwrap(),
            vec!["ready-pr".to_string()]
        );
    }

    #[test]
    fn ancestry_collapse_risk_tracks_the_old_base_branch_after_publication() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let target_head_sha = commit_file(&repo, "gamma.txt", "gamma\n", "feat: gamma pr:gamma");
        let future_old_base_sha = commit_file(&repo, "beta.txt", "beta\n", "feat: beta pr:beta");
        let transitions = vec![DraftProtectedBaseTransition {
            group_idx: 0,
            remote_pr_number: 3,
            head_branch: "dank-spr/gamma".to_string(),
            target_head_sha: target_head_sha.clone(),
            current_base_ref: "dank-spr/beta".to_string(),
            desired_base_ref: "main".to_string(),
        }];
        let planned_pushes = vec![PlannedPush {
            branch: "dank-spr/beta".to_string(),
            target_sha: future_old_base_sha,
            remote_exists: true,
            kind: PushKind::Force,
        }];

        assert_eq!(
            ancestry_collapse_risk_transitions(&transitions, &planned_pushes, &HashMap::new())
                .unwrap(),
            transitions
        );
    }

    #[test]
    fn ancestry_collapse_risk_rejects_unavailable_old_base_objects() {
        let _lock = lock_cwd();
        let dir = init_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);
        let target_head_sha = commit_file(&repo, "gamma.txt", "gamma\n", "feat: gamma pr:gamma");
        let transitions = vec![DraftProtectedBaseTransition {
            group_idx: 0,
            remote_pr_number: 3,
            head_branch: "dank-spr/gamma".to_string(),
            target_head_sha,
            current_base_ref: "main".to_string(),
            desired_base_ref: "dank-spr/alpha".to_string(),
        }];
        let remote_map = HashMap::from([(
            "main".to_string(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
        )]);

        let error = ancestry_collapse_risk_transitions(&transitions, &[], &remote_map).unwrap_err();

        assert!(error.to_string().contains("git merge-base --is-ancestor"));
    }

    fn group(tag: &str) -> Group {
        Group {
            marker: crate::group_markers::GroupMarker::PrLabel(tag.to_string()),
            subjects: vec![format!("feat: {tag}")],
            commits: vec![format!("{tag}1")],
            first_message: Some(format!("feat: {tag} pr:{tag}")),
            ignored_after: Vec::new(),
        }
    }

    #[test]
    fn ignored_only_suffix_collision_does_not_block_empty_publishable_prefix() {
        let groups = vec![group("alpha"), group("Alpha")];

        assert!(group_branch_identities(&groups, "dank-spr/").is_err());

        let (pushable_groups, skipped_handles) =
            split_groups_for_update(&["ignored".to_string()], groups);
        group_branch_identities(&pushable_groups, "dank-spr/").unwrap();

        build_from_groups(
            "main",
            "dank-spr/",
            &skipped_handles,
            false,
            ExecutionMode::Apply,
            PrDescriptionMode::Overwrite,
            None,
            pushable_groups,
            ListOrder::RecentOnTop,
            false,
            180,
            LocalPrBranchSyncPolicy::Off,
        )
        .unwrap();
    }

    #[test]
    // Verifies: GitHub RFC3339 timestamps parse into the expected UTC instant.
    // Catches: regressions in timestamp parsing format or timezone handling.
    fn parse_github_timestamp_rfc3339_parses_valid_timestamp() {
        let parsed = parse_github_timestamp_rfc3339("2026-02-20T12:34:56Z").unwrap();
        let expected = OffsetDateTime::from_unix_timestamp(1_771_590_896).unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn build_from_tags_rejects_case_colliding_publishable_groups() {
        let _lock = lock_cwd();
        let dir = init_case_conflicting_stack_repo();
        let repo = dir.path().to_path_buf();
        let _guard = DirGuard::change_to(&repo);

        let err = build_from_tags(
            "main",
            "HEAD",
            "dank-spr/",
            "ignore",
            true,
            ExecutionMode::DryRun,
            PrDescriptionMode::Overwrite,
            None,
            ListOrder::RecentOnBottom,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("pr:alpha and pr:Alpha derive conflicting branch names"),
            "unexpected error: {err}"
        );
    }
}
