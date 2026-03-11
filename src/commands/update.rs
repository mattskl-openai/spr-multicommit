use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, Duration as TimeDuration, OffsetDateTime};
use tracing::{info, warn};

use crate::branch_names::{
    canonical_branch_conflict_key, group_branch_identities, CanonicalBranchConflictKey,
};
use crate::commands::common;
use crate::config::{ListOrder, PrDescriptionMode};
use crate::git::{get_remote_branches_sha, gh_rw, git_is_ancestor, git_rw, sanitize_gh_base_ref};
use crate::github::{
    fetch_pr_bodies_graphql, get_repo_owner_name, graphql_escape, list_open_prs_for_heads,
    list_recent_terminal_prs_for_heads, upsert_pr_cached, TerminalPrState,
};
use crate::limit::{apply_limit_groups, Limit};
use crate::parsing::{derive_groups_between_with_ignored, split_groups_for_update, Group};

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
/// threshold is non-zero. It reuses the caller-provided `prs_by_head` map so only heads without
/// open PRs are queried for terminal history; querying all heads would duplicate the open-PR
/// lookup and could produce misleading results when a branch has both open and historical PRs.
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

const MAX_BASE_UPDATES_PER_MUTATION: usize = 50;
const MAX_BASE_MUTATION_CHARS: usize = 20_000;
const MAX_BODY_UPDATES_PER_MUTATION: usize = 1;
const MAX_BODY_MUTATION_CHARS: usize = 100_000;

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

fn is_resource_limit_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("RESOURCE_LIMITS_EXCEEDED")
        || msg.contains("Resource limits for this query exceeded")
}

fn run_update_chunk(dry: bool, update_inputs: &[String]) -> Result<()> {
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
        dry,
        ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
    )?;
    Ok(())
}

fn run_update_chunk_with_retry(
    dry: bool,
    update_inputs: &[String],
    pb: &ProgressBar,
) -> Result<()> {
    if update_inputs.is_empty() {
        return Ok(());
    }
    match run_update_chunk(dry, update_inputs) {
        Ok(()) => {
            pb.inc(update_inputs.len() as u64);
            Ok(())
        }
        Err(e) if is_resource_limit_error(&e) && update_inputs.len() > 1 => {
            info!(
                "Resource limits for this query exceeded; retrying with smaller chunks ({} updates)",
                update_inputs.len()
            );
            let mid = update_inputs.len() / 2;
            let (left, right) = update_inputs.split_at(mid);
            run_update_chunk_with_retry(dry, left, pb)?;
            run_update_chunk_with_retry(dry, right, pb)?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn run_update_mutations(
    dry: bool,
    update_inputs: Vec<String>,
    label: &str,
    max_ops: usize,
    max_chars: usize,
    prefer_single: bool,
) -> Result<()> {
    if update_inputs.is_empty() {
        return Ok(());
    }
    let total_updates = update_inputs.len();
    let pb = ProgressBar::new(total_updates as u64);
    pb.set_style(
        ProgressStyle::with_template(&format!("{{spinner}} {} {{pos}}/{{len}} PR(s)…", label))
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.enable_steady_tick(Duration::from_millis(120));
    let chunks = if prefer_single && mutation_len_for_inputs(&update_inputs) <= max_chars {
        vec![update_inputs]
    } else {
        chunk_update_inputs(&update_inputs, max_ops, max_chars)
    };
    for chunk in chunks {
        if let Err(e) = run_update_chunk_with_retry(dry, &chunk, &pb) {
            pb.finish_and_clear();
            return Err(e);
        }
    }
    pb.finish_and_clear();
    Ok(())
}

fn ignored_boundary_warning(skipped_handles: &[String]) -> String {
    format!(
        "Skipping PR groups above the ignored block. GitHub PRs above an ignored block include the ignored commits, which defeats the point of `pr:ignore`. These groups stay local-only: {}",
        skipped_handles.join(", ")
    )
}

/// Bootstrap/refresh stack from already-parsed PR groups.
///
/// `groups` must be in local stack order (bottom-up); that order is still used for base
/// chaining and local PR numbering even when display is reversed. `list_order` controls
/// the order in which groups are visited for rebuild logging and list output. If a caller
/// shuffles `groups`, PR base updates will target the wrong branches. Groups above the first
/// ignored block are kept local-only because publishing them would drag the ignored commits
/// into their GitHub diffs.
#[allow(clippy::too_many_arguments)]
pub fn build_from_groups(
    base: &str,
    prefix: &str,
    skipped_handles: &[String],
    no_pr: bool,
    dry: bool,
    pr_description_mode: PrDescriptionMode,
    limit: Option<Limit>,
    mut groups: Vec<Group>,
    list_order: ListOrder,
    allow_branch_reuse: bool,
    branch_reuse_guard_days: u32,
) -> Result<()> {
    if groups.is_empty() {
        if skipped_handles.is_empty() {
            info!("No groups discovered; nothing to do.");
        } else {
            warn!("{}", ignored_boundary_warning(skipped_handles));
            info!("No pushable groups remain after applying the ignored-block rule.");
        }
        return Ok(());
    }

    // Apply extent limits
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
        return Ok(());
    }
    let total_groups = groups.len();
    let branch_identities = group_branch_identities(&groups, prefix)?;

    info!("Preparing {} group(s)…", groups.len());

    // Build bottom→top and collect PR refs for the visual update pass.
    let mut just_created_numbers: Vec<u64> = vec![];
    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let pr_list = list_open_prs_for_heads(&heads)?;
    let mut prs_by_head: HashMap<CanonicalBranchConflictKey, u64> = HashMap::new();
    let mut current_base_by_number: HashMap<u64, String> = HashMap::new();
    for p in pr_list {
        prs_by_head.insert(head_key(&p.head), p.number);
        current_base_by_number.insert(p.number, p.base);
    }
    enforce_branch_reuse_guard(
        no_pr,
        allow_branch_reuse,
        branch_reuse_guard_days,
        &heads,
        &prs_by_head,
    )?;
    // Allow force-push within the selected scope when branch diverged from remote

    // Batch fetch remote SHAs for all target branches
    let mut branch_names = heads.clone();
    // Ensure we also include the repository base branch for remote SHA comparisons
    let base_ref_for_remote = sanitize_gh_base_ref(base);
    if !branch_names.contains(&base_ref_for_remote) {
        branch_names.push(base_ref_for_remote);
    }
    let remote_map = get_remote_branches_sha(&branch_names)?; // branch -> sha

    // Stage push actions to batch git push calls
    #[derive(Clone, Copy, PartialEq)]
    enum PushKind {
        Skip,
        FastForward,
        Force,
    }
    struct PlannedPush {
        branch: String,
        target_sha: String,
        kind: PushKind,
    }
    let mut planned: Vec<PlannedPush> = vec![];

    let display_indices = list_order.display_indices(groups.len());
    for (display_idx, group_idx) in display_indices.iter().enumerate() {
        let g = &groups[*group_idx];
        let branch = branch_identities[*group_idx].exact.clone();
        info!(
            "({}/{}) Rebuilding branch {}",
            display_idx + 1,
            total_groups,
            branch
        );

        // Use local commit SHA as source of truth; avoid rewriting commits when possible
        let remote_head = remote_map.get(&branch).cloned();
        let target_sha = g
            .commits
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("Group {} has no commits", g.tag))?;
        let kind = if remote_head.as_deref() == Some(target_sha.as_str()) {
            PushKind::Skip
        } else if let Some(ref remote_sha) = remote_head {
            let ff_ok = git_is_ancestor(remote_sha, &target_sha)?;
            if ff_ok {
                PushKind::FastForward
            } else {
                PushKind::Force
            }
        } else {
            // No remote exists; create the branch
            PushKind::FastForward
        };
        planned.push(PlannedPush {
            branch: branch.clone(),
            target_sha: target_sha.clone(),
            kind,
        });
    }

    // Before pushing: If not all PRs are already chained correctly, temporarily set all existing PRs to the repo base
    if !no_pr {
        // Gather existing PR numbers and head branches in the local stack order (bottom→top)
        let mut numbers_full_pre: Vec<u64> = vec![];
        let mut head_by_number_pre: HashMap<u64, String> = HashMap::new();
        for identity in &branch_identities {
            if let Some(n) = pr_number_for_head(&prs_by_head, &identity.exact) {
                numbers_full_pre.push(n);
                head_by_number_pre.insert(n, identity.exact.clone());
            }
        }
        if !numbers_full_pre.is_empty() {
            // Compute desired chained base per existing PR using local groups
            let mut desired_base_by_number_pre: HashMap<u64, String> = HashMap::new();
            for (head, want_base) in common::build_head_base_chain(base, &groups, prefix)? {
                if let Some(num) = pr_number_for_head(&prs_by_head, &head) {
                    desired_base_by_number_pre.insert(num, want_base.clone());
                }
            }

            // Check if all existing PRs already point to correct base
            let mut all_correct = true;
            for (num, want_base) in &desired_base_by_number_pre {
                let want_base_s = sanitize_gh_base_ref(want_base);
                if current_base_by_number
                    .get(num)
                    .map(|b| sanitize_gh_base_ref(b) != want_base_s)
                    .unwrap_or(true)
                {
                    all_correct = false;
                    break;
                }
            }

            if !all_correct {
                // Temporarily set base of all existing PRs to the repo base (e.g., main)
                let bodies_by_number_pre = fetch_pr_bodies_graphql(&numbers_full_pre)?;
                let mut base_updates_pre: Vec<String> = Vec::new();
                for (num, info) in bodies_by_number_pre.iter() {
                    // Skip if already on base
                    let base_target = sanitize_gh_base_ref(base);
                    if current_base_by_number
                        .get(num)
                        .map(|b| sanitize_gh_base_ref(b) == base_target)
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    // Avoid GitHub error when base/head are identical on remote (no new commits)
                    let mut shas_equal = false;
                    if let Some(head_branch) = head_by_number_pre.get(num) {
                        if let (Some(head_sha), Some(base_sha)) =
                            (remote_map.get(head_branch), remote_map.get(&base_target))
                        {
                            if head_sha == base_sha {
                                shas_equal = true;
                            }
                        }
                    }
                    if shas_equal {
                        continue;
                    }
                    let fields = [
                        format!("pullRequestId:\"{}\"", info.id),
                        format!("baseRefName:\"{}\"", graphql_escape(&base_target)),
                    ];
                    base_updates_pre.push(fields.join(", "));
                }
                if !base_updates_pre.is_empty() {
                    run_update_mutations(
                        dry,
                        base_updates_pre,
                        "Updating PR bases",
                        MAX_BASE_UPDATES_PER_MUTATION,
                        MAX_BASE_MUTATION_CHARS,
                        true,
                    )?;
                }
            }
        }
    }

    // Execute batched pushes: first fast-forward, then force-with-lease
    let ff_refspecs: Vec<String> = planned
        .iter()
        .filter(|p| p.kind == PushKind::FastForward)
        .map(|p| format!("{}:refs/heads/{}", p.target_sha, p.branch))
        .collect();
    if !ff_refspecs.is_empty() {
        // Build argv: ["push", "origin", refspecs...]
        let mut argv: Vec<String> = vec!["push".into(), "origin".into()];
        argv.extend(ff_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner} Pushing {pos} branch(es) (-ff)…")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        pb.set_position(ff_refspecs.len() as u64);
        pb.enable_steady_tick(Duration::from_millis(120));
        let res = git_rw(dry, &args);
        pb.finish_and_clear();
        res?;
    }
    // Perform force-with-lease for diverged branches in scope
    let force_refspecs: Vec<String> = planned
        .iter()
        .filter(|p| p.kind == PushKind::Force)
        .map(|p| format!("{}:refs/heads/{}", p.target_sha, p.branch))
        .collect();
    if !force_refspecs.is_empty() {
        // Use explicit lease SHAs so we don't depend on remote-tracking refs being up-to-date.
        let force_leases: Vec<String> = planned
            .iter()
            .filter(|p| p.kind == PushKind::Force)
            .filter_map(|p| {
                remote_map
                    .get(&p.branch)
                    .map(|sha| format!("--force-with-lease=refs/heads/{}:{}", p.branch, sha))
            })
            .collect();
        let mut argv: Vec<String> = vec!["push".into(), "origin".into()];
        if force_leases.is_empty() {
            // Fallback to default lease behavior if we couldn't resolve remote SHAs.
            argv.push("--force-with-lease".into());
        } else {
            argv.extend(force_leases);
        }
        argv.extend(force_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner} Pushing {pos} branch(es) (-force-with-lease)…")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        pb.set_position(force_refspecs.len() as u64);
        pb.enable_steady_tick(Duration::from_millis(120));
        let res = git_rw(dry, &args);
        pb.finish_and_clear();
        res?;
    }

    // After pushes, (create or) update PRs
    let mut parent_branch = base.to_string();
    for (g, identity) in groups.iter().zip(branch_identities.iter()) {
        let branch = identity.exact.clone();
        if !no_pr {
            let was_known = prs_by_head.contains_key(&identity.conflict_key);
            let num = upsert_pr_cached(
                &branch,
                &sanitize_gh_base_ref(&parent_branch),
                &g.pr_title()?,
                &g.pr_body()?,
                dry,
                &mut prs_by_head,
            )?;
            if !was_known {
                just_created_numbers.push(num);
            }
        }
        parent_branch = branch;
    }

    if !no_pr {
        // Derive full stack order purely from local groups (bottom→top)
        let mut numbers_full: Vec<u64> = vec![];
        for identity in &branch_identities {
            if let Some(n) = pr_number_for_head(&prs_by_head, &identity.exact) {
                numbers_full.push(n);
            }
        }
        // Build desired stack blocks and base refs from local commits
        let mut desired_stack_by_number: HashMap<u64, String> = HashMap::new();
        let mut base_body_by_number: HashMap<u64, String> = HashMap::new();
        let mut desired_base_by_number: HashMap<u64, String> = HashMap::new();
        let chain = common::build_head_base_chain(base, &groups, prefix)?;
        let numbers_rev: Vec<u64> = numbers_full.iter().cloned().rev().collect();
        for (head_branch, want_base_ref) in chain {
            if let Some(num) = pr_number_for_head(&prs_by_head, &head_branch) {
                desired_base_by_number.insert(num, want_base_ref.clone());
                // Stack visual (optional rewrite)
                if let Some((g, _identity)) = groups
                    .iter()
                    .zip(branch_identities.iter())
                    .find(|(_group, identity)| identity.exact == head_branch)
                {
                    let base = g.pr_body_base()?;
                    base_body_by_number.insert(num, base);
                    let mut lines = String::new();
                    for n in &numbers_rev {
                        let marker = if *n == num {
                            "➡"
                        } else {
                            crate::format::EM_SPACE
                        };
                        lines.push_str(&format!("- {} #{}\n", marker, n));
                    }
                    let stack_block = format!(
                        "<!-- spr-stack:start -->\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n<!-- spr-stack:end -->",
                        lines.trim_end(),
                    );
                    desired_stack_by_number.insert(num, stack_block);
                }
            }
        }

        // Fetch PR ids/bodies for union of all PRs we may rewrite bodies for
        let mut fetch_set: std::collections::HashSet<u64> = numbers_full.iter().cloned().collect();
        for &n in desired_stack_by_number.keys() {
            fetch_set.insert(n);
        }
        for &n in desired_base_by_number.keys() {
            fetch_set.insert(n);
        }
        let fetch_list: Vec<u64> = fetch_set.into_iter().collect();
        let bodies_by_number = fetch_pr_bodies_graphql(&fetch_list)?;
        let mut body_updates: Vec<String> = Vec::new();
        let mut base_updates: Vec<String> = Vec::new();
        // Bodies: always update (full or stack-only based on config)
        for (&num, stack_block) in &desired_stack_by_number {
            if let Some(info) = bodies_by_number.get(&num) {
                match pr_description_mode {
                    PrDescriptionMode::Overwrite => {
                        if let Some(base) = base_body_by_number.get(&num) {
                            let body = if base.trim().is_empty() {
                                stack_block.clone()
                            } else {
                                format!("{}\n\n{}", base, stack_block)
                            };
                            let fields = [
                                format!("pullRequestId:\"{}\"", info.id),
                                format!("body:\"{}\"", graphql_escape(&body)),
                            ];
                            body_updates.push(fields.join(", "));
                        }
                    }
                    PrDescriptionMode::StackOnly => {
                        let body = update_stack_block(&info.body, stack_block);
                        let fields = [
                            format!("pullRequestId:\"{}\"", info.id),
                            format!("body:\"{}\"", graphql_escape(&body)),
                        ];
                        body_updates.push(fields.join(", "));
                    }
                }
            }
        }
        // Bases: set post-push to ensure final linkage
        for (&num, want_base) in &desired_base_by_number {
            if let Some(info) = bodies_by_number.get(&num) {
                let fields = [
                    format!("pullRequestId:\"{}\"", info.id),
                    format!(
                        "baseRefName:\"{}\"",
                        graphql_escape(&sanitize_gh_base_ref(want_base))
                    ),
                ];
                base_updates.push(fields.join(", "));
            }
        }
        if !base_updates.is_empty() || !body_updates.is_empty() {
            if !base_updates.is_empty() {
                run_update_mutations(
                    dry,
                    base_updates,
                    "Updating PR bases",
                    MAX_BASE_UPDATES_PER_MUTATION,
                    MAX_BASE_MUTATION_CHARS,
                    true,
                )?;
            }
            if !body_updates.is_empty() {
                run_update_mutations(
                    dry,
                    body_updates,
                    "Updating PR descriptions",
                    MAX_BODY_UPDATES_PER_MUTATION,
                    MAX_BODY_MUTATION_CHARS,
                    false,
                )?;
            }
        } else {
            info!("All PR descriptions/base refs up-to-date; no edits needed");
        }
    }

    // Print full stack PR list in bottom→top order: "- <url> - <title>"
    if !no_pr {
        let mut ordered: Vec<(u64, String)> = vec![];
        let display_indices = list_order.display_indices(groups.len());
        for group_idx in display_indices {
            let g = &groups[group_idx];
            let head_branch = &branch_identities[group_idx].exact;
            if let Some(n) = pr_number_for_head(&prs_by_head, head_branch) {
                // Use local group title (source of truth for desired title)
                let title = g.pr_title().unwrap_or_else(|_| String::new());
                ordered.push((n, title));
            }
        }
        if !ordered.is_empty() {
            if let Ok((owner, name)) = get_repo_owner_name() {
                info!("PRs:");
                for (n, title) in ordered {
                    let url = format!("https://github.com/{}/{}/pull/{}", owner, name, n);
                    info!("  {} - {}", url, title);
                }
            }
        }
    }

    Ok(())
}

/// Bootstrap/refresh stack from pr:<tag> markers on `from` vs merge-base(base, from).
///
/// This derives groups in local stack order and forwards `list_order` so rebuild progress
/// and printed lists follow the same display order as `spr list`.
#[allow(clippy::too_many_arguments)]
pub fn build_from_tags(
    base: &str,
    from: &str,
    prefix: &str,
    ignore_tag: &str,
    no_pr: bool,
    dry: bool,
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
        dry,
        pr_description_mode,
        limit,
        groups,
        list_order,
        true,
        0,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        branch_reuse_guard_window, build_from_groups, build_from_tags, head_key,
        heads_without_open_prs, ignored_boundary_warning, parse_github_timestamp_rfc3339,
        pr_number_for_head, recent_pr_age, recent_pr_age_blocks_recreation, terminal_pr_action,
    };
    use crate::branch_names::group_branch_identities;
    use crate::config::{ListOrder, PrDescriptionMode};
    use crate::github::TerminalPrState;
    use crate::parsing::{split_groups_for_update, Group};
    use crate::test_support::{init_case_conflicting_stack_repo, lock_cwd, DirGuard};
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
            false,
            PrDescriptionMode::Overwrite,
            None,
            pushable_groups,
            ListOrder::RecentOnTop,
            false,
            180,
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
            true,
            PrDescriptionMode::Overwrite,
            None,
            ListOrder::RecentOnBottom,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("pr:alpha and pr:Alpha derive conflicting synthetic branch names"),
            "unexpected error: {err}"
        );
    }
}
