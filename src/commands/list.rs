//! Display helpers for `spr list` output.
//!
//! The local stack order is derived bottom-up from commits and is the source of truth for
//! local PR numbers and commit indices. `ListOrder` only affects which groups or commits
//! are shown first; it does not change the underlying numbering.

use anyhow::Result;
use tracing::info;

use crate::config::ListOrder;
use crate::github::{fetch_pr_ci_review_status, list_open_prs_for_heads};
use crate::parsing::derive_local_groups;

/// Print a per-PR summary for the current local stack.
///
/// The local stack order is derived bottom-up from commits, so local PR numbers are based
/// on that ordering even when `list_order` reverses the display. If a caller assumes the
/// first printed line is "LPR #1" in display order, the labels will be wrong under
/// `RecentOnTop`.
///
/// Errors are returned when local groups or GitHub metadata cannot be loaded.
pub fn list_prs_display(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    list_order: ListOrder,
) -> Result<()> {
    // Derive stack from local commits (source of truth)
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No groups discovered; nothing to list.");
        return Ok(());
    }

    // Fetch PRs to annotate with numbers and statuses when available.
    // Optimize by querying only the heads in this local stack rather than scanning many PRs.
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?; // may be empty; that's fine
    let mut status_map: std::collections::HashMap<u64, crate::github::PrCiReviewStatus> =
        std::collections::HashMap::new();
    if !prs.is_empty() {
        let numbers: Vec<u64> = prs.iter().map(|p| p.number).collect();
        if let Ok(m) = fetch_pr_ci_review_status(&numbers) {
            status_map = m;
        }
    }

    // Header showing columns for CI and Review status
    info!("┏━━{}CI status", crate::format::EM_SPACE);
    info!("┃┏━{}review status", crate::format::EM_SPACE);

    let display_indices = list_order.display_indices(groups.len());
    for group_idx in display_indices {
        let g = &groups[group_idx];
        let head_branch = format!("{}{}", prefix, g.tag);
        let num = prs.iter().find(|p| p.head == head_branch).map(|p| p.number);
        let count = g.commits.len();
        let plural = if count == 1 { "commit" } else { "commits" };
        let first_sha = g.commits.first().map(|s| s.as_str()).unwrap_or("");
        let short = if first_sha.len() >= 8 {
            &first_sha[..8]
        } else {
            first_sha
        };
        let remote_pr_num_str = match num {
            Some(n) => format!(" (#{})", n),
            None => "".to_string(),
        };
        // Status icons
        let (ci_icon, rv_icon) = if let Some(n) = num {
            if let Some(st) = status_map.get(&n) {
                let ci_icon = match st.ci_state.as_str() {
                    "SUCCESS" => "✓",
                    "FAILURE" | "ERROR" => "✗",
                    "PENDING" | "EXPECTED" => "◐",
                    _ => "?",
                };
                let rv_icon = match st.review_decision.as_str() {
                    "APPROVED" => "✓",
                    "CHANGES_REQUESTED" => "✗",
                    "REVIEW_REQUIRED" => "◐",
                    _ => "?",
                };
                (ci_icon, rv_icon)
            } else {
                ("?", "?")
            }
        } else {
            ("?", "?")
        };
        let local_pr_num = group_idx + 1;
        info!(
            "{}{} LPR #{} - {} : {}{} - {} {}",
            ci_icon,
            rv_icon,
            local_pr_num,
            short,
            head_branch,
            remote_pr_num_str,
            count,
            plural
        );
        let subject_idx = if list_order == ListOrder::RecentOnTop {
            g.subjects.len().saturating_sub(1)
        } else {
            0
        };
        let first_subject = g
            .subjects
            .get(subject_idx)
            .map(|s| s.as_str())
            .unwrap_or("");
        info!(
            "{s}{s}{s}{s}{s}{subject}",
            s = crate::format::EM_SPACE,
            subject = first_subject
        );
    }
    Ok(())
}

/// Print commits grouped by local PR, keeping commit indices in bottom-up order.
///
/// The commit indices are global and tied to the local stack ordering. When `list_order`
/// is `RecentOnTop`, commits are shown newest-first but their indices still count from the
/// bottom. If a caller treats the visible order as the numbering order, the output will
/// look inconsistent to users.
///
/// Errors are returned when local groups or PR metadata cannot be loaded.
pub fn list_commits_display(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    list_order: ListOrder,
) -> Result<()> {
    // Derive stack from local commits (source of truth)
    let (_merge_base, groups) = derive_local_groups(base, ignore_tag)?;
    if groups.is_empty() {
        info!("No groups discovered; nothing to list.");
        return Ok(());
    }

    // Fetch PRs to annotate groups with remote numbers when available
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?; // may be empty; that's fine

    // Precompute global commit numbering in bottom-up stack order.
    let mut group_start_idx: Vec<usize> = Vec::with_capacity(groups.len());
    let mut commit_counter: usize = 1; // 1-based, bottom-up
    for g in &groups {
        group_start_idx.push(commit_counter);
        commit_counter += g.commits.len();
    }

    let display_indices = list_order.display_indices(groups.len());
    for group_idx in display_indices {
        let g = &groups[group_idx];
        let head_branch = format!("{}{}", prefix, g.tag);
        let num = prs.iter().find(|p| p.head == head_branch).map(|p| p.number);
        let remote_pr_num_str = match num {
            Some(n) => format!(" (#{})", n),
            None => String::new(),
        };

        // Group separator with local PR number
        info!(
            "===== Local PR #{}{} : {} =====",
            group_idx + 1,
            remote_pr_num_str,
            head_branch
        );

        let start_idx = group_start_idx[group_idx];
        let commit_indices: Vec<usize> = if list_order == ListOrder::RecentOnTop {
            (0..g.commits.len()).rev().collect()
        } else {
            (0..g.commits.len()).collect()
        };
        for j in commit_indices {
            let sha = &g.commits[j];
            let commit_idx = start_idx + j;
            let short = if sha.len() >= 8 { &sha[..8] } else { sha };
            let subject = g.subjects.get(j).map(|s| s.as_str()).unwrap_or("");
            info!("{:>4}  {} - {}", commit_idx, short, subject);
        }
        // Blank line between groups for readability
        info!("");
    }
    Ok(())
}
