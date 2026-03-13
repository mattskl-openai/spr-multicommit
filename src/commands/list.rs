//! Display helpers for `spr list` output.
//!
//! The local stack order is derived bottom-up from commits and is the source of truth for
//! local PR numbers and commit indices. `ListOrder` only affects which groups or commits
//! are shown first; it does not change the underlying numbering.
//!
//! For `spr list pr`, the leading two-character status slot is:
//! - `CI` + `Review` symbols for open PRs
//! - `⑃M` for merged PRs
//! - `??` when no matching PR metadata is available

use anyhow::Result;
use std::collections::HashMap;
use tracing::info;

use crate::branch_names::{canonical_branch_conflict_key, group_branch_identities};
use crate::config::ListOrder;
use crate::github::{
    fetch_pr_ci_review_status, list_open_or_merged_prs_for_heads, list_open_prs_for_heads,
    PrCiReviewStatus, PrState,
};
use crate::parsing::derive_local_groups;

/// Maps remote PR state into the two-character status slot used by `spr list pr`.
///
/// Open PRs show CI and review icons independently, while merged PRs intentionally use the
/// fixed marker `⑃M` so they are visually distinct from open green PRs (`✓✓`). If callers
/// pass an open PR number that is missing from `status_map`, this returns `??`; displaying
/// anything else would incorrectly imply CI/review information was fetched.
fn status_icons(
    pr_state: Option<PrState>,
    pr_number: Option<u64>,
    status_map: &HashMap<u64, PrCiReviewStatus>,
) -> (&'static str, &'static str) {
    match pr_state {
        Some(PrState::Merged) => ("⑃", "M"),
        Some(PrState::Open) => {
            if let Some(n) = pr_number {
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
            }
        }
        None => ("?", "?"),
    }
}

/// Return the subject shown on the indented summary line for a PR group.
///
/// This is always the first (oldest) commit subject in the group. Display ordering only
/// controls which groups are listed first and must not change the per-group summary subject.
fn summary_subject(subjects: &[String], _list_order: ListOrder) -> &str {
    subjects.first().map(|s| s.as_str()).unwrap_or("")
}

fn stable_handle_text(tag: &str) -> String {
    format!("pr:{tag}")
}

struct PrSummaryLine<'a> {
    ci_icon: &'a str,
    rv_icon: &'a str,
    local_pr_num: usize,
    stable_handle: &'a str,
    short: &'a str,
    head_branch: &'a str,
    pr_number: Option<u64>,
    count: usize,
}

fn format_pr_summary_line(line: PrSummaryLine<'_>) -> String {
    let remote_pr_num = if let Some(pr_number) = line.pr_number {
        format!(" (#{pr_number})")
    } else {
        String::new()
    };
    let plural = if line.count == 1 { "commit" } else { "commits" };
    format!(
        "{}{} LPR #{} / {} - {} : {}{} - {} {}",
        line.ci_icon,
        line.rv_icon,
        line.local_pr_num,
        line.stable_handle,
        line.short,
        line.head_branch,
        remote_pr_num,
        line.count,
        plural
    )
}

fn format_commit_group_header(
    local_pr_num: usize,
    stable_handle: &str,
    pr_number: Option<u64>,
    head_branch: &str,
) -> String {
    let remote_pr_num = if let Some(pr_number) = pr_number {
        format!(" (#{pr_number})")
    } else {
        String::new()
    };
    format!("===== Local PR #{local_pr_num} / {stable_handle}{remote_pr_num} : {head_branch} =====")
}

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
    let branch_identities = group_branch_identities(&groups, prefix)?;

    // Fetch PRs to annotate with numbers and statuses when available.
    // Optimize by querying only the heads in this local stack rather than scanning many PRs.
    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_or_merged_prs_for_heads(&heads)?; // may be empty; that's fine
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();
    let mut status_map: HashMap<u64, PrCiReviewStatus> = HashMap::new();
    if !prs.is_empty() {
        let numbers: Vec<u64> = prs
            .iter()
            .filter(|p| p.state == PrState::Open)
            .map(|p| p.number)
            .collect();
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
        let identity = &branch_identities[group_idx];
        let head_branch = &identity.exact;
        let pr = prs_by_head.get(&identity.conflict_key).copied();
        let num = pr.map(|p| p.number);
        let pr_state = pr.map(|p| p.state);
        let count = g.commits.len();
        let first_sha = g.commits.first().map(|s| s.as_str()).unwrap_or("");
        let short = if first_sha.len() >= 8 {
            &first_sha[..8]
        } else {
            first_sha
        };
        let (ci_icon, rv_icon) = status_icons(pr_state, num, &status_map);
        let local_pr_num = group_idx + 1;
        let stable_handle = stable_handle_text(&g.tag);
        info!(
            "{}",
            format_pr_summary_line(PrSummaryLine {
                ci_icon,
                rv_icon,
                local_pr_num,
                stable_handle: &stable_handle,
                short,
                head_branch,
                pr_number: num,
                count,
            })
        );
        let first_subject = summary_subject(&g.subjects, list_order);
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
    let branch_identities = group_branch_identities(&groups, prefix)?;

    // Fetch PRs to annotate groups with remote numbers when available
    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_prs_for_heads(&heads)?; // may be empty; that's fine
    let prs_by_head: HashMap<_, _> = prs
        .iter()
        .map(|pr| (canonical_branch_conflict_key(&pr.head), pr))
        .collect();

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
        let identity = &branch_identities[group_idx];
        let head_branch = &identity.exact;
        let num = prs_by_head.get(&identity.conflict_key).map(|pr| pr.number);
        let stable_handle = stable_handle_text(&g.tag);

        // Group separator with local PR number
        info!(
            "{}",
            format_commit_group_header(group_idx + 1, &stable_handle, num, head_branch)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ListOrder;

    #[test]
    fn status_icons_uses_merged_marker() {
        let status_map = HashMap::new();
        assert_eq!(
            status_icons(Some(PrState::Merged), Some(42), &status_map),
            ("⑃", "M")
        );
    }

    #[test]
    fn status_icons_maps_open_ci_and_review_states() {
        let mut status_map = HashMap::new();
        status_map.insert(
            7,
            PrCiReviewStatus {
                ci_state: "SUCCESS".to_string(),
                review_decision: "APPROVED".to_string(),
            },
        );
        assert_eq!(
            status_icons(Some(PrState::Open), Some(7), &status_map),
            ("✓", "✓")
        );
    }

    #[test]
    fn status_icons_unknown_when_status_missing() {
        let status_map = HashMap::new();
        assert_eq!(
            status_icons(Some(PrState::Open), Some(99), &status_map),
            ("?", "?")
        );
    }

    #[test]
    fn summary_subject_uses_first_commit_subject_for_any_display_order() {
        let subjects = vec![
            "oldest commit subject".to_string(),
            "newest commit subject".to_string(),
        ];

        assert_eq!(
            summary_subject(&subjects, ListOrder::RecentOnBottom),
            "oldest commit subject"
        );
        assert_eq!(
            summary_subject(&subjects, ListOrder::RecentOnTop),
            "oldest commit subject"
        );
    }

    #[test]
    fn pr_summary_line_includes_stable_handle() {
        let line = format_pr_summary_line(PrSummaryLine {
            ci_icon: "✓",
            rv_icon: "✓",
            local_pr_num: 2,
            stable_handle: "pr:beta",
            short: "abcdef12",
            head_branch: "dank-spr/beta",
            pr_number: Some(17),
            count: 3,
        });

        assert_eq!(
            line,
            "✓✓ LPR #2 / pr:beta - abcdef12 : dank-spr/beta (#17) - 3 commits"
        );
    }

    #[test]
    fn commit_group_header_includes_stable_handle_for_any_display_order() {
        assert_eq!(
            format_commit_group_header(2, "pr:beta", Some(17), "dank-spr/beta"),
            "===== Local PR #2 / pr:beta (#17) : dank-spr/beta ====="
        );
        assert_eq!(
            format_commit_group_header(2, "pr:beta", None, "dank-spr/beta"),
            "===== Local PR #2 / pr:beta : dank-spr/beta ====="
        );
    }
}
