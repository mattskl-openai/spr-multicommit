//! Stack discovery and human rendering for `spr list`.
//!
//! The local stack order is derived bottom-up from commits and is the source of truth for
//! local PR numbers and commit indices. `ListOrder` only affects which groups or commits
//! are shown first in human output; it does not change the canonical JSON ordering.
//!
//! For `spr list pr`, the leading two-character status slot is:
//! - `CI` + `Review` symbols for open PRs
//! - `⑃M` for merged PRs
//! - `??` when no matching PR metadata is available

use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use tracing::info;

use crate::branch_names::{
    canonical_branch_conflict_key, find_group_branch_name_collision, group_branch_identities,
    CanonicalBranchConflictKey, GroupBranchIdentity, GroupBranchNameCollision,
};
use crate::config::{ListOrder, LocalPrBranchSyncPolicy};
use crate::github::{
    fetch_pr_ci_review_status, list_open_or_merged_prs_for_heads, PrCiReviewStatus, PrCiState,
    PrInfoWithState, PrReviewDecision, PrState,
};
use crate::parsing::{derive_local_groups, Group};

#[derive(Debug)]
pub enum ReadOnlyQueryError {
    SyntheticBranchNameCollision(GroupBranchNameCollision),
    Internal(anyhow::Error),
}

impl std::fmt::Display for ReadOnlyQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SyntheticBranchNameCollision(collision) => write!(f, "{collision}"),
            Self::Internal(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ReadOnlyQueryError {}

impl From<anyhow::Error> for ReadOnlyQueryError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemotePrMetadata {
    #[serde(flatten)]
    pub state: RemotePrState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemotePrState {
    NoRemote,
    RemoteWithoutCiReview {
        pr_number: u64,
        url: String,
        base_branch: String,
        state: PrState,
    },
    RemoteWithCiReview {
        pr_number: u64,
        url: String,
        base_branch: String,
        state: PrState,
        ci_review_status: PrCiReviewStatus,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrGroupData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub first_commit_sha: String,
    pub commit_count: usize,
    pub first_subject: String,
    pub remote: RemotePrMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrListData {
    pub groups: Vec<PrGroupData>,
    pub local_pr_branch_drift: Vec<crate::local_pr_branches::LocalPrBranchAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommitEntryData {
    pub global_commit_index: usize,
    pub sha: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommitGroupData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub remote: RemotePrMetadata,
    pub commits: Vec<CommitEntryData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommitListData {
    pub groups: Vec<CommitGroupData>,
    pub local_pr_branch_drift: Vec<crate::local_pr_branches::LocalPrBranchAction>,
}

/// Maps remote PR state into the two-character status slot used by `spr list pr`.
///
/// Open PRs show CI and review icons independently, while merged PRs intentionally use the
/// fixed marker `⑃M` so they are visually distinct from open green PRs (`✓✓`). If callers
/// pass an open PR that is missing `ci_review_status`, this returns `??`; displaying anything
/// else would incorrectly imply CI/review information was fetched.
fn status_icons(remote: &RemotePrMetadata) -> (&'static str, &'static str) {
    match &remote.state {
        RemotePrState::NoRemote => ("?", "?"),
        RemotePrState::RemoteWithoutCiReview {
            state: PrState::Merged,
            ..
        }
        | RemotePrState::RemoteWithCiReview {
            state: PrState::Merged,
            ..
        } => ("⑃", "M"),
        RemotePrState::RemoteWithoutCiReview { .. } => ("?", "?"),
        RemotePrState::RemoteWithCiReview {
            ci_review_status, ..
        } => {
            let ci_icon = match ci_review_status.ci_state {
                PrCiState::Success => "✓",
                PrCiState::Failure | PrCiState::Error => "✗",
                PrCiState::Pending | PrCiState::Expected => "◐",
                PrCiState::Unknown => "?",
            };
            let rv_icon = match ci_review_status.review_decision {
                PrReviewDecision::Approved => "✓",
                PrReviewDecision::ChangesRequested => "✗",
                PrReviewDecision::ReviewRequired => "◐",
                PrReviewDecision::Unknown => "?",
            };
            (ci_icon, rv_icon)
        }
    }
}

fn remote_pr_metadata(
    pr_number: u64,
    url: String,
    base_branch: String,
    state: PrState,
    ci_review_status: Option<PrCiReviewStatus>,
) -> RemotePrMetadata {
    let state = if let Some(ci_review_status) = ci_review_status {
        RemotePrState::RemoteWithCiReview {
            pr_number,
            url,
            base_branch,
            state,
            ci_review_status,
        }
    } else {
        RemotePrState::RemoteWithoutCiReview {
            pr_number,
            url,
            base_branch,
            state,
        }
    };

    RemotePrMetadata { state }
}

fn short_sha(sha: &str) -> &str {
    if sha.len() >= 8 {
        &sha[..8]
    } else {
        sha
    }
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

fn derive_groups_and_identities(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> std::result::Result<(Vec<Group>, Vec<GroupBranchIdentity>), ReadOnlyQueryError> {
    let (_merge_base, groups) =
        derive_local_groups(base, ignore_tag).map_err(ReadOnlyQueryError::Internal)?;
    if let Some(collision) = find_group_branch_name_collision(&groups, prefix) {
        Err(ReadOnlyQueryError::SyntheticBranchNameCollision(collision))
    } else {
        let identities =
            group_branch_identities(&groups, prefix).map_err(ReadOnlyQueryError::Internal)?;
        Ok((groups, identities))
    }
}

fn fetch_remote_pr_metadata(
    branch_identities: &[GroupBranchIdentity],
) -> Result<HashMap<CanonicalBranchConflictKey, RemotePrMetadata>> {
    let heads: Vec<String> = branch_identities
        .iter()
        .map(|identity| identity.exact.clone())
        .collect();
    let prs = list_open_or_merged_prs_for_heads(&heads)?;
    let open_numbers: Vec<u64> = prs
        .iter()
        .filter(|pr| pr.state == PrState::Open)
        .map(|pr| pr.number)
        .collect();
    let status_map = if open_numbers.is_empty() {
        Some(HashMap::new())
    } else {
        fetch_pr_ci_review_status(&open_numbers).ok()
    };

    Ok(build_remote_pr_metadata(prs, status_map.as_ref()))
}

fn build_remote_pr_metadata(
    prs: Vec<PrInfoWithState>,
    status_map: Option<&HashMap<u64, PrCiReviewStatus>>,
) -> HashMap<CanonicalBranchConflictKey, RemotePrMetadata> {
    prs.into_iter()
        .map(|pr| {
            let ci_review_status = if pr.state == PrState::Open {
                status_map.and_then(|status_map| status_map.get(&pr.number).cloned())
            } else {
                None
            };
            (
                canonical_branch_conflict_key(&pr.head),
                remote_pr_metadata(pr.number, pr.url, pr.base, pr.state, ci_review_status),
            )
        })
        .collect()
}

fn build_pr_list_data(
    groups: &[Group],
    branch_identities: &[GroupBranchIdentity],
    remote_by_head: &HashMap<CanonicalBranchConflictKey, RemotePrMetadata>,
    local_pr_branch_drift: Vec<crate::local_pr_branches::LocalPrBranchAction>,
) -> PrListData {
    let groups = groups
        .iter()
        .enumerate()
        .map(|(group_idx, group)| {
            let identity = &branch_identities[group_idx];
            PrGroupData {
                local_pr_number: group_idx + 1,
                stable_handle: crate::commands::common::group_selector_text(group),
                head_branch: identity.exact.clone(),
                first_commit_sha: group.commits.first().cloned().unwrap_or_default(),
                commit_count: group.commits.len(),
                first_subject: group.subjects.first().cloned().unwrap_or_default(),
                remote: remote_by_head
                    .get(&identity.conflict_key)
                    .cloned()
                    .unwrap_or(RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    }),
            }
        })
        .collect();

    PrListData {
        groups,
        local_pr_branch_drift,
    }
}

fn build_commit_list_data(
    groups: &[Group],
    branch_identities: &[GroupBranchIdentity],
    remote_by_head: &HashMap<CanonicalBranchConflictKey, RemotePrMetadata>,
    local_pr_branch_drift: Vec<crate::local_pr_branches::LocalPrBranchAction>,
) -> CommitListData {
    let group_start_indices: Vec<usize> = groups
        .iter()
        .scan(1, |next_index, group| {
            let start_index = *next_index;
            *next_index += group.commits.len();
            Some(start_index)
        })
        .collect();

    let groups = groups
        .iter()
        .enumerate()
        .map(|(group_idx, group)| {
            let identity = &branch_identities[group_idx];
            let commits = group
                .commits
                .iter()
                .zip(group.subjects.iter())
                .enumerate()
                .map(|(commit_offset, (sha, subject))| CommitEntryData {
                    global_commit_index: group_start_indices[group_idx] + commit_offset,
                    sha: sha.clone(),
                    subject: subject.clone(),
                })
                .collect();
            CommitGroupData {
                local_pr_number: group_idx + 1,
                stable_handle: crate::commands::common::group_selector_text(group),
                head_branch: identity.exact.clone(),
                remote: remote_by_head
                    .get(&identity.conflict_key)
                    .cloned()
                    .unwrap_or(RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    }),
                commits,
            }
        })
        .collect();

    CommitListData {
        groups,
        local_pr_branch_drift,
    }
}

pub fn collect_pr_list_data_for_json(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> std::result::Result<PrListData, ReadOnlyQueryError> {
    let (groups, branch_identities) = derive_groups_and_identities(base, prefix, ignore_tag)?;
    let remote_by_head =
        fetch_remote_pr_metadata(&branch_identities).map_err(ReadOnlyQueryError::Internal)?;
    let targets = crate::local_pr_branches::targets_from_groups(prefix, &groups)
        .map_err(ReadOnlyQueryError::Internal)?;
    let local_pr_branch_drift =
        crate::local_pr_branches::plan_local_pr_branch_drift(local_pr_branch_policy, &targets)
            .map_err(ReadOnlyQueryError::Internal)?;
    Ok(build_pr_list_data(
        &groups,
        &branch_identities,
        &remote_by_head,
        local_pr_branch_drift,
    ))
}

pub fn collect_pr_list_data(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> Result<PrListData> {
    collect_pr_list_data_for_json(base, prefix, ignore_tag, local_pr_branch_policy)
        .map_err(anyhow::Error::from)
}

pub fn collect_commit_list_data_for_json(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> std::result::Result<CommitListData, ReadOnlyQueryError> {
    let (groups, branch_identities) = derive_groups_and_identities(base, prefix, ignore_tag)?;
    let remote_by_head =
        fetch_remote_pr_metadata(&branch_identities).map_err(ReadOnlyQueryError::Internal)?;
    let targets = crate::local_pr_branches::targets_from_groups(prefix, &groups)
        .map_err(ReadOnlyQueryError::Internal)?;
    let local_pr_branch_drift =
        crate::local_pr_branches::plan_local_pr_branch_drift(local_pr_branch_policy, &targets)
            .map_err(ReadOnlyQueryError::Internal)?;
    Ok(build_commit_list_data(
        &groups,
        &branch_identities,
        &remote_by_head,
        local_pr_branch_drift,
    ))
}

pub fn collect_commit_list_data(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> Result<CommitListData> {
    collect_commit_list_data_for_json(base, prefix, ignore_tag, local_pr_branch_policy)
        .map_err(anyhow::Error::from)
}

fn render_pr_list(data: &PrListData, list_order: ListOrder) -> Vec<String> {
    if data.groups.is_empty() {
        vec!["No groups discovered; nothing to list.".to_string()]
    } else {
        let mut lines = vec![
            format!("┏━━{}CI status", crate::format::EM_SPACE),
            format!("┃┏━{}review status", crate::format::EM_SPACE),
        ];
        for group_idx in list_order.display_indices(data.groups.len()) {
            let group = &data.groups[group_idx];
            let (ci_icon, rv_icon) = status_icons(&group.remote);
            let pr_number = match &group.remote.state {
                RemotePrState::NoRemote => None,
                RemotePrState::RemoteWithoutCiReview { pr_number, .. }
                | RemotePrState::RemoteWithCiReview { pr_number, .. } => Some(*pr_number),
            };
            lines.push(format_pr_summary_line(PrSummaryLine {
                ci_icon,
                rv_icon,
                local_pr_num: group.local_pr_number,
                stable_handle: &group.stable_handle,
                short: short_sha(&group.first_commit_sha),
                head_branch: &group.head_branch,
                pr_number,
                count: group.commit_count,
            }));
            lines.push(format!(
                "{s}{s}{s}{s}{s}{subject}",
                s = crate::format::EM_SPACE,
                subject = group.first_subject
            ));
        }
        lines
    }
}

fn render_commit_list(data: &CommitListData, list_order: ListOrder) -> Vec<String> {
    if data.groups.is_empty() {
        vec!["No groups discovered; nothing to list.".to_string()]
    } else {
        let mut lines = Vec::new();
        for group_idx in list_order.display_indices(data.groups.len()) {
            let group = &data.groups[group_idx];
            let remote_pr_number = match &group.remote.state {
                RemotePrState::RemoteWithoutCiReview {
                    state: PrState::Open,
                    pr_number,
                    ..
                }
                | RemotePrState::RemoteWithCiReview {
                    state: PrState::Open,
                    pr_number,
                    ..
                } => Some(*pr_number),
                _ => None,
            };
            lines.push(format_commit_group_header(
                group.local_pr_number,
                &group.stable_handle,
                remote_pr_number,
                &group.head_branch,
            ));
            let commit_iter: Box<dyn Iterator<Item = &CommitEntryData>> =
                if list_order == ListOrder::RecentOnTop {
                    Box::new(group.commits.iter().rev())
                } else {
                    Box::new(group.commits.iter())
                };
            for commit in commit_iter {
                lines.push(format!(
                    "{:>4}  {} - {}",
                    commit.global_commit_index,
                    short_sha(&commit.sha),
                    commit.subject
                ));
            }
            lines.push(String::new());
        }
        lines
    }
}

fn render_local_pr_branch_drift(
    drift: &[crate::local_pr_branches::LocalPrBranchAction],
) -> Vec<String> {
    drift
        .iter()
        .map(|action| match action.action {
            crate::local_pr_branches::LocalPrBranchActionKind::Created => format!(
                "local branch {} is missing; run `spr sync-local-branches`",
                action.branch
            ),
            crate::local_pr_branches::LocalPrBranchActionKind::Updated => format!(
                "local branch {} is stale; run `spr sync-local-branches`",
                action.branch
            ),
            crate::local_pr_branches::LocalPrBranchActionKind::Blocked => format!(
                "local branch {} is stale but checked out in a worktree; run `spr sync-local-branches` after freeing it",
                action.branch
            ),
            crate::local_pr_branches::LocalPrBranchActionKind::Skipped => {
                unreachable!("drift output excludes skipped local PR branch actions")
            }
        })
        .collect()
}

/// Print a per-PR summary for the current local stack.
///
/// The local stack order is derived bottom-up from commits, so local PR numbers are based
/// on that ordering even when `list_order` reverses the display. If a caller assumes the
/// first printed line is "LPR #1" in display order, the labels will be wrong under
/// `RecentOnTop`.
pub fn list_prs_display(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    list_order: ListOrder,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> Result<()> {
    let data = collect_pr_list_data(base, prefix, ignore_tag, local_pr_branch_policy)?;
    for line in render_pr_list(&data, list_order) {
        info!("{line}");
    }
    for line in render_local_pr_branch_drift(&data.local_pr_branch_drift) {
        info!("{line}");
    }
    Ok(())
}

/// Print commits grouped by local PR, keeping commit indices in bottom-up order.
///
/// The commit indices are global and tied to the local stack ordering. When `list_order`
/// is `RecentOnTop`, commits are shown newest-first but their indices still count from the
/// bottom. If a caller treats the visible order as the numbering order, the output will
/// look inconsistent to users.
pub fn list_commits_display(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    list_order: ListOrder,
    local_pr_branch_policy: LocalPrBranchSyncPolicy,
) -> Result<()> {
    let data = collect_commit_list_data(base, prefix, ignore_tag, local_pr_branch_policy)?;
    for line in render_commit_list(&data, list_order) {
        info!("{line}");
    }
    for line in render_local_pr_branch_drift(&data.local_pr_branch_drift) {
        info!("{line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ListOrder;
    use crate::test_support::{init_case_conflicting_stack_repo, lock_cwd, DirGuard};

    #[test]
    fn status_icons_uses_merged_marker() {
        assert_eq!(
            status_icons(&remote_pr_metadata(
                42,
                "https://github.com/o/r/pull/42".to_string(),
                "main".to_string(),
                PrState::Merged,
                None,
            )),
            ("⑃", "M")
        );
    }

    #[test]
    fn status_icons_maps_open_ci_and_review_states() {
        assert_eq!(
            status_icons(&remote_pr_metadata(
                7,
                "https://github.com/o/r/pull/7".to_string(),
                "main".to_string(),
                PrState::Open,
                Some(PrCiReviewStatus {
                    ci_state: PrCiState::Success,
                    review_decision: PrReviewDecision::Approved,
                }),
            )),
            ("✓", "✓")
        );
    }

    #[test]
    fn status_icons_unknown_when_status_missing() {
        assert_eq!(
            status_icons(&remote_pr_metadata(
                99,
                "https://github.com/o/r/pull/99".to_string(),
                "main".to_string(),
                PrState::Open,
                None,
            )),
            ("?", "?")
        );
    }

    #[test]
    fn short_sha_truncates_only_long_values() {
        assert_eq!(short_sha("abcdef123456"), "abcdef12");
        assert_eq!(short_sha("abc123"), "abc123");
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

    fn group(tag: &str, commits: &[(&str, &str)]) -> Group {
        Group {
            marker: crate::group_markers::GroupMarker::PrLabel(tag.to_string()),
            subjects: commits
                .iter()
                .map(|(_, subject)| (*subject).to_string())
                .collect(),
            commits: commits.iter().map(|(sha, _)| (*sha).to_string()).collect(),
            first_message: None,
            ignored_after: Vec::new(),
        }
    }

    fn branch_group(branch_name: &str, commits: &[(&str, &str)]) -> Group {
        Group {
            marker: crate::group_markers::GroupMarker::BranchName(branch_name.to_string()),
            subjects: commits
                .iter()
                .map(|(_, subject)| (*subject).to_string())
                .collect(),
            commits: commits.iter().map(|(sha, _)| (*sha).to_string()).collect(),
            first_message: None,
            ignored_after: Vec::new(),
        }
    }

    #[test]
    fn build_pr_list_data_uses_canonical_group_order() {
        let groups = vec![
            group("alpha", &[("aaaaaaaa1", "feat: alpha")]),
            group("beta", &[("bbbbbbbb1", "feat: beta")]),
        ];
        let branch_identities = vec![
            GroupBranchIdentity::new("dank-spr/alpha".to_string()),
            GroupBranchIdentity::new("dank-spr/beta".to_string()),
        ];
        let remote_by_head = HashMap::from([(
            canonical_branch_conflict_key("dank-spr/beta"),
            remote_pr_metadata(
                17,
                "https://github.com/o/r/pull/17".to_string(),
                "main".to_string(),
                PrState::Open,
                Some(PrCiReviewStatus {
                    ci_state: PrCiState::Success,
                    review_decision: PrReviewDecision::Approved,
                }),
            ),
        )]);

        let data = build_pr_list_data(&groups, &branch_identities, &remote_by_head, Vec::new());
        assert_eq!(data.groups[0].local_pr_number, 1);
        assert_eq!(data.groups[0].stable_handle, "pr:alpha");
        assert_eq!(data.groups[1].local_pr_number, 2);
        assert_eq!(data.groups[1].stable_handle, "pr:beta");
        assert!(matches!(
            data.groups[0].remote.state,
            RemotePrState::NoRemote
        ));
        assert!(matches!(
            &data.groups[1].remote.state,
            RemotePrState::RemoteWithCiReview { url, .. }
                if url == "https://github.com/o/r/pull/17"
        ));
    }

    #[test]
    fn build_pr_list_data_renders_explicit_branch_selectors_and_heads() {
        let groups = vec![
            branch_group("feature/login", &[("aaaaaaaa1", "feat: login")]),
            group("beta", &[("bbbbbbbb1", "feat: beta")]),
        ];
        let branch_identities = vec![
            GroupBranchIdentity::new("feature/login".to_string()),
            GroupBranchIdentity::new("dank-spr/beta".to_string()),
        ];

        let data = build_pr_list_data(&groups, &branch_identities, &HashMap::new(), Vec::new());

        assert_eq!(data.groups[0].stable_handle, "branch:feature/login");
        assert_eq!(data.groups[0].head_branch, "feature/login");
        assert_eq!(data.groups[1].stable_handle, "pr:beta");
        assert_eq!(data.groups[1].head_branch, "dank-spr/beta");
    }

    #[test]
    fn build_remote_pr_metadata_keeps_open_prs_when_status_map_is_empty() {
        let status_map = HashMap::new();
        let metadata = build_remote_pr_metadata(
            vec![
                PrInfoWithState {
                    number: 17,
                    head: "dank-spr/alpha".to_string(),
                    base: "main".to_string(),
                    state: PrState::Open,
                    url: "https://github.com/o/r/pull/17".to_string(),
                },
                PrInfoWithState {
                    number: 18,
                    head: "dank-spr/beta".to_string(),
                    base: "main".to_string(),
                    state: PrState::Merged,
                    url: "https://github.com/o/r/pull/18".to_string(),
                },
            ],
            Some(&status_map),
        );

        assert_eq!(
            metadata.get(&canonical_branch_conflict_key("dank-spr/alpha")),
            Some(&remote_pr_metadata(
                17,
                "https://github.com/o/r/pull/17".to_string(),
                "main".to_string(),
                PrState::Open,
                None,
            ))
        );
        assert_eq!(
            metadata.get(&canonical_branch_conflict_key("dank-spr/beta")),
            Some(&remote_pr_metadata(
                18,
                "https://github.com/o/r/pull/18".to_string(),
                "main".to_string(),
                PrState::Merged,
                None,
            ))
        );
    }

    #[test]
    fn build_commit_list_data_uses_canonical_group_and_commit_order() {
        let groups = vec![
            group(
                "alpha",
                &[
                    ("aaaaaaaa1", "feat: alpha one"),
                    ("aaaaaaaa2", "feat: alpha two"),
                ],
            ),
            group("beta", &[("bbbbbbbb1", "feat: beta one")]),
        ];
        let branch_identities = vec![
            GroupBranchIdentity::new("dank-spr/alpha".to_string()),
            GroupBranchIdentity::new("dank-spr/beta".to_string()),
        ];
        let remote_by_head = HashMap::from([(
            canonical_branch_conflict_key("dank-spr/alpha"),
            remote_pr_metadata(
                11,
                "https://github.com/o/r/pull/11".to_string(),
                "main".to_string(),
                PrState::Open,
                None,
            ),
        )]);

        let data = build_commit_list_data(&groups, &branch_identities, &remote_by_head, Vec::new());

        assert_eq!(data.groups[0].stable_handle, "pr:alpha");
        assert_eq!(
            data.groups[0]
                .commits
                .iter()
                .map(|commit| commit.global_commit_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(data.groups[1].stable_handle, "pr:beta");
        assert_eq!(data.groups[1].commits[0].global_commit_index, 3);
    }

    #[test]
    fn render_pr_list_preserves_recent_on_top_human_order() {
        let data = PrListData {
            groups: vec![
                PrGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    first_commit_sha: "aaaaaaaa1".to_string(),
                    commit_count: 1,
                    first_subject: "feat: alpha".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    },
                },
                PrGroupData {
                    local_pr_number: 2,
                    stable_handle: "pr:beta".to_string(),
                    head_branch: "dank-spr/beta".to_string(),
                    first_commit_sha: "bbbbbbbb1".to_string(),
                    commit_count: 1,
                    first_subject: "feat: beta".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    },
                },
            ],
            local_pr_branch_drift: Vec::new(),
        };

        let lines = render_pr_list(&data, ListOrder::RecentOnTop);

        assert_eq!(
            lines[2],
            "?? LPR #2 / pr:beta - bbbbbbbb : dank-spr/beta - 1 commit"
        );
        assert_eq!(
            lines[3],
            format!("{s}{s}{s}{s}{s}feat: beta", s = crate::format::EM_SPACE)
        );
        assert_eq!(
            lines[4],
            "?? LPR #1 / pr:alpha - aaaaaaaa : dank-spr/alpha - 1 commit"
        );
    }

    #[test]
    fn render_commit_list_preserves_recent_on_top_human_order() {
        let data = CommitListData {
            groups: vec![
                CommitGroupData {
                    local_pr_number: 1,
                    stable_handle: "pr:alpha".to_string(),
                    head_branch: "dank-spr/alpha".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    },
                    commits: vec![
                        CommitEntryData {
                            global_commit_index: 1,
                            sha: "aaaaaaaa1".to_string(),
                            subject: "feat: alpha one".to_string(),
                        },
                        CommitEntryData {
                            global_commit_index: 2,
                            sha: "aaaaaaaa2".to_string(),
                            subject: "feat: alpha two".to_string(),
                        },
                    ],
                },
                CommitGroupData {
                    local_pr_number: 2,
                    stable_handle: "pr:beta".to_string(),
                    head_branch: "dank-spr/beta".to_string(),
                    remote: RemotePrMetadata {
                        state: RemotePrState::NoRemote,
                    },
                    commits: vec![CommitEntryData {
                        global_commit_index: 3,
                        sha: "bbbbbbbb1".to_string(),
                        subject: "feat: beta one".to_string(),
                    }],
                },
            ],
            local_pr_branch_drift: Vec::new(),
        };

        let lines = render_commit_list(&data, ListOrder::RecentOnTop);

        assert_eq!(
            lines[0],
            "===== Local PR #2 / pr:beta : dank-spr/beta ====="
        );
        assert_eq!(lines[1], "   3  bbbbbbbb - feat: beta one");
        assert_eq!(
            lines[3],
            "===== Local PR #1 / pr:alpha : dank-spr/alpha ====="
        );
        assert_eq!(lines[4], "   2  aaaaaaaa - feat: alpha two");
        assert_eq!(lines[5], "   1  aaaaaaaa - feat: alpha one");
    }

    #[test]
    fn render_local_pr_branch_drift_suggests_explicit_reconciliation() {
        let lines = render_local_pr_branch_drift(&[
            crate::local_pr_branches::LocalPrBranchAction {
                stable_handle: "pr:alpha".to_string(),
                branch: "dank-spr/alpha".to_string(),
                old_tip: Some("aaaaaaaa1".to_string()),
                new_tip: "bbbbbbbb1".to_string(),
                action: crate::local_pr_branches::LocalPrBranchActionKind::Updated,
                reason: "move existing local branch to target".to_string(),
                backup_tag: None,
            },
            crate::local_pr_branches::LocalPrBranchAction {
                stable_handle: "pr:beta".to_string(),
                branch: "dank-spr/beta".to_string(),
                old_tip: None,
                new_tip: "cccccccc1".to_string(),
                action: crate::local_pr_branches::LocalPrBranchActionKind::Created,
                reason: "create missing local branch at target".to_string(),
                backup_tag: None,
            },
        ]);

        assert_eq!(
            lines,
            vec![
                "local branch dank-spr/alpha is stale; run `spr sync-local-branches`",
                "local branch dank-spr/beta is missing; run `spr sync-local-branches`",
            ]
        );
    }

    #[test]
    fn collect_pr_list_data_for_json_returns_typed_collision_error() {
        let _lock = lock_cwd();
        let repo = init_case_conflicting_stack_repo();
        let _guard = DirGuard::change_to(repo.path());

        let err = collect_pr_list_data_for_json(
            "main",
            "dank-spr/",
            "ignore",
            LocalPrBranchSyncPolicy::Off,
        )
        .expect_err("collision");

        match err {
            ReadOnlyQueryError::SyntheticBranchNameCollision(collision) => {
                assert_eq!(collision.first.selector, "pr:alpha");
                assert_eq!(collision.second.selector, "pr:Alpha");
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
