//! GitHub API helpers used by `spr` commands.
//!
//! This module centralizes read/write calls to GitHub so command modules can operate on
//! typed results instead of raw JSON. The status-list path relies on branch-name lookups:
//! for each local stack head, we resolve either the currently open PR or (if none is open)
//! the latest merged PR for that exact synthetic branch name, while still treating case-only
//! remote head variants as explicit conflicts. The update preflight separately looks up the
//! latest terminal PR event for a canonical head ref so branch-name reuse can be blocked after
//! recent merges or closes.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::info;

use crate::branch_names::{canonical_branch_conflict_key, CanonicalBranchConflictKey};
use crate::git::{gh_ro, gh_rw, git_ro};

#[derive(Debug, Deserialize, Clone)]
pub struct PrInfo {
    pub number: u64,
    pub head: String,
    pub base: String,
}

/// Open PR metadata used by stack guardrails that depend on auto-merge state.
///
/// `auto_merge_enabled` reflects whether GitHub currently has an `autoMergeRequest`
/// on the PR. Callers should treat `false` as "auto-merge is not enabled right now",
/// not as a promise about repository policy or mergeability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPrAutomergeInfo {
    pub number: u64,
    pub head: String,
    pub auto_merge_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrState {
    /// The head ref currently has an open pull request.
    Open,
    /// The head ref has no open pull request but does have a merged pull request.
    Merged,
}

/// Pull request identity plus the state classification used by stack status display.
///
/// The `head` is expected to match a synthetic stack branch name (for example
/// `dank-spr/foo`), and `state` indicates whether this record came from an open PR query
/// or a merged PR fallback query.
#[derive(Debug, Clone)]
pub struct PrInfoWithState {
    pub number: u64,
    pub head: String,
    pub state: PrState,
}

/// Terminal state for a branch-name reuse guard lookup.
///
/// `Merged` and `Closed` are intentionally distinct because the update preflight needs to
/// report whether a recent terminal event came from a merge or a manual close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPrState {
    /// The prior PR on this head branch was merged.
    Merged,
    /// The prior PR on this head branch was closed without merging.
    Closed,
}

/// Latest terminal PR metadata for a specific head branch.
///
/// The `head` is the exact GitHub head ref name that matched the query. `terminal_at` is the
/// GitHub RFC3339 timestamp string for the event that matters to the branch-name reuse guard:
/// `mergedAt` for merged PRs and `closedAt` for closed PRs.
#[derive(Debug, Clone)]
pub struct TerminalPrInfo {
    pub number: u64,
    pub head: String,
    pub state: TerminalPrState,
    pub terminal_at: String,
    pub url: String,
}

const HEAD_SEARCH_LIMIT: usize = 100;
const OPEN_CONFLICT_SEARCH_LIMIT: usize = 2;
const HEAD_SEARCH_FIELDS: &str =
    "number,headRefName,baseRefName,state,mergedAt,closedAt,url,autoMergeRequest";
const EXACT_HEAD_QUERY_LIMIT: usize = 10;
const EXACT_PR_GRAPHQL_FIELDS: &str =
    "number headRefName baseRefName state mergedAt closedAt url autoMergeRequest { enabledAt }";
const TERMINAL_PR_SEARCH_GRAPHQL_FIELDS: &str = "number headRefName state mergedAt closedAt url";

#[derive(Debug, Deserialize, Clone)]
struct HeadSearchPr {
    number: u64,
    #[serde(rename = "headRefName")]
    head: String,
    #[serde(rename = "baseRefName", default)]
    base: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(rename = "mergedAt", default)]
    merged_at: Option<String>,
    #[serde(rename = "closedAt", default)]
    closed_at: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(rename = "autoMergeRequest", default)]
    auto_merge_request: Option<serde_json::Value>,
}

fn head_key(head: &str) -> CanonicalBranchConflictKey {
    canonical_branch_conflict_key(head)
}

fn head_search_query(head: &str) -> String {
    format!("head:{head}")
}

fn open_conflict_search_query(repo: &str, head: &str) -> String {
    format!("repo:{repo} is:pr is:open head:{head}")
}

fn recent_terminal_search_query(repo: &str, head: &str, closed_since: OffsetDateTime) -> String {
    let closed_since = closed_since.date();
    format!("repo:{repo} is:pr is:closed head:{head} closed:>={closed_since} sort:closed-desc")
}

fn filter_head_search_matches(requested_head: &str, prs: &[HeadSearchPr]) -> Vec<HeadSearchPr> {
    let requested_key = head_key(requested_head);
    prs.iter()
        .filter(|pr| head_key(&pr.head) == requested_key)
        .cloned()
        .collect()
}

fn filter_case_variant_head_search_matches(
    requested_head: &str,
    prs: &[HeadSearchPr],
) -> Vec<HeadSearchPr> {
    filter_head_search_matches(requested_head, prs)
        .into_iter()
        .filter(|pr| pr.head != requested_head)
        .collect()
}

fn list_prs_for_search_query(query: &str, state: &str, limit: usize) -> Result<Vec<HeadSearchPr>> {
    let limit = limit.to_string();
    let json = gh_ro(
        [
            "pr",
            "list",
            "--state",
            state,
            "--search",
            query,
            "--limit",
            &limit,
            "--json",
            HEAD_SEARCH_FIELDS,
        ]
        .as_slice(),
    )?;
    serde_json::from_str(&json).map_err(Into::into)
}

fn list_exact_prs_for_heads(
    heads: &[String],
    states: &[&str],
    limit: usize,
) -> Result<HashMap<String, Vec<HeadSearchPr>>> {
    let mut matches_by_head: HashMap<String, Vec<HeadSearchPr>> = heads
        .iter()
        .map(|head| (head.clone(), Vec::new()))
        .collect();
    if heads.is_empty() {
        return Ok(matches_by_head);
    }

    let (owner, name) = get_repo_owner_name()?;
    let states = states.join(",");
    let mut query =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, head) in heads.iter().enumerate() {
        query.push_str(&format!(
            "pr{}: pullRequests(headRefName:\"{}\", states:[{}], first:{}, orderBy:{{field:UPDATED_AT,direction:DESC}}) {{ nodes {{ {} }} }} ",
            i,
            graphql_escape(head),
            states,
            limit,
            EXACT_PR_GRAPHQL_FIELDS,
        ));
    }
    query.push_str("} }");
    let json = gh_ro(
        [
            "api",
            "graphql",
            "-f",
            &format!("query={}", query),
            "-F",
            &format!("owner={}", owner),
            "-F",
            &format!("name={}", name),
        ]
        .as_slice(),
    )?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    let repo = &value["data"]["repository"];
    for (i, head) in heads.iter().enumerate() {
        let key = format!("pr{}", i);
        let matches = repo[&key]["nodes"]
            .as_array()
            .map(|nodes| {
                nodes
                    .iter()
                    .cloned()
                    .map(serde_json::from_value)
                    .collect::<std::result::Result<Vec<HeadSearchPr>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        matches_by_head.insert(head.clone(), matches);
    }

    Ok(matches_by_head)
}

fn list_prs_for_search_query_exhaustive(query: &str, state: &str) -> Result<Vec<HeadSearchPr>> {
    let mut limit = HEAD_SEARCH_LIMIT;
    loop {
        let prs = list_prs_for_search_query(query, state, limit)?;
        if prs.len() < limit {
            return Ok(prs);
        }
        let Some(next_limit) = limit.checked_mul(2) else {
            return Ok(prs);
        };
        limit = next_limit;
    }
}

fn list_open_conflicting_prs_for_heads_search(
    heads: &[String],
) -> Result<HashMap<String, Vec<HeadSearchPr>>> {
    let mut matches_by_head: HashMap<String, Vec<HeadSearchPr>> = heads
        .iter()
        .map(|head| (head.clone(), Vec::new()))
        .collect();
    if heads.is_empty() {
        return Ok(matches_by_head);
    }

    let (owner, name) = get_repo_owner_name()?;
    let repo = format!("{owner}/{name}");
    let mut query = String::from("query { ");
    for (i, head) in heads.iter().enumerate() {
        let search_query = open_conflict_search_query(&repo, head);
        query.push_str(&format!(
            "pr{}: search(query:\"{}\", type:ISSUE, first:{}) {{ nodes {{ ... on PullRequest {{ {} }} }} }} ",
            i,
            graphql_escape(&search_query),
            OPEN_CONFLICT_SEARCH_LIMIT,
            EXACT_PR_GRAPHQL_FIELDS,
        ));
    }
    query.push('}');

    let json = gh_ro(["api", "graphql", "-f", &format!("query={query}")].as_slice())?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    let data = &value["data"];
    for (i, head) in heads.iter().enumerate() {
        let key = format!("pr{}", i);
        let prs = data[&key]["nodes"]
            .as_array()
            .map(|nodes| {
                nodes
                    .iter()
                    .cloned()
                    .map(serde_json::from_value)
                    .collect::<std::result::Result<Vec<HeadSearchPr>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        matches_by_head.insert(
            head.clone(),
            filter_case_variant_head_search_matches(head, &prs),
        );
    }

    Ok(matches_by_head)
}

fn list_conflicting_prs_for_heads_search_exhaustive(
    heads: &[String],
    state: &str,
) -> Result<HashMap<String, Vec<HeadSearchPr>>> {
    if heads.is_empty() {
        Ok(HashMap::new())
    } else {
        heads
            .iter()
            .map(|head| {
                let prs = list_prs_for_search_query_exhaustive(&head_search_query(head), state)?;
                Ok((
                    head.clone(),
                    filter_case_variant_head_search_matches(head, &prs),
                ))
            })
            .collect()
    }
}

fn partition_exact_head_matches(
    requested_head: &str,
    matches: Vec<HeadSearchPr>,
) -> (Vec<HeadSearchPr>, Vec<HeadSearchPr>) {
    matches
        .into_iter()
        .partition(|pr| pr.head == requested_head)
}

fn exact_head_conflict_error(
    requested_head: &str,
    state_label: &str,
    exact_matches: &[HeadSearchPr],
    case_variant_matches: &[HeadSearchPr],
) -> anyhow::Error {
    let mut conflicting = exact_matches.to_vec();
    conflicting.extend_from_slice(case_variant_matches);
    anyhow!(
        "Refusing to match {} branch {} to non-exact GitHub head refs under case-insensitive comparison: {}. Exact headRefName matches are required here; rename/restack the local branch or fix the remote PR head spelling.",
        state_label,
        requested_head,
        describe_matching_prs(&conflicting)
    )
}

fn describe_matching_prs(prs: &[HeadSearchPr]) -> String {
    prs.iter()
        .map(|pr| format!("#{} ({})", pr.number, pr.head))
        .collect::<Vec<_>>()
        .join(", ")
}

fn select_single_open_pr_match(
    requested_head: &str,
    matches: Vec<HeadSearchPr>,
) -> Result<Option<HeadSearchPr>> {
    let (exact_matches, case_variant_matches) =
        partition_exact_head_matches(requested_head, matches);
    if exact_matches.len() == 1 && case_variant_matches.is_empty() {
        Ok(exact_matches.into_iter().next())
    } else if exact_matches.is_empty() && case_variant_matches.is_empty() {
        Ok(None)
    } else {
        Err(exact_head_conflict_error(
            requested_head,
            "open PR",
            &exact_matches,
            &case_variant_matches,
        ))
    }
}

fn parse_merged_at(pr: &HeadSearchPr, requested_head: &str) -> Result<OffsetDateTime> {
    let merged_at = pr.merged_at.as_deref().ok_or_else(|| {
        anyhow!(
            "Merged PR #{} missing mergedAt for {}",
            pr.number,
            requested_head
        )
    })?;
    parse_github_datetime_rfc3339(merged_at, requested_head)
}

fn select_latest_merged_pr_match(
    requested_head: &str,
    matches: &[HeadSearchPr],
) -> Result<Option<HeadSearchPr>> {
    let (exact_matches, case_variant_matches) =
        partition_exact_head_matches(requested_head, matches.to_vec());
    if !case_variant_matches.is_empty() {
        return Err(exact_head_conflict_error(
            requested_head,
            "status-bearing PR",
            &exact_matches,
            &case_variant_matches,
        ));
    }
    let mut latest: Option<(OffsetDateTime, HeadSearchPr)> = None;
    for pr in &exact_matches {
        let Some(state) = pr.state.as_deref() else {
            continue;
        };
        if state != "MERGED" {
            continue;
        }
        let merged_at = parse_merged_at(pr, requested_head)?;
        if latest
            .as_ref()
            .map(|(current, _)| merged_at > *current)
            .unwrap_or(true)
        {
            latest = Some((merged_at, pr.clone()));
        }
    }
    Ok(latest.map(|(_, pr)| pr))
}

fn head_search_pr_to_info(pr: &HeadSearchPr, requested_head: &str) -> Result<PrInfo> {
    let base = pr.base.clone().ok_or_else(|| {
        anyhow!(
            "PR #{} missing baseRefName for {}",
            pr.number,
            requested_head
        )
    })?;
    Ok(PrInfo {
        number: pr.number,
        head: pr.head.clone(),
        base,
    })
}

fn terminal_info_from_head_search(
    pr: &HeadSearchPr,
    requested_head: &str,
) -> Result<TerminalPrInfo> {
    let Some(state) = pr.state.as_deref() else {
        return Err(anyhow!(
            "Terminal PR #{} missing state for {}",
            pr.number,
            requested_head
        ));
    };
    if state == "OPEN" {
        return Err(anyhow!(
            "Terminal PR #{} unexpectedly has OPEN state for {}",
            pr.number,
            requested_head
        ));
    }
    let terminal_state = if state == "MERGED" {
        TerminalPrState::Merged
    } else if state == "CLOSED" {
        TerminalPrState::Closed
    } else {
        return Err(anyhow!(
            "Terminal PR #{} has unsupported state {} for {}",
            pr.number,
            state,
            requested_head
        ));
    };
    let terminal_at = if terminal_state == TerminalPrState::Merged {
        pr.merged_at.as_deref().ok_or_else(|| {
            anyhow!(
                "Merged PR #{} missing mergedAt for {}",
                pr.number,
                requested_head
            )
        })?
    } else {
        pr.closed_at.as_deref().ok_or_else(|| {
            anyhow!(
                "Closed PR #{} missing closedAt for {}",
                pr.number,
                requested_head
            )
        })?
    };
    let url = pr.url.as_deref().ok_or_else(|| {
        anyhow!(
            "Terminal PR #{} missing url for {}",
            pr.number,
            requested_head
        )
    })?;
    Ok(TerminalPrInfo {
        number: pr.number,
        head: pr.head.clone(),
        state: terminal_state,
        terminal_at: terminal_at.to_string(),
        url: url.to_string(),
    })
}

#[derive(Clone)]
pub struct PrBodyInfo {
    pub id: String,
    pub body: String,
}

pub fn fetch_pr_bodies_graphql(numbers: &[u64]) -> Result<HashMap<u64, PrBodyInfo>> {
    let mut out = HashMap::new();
    if numbers.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;
    let mut q =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, n) in numbers.iter().enumerate() {
        q.push_str(&format!(
            "pr{}: pullRequest(number: {}) {{ id body }} ",
            i, n
        ));
    }
    q.push_str("} }");
    let json = gh_ro(
        [
            "api",
            "graphql",
            "-f",
            &format!("query={}", q),
            "-F",
            &format!("owner={}", owner),
            "-F",
            &format!("name={}", name),
        ]
        .as_slice(),
    )?;
    let v: serde_json::Value = serde_json::from_str(&json)?;
    let repo = &v["data"]["repository"];
    for (i, n) in numbers.iter().enumerate() {
        let key = format!("pr{}", i);
        let id = repo[&key]["id"].as_str().unwrap_or("").to_string();
        let body = repo[&key]["body"].as_str().unwrap_or("").to_string();
        out.insert(*n, PrBodyInfo { id, body });
    }
    Ok(out)
}

#[derive(Clone)]
pub struct PrCiReviewStatus {
    pub ci_state: String, // SUCCESS | FAILURE | ERROR | PENDING | EXPECTED | UNKNOWN
    pub review_decision: String, // APPROVED | CHANGES_REQUESTED | REVIEW_REQUIRED | UNKNOWN
}

pub fn fetch_pr_ci_review_status(numbers: &[u64]) -> Result<HashMap<u64, PrCiReviewStatus>> {
    let mut out = HashMap::new();
    if numbers.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;
    let mut q =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, n) in numbers.iter().enumerate() {
        q.push_str(&format!(
            "pr{}: pullRequest(number: {}) {{ reviewDecision isDraft reviewRequests(first:1){{ totalCount }} reviews(last:50, states:[APPROVED,CHANGES_REQUESTED]){{ nodes {{ state }} }} commits(last:1) {{ nodes {{ commit {{ statusCheckRollup {{ state }} }} }} }} }} ",
            i, n
        ));
    }
    q.push_str("} }");
    let json = gh_ro(
        [
            "api",
            "graphql",
            "-f",
            &format!("query={}", q),
            "-F",
            &format!("owner={}", owner),
            "-F",
            &format!("name={}", name),
        ]
        .as_slice(),
    )?;
    let v: serde_json::Value = serde_json::from_str(&json)?;
    let repo = &v["data"]["repository"];
    for (i, n) in numbers.iter().enumerate() {
        let key = format!("pr{}", i);
        let mut review = repo[&key]["reviewDecision"]
            .as_str()
            .unwrap_or("")
            .to_string();
        // Default when missing (no CI configured) → treat as passing
        let mut ci = String::from("SUCCESS");
        if let Some(nodes) = repo[&key]["commits"]["nodes"].as_array() {
            if let Some(node) = nodes.first() {
                if let Some(state) = node["commit"]["statusCheckRollup"]["state"].as_str() {
                    ci = state.to_string();
                }
            }
        }
        if review.is_empty() {
            // Fallback heuristic when reviewDecision is not available (e.g., no protected branch rules)
            let mut has_changes_requested = false;
            let mut has_approved = false;
            if let Some(nodes) = repo[&key]["reviews"]["nodes"].as_array() {
                for node in nodes {
                    match node["state"].as_str().unwrap_or("") {
                        "CHANGES_REQUESTED" => has_changes_requested = true,
                        "APPROVED" => has_approved = true,
                        _ => {}
                    }
                }
            }
            if has_changes_requested {
                review = "CHANGES_REQUESTED".to_string();
            } else if has_approved {
                review = "APPROVED".to_string();
            } else {
                review = "REVIEW_REQUIRED".to_string();
            }
        }

        out.insert(
            *n,
            PrCiReviewStatus {
                ci_state: ci,
                review_decision: review,
            },
        );
    }
    Ok(out)
}

pub fn get_repo_owner_name() -> Result<(String, String)> {
    let url = git_ro(["config", "--get", "remote.origin.url"].as_slice())?
        .trim()
        .to_string();
    if let Some(idx) = url.find("://") {
        let rest = &url[idx + 3..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 3 {
            let owner = parts[1].to_string();
            let mut name = parts[2].to_string();
            if let Some(s) = name.strip_suffix(".git") {
                name = s.to_string();
            }
            return Ok((owner, name));
        }
    } else if let Some(pos) = url.find(":") {
        // git@github.com:owner/name.git
        let rest = &url[pos + 1..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 2 {
            let owner = parts[0].to_string();
            let mut name = parts[1].to_string();
            if let Some(s) = name.strip_suffix(".git") {
                name = s.to_string();
            }
            return Ok((owner, name));
        }
    }
    anyhow::bail!("Unable to parse remote.origin.url: {}", url)
}

pub fn graphql_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

/// Fetch open PRs for a specific set of head branches.
///
/// Matching requires an exact `headRefName` for reuse and treats case-only
/// variants as conflicts instead of silently reusing them.
pub fn list_open_prs_for_heads(heads: &[String]) -> Result<Vec<PrInfo>> {
    let mut out: Vec<PrInfo> = Vec::new();
    if heads.is_empty() {
        return Ok(out);
    }
    let exact_matches_by_head = list_exact_prs_for_heads(heads, &["OPEN"], EXACT_HEAD_QUERY_LIMIT)?;
    let conflict_matches_by_head = list_open_conflicting_prs_for_heads_search(heads)?;
    for head in heads {
        let mut matches = exact_matches_by_head.get(head).cloned().unwrap_or_default();
        matches.extend(
            conflict_matches_by_head
                .get(head)
                .cloned()
                .unwrap_or_default(),
        );
        if let Some(pr) = select_single_open_pr_match(head, matches)? {
            out.push(head_search_pr_to_info(&pr, head)?);
        }
    }

    Ok(out)
}

#[cfg(test)]
fn parse_open_pr_automerge_node(
    node: &serde_json::Value,
    requested_head: &str,
) -> Result<OpenPrAutomergeInfo> {
    let number = node["number"]
        .as_u64()
        .ok_or_else(|| anyhow!("Open PR result missing number for {}", requested_head))?;
    let head = node["headRefName"]
        .as_str()
        .ok_or_else(|| anyhow!("Open PR result missing headRefName for {}", requested_head))?;

    Ok(OpenPrAutomergeInfo {
        number,
        head: head.to_string(),
        auto_merge_enabled: !node["autoMergeRequest"].is_null(),
    })
}

/// Fetch the current open PR for `head`, including whether auto-merge is enabled.
///
/// Returns `Ok(None)` when no open PR exists for that head branch.
pub fn get_open_pr_automerge_for_head(head: &str) -> Result<Option<OpenPrAutomergeInfo>> {
    let requested_heads = [head.to_string()];
    let mut matches =
        list_exact_prs_for_heads(&requested_heads, &["OPEN"], EXACT_HEAD_QUERY_LIMIT)?
            .remove(head)
            .unwrap_or_default();
    matches.extend(
        list_open_conflicting_prs_for_heads_search(&requested_heads)?
            .remove(head)
            .unwrap_or_default(),
    );
    if let Some(pr) = select_single_open_pr_match(head, matches)? {
        Ok(Some(OpenPrAutomergeInfo {
            number: pr.number,
            head: pr.head,
            auto_merge_enabled: pr.auto_merge_request.is_some(),
        }))
    } else {
        Ok(None)
    }
}

/// Parse a GitHub GraphQL `DateTime` string and attach head-specific error context.
fn parse_github_datetime_rfc3339(s: &str, context: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).map_err(|e| {
        anyhow!(
            "Failed to parse GitHub DateTime for {}: {} ({})",
            context,
            s,
            e
        )
    })
}

/// Fetches the newest recent closed-or-merged PR for each requested head branch.
///
/// The query is bounded by the caller's coarse closed-date cutoff and sorted by GitHub's
/// `closed` qualifier, so the branch-reuse guard can answer "is there any recent terminal PR on
/// this case-insensitive head identity?" with one search result per head instead of scanning full
/// history. Callers should still compare the returned timestamp precisely because GitHub's search
/// qualifier is date-based, not full-RFC3339. The returned `terminal_at` remains state-specific:
/// `mergedAt` for merged PRs and `closedAt` for manually closed PRs.
pub fn list_recent_terminal_prs_for_heads(
    heads: &[String],
    closed_since: OffsetDateTime,
) -> Result<Vec<TerminalPrInfo>> {
    let mut out: Vec<TerminalPrInfo> = Vec::new();
    if heads.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;
    let repo = format!("{owner}/{name}");
    let mut query = String::from("query { ");
    for (i, head) in heads.iter().enumerate() {
        let search_query = recent_terminal_search_query(&repo, head, closed_since);
        query.push_str(&format!(
            "pr{}: search(query:\"{}\", type:ISSUE, first:1) {{ nodes {{ ... on PullRequest {{ {} }} }} }} ",
            i,
            graphql_escape(&search_query),
            TERMINAL_PR_SEARCH_GRAPHQL_FIELDS,
        ));
    }
    query.push('}');

    let json = gh_ro(["api", "graphql", "-f", &format!("query={query}")].as_slice())?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    let data = &value["data"];
    for (i, requested_head) in heads.iter().enumerate() {
        let key = format!("pr{}", i);
        let pr = data[&key]["nodes"]
            .as_array()
            .and_then(|nodes| nodes.first())
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        if let Some(pr) = pr {
            out.push(terminal_info_from_head_search(&pr, requested_head)?);
        }
    }

    Ok(out)
}

/// Fetches one status-bearing PR per requested head branch.
///
/// For each entry in `heads`, this function first checks for an open PR and falls back to
/// a merged PR only when no open PR exists. That precedence keeps status output focused on
/// active review state and avoids showing a stale merged marker when a new exact-head PR has
/// been opened.
///
/// The return vector contains at most one item per requested head; heads with no open or
/// merged PR are omitted entirely. Callers should treat absence as "no known remote PR".
///
/// If a caller incorrectly assumes one output row per input head, it can misalign local and
/// remote state and display incorrect status icons.
///
/// # Errors
///
/// Returns an error when `gh pr list` fails or when the JSON response cannot be parsed.
pub fn list_open_or_merged_prs_for_heads(heads: &[String]) -> Result<Vec<PrInfoWithState>> {
    let mut out: Vec<PrInfoWithState> = Vec::new();
    if heads.is_empty() {
        return Ok(out);
    }
    let exact_open_matches_by_head =
        list_exact_prs_for_heads(heads, &["OPEN"], EXACT_HEAD_QUERY_LIMIT)?;
    let open_conflicts_by_head = list_open_conflicting_prs_for_heads_search(heads)?;
    let mut heads_without_open_prs = Vec::new();
    for head in heads {
        let mut open_matches = exact_open_matches_by_head
            .get(head)
            .cloned()
            .unwrap_or_default();
        open_matches.extend(
            open_conflicts_by_head
                .get(head)
                .cloned()
                .unwrap_or_default(),
        );
        if let Some(pr) = select_single_open_pr_match(head, open_matches)? {
            out.push(PrInfoWithState {
                number: pr.number,
                head: pr.head,
                state: PrState::Open,
            });
        } else {
            heads_without_open_prs.push(head.clone());
        }
    }
    if !heads_without_open_prs.is_empty() {
        let exact_merged_matches_by_head =
            list_exact_prs_for_heads(&heads_without_open_prs, &["MERGED"], EXACT_HEAD_QUERY_LIMIT)?;
        let merged_conflicts_by_head =
            list_conflicting_prs_for_heads_search_exhaustive(&heads_without_open_prs, "merged")?;
        for head in &heads_without_open_prs {
            let mut merged_matches = exact_merged_matches_by_head
                .get(head)
                .cloned()
                .unwrap_or_default();
            merged_matches.extend(
                merged_conflicts_by_head
                    .get(head)
                    .cloned()
                    .unwrap_or_default(),
            );
            if let Some(pr) = select_latest_merged_pr_match(head, &merged_matches)? {
                out.push(PrInfoWithState {
                    number: pr.number,
                    head: pr.head,
                    state: PrState::Merged,
                });
            }
        }
    }

    Ok(out)
}

/// List PRs for a given head branch across all states
/// Return the set of branch names (head refs) that currently have an OPEN PR
pub fn list_open_pr_heads() -> Result<HashSet<String>> {
    let json = gh_ro(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            "200",
            "--json",
            "headRefName",
        ]
        .as_slice(),
    )?;
    #[derive(Deserialize)]
    struct Raw {
        #[serde(rename = "headRefName")]
        head_ref_name: String,
    }
    let raws: Vec<Raw> = serde_json::from_str(&json)?;
    let mut set = HashSet::new();
    for r in raws {
        set.insert(r.head_ref_name);
    }
    Ok(set)
}

/// Creates a new pull request for the given branch and parent if one does not already exist,
/// and returns the PR number. If a PR for the branch already exists (as tracked in `prs_by_head`),
/// returns its number without making any changes. The function updates the `prs_by_head` map as needed.
/// If `dry` is true, no actual changes are made on GitHub.
pub fn upsert_pr_cached(
    branch: &str,
    parent: &str,
    title: &str,
    body: &str,
    dry: bool,
    prs_by_head: &mut HashMap<CanonicalBranchConflictKey, u64>,
) -> Result<u64> {
    let branch_key = head_key(branch);
    if let Some(&num) = prs_by_head.get(&branch_key) {
        // Defer edits to the final pass
        return Ok(num);
    }
    // Create PR and retrieve number in a single API call
    let (owner, name) = get_repo_owner_name()?;
    let path = format!("repos/{}/{}/pulls", owner, name);
    let created_number = gh_rw(
        dry,
        [
            "api",
            &path,
            "-X",
            "POST",
            "-f",
            &format!("head={}", branch),
            "-f",
            &format!("base={}", parent),
            "-f",
            &format!("title={}", title),
            "-f",
            &format!("body={}", body),
            "--jq",
            ".number",
        ]
        .as_slice(),
    )?;
    let mut num: u64 = created_number.trim().parse().unwrap_or(0);
    if num == 0 && !dry {
        let post_create_heads = [branch.to_string()];
        let mut post_create_matches =
            list_exact_prs_for_heads(&post_create_heads, &["OPEN"], EXACT_HEAD_QUERY_LIMIT)?
                .remove(branch)
                .unwrap_or_default();
        post_create_matches.extend(
            list_open_conflicting_prs_for_heads_search(&post_create_heads)?
                .remove(branch)
                .unwrap_or_default(),
        );
        if let Some(existing) = select_single_open_pr_match(branch, post_create_matches)? {
            num = existing.number;
        }
    }
    if num == 0 {
        return Err(anyhow!("Failed to determine PR number for {}", branch));
    }
    prs_by_head.insert(branch_key, num);
    Ok(num)
}

/// Append a warning line to a specific PR body (idempotent). Returns Ok(()) whether updated or skipped.
pub fn append_warning_to_pr(number: u64, warning: &str, dry: bool) -> Result<()> {
    let bodies = fetch_pr_bodies_graphql(&[number])?;
    if let Some(info) = bodies.get(&number) {
        let body = info.body.clone();
        if body.contains(warning) {
            info!("Warning already present in PR #{}; skipping", number);
            return Ok(());
        }
        let new_body = if body.trim().is_empty() {
            warning.to_string()
        } else {
            format!("{}\n\n{}", warning, body)
        };
        info!("Appending warning to PR #{} on GitHub...", number);
        let mut m = String::from("mutation {");
        m.push_str(&format!(
            "u: updatePullRequest(input:{{pullRequestId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
            info.id,
            graphql_escape(&new_body)
        ));
        m.push('}');
        gh_rw(
            dry,
            ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
        )?;
        info!("Appended warning to PR #{}", number);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        filter_case_variant_head_search_matches, filter_head_search_matches,
        list_conflicting_prs_for_heads_search_exhaustive, list_exact_prs_for_heads,
        list_open_or_merged_prs_for_heads, list_open_prs_for_heads,
        list_recent_terminal_prs_for_heads, parse_open_pr_automerge_node,
        select_latest_merged_pr_match, select_single_open_pr_match, HeadSearchPr, PrState,
        TerminalPrState, EXACT_HEAD_QUERY_LIMIT,
    };
    use crate::test_support::lock_cwd;
    use serde_json::{json, Value};
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
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

    fn head_search_pr(
        number: u64,
        head: &str,
        state: &str,
        merged_at: Option<&str>,
        closed_at: Option<&str>,
    ) -> HeadSearchPr {
        HeadSearchPr {
            number,
            head: head.to_string(),
            base: Some("main".to_string()),
            state: Some(state.to_string()),
            merged_at: merged_at.map(str::to_string),
            closed_at: closed_at.map(str::to_string),
            url: Some(format!("https://github.com/o/r/pull/{number}")),
            auto_merge_request: None,
        }
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

    fn graphql_nodes_response(nodes_by_alias: &[(&str, Value)]) -> String {
        let mut repo = serde_json::Map::new();
        for (alias, nodes) in nodes_by_alias {
            repo.insert((*alias).to_string(), json!({ "nodes": nodes }));
        }
        json!({ "data": { "repository": repo } }).to_string()
    }

    fn graphql_search_response(nodes_by_alias: &[(&str, Value)]) -> String {
        let mut data = serde_json::Map::new();
        for (alias, nodes) in nodes_by_alias {
            data.insert((*alias).to_string(), json!({ "nodes": nodes }));
        }
        json!({ "data": data }).to_string()
    }

    fn install_gh_graphql_and_list_wrapper(
        exact_open_json: &str,
        exact_merged_json: &str,
        exact_terminal_json: &str,
        search_open_json: &str,
        search_merged_json: &str,
        search_all_json: &str,
    ) -> (TempDir, TempDir, EnvVarGuard, String) {
        let data_dir = tempfile::tempdir().unwrap();
        let log_path = data_dir.path().join("gh.log");
        let exact_open_path = data_dir.path().join("exact-open.json");
        let exact_merged_path = data_dir.path().join("exact-merged.json");
        let exact_terminal_path = data_dir.path().join("exact-terminal.json");
        let search_open_path = data_dir.path().join("search-open.json");
        let search_merged_path = data_dir.path().join("search-merged.json");
        let search_all_path = data_dir.path().join("search-all.json");
        fs::write(&exact_open_path, exact_open_json).unwrap();
        fs::write(&exact_merged_path, exact_merged_json).unwrap();
        fs::write(&exact_terminal_path, exact_terminal_json).unwrap();
        fs::write(&search_open_path, search_open_json).unwrap();
        fs::write(&search_merged_path, search_merged_json).unwrap();
        fs::write(&search_all_path, search_all_json).unwrap();

        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  query_arg=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"-f\" ]; then\n      query_arg=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$query_arg\" in\n    *\"states:[OPEN]\"*) cat \"{}\" ;;\n    *\"states:[CLOSED,MERGED]\"*) cat \"{}\" ;;\n    *\"states:[MERGED]\"*) cat \"{}\" ;;\n    *\"is:pr is:open head:\"*) cat \"{}\" ;;\n    *) echo '{{\"data\":{{\"repository\":{{}}}}}}' ;;\n  esac\n  exit 0\nfi\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  state=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"--state\" ]; then\n      state=\"$2\"\n      break\n    fi\n    shift\n  done\n  case \"$state\" in\n    open) cat \"{}\" ;;\n    merged) cat \"{}\" ;;\n    all) cat \"{}\" ;;\n    *) echo \"[]\" ;;\n  esac\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            exact_open_path.display(),
            exact_terminal_path.display(),
            exact_merged_path.display(),
            search_open_path.display(),
            search_open_path.display(),
            search_merged_path.display(),
            search_all_path.display(),
        );
        let (wrapper_dir, path_guard) = install_gh_wrapper(&script);

        (
            wrapper_dir,
            data_dir,
            path_guard,
            log_path.display().to_string(),
        )
    }

    #[test]
    fn parse_open_pr_automerge_node_detects_enabled_automerge() {
        let node = json!({
            "number": 17,
            "headRefName": "dank-spr/example",
            "autoMergeRequest": {
                "enabledAt": "2026-03-01T12:00:00Z"
            }
        });

        let info = parse_open_pr_automerge_node(&node, "dank-spr/example").unwrap();

        assert_eq!(info.number, 17);
        assert_eq!(info.head, "dank-spr/example");
        assert!(info.auto_merge_enabled);
    }

    #[test]
    fn parse_open_pr_automerge_node_detects_disabled_automerge() {
        let node = json!({
            "number": 18,
            "headRefName": "dank-spr/example",
            "autoMergeRequest": null
        });

        let info = parse_open_pr_automerge_node(&node, "dank-spr/example").unwrap();

        assert_eq!(info.number, 18);
        assert_eq!(info.head, "dank-spr/example");
        assert!(!info.auto_merge_enabled);
    }

    #[test]
    fn filter_head_search_matches_treats_case_only_heads_as_equivalent() {
        let matches = filter_head_search_matches(
            "dank-spr/example",
            &[
                head_search_pr(17, "dank-spr/Example", "OPEN", None, None),
                head_search_pr(18, "dank-spr/other", "OPEN", None, None),
            ],
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].number, 17);
        assert_eq!(matches[0].head, "dank-spr/Example");
    }

    #[test]
    fn filter_case_variant_head_search_matches_drops_exact_spelling() {
        let matches = filter_case_variant_head_search_matches(
            "dank-spr/example",
            &[
                head_search_pr(17, "dank-spr/example", "OPEN", None, None),
                head_search_pr(18, "dank-spr/Example", "OPEN", None, None),
            ],
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].number, 18);
        assert_eq!(matches[0].head, "dank-spr/Example");
    }

    #[test]
    fn select_single_open_pr_match_rejects_multiple_case_folded_matches() {
        let err = select_single_open_pr_match(
            "dank-spr/example",
            vec![
                head_search_pr(17, "dank-spr/example", "OPEN", None, None),
                head_search_pr(18, "dank-spr/Example", "OPEN", None, None),
            ],
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Exact headRefName matches are required here"));
        assert!(err.to_string().contains("#17 (dank-spr/example)"));
        assert!(err.to_string().contains("#18 (dank-spr/Example)"));
    }

    #[test]
    fn select_single_open_pr_match_rejects_case_variant_without_exact_match() {
        let err = select_single_open_pr_match(
            "dank-spr/example",
            vec![head_search_pr(17, "dank-spr/Example", "OPEN", None, None)],
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Exact headRefName matches are required here"));
        assert!(err.to_string().contains("#17 (dank-spr/Example)"));
    }

    #[test]
    fn select_latest_merged_pr_match_uses_latest_exact_match() {
        let selected = select_latest_merged_pr_match(
            "dank-spr/example",
            &[
                head_search_pr(
                    11,
                    "dank-spr/example",
                    "MERGED",
                    Some("2026-02-01T00:00:00Z"),
                    Some("2026-02-01T00:00:00Z"),
                ),
                head_search_pr(
                    22,
                    "dank-spr/example",
                    "MERGED",
                    Some("2026-02-10T00:00:00Z"),
                    Some("2026-02-10T00:00:00Z"),
                ),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(selected.number, 22);
        assert_eq!(selected.head, "dank-spr/example");
    }

    #[test]
    fn select_latest_merged_pr_match_rejects_case_variant_without_exact_match() {
        let err = select_latest_merged_pr_match(
            "dank-spr/example",
            &[head_search_pr(
                11,
                "dank-spr/Example",
                "MERGED",
                Some("2026-02-01T00:00:00Z"),
                Some("2026-02-01T00:00:00Z"),
            )],
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Exact headRefName matches are required here"));
        assert!(err.to_string().contains("#11 (dank-spr/Example)"));
    }

    #[test]
    fn list_exact_prs_for_heads_queries_exact_head_refs() {
        let _lock = lock_cwd();
        let exact_open_json = graphql_nodes_response(&[
            (
                "pr0",
                json!([{
                    "number": 17,
                    "headRefName": "skilltest/alpha",
                    "baseRefName": "main",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/17",
                    "autoMergeRequest": null
                }]),
            ),
            (
                "pr1",
                json!([{
                    "number": 18,
                    "headRefName": "skilltest/beta",
                    "baseRefName": "skilltest/alpha",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/18",
                    "autoMergeRequest": null
                }]),
            ),
        ]);
        let (_wrapper_dir, _data_dir, _path_guard, log_path) = install_gh_graphql_and_list_wrapper(
            &exact_open_json,
            &graphql_nodes_response(&[]),
            &graphql_nodes_response(&[]),
            "[]",
            "[]",
            "[]",
        );

        let matches_by_head = list_exact_prs_for_heads(
            &["skilltest/alpha".to_string(), "skilltest/beta".to_string()],
            &["OPEN"],
            EXACT_HEAD_QUERY_LIMIT,
        )
        .unwrap();

        assert_eq!(matches_by_head["skilltest/alpha"][0].number, 17);
        assert_eq!(matches_by_head["skilltest/beta"][0].number, 18);
        let log = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("api graphql"));
        assert!(lines[0].contains("states:[OPEN]"));
        assert!(lines[0].contains("skilltest/alpha"));
        assert!(lines[0].contains("skilltest/beta"));
    }

    #[test]
    fn list_open_prs_for_heads_queries_exact_heads_and_searches_conflicts() {
        let _lock = lock_cwd();
        let exact_open_json = graphql_nodes_response(&[
            (
                "pr0",
                json!([{
                    "number": 17,
                    "headRefName": "skilltest/alpha",
                    "baseRefName": "main",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/17",
                    "autoMergeRequest": null
                }]),
            ),
            (
                "pr1",
                json!([{
                    "number": 18,
                    "headRefName": "skilltest/beta",
                    "baseRefName": "skilltest/alpha",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/18",
                    "autoMergeRequest": null
                }]),
            ),
        ]);
        let open_search_json = graphql_search_response(&[
            (
                "pr0",
                json!([{
                    "number": 17,
                    "headRefName": "skilltest/alpha",
                    "baseRefName": "main",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/17",
                    "autoMergeRequest": null
                }]),
            ),
            (
                "pr1",
                json!([{
                    "number": 18,
                    "headRefName": "skilltest/beta",
                    "baseRefName": "skilltest/alpha",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": "https://github.com/o/r/pull/18",
                    "autoMergeRequest": null
                }]),
            ),
        ]);
        let (_wrapper_dir, _data_dir, _path_guard, log_path) = install_gh_graphql_and_list_wrapper(
            &exact_open_json,
            &graphql_nodes_response(&[]),
            &graphql_nodes_response(&[]),
            &open_search_json,
            "[]",
            "[]",
        );

        let prs =
            list_open_prs_for_heads(&["skilltest/alpha".to_string(), "skilltest/beta".to_string()])
                .unwrap();

        assert_eq!(prs.len(), 2);
        assert_eq!(prs[0].number, 17);
        assert_eq!(prs[1].number, 18);

        let log = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("api graphql"));
        assert!(lines[0].contains("states:[OPEN]"));
        assert!(lines[1].contains("api graphql"));
        assert!(lines[1].contains("is:pr is:open head:skilltest/alpha"));
        assert!(lines[1].contains("is:pr is:open head:skilltest/beta"));
        assert!(lines[1].contains("first:2"));
    }

    #[test]
    fn list_open_or_merged_prs_for_heads_uses_exact_open_then_exact_merged() {
        let _lock = lock_cwd();
        let exact_merged_json = graphql_nodes_response(&[
            (
                "pr0",
                json!([{
                    "number": 21,
                    "headRefName": "skilltest/alpha",
                    "baseRefName": "main",
                    "state": "MERGED",
                    "mergedAt": "2026-02-01T00:00:00Z",
                    "closedAt": "2026-02-01T00:00:00Z",
                    "url": "https://github.com/o/r/pull/21",
                    "autoMergeRequest": null
                }]),
            ),
            (
                "pr1",
                json!([{
                    "number": 22,
                    "headRefName": "skilltest/beta",
                    "baseRefName": "main",
                    "state": "MERGED",
                    "mergedAt": "2026-02-02T00:00:00Z",
                    "closedAt": "2026-02-02T00:00:00Z",
                    "url": "https://github.com/o/r/pull/22",
                    "autoMergeRequest": null
                }]),
            ),
        ]);
        let open_search_json = graphql_search_response(&[("pr0", json!([])), ("pr1", json!([]))]);
        let (_wrapper_dir, _data_dir, _path_guard, log_path) = install_gh_graphql_and_list_wrapper(
            "[]",
            &exact_merged_json,
            &graphql_nodes_response(&[]),
            &open_search_json,
            "[]",
            "[]",
        );

        let prs = list_open_or_merged_prs_for_heads(&[
            "skilltest/alpha".to_string(),
            "skilltest/beta".to_string(),
        ])
        .unwrap();

        assert_eq!(prs.len(), 2);
        assert_eq!(prs[0].number, 21);
        assert_eq!(prs[0].state, PrState::Merged);
        assert_eq!(prs[1].number, 22);
        assert_eq!(prs[1].state, PrState::Merged);

        let log = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(lines[0].contains("api graphql"));
        assert!(lines[0].contains("states:[OPEN]"));
        assert!(lines[1].contains("api graphql"));
        assert!(lines[1].contains("is:pr is:open head:skilltest/alpha"));
        assert!(lines[1].contains("is:pr is:open head:skilltest/beta"));
        assert!(lines[2].contains("api graphql"));
        assert!(lines[2].contains("states:[MERGED]"));
        assert!(lines[3].contains("pr list --state merged --search head:skilltest/alpha"));
        assert!(lines[4].contains("pr list --state merged --search head:skilltest/beta"));
    }

    #[test]
    fn list_conflicting_prs_for_heads_search_exhaustive_retries_until_full_head_search_is_exhausted(
    ) {
        let _lock = lock_cwd();
        let data_dir = tempfile::tempdir().unwrap();
        let log_path = data_dir.path().join("gh.log");
        let capped_path = data_dir.path().join("open-100.json");
        let expanded_path = data_dir.path().join("open-200.json");
        let filler_entries: Vec<serde_json::Value> = (0..100)
            .map(|idx| {
                json!({
                    "number": idx + 1,
                    "headRefName": format!("skilltest/filler-{idx:03}"),
                    "baseRefName": "main",
                    "state": "OPEN",
                    "mergedAt": null,
                    "closedAt": null,
                    "url": format!("https://github.com/o/r/pull/{}", idx + 1),
                    "autoMergeRequest": null
                })
            })
            .collect();
        let mut expanded_entries = filler_entries.clone();
        expanded_entries.push(json!({
            "number": 999,
            "headRefName": "skilltest/Target",
            "baseRefName": "main",
            "state": "OPEN",
            "mergedAt": null,
            "closedAt": null,
            "url": "https://github.com/o/r/pull/999",
            "autoMergeRequest": null
        }));
        fs::write(
            &capped_path,
            serde_json::to_string(&filler_entries).unwrap(),
        )
        .unwrap();
        fs::write(
            &expanded_path,
            serde_json::to_string(&expanded_entries).unwrap(),
        )
        .unwrap();
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"list\" ]; then\n  limit=\"\"\n  while [ \"$#\" -gt 0 ]; do\n    if [ \"$1\" = \"--limit\" ]; then\n      limit=\"$2\"\n      break\n    fi\n    shift\n  done\n  if [ \"$limit\" = \"100\" ]; then\n    cat \"{}\"\n  else\n    cat \"{}\"\n  fi\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            capped_path.display(),
            expanded_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let mut matches_by_head = list_conflicting_prs_for_heads_search_exhaustive(
            &["skilltest/target".to_string()],
            "open",
        )
        .unwrap();
        let matches = matches_by_head.remove("skilltest/target").unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].number, 999);
        assert_eq!(matches[0].head, "skilltest/Target");
        let log = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("--limit 100"));
        assert!(lines[1].contains("--limit 200"));
        assert!(lines[0].contains("--search head:skilltest/target"));
        assert!(lines[1].contains("--search head:skilltest/target"));
    }

    #[test]
    fn list_recent_terminal_prs_for_heads_queries_recent_closed_search() {
        let _lock = lock_cwd();
        let search_json = graphql_search_response(&[
            (
                "pr0",
                json!([
                    {
                        "number": 11,
                        "headRefName": "skilltest/alpha",
                        "state": "MERGED",
                        "mergedAt": "2026-02-01T00:00:00Z",
                        "closedAt": "2026-02-01T00:00:01Z",
                        "url": "https://github.com/o/r/pull/11"
                    }
                ]),
            ),
            (
                "pr1",
                json!([
                    {
                        "number": 22,
                        "headRefName": "skilltest/Beta",
                        "state": "CLOSED",
                        "mergedAt": null,
                        "closedAt": "2026-02-10T00:00:00Z",
                        "url": "https://github.com/o/r/pull/22"
                    }
                ]),
            ),
        ]);
        let data_dir = tempfile::tempdir().unwrap();
        let log_path = data_dir.path().join("gh.log");
        let response_path = data_dir.path().join("search.json");
        fs::write(&response_path, search_json).unwrap();
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"api\" ] && [ \"$2\" = \"graphql\" ]; then\n  cat \"{}\"\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display(),
            response_path.display(),
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);
        let closed_since = time::OffsetDateTime::parse(
            "2026-02-01T12:34:56Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();

        let prs = list_recent_terminal_prs_for_heads(
            &["skilltest/alpha".to_string(), "skilltest/beta".to_string()],
            closed_since,
        )
        .unwrap();

        assert_eq!(prs.len(), 2);
        assert_eq!(prs[0].number, 11);
        assert_eq!(prs[0].state, TerminalPrState::Merged);
        assert_eq!(prs[0].terminal_at, "2026-02-01T00:00:00Z");
        assert_eq!(prs[1].number, 22);
        assert_eq!(prs[1].state, TerminalPrState::Closed);
        assert_eq!(prs[1].terminal_at, "2026-02-10T00:00:00Z");

        let log = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("api graphql"));
        assert!(lines[0]
            .contains("is:pr is:closed head:skilltest/alpha closed:>=2026-02-01 sort:closed-desc"));
        assert!(lines[0]
            .contains("is:pr is:closed head:skilltest/beta closed:>=2026-02-01 sort:closed-desc"));
    }
}
