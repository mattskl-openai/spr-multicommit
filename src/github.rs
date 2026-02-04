//! GitHub API helpers used by `spr` commands.
//!
//! This module centralizes read/write calls to GitHub so command modules can operate on
//! typed results instead of raw JSON. The status-list path relies on branch-name lookups:
//! for each local stack head, we resolve either the currently open PR or (if none is open)
//! the latest merged PR for that same head ref.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use tracing::info;

use crate::git::{gh_ro, gh_rw, git_ro};

#[derive(Debug, Deserialize, Clone)]
pub struct PrInfo {
    pub number: u64,
    pub head: String,
    pub base: String,
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

/// Fetch open PRs for a specific set of head branches (exact matches), returning
/// only those heads that currently have an open PR. This avoids scanning a large
/// number of unrelated PRs in big repositories.
pub fn list_open_prs_for_heads(heads: &[String]) -> Result<Vec<PrInfo>> {
    let mut out: Vec<PrInfo> = Vec::new();
    if heads.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;

    // Build a single GraphQL query with aliases per head branch
    let mut q =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, head) in heads.iter().enumerate() {
        q.push_str(&format!(
            "pr{}: pullRequests(headRefName:\"{}\", states:[OPEN], first:1) {{ nodes {{ number headRefName baseRefName }} }} ",
            i,
            graphql_escape(head)
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
    for (i, _head) in heads.iter().enumerate() {
        let key = format!("pr{}", i);
        if let Some(nodes) = repo[&key]["nodes"].as_array() {
            if let Some(node) = nodes.first() {
                let number = node["number"].as_u64().unwrap_or(0);
                if number > 0 {
                    let head = node["headRefName"].as_str().unwrap_or("").to_string();
                    let base = node["baseRefName"].as_str().unwrap_or("").to_string();
                    out.push(PrInfo { number, head, base });
                }
            }
        }
    }

    Ok(out)
}

/// Fetches one status-bearing PR per requested head branch.
///
/// For each entry in `heads`, this function first checks for an open PR and falls back to
/// a merged PR only when no open PR exists. That precedence keeps status output focused on
/// active review state and avoids showing a stale merged marker when a new PR has been
/// opened from the same branch name.
///
/// The return vector contains at most one item per requested head; heads with no open or
/// merged PR are omitted entirely. Callers should treat absence as "no known remote PR".
///
/// If a caller incorrectly assumes one output row per input head, it can misalign local and
/// remote state and display incorrect status icons.
///
/// # Errors
///
/// Returns an error when repository identification fails, when `gh api graphql` fails, or
/// when the GraphQL response cannot be parsed.
pub fn list_open_or_merged_prs_for_heads(heads: &[String]) -> Result<Vec<PrInfoWithState>> {
    let mut out: Vec<PrInfoWithState> = Vec::new();
    if heads.is_empty() {
        return Ok(out);
    }
    let (owner, name) = get_repo_owner_name()?;

    // Build one query with two aliases per head: open and merged.
    let mut q =
        String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, head) in heads.iter().enumerate() {
        q.push_str(&format!(
            "openPr{}: pullRequests(headRefName:\"{}\", states:[OPEN], first:1) {{ nodes {{ number headRefName baseRefName }} }} ",
            i,
            graphql_escape(head)
        ));
        q.push_str(&format!(
            "mergedPr{}: pullRequests(headRefName:\"{}\", states:[MERGED], first:1) {{ nodes {{ number headRefName baseRefName }} }} ",
            i,
            graphql_escape(head)
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
    for (i, _head) in heads.iter().enumerate() {
        let open_key = format!("openPr{}", i);
        let merged_key = format!("mergedPr{}", i);

        let open_node = repo[&open_key]["nodes"]
            .as_array()
            .and_then(|nodes| nodes.first());
        let merged_node = repo[&merged_key]["nodes"]
            .as_array()
            .and_then(|nodes| nodes.first());

        let (node, state) = if let Some(node) = open_node {
            (node, PrState::Open)
        } else if let Some(node) = merged_node {
            (node, PrState::Merged)
        } else {
            continue;
        };

        let number = node["number"].as_u64().unwrap_or(0);
        if number > 0 {
            let head = node["headRefName"].as_str().unwrap_or("").to_string();
            out.push(PrInfoWithState {
                number,
                head,
                state,
            });
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
    prs_by_head: &mut HashMap<String, u64>,
) -> Result<u64> {
    if let Some(&num) = prs_by_head.get(branch) {
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
        // Fallback: query the number if jq parse failed for some reason
        let json = gh_ro(
            [
                "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json",
                "number",
            ]
            .as_slice(),
        )?;
        #[derive(Deserialize)]
        struct V {
            number: u64,
        }
        let arr: Vec<V> = serde_json::from_str(&json)?;
        num = arr.first().map(|v| v.number).unwrap_or(0);
    }
    if num == 0 {
        return Err(anyhow!("Failed to determine PR number for {}", branch));
    }
    prs_by_head.insert(branch.to_string(), num);
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
