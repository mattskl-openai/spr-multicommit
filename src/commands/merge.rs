use anyhow::{anyhow, bail, Result};
use tracing::warn;

use crate::git::{
    gh_rw, git_ro, git_rw, normalize_branch_name, sanitize_gh_base_ref, to_remote_ref,
};
use crate::github::{list_spr_prs, fetch_pr_bodies_graphql, graphql_escape};

pub fn merge_prs_until(base: &str, prefix: &str, n: usize, dry: bool) -> Result<()> {
    if n == 0 {
        bail!("--until must be >= 1");
    }
    let base_n = normalize_branch_name(base);
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }
    let root = prs
        .iter()
        .find(|p| p.base == base_n)
        .ok_or_else(|| anyhow!("No root PR with base `{}`", base_n))?;

    // Build ordered chain bottom-up
    let mut ordered: Vec<&crate::github::PrInfo> = vec![];
    let mut cur = root;
    loop {
        ordered.push(cur);
        if let Some(next) = prs.iter().find(|p| p.base == cur.head) {
            cur = next;
        } else {
            break;
        }
    }
    if ordered.is_empty() {
        bail!("No PR chain found");
    }

    let take_n = n.min(ordered.len());
    let segment = &ordered[..take_n];

    // Verify each has exactly one unique commit over its parent
    git_rw(dry, ["fetch", "origin"].as_slice())?; // ensure remotes up to date
    let mut offenders: Vec<u64> = vec![];
    for (i, pr) in segment.iter().enumerate() {
        let parent = if i == 0 {
            base_n.clone()
        } else {
            segment[i - 1].head.clone()
        };
        let parent_ref = to_remote_ref(&parent);
        let child_ref = to_remote_ref(&pr.head);
        let cnt_s = git_ro(
            [
                "rev-list",
                "--count",
                &format!("{}..{}", parent_ref, child_ref),
            ]
            .as_slice(),
        )?;
        let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
        if cnt != 1 {
            offenders.push(pr.number);
        }
    }
    if !offenders.is_empty() {
        warn!(
            "The following PRs have != 1 commit: {}",
            offenders
                .iter()
                .map(|x| format!("#{}", x))
                .collect::<Vec<_>>()
                .join(", ")
        );
        bail!("Run `spr prep` to squash them first");
    }

    // Batch: set base of Nth PR, merge it (rebase), and close others with a comment via GraphQL
    let nth = segment[take_n - 1];
    let mut nums: Vec<u64> = vec![nth.number];
    for pr in &segment[..take_n - 1] { nums.push(pr.number); }
    let bodies = fetch_pr_bodies_graphql(&nums)?;
    let nth_id = bodies.get(&nth.number).map(|x| x.id.clone()).unwrap_or_default();
    if nth_id.is_empty() { bail!("Failed to fetch GraphQL id for PR #{}", nth.number); }

    let mut m = String::from("mutation {");
    // Ensure base for nth
    m.push_str(&format!(
        "b0: updatePullRequest(input:{{pullRequestId:\"{}\", baseRefName:\"{}\"}}){{ clientMutationId }} ",
        nth_id,
        graphql_escape(&sanitize_gh_base_ref(base))
    ));
    // Merge nth with REBASE
    m.push_str(&format!(
        "m0: mergePullRequest(input:{{pullRequestId:\"{}\", mergeMethod:REBASE}}){{ clientMutationId }} ",
        nth_id
    ));
    // Close others with a comment
    for (i, pr) in segment[..take_n - 1].iter().enumerate() {
        let id = bodies.get(&pr.number).map(|x| x.id.clone()).unwrap_or_default();
        if id.is_empty() { continue; }
        let idx = i + 1;
        let comment = format!("Merged as part of PR #{}", nth.number);
        m.push_str(&format!(
            "c{}: addComment(input:{{subjectId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ",
            idx,
            id,
            graphql_escape(&comment)
        ));
        m.push_str(&format!(
            "x{}: closePullRequest(input:{{pullRequestId:\"{}\"}}){{ clientMutationId }} ",
            idx,
            id
        ));
    }
    m.push('}');
    gh_rw(dry, ["api", "graphql", "-f", &format!("query={}", m)].as_slice())?;

    Ok(())
}
