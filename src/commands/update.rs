use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tracing::info;
use regex::Regex;

use crate::git::{get_remote_branches_sha, git_is_ancestor, git_ro, git_rw, sanitize_gh_base_ref, gh_rw, normalize_branch_name};
use crate::github::{list_spr_prs, upsert_pr_cached, PrRef, fetch_pr_bodies_graphql, graphql_escape};
use crate::limit::{apply_limit_groups, Limit};
use crate::parsing::{parse_groups, Group};

/// Bootstrap/refresh stack from pr:<tag> markers on `from` vs merge-base(base, from).
pub fn build_from_tags(
    base: &str,
    from: &str,
    prefix: &str,
    no_pr: bool,
    dry: bool,
    limit: Option<Limit>,
) -> Result<()> {
    let merge_base = git_ro(["merge-base", base, from].as_slice())?
        .trim()
        .to_string();
    let lines = git_ro(
        [
            "log",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &format!("{merge_base}..{from}"),
        ]
        .as_slice(),
    )?;
    let mut groups: Vec<Group> = parse_groups(&lines)?;

    if groups.is_empty() {
        info!("No groups discovered; nothing to do.");
        return Ok(());
    }

    // Apply extent limits
    groups = apply_limit_groups(groups, limit)?;
    let total_groups = groups.len();

    info!(
        "Preparing {} group(s): {}",
        groups.len(),
        groups
            .iter()
            .map(|g| g.tag.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Build bottom→top and collect PR refs for the visual update pass.
    let mut parent_branch = base.to_string();
    let mut stack: Vec<PrRef> = vec![];
    // Prefetch open PRs to reduce per-branch lookups
    let mut prs_by_head: HashMap<String, u64> = list_spr_prs(prefix)?
        .into_iter()
        .map(|p| (p.head, p.number))
        .collect();
    // Allow force-push within the selected scope when branch diverged from remote

    // Batch fetch remote SHAs for all target branches
    let branch_names: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
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

    for (idx, g) in groups.iter_mut().enumerate() {
        let branch = format!("{}{}", prefix, g.tag);
        info!(
            "({}/{}) Rebuilding branch {}",
            idx + 1,
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
            info!("No changes for {}; skipping push", branch);
            PushKind::Skip
        } else if let Some(ref remote_sha) = remote_head {
            let ff_ok = git_is_ancestor(remote_sha, &target_sha)?;
            if ff_ok { PushKind::FastForward } else { info!("Diverged: forcing update for {}", branch); PushKind::Force }
        } else {
            // No remote exists; create the branch
            PushKind::FastForward
        };
        planned.push(PlannedPush {
            branch: branch.clone(),
            target_sha: target_sha.clone(),
            kind,
        });

        parent_branch = branch;
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
        git_rw(dry, &args)?;
    }
    // Perform force-with-lease for diverged branches in scope
    let force_refspecs: Vec<String> = planned
        .iter()
        .filter(|p| p.kind == PushKind::Force)
        .map(|p| format!("{}:refs/heads/{}", p.target_sha, p.branch))
        .collect();
    if !force_refspecs.is_empty() {
        let mut argv: Vec<String> = vec!["push".into(), "--force-with-lease".into(), "origin".into()];
        argv.extend(force_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        git_rw(dry, &args)?;
    }

    // After pushes, (create or) update PRs
    let mut parent_branch = base.to_string();
    for (idx, g) in groups.iter().enumerate() {
        let branch = format!("{}{}", prefix, g.tag);
        if !no_pr && planned.get(idx).map(|p| p.kind != PushKind::Skip).unwrap_or(false) {
            let num = upsert_pr_cached(
                &branch,
                &sanitize_gh_base_ref(&parent_branch),
                &g.pr_title()?,
                &g.pr_body()?,
                dry,
                &mut prs_by_head,
            )?;
            stack.push(PrRef {
                number: num,
                head: branch.clone(),
                base: parent_branch.clone(),
            });
        }
        parent_branch = branch;
    }

    if !no_pr {
        // Build full-chain PR numbers (bottom-up) to avoid visual orphaning
        let prs_all = list_spr_prs(prefix)?;
        let root = prs_all.iter().find(|p| p.base == normalize_branch_name(base));
        let mut full_order: Vec<u64> = vec![];
        if let Some(r) = root {
            let mut cur_head = r.head.clone();
            let mut cur_num = r.number;
            full_order.push(cur_num);
            loop {
                if let Some(next) = prs_all.iter().find(|p| p.base == cur_head) {
                    full_order.push(next.number);
                    cur_head = next.head.clone();
                } else { break; }
            }
        }
        let numbers_full: Vec<u64> = if full_order.is_empty() { stack.iter().map(|p| p.number).collect() } else { full_order };
        let numbers_rev: Vec<u64> = numbers_full.iter().cloned().rev().collect();
        // Build desired bodies from local commits for the PRs we touched
        let mut desired_by_number: HashMap<u64, String> = HashMap::new();
        for (idx, g) in groups.iter().enumerate() {
            if let Some(pr) = stack.get(idx) {
                let base = g.pr_body_base()?;
                let mut lines = String::new();
                let em_space = "\u{2003}"; // U+2003 EM SPACE for indentation
                for n in &numbers_rev {
                    let marker = if *n == pr.number { "➡" } else { em_space };
                    lines.push_str(&format!("- {} #{}\n", marker, n));
                }
                let stack_block = format!(
                    "<!-- spr-stack:start -->\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n<!-- spr-stack:end -->",
                    lines.trim_end(),
                );
                let body = if base.trim().is_empty() { stack_block.clone() } else { format!("{}\n\n{}", base, stack_block) };
                desired_by_number.insert(pr.number, body);
            }
        }

        // Fetch current bodies to diff and batch-update only those that change
        let bodies_by_number = fetch_pr_bodies_graphql(&numbers_full)?;
        let mut to_update: Vec<(u64, String, String)> = vec![]; // (number, id, new_body)
        let re_stack = Regex::new(&format!(
            r"(?s){}.*?{}",
            regex::escape("<!-- spr-stack:start -->"),
            regex::escape("<!-- spr-stack:end -->")
        ))?;
        for pr in &stack {
            if let (Some(info), Some(desired)) = (bodies_by_number.get(&pr.number), desired_by_number.get(&pr.number)) {
                let current = info.body.clone();
                // Normalize: remove any existing stack block then re-add desired to generate comparable current
                let base_current = re_stack.replace(&current, "").trim().to_string();
                let reconstructed_current = if base_current.trim().is_empty() {
                    desired.clone()
                } else {
                    // desired already contains base + block; compare directly against current
                    format!("{}", current.trim())
                };
                if desired.trim() != reconstructed_current.trim() {
                    if !info.id.is_empty() {
                        to_update.push((pr.number, info.id.clone(), desired.clone()));
                    }
                }
            }
        }
        if !to_update.is_empty() {
            let mut m = String::from("mutation {");
            for (i, (_num, id, body)) in to_update.iter().enumerate() {
                m.push_str(&format!("m{}: updatePullRequest(input:{{pullRequestId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ", i, id, graphql_escape(body)));
            }
            m.push_str("}");
            gh_rw(dry, ["api", "graphql", "-f", &format!("query={}", m)].as_slice())?;
            for (num, _, _) in to_update { info!("Updated PR #{} description", num); }
        } else {
            info!("All PR descriptions up-to-date; no edits needed");
        }
    }

    Ok(())
}
