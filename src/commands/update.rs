use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tracing::info;

use crate::git::{
    get_remote_branches_sha, gh_rw, git_is_ancestor, git_ro, git_rw, sanitize_gh_base_ref,
};
use crate::github::{
    fetch_pr_bodies_graphql, graphql_escape, list_spr_prs, upsert_pr_cached,
};
use crate::limit::{apply_limit_groups, Limit};
use crate::parsing::{parse_groups, Group};

/// Bootstrap/refresh stack from pr:<tag> markers on `from` vs merge-base(base, from).
pub fn build_from_tags(
    base: &str,
    from: &str,
    prefix: &str,
    no_pr: bool,
    dry: bool,
    _update_pr_body: bool,
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
    let mut just_created_numbers: Vec<u64> = vec![];
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
            if ff_ok {
                PushKind::FastForward
            } else {
                info!("Diverged: forcing update for {}", branch);
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
        let mut argv: Vec<String> =
            vec!["push".into(), "--force-with-lease".into(), "origin".into()];
        argv.extend(force_refspecs.clone());
        let args: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        git_rw(dry, &args)?;
    }

    // After pushes, (create or) update PRs
    let mut parent_branch = base.to_string();
    for g in groups.iter() {
        let branch = format!("{}{}", prefix, g.tag);
        if !no_pr {
            let was_known = prs_by_head.contains_key(&branch);
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
        for g in &groups {
            let head_branch = format!("{}{}", prefix, g.tag);
            if let Some(&n) = prs_by_head.get(&head_branch) {
                numbers_full.push(n);
            }
        }
        let numbers_rev: Vec<u64> = numbers_full.iter().cloned().rev().collect();
        // Build desired bodies and base refs from local commits
        let mut desired_by_number: HashMap<u64, String> = HashMap::new();
        let mut desired_base_by_number: HashMap<u64, String> = HashMap::new();
        let mut want_base_ref = sanitize_gh_base_ref(base);
        for g in groups.iter() {
            let head_branch = format!("{}{}", prefix, g.tag);
            if let Some(&num) = prs_by_head.get(&head_branch) {
                // Stack visual (optional rewrite)
                let base = g.pr_body_base()?;
                let mut lines = String::new();
                let em_space = "\u{2003}"; // U+2003 EM SPACE for indentation
                for n in &numbers_rev {
                    let marker = if *n == num { "➡" } else { em_space };
                    lines.push_str(&format!("- {} #{}\n", marker, n));
                }
                let stack_block = format!(
                    "<!-- spr-stack:start -->\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n<!-- spr-stack:end -->",
                    lines.trim_end(),
                );
                let body = if base.trim().is_empty() {
                    stack_block.clone()
                } else {
                    format!("{}\n\n{}", base, stack_block)
                };
                desired_by_number.insert(num, body);
                // Base linkage (always set according to local stack)
                desired_base_by_number.insert(num, want_base_ref.clone());
                want_base_ref = head_branch;
            }
        }

        // Fetch PR ids/bodies for union of all PRs in local stack (for base) and those we may rewrite bodies for
        let mut fetch_set: std::collections::HashSet<u64> = numbers_full.iter().cloned().collect();
        for &n in desired_by_number.keys() {
            fetch_set.insert(n);
        }
        for &n in desired_base_by_number.keys() {
            fetch_set.insert(n);
        }
        let fetch_list: Vec<u64> = fetch_set.into_iter().collect();
        let bodies_by_number = fetch_pr_bodies_graphql(&fetch_list)?;
        // Combine updates per PR: (id, maybe_body, maybe_base)
        struct UpdateSpec {
            id: String,
            body: Option<String>,
            base: Option<String>,
        }
        let mut update_specs: HashMap<u64, UpdateSpec> = HashMap::new();
        // Bodies: always update
        for (&num, desired) in &desired_by_number {
            if let Some(info) = bodies_by_number.get(&num) {
                let entry = update_specs.entry(num).or_insert(UpdateSpec {
                    id: info.id.clone(),
                    body: None,
                    base: None,
                });
                entry.body = Some(desired.clone());
            }
        }
        // Bases
        for (&num, want_base) in &desired_base_by_number {
            if let Some(info) = bodies_by_number.get(&num) {
                let entry = update_specs.entry(num).or_insert(UpdateSpec {
                    id: info.id.clone(),
                    body: None,
                    base: None,
                });
                entry.base = Some(sanitize_gh_base_ref(want_base));
            }
        }
        if !update_specs.is_empty() {
            info!(
                "Updating {} PR(s) on GitHub (bodies and/or base refs)... this might take a few seconds.",
                update_specs.len()
            );
            let mut m = String::from("mutation {");
            for (i, (_num, spec)) in update_specs.into_iter().enumerate() {
                let mut fields: Vec<String> = vec![format!("pullRequestId:\"{}\"", spec.id)];
                if let Some(b) = spec.body {
                    fields.push(format!("body:\"{}\"", graphql_escape(&b)));
                }
                if let Some(base_ref) = spec.base {
                    fields.push(format!("baseRefName:\"{}\"", graphql_escape(&base_ref)));
                }
                m.push_str(&format!(
                    "m{}: updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
                    i,
                    fields.join(", ")
                ));
            }
            m.push('}');
            gh_rw(
                dry,
                ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
            )?;
        } else {
            info!("All PR descriptions/base refs up-to-date; no edits needed");
        }
    }

    Ok(())
}
