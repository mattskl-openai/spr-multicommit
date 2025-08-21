use anyhow::{anyhow, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

use crate::commands::common;
use crate::git::{get_remote_branches_sha, gh_rw, git_is_ancestor, git_rw, sanitize_gh_base_ref};
use crate::github::{
    fetch_pr_bodies_graphql, get_repo_owner_name, graphql_escape, list_open_prs_for_heads,
    upsert_pr_cached,
};
use crate::limit::{apply_limit_groups, Limit};
use crate::parsing::{derive_groups_between, Group};

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
    let (_merge_base, mut groups): (String, Vec<Group>) = derive_groups_between(base, from)?;

    if groups.is_empty() {
        info!("No groups discovered; nothing to do.");
        return Ok(());
    }

    // Apply extent limits
    groups = apply_limit_groups(groups, limit)?;
    let total_groups = groups.len();

    info!("Preparing {} group(s)…", groups.len());

    // Build bottom→top and collect PR refs for the visual update pass.
    let mut just_created_numbers: Vec<u64> = vec![];
    // Prefetch open PRs to reduce per-branch lookups
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let pr_list = list_open_prs_for_heads(&heads)?;
    let mut prs_by_head: HashMap<String, u64> = HashMap::new();
    let mut current_base_by_number: HashMap<u64, String> = HashMap::new();
    for p in pr_list {
        prs_by_head.insert(p.head.clone(), p.number);
        current_base_by_number.insert(p.number, p.base);
    }
    // Allow force-push within the selected scope when branch diverged from remote

    // Batch fetch remote SHAs for all target branches
    let mut branch_names: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
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
        for g in &groups {
            let head_branch = format!("{}{}", prefix, g.tag);
            if let Some(&n) = prs_by_head.get(&head_branch) {
                numbers_full_pre.push(n);
                head_by_number_pre.insert(n, head_branch.clone());
            }
        }
        if !numbers_full_pre.is_empty() {
            // Compute desired chained base per existing PR using local groups
            let mut desired_base_by_number_pre: HashMap<u64, String> = HashMap::new();
            for (head, want_base) in common::build_head_base_chain(base, &groups, prefix) {
                if let Some(&num) = prs_by_head.get(&head) {
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
                let mut m = String::from("mutation {");
                let mut update_count = 0usize;
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
                    let fields = vec![
                        format!("pullRequestId:\"{}\"", info.id),
                        format!("baseRefName:\"{}\"", graphql_escape(&base_target)),
                    ];
                    m.push_str(&format!(
                        "m{}: updatePullRequest(input:{{{}}}){{ clientMutationId }} ",
                        update_count,
                        fields.join(", ")
                    ));
                    update_count += 1;
                }
                m.push('}');
                if update_count > 0 {
                    let pb = ProgressBar::new_spinner();
                    pb.set_style(
                        ProgressStyle::with_template("{spinner} Updating {pos} PR(s)…")
                            .unwrap()
                            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
                    );
                    pb.set_position(update_count as u64);
                    pb.enable_steady_tick(Duration::from_millis(120));
                    let res = gh_rw(
                        dry,
                        ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
                    );
                    pb.finish_and_clear();
                    res?;
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
        let mut argv: Vec<String> =
            vec!["push".into(), "--force-with-lease".into(), "origin".into()];
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
        let chain = common::build_head_base_chain(base, &groups, prefix);
        for (head_branch, want_base_ref) in chain {
            if let Some(&num) = prs_by_head.get(&head_branch) {
                // Stack visual (optional rewrite)
                if let Some(g) = groups
                    .iter()
                    .find(|g| format!("{}{}", prefix, g.tag) == head_branch)
                {
                    let base = g.pr_body_base()?;
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
                    let body = if base.trim().is_empty() {
                        stack_block.clone()
                    } else {
                        format!("{}\n\n{}", base, stack_block)
                    };
                    desired_by_number.insert(num, body);
                    desired_base_by_number.insert(num, want_base_ref.clone());
                }
            }
        }

        // Fetch PR ids/bodies for union of all PRs we may rewrite bodies for
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
        // Bases: set post-push to ensure final linkage
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
            let mut m = String::from("mutation {");
            let total_updates = update_specs.len();
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
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner} Updating {pos} PR(s)…")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            pb.set_position(total_updates as u64);
            pb.enable_steady_tick(Duration::from_millis(120));
            let res = gh_rw(
                dry,
                ["api", "graphql", "-f", &format!("query={}", m)].as_slice(),
            );
            pb.finish_and_clear();
            res?;
        } else {
            info!("All PR descriptions/base refs up-to-date; no edits needed");
        }
    }

    // Print full stack PR list in bottom→top order: "- <url> - <title>"
    if !no_pr {
        let mut ordered: Vec<(u64, String)> = vec![];
        for g in &groups {
            let head_branch = format!("{}{}", prefix, g.tag);
            if let Some(&n) = prs_by_head.get(&head_branch) {
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
