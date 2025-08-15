use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tracing::info;

use crate::git::{get_remote_branches_sha, git_is_ancestor, git_ro, git_rw, sanitize_gh_base_ref};
use crate::github::{list_spr_prs, update_stack_bodies, upsert_pr_cached, PrRef};
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
    let mut force_from_now = false; // flip to true on first divergence with remote

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
        } else {
            if let Some(ref remote_sha) = remote_head {
                let ff_ok = git_is_ancestor(remote_sha, &target_sha)?;
                if !ff_ok {
                    force_from_now = true;
                }
            }
            if force_from_now {
                PushKind::Force
            } else {
                PushKind::FastForward
            }
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
    for g in &groups {
        let branch = format!("{}{}", prefix, g.tag);
        if !no_pr {
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

    if !no_pr && !dry {
        update_stack_bodies(&stack, dry)?;
    } else if !no_pr && dry {
        info!(
            "DRY-RUN: would update PR descriptions with stack visual for {} PRs",
            stack.len()
        );
    }

    Ok(())
}
