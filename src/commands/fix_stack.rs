use anyhow::{bail, Result};
use tracing::{info, warn};

use crate::git::{gh_rw, git_ro, normalize_branch_name, sanitize_gh_base_ref};
use crate::github::list_spr_prs;
use crate::parsing::parse_groups;

pub fn fix_stack(base: &str, prefix: &str, dry: bool) -> Result<()> {
    let base_n = normalize_branch_name(base);
    // Build local expected stack from base..HEAD
    let merge_base = git_ro(["merge-base", base, "HEAD"].as_slice())?
        .trim()
        .to_string();
    let lines = git_ro(
        [
            "log",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &format!("{}..HEAD", merge_base),
        ]
        .as_slice(),
    )?;
    let groups = parse_groups(&lines)?;
    if groups.is_empty() {
        info!("No local groups found; nothing to fix.");
        return Ok(());
    }

    // Existing PRs map by head
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Expected connectivity bottom-up
    let mut expected: Vec<(String, String)> = vec![]; // (head, base)
    let mut parent = base_n.clone();
    for g in &groups {
        let head = format!("{}{}", prefix, g.tag);
        expected.push((head.clone(), parent.clone()));
        parent = head;
    }

    // Apply base edits where needed
    for (head, want_base) in expected {
        if let Some(pr) = prs.iter().find(|p| p.head == head) {
            if pr.base != want_base {
                info!(
                    "Updating base of {} (#{}) from {} to {}",
                    head, pr.number, pr.base, want_base
                );
                gh_rw(
                    dry,
                    [
                        "pr",
                        "edit",
                        &format!("#{}", pr.number),
                        "--base",
                        &sanitize_gh_base_ref(&want_base),
                    ]
                    .as_slice(),
                )?;
            } else {
                info!("{} (#{}) already basing on {}", head, pr.number, want_base);
            }
        } else {
            warn!("No open PR found for {}; skipping", head);
        }
    }
    Ok(())
}
