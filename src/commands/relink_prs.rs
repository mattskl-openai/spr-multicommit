use anyhow::{bail, Result};
use tracing::{info, warn};

use crate::commands::common;
use crate::git::{gh_rw, normalize_branch_name, sanitize_gh_base_ref};
use crate::github::list_open_prs_for_heads;
use crate::parsing::derive_local_groups;

pub fn relink_prs(base: &str, prefix: &str, dry: bool) -> Result<()> {
    let base_n = normalize_branch_name(base);
    // Build local expected stack from base..HEAD
    let (_merge_base, groups) = derive_local_groups(base)?;
    if groups.is_empty() {
        info!("No local groups found; nothing to fix.");
        return Ok(());
    }

    // Existing PRs map by head
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Expected connectivity bottom-up
    let expected = common::build_head_base_chain(&base_n, &groups, prefix);

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
