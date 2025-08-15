use anyhow::{bail, Result};
use tracing::{info, warn};

use crate::git::{gh_rw, git_ro, git_rw, normalize_branch_name, sanitize_gh_base_ref};
use crate::github::{list_spr_prs, PrInfo, PrRef};
use crate::limit::{apply_limit_prs_for_restack, Limit};

/// Restack existing spr/* PRs by rebase --onto Parent → Child, bottom→top.
pub fn restack_existing(
    base: &str,
    prefix: &str,
    no_pr: bool,
    dry: bool,
    limit: Option<Limit>,
) -> Result<()> {
    let base_n = normalize_branch_name(base);
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Build linear chains for each root (baseRefName == base)
    let roots: Vec<&PrInfo> = prs.iter().filter(|p| p.base == base_n).collect();
    if roots.is_empty() {
        bail!(
            "Could not find a root PR basing on `{}`. Ensure one PR has base `{}`.",
            base_n,
            base_n
        );
    }
    if roots.len() > 1 {
        warn!(
            "Found {} roots basing on `{}`; processing each chain independently.",
            roots.len(),
            base_n
        );
    }

    let mut overall_stack: Vec<PrRef> = vec![];

    for root in roots {
        let mut order: Vec<&PrInfo> = vec![];
        let mut cur = root;
        loop {
            order.push(cur);
            if let Some(next) = prs.iter().find(|p| p.base == cur.head) {
                cur = next;
            } else {
                break;
            }
        }

        // Apply extent limits for restack
        let order = apply_limit_prs_for_restack(&base_n, &order, limit)?;

        info!(
            "Restacking {} PR(s): {}",
            order.len(),
            order
                .iter()
                .map(|p| p.head.as_str())
                .collect::<Vec<_>>()
                .join(" → ")
        );

        // Ensure we only fetch once per chain
        git_rw(dry, ["fetch", "origin"].as_slice())?;
        for win in order.windows(2) {
            let parent = &win[0].head;
            let child = &win[1].head;

            info!("Rebasing {child} onto {parent}");
            git_ro(["checkout", child].as_slice())?;
            git_rw(
                dry,
                [
                    "merge",
                    "--no-ff",
                    parent,
                    "-m",
                    &format!("spr: merge {} into {}", parent, child),
                ]
                .as_slice(),
            )?;
            git_rw(dry, ["push", "origin", child].as_slice())?;

            if !no_pr {
                gh_rw(
                    dry,
                    ["pr", "edit", child, "--base", &sanitize_gh_base_ref(parent)].as_slice(),
                )?;
            }
        }

        // Collect for the visual pass (bottom→top order)
        for pr in &order {
            overall_stack.push(PrRef { number: pr.number });
        }
    }

    if !no_pr && !dry {
        crate::github::update_stack_bodies(&overall_stack, dry)?;
    } else if !no_pr && dry {
        info!(
            "DRY-RUN: would update PR descriptions with stack visual for {} PRs",
            overall_stack.len()
        );
    }

    Ok(())
}
