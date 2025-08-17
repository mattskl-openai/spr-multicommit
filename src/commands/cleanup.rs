use anyhow::Result;
use tracing::info;

use crate::git::{git_rw, list_remote_branches_with_prefix};
use crate::github::list_open_pr_heads;

/// Delete remote branches that start with the configured prefix and have only closed PRs (or no PRs)
pub fn cleanup_remote_branches(prefix: &str, dry: bool) -> Result<()> {
    let branches = list_remote_branches_with_prefix(prefix)?;
    if branches.is_empty() {
        info!("No remote branches found with prefix {}", prefix);
        return Ok(());
    }

    let open_heads = list_open_pr_heads()?;
    let mut to_delete: Vec<String> = vec![];
    let mut skipped: usize = 0;
    for name in branches {
        if open_heads.contains(&name) {
            skipped += 1;
        } else {
            to_delete.push(name);
        }
    }

    if to_delete.is_empty() {
        info!("Nothing to delete; {} branch(es) have open PRs", skipped);
        return Ok(());
    }

    info!(
        "Deleting {} remote branch(es) with no open PRs…",
        to_delete.len()
    );
    // Batch delete in a single push
    let mut owned_args: Vec<String> = vec!["push".into(), "origin".into(), "--delete".into()];
    owned_args.extend(to_delete.iter().cloned());
    let as_strs: Vec<&str> = owned_args.iter().map(|s| s.as_str()).collect();
    let _ = git_rw(dry, &as_strs)?;
    info!(
        "Deleted {} branch(es); skipped {} with open PRs",
        to_delete.len(),
        skipped
    );
    Ok(())
}
