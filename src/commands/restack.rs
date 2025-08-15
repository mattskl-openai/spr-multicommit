use anyhow::Result;
use tracing::info;

use crate::git::{git_ro, git_rw};
use crate::parsing::parse_groups;

/// Restack the top of the local stack after the bottom `after` PRs.
///
/// Steps:
/// - Compute groups (PRs) from `base..HEAD`
/// - Let K = total commits in the first `after` groups
/// - Rebase the remaining commits onto `base` using:
///   `git rebase --onto <base> <upstream> <current-branch>` where
///   `<upstream>` is the tip commit of the Nth group (or merge-base if N==0)
pub fn restack_after(base: &str, _prefix: &str, after: usize, dry: bool) -> Result<()> {
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
        info!("No local PR groups found; nothing to restack.");
        return Ok(());
    }
    if after >= groups.len() {
        info!(
            "after={} >= {} group(s); nothing to restack.",
            after,
            groups.len()
        );
        return Ok(());
    }

    // K = number of commits in the first N groups
    let mut first_commits: Vec<String> = vec![];
    for g in groups.iter().take(after) {
        first_commits.extend(g.commits.iter().cloned());
    }
    let k = first_commits.len();
    if k == 0 {
        info!(
            "First {} PR(s) contain zero commits; rebasing entire stack onto {}",
            after, base
        );
    } else {
        info!("K = {} commits in the first {} PR(s)", k, after);
    }

    // Determine upstream for rebase: first commit after the first N groups, or merge-base if none
    let upstream = if k == 0 {
        merge_base
    } else {
        first_commits[k].clone()
    };

    let cur_branch = git_ro(["rev-parse", "--abbrev-ref", "HEAD"].as_slice())?
        .trim()
        .to_string();
    info!(
        "Rebasing commits after first {} PR(s) of {} onto {} (upstream = {})",
        after, cur_branch, base, upstream
    );
    git_rw(
        dry,
        ["rebase", "--onto", base, &upstream, &cur_branch].as_slice(),
    )?;

    Ok(())
}
