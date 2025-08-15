use anyhow::Result;
use tracing::info;

use crate::git::git_ro;
use crate::github::list_spr_prs;
use crate::parsing::parse_groups;

pub fn list_prs_display(base: &str, prefix: &str) -> Result<()> {
    // Derive stack from local commits (source of truth)
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
        info!("No groups discovered; nothing to list.");
        return Ok(());
    }

    // Fetch PRs to annotate with numbers when available
    let prs = list_spr_prs(prefix)?; // may be empty; that's fine

    for (i, g) in groups.iter().enumerate() {
        let head_branch = format!("{}{}", prefix, g.tag);
        let num = prs.iter().find(|p| p.head == head_branch).map(|p| p.number);
        let count = g.commits.len();
        match num {
            Some(n) => info!("{}: {} (#{}) - {} commit(s)", i + 1, head_branch, n, count),
            None => info!("{}: {} - {} commit(s)", i + 1, head_branch, count),
        }
    }
    Ok(())
}
