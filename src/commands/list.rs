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
        let plural = if count == 1 { "commit" } else { "commits" };
        let tip_sha = g.commits.last().map(|s| s.as_str()).unwrap_or("");
        let short = if tip_sha.len() >= 8 {
            &tip_sha[..8]
        } else {
            tip_sha
        };
        let remote_pr_num_str = match num {
            Some(n) => format!(" (#{})", n),
            None => "".to_string(),
        };
        info!(
            "Local PR #{} - {} : {}{} - {} {}",
            i + 1,
            short,
            head_branch,
            remote_pr_num_str,
            count,
            plural
        );
    }
    Ok(())
}
