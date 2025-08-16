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
        let first_sha = g.commits.first().map(|s| s.as_str()).unwrap_or("");
        let short = if first_sha.len() >= 8 {
            &first_sha[..8]
        } else {
            first_sha
        };
        let remote_pr_num_str = match num {
            Some(n) => format!(" (#{})", n),
            None => "".to_string(),
        };
        info!(
            "LPR #{} - {} : {}{} - {} {}",
            i + 1,
            short,
            head_branch,
            remote_pr_num_str,
            count,
            plural
        );
        let first_subject = g.subjects.first().map(|s| s.as_str()).unwrap_or("");
        info!("  {}", first_subject);
    }
    Ok(())
}

pub fn list_commits_display(base: &str, prefix: &str) -> Result<()> {
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

    // Fetch PRs to annotate groups with remote numbers when available
    let prs = list_spr_prs(prefix)?; // may be empty; that's fine

    let mut commit_counter: usize = 0; // global, bottom-up
    for (i, g) in groups.iter().enumerate() {
        let head_branch = format!("{}{}", prefix, g.tag);
        let num = prs.iter().find(|p| p.head == head_branch).map(|p| p.number);
        let remote_pr_num_str = match num {
            Some(n) => format!(" (#{})", n),
            None => String::new(),
        };

        // Group separator with local PR number
        info!(
            "===== Local PR #{}{} : {} =====",
            i + 1,
            remote_pr_num_str,
            head_branch
        );

        for (j, sha) in g.commits.iter().enumerate() {
            commit_counter += 1;
            let short = if sha.len() >= 8 { &sha[..8] } else { sha };
            let subject = g.subjects.get(j).map(|s| s.as_str()).unwrap_or("");
            info!("{:>4}  {} - {}", commit_counter, short, subject);
        }
        // Blank line between groups for readability
        info!("");
    }
    Ok(())
}
