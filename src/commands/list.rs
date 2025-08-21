use anyhow::Result;
use tracing::info;

use crate::github::{fetch_pr_ci_review_status, list_open_prs_for_heads};
use crate::parsing::derive_local_groups;

pub fn list_prs_display(base: &str, prefix: &str) -> Result<()> {
    // Derive stack from local commits (source of truth)
    let (_merge_base, groups) = derive_local_groups(base)?;
    if groups.is_empty() {
        info!("No groups discovered; nothing to list.");
        return Ok(());
    }

    // Fetch PRs to annotate with numbers and statuses when available.
    // Optimize by querying only the heads in this local stack rather than scanning many PRs.
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?; // may be empty; that's fine
    let mut status_map: std::collections::HashMap<u64, crate::github::PrCiReviewStatus> =
        std::collections::HashMap::new();
    if !prs.is_empty() {
        let numbers: Vec<u64> = prs.iter().map(|p| p.number).collect();
        if let Ok(m) = fetch_pr_ci_review_status(&numbers) {
            status_map = m;
        }
    }

    // Header showing columns for CI and Review status
    info!("┏━━{}CI status", crate::format::EM_SPACE);
    info!("┃┏━{}review status", crate::format::EM_SPACE);

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
        // Status icons
        let (ci_icon, rv_icon) = if let Some(n) = num {
            if let Some(st) = status_map.get(&n) {
                let ci_icon = match st.ci_state.as_str() {
                    "SUCCESS" => "✓",
                    "FAILURE" | "ERROR" => "✗",
                    "PENDING" | "EXPECTED" => "◐",
                    _ => "?",
                };
                let rv_icon = match st.review_decision.as_str() {
                    "APPROVED" => "✓",
                    "CHANGES_REQUESTED" => "✗",
                    "REVIEW_REQUIRED" => "◐",
                    _ => "?",
                };
                (ci_icon, rv_icon)
            } else {
                ("?", "?")
            }
        } else {
            ("?", "?")
        };
        info!(
            "{}{} LPR #{} - {} : {}{} - {} {}",
            ci_icon,
            rv_icon,
            i + 1,
            short,
            head_branch,
            remote_pr_num_str,
            count,
            plural
        );
        let first_subject = g.subjects.first().map(|s| s.as_str()).unwrap_or("");
        info!(
            "{s}{s}{s}{s}{s}{subject}",
            s = crate::format::EM_SPACE,
            subject = first_subject
        );
    }
    Ok(())
}

pub fn list_commits_display(base: &str, prefix: &str) -> Result<()> {
    // Derive stack from local commits (source of truth)
    let (_merge_base, groups) = derive_local_groups(base)?;
    if groups.is_empty() {
        info!("No groups discovered; nothing to list.");
        return Ok(());
    }

    // Fetch PRs to annotate groups with remote numbers when available
    let heads: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let prs = list_open_prs_for_heads(&heads)?; // may be empty; that's fine

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
