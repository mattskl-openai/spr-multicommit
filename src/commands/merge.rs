use anyhow::{anyhow, bail, Result};
use tracing::warn;

use crate::git::{
    gh_rw, git_ro, git_rw, normalize_branch_name, sanitize_gh_base_ref, to_remote_ref,
};
use crate::github::list_spr_prs;

pub fn merge_prs_until(base: &str, prefix: &str, n: usize, dry: bool) -> Result<()> {
    if n == 0 {
        bail!("--until must be >= 1");
    }
    let base_n = normalize_branch_name(base);
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }
    let root = prs
        .iter()
        .find(|p| p.base == base_n)
        .ok_or_else(|| anyhow!("No root PR with base `{}`", base_n))?;

    // Build ordered chain bottom-up
    let mut ordered: Vec<&crate::github::PrInfo> = vec![];
    let mut cur = root;
    loop {
        ordered.push(cur);
        if let Some(next) = prs.iter().find(|p| p.base == cur.head) {
            cur = next;
        } else {
            break;
        }
    }
    if ordered.is_empty() {
        bail!("No PR chain found");
    }

    let take_n = n.min(ordered.len());
    let segment = &ordered[..take_n];

    // Verify each has exactly one unique commit over its parent
    git_rw(dry, ["fetch", "origin"].as_slice())?; // ensure remotes up to date
    let mut offenders: Vec<u64> = vec![];
    for (i, pr) in segment.iter().enumerate() {
        let parent = if i == 0 {
            base_n.clone()
        } else {
            segment[i - 1].head.clone()
        };
        let parent_ref = to_remote_ref(&parent);
        let child_ref = to_remote_ref(&pr.head);
        let cnt_s = git_ro(
            [
                "rev-list",
                "--count",
                &format!("{}..{}", parent_ref, child_ref),
            ]
            .as_slice(),
        )?;
        let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
        if cnt != 1 {
            offenders.push(pr.number);
        }
    }
    if !offenders.is_empty() {
        warn!(
            "The following PRs have != 1 commit: {}",
            offenders
                .iter()
                .map(|x| format!("#{}", x))
                .collect::<Vec<_>>()
                .join(", ")
        );
        bail!("Run `spr prep` to squash them first");
    }

    // Change base of Nth PR to actual base and merge it with rebase
    let nth = segment[take_n - 1];
    gh_rw(
        dry,
        [
            "pr",
            "edit",
            &format!("#{}", nth.number),
            "--base",
            &sanitize_gh_base_ref(base),
        ]
        .as_slice(),
    )?;
    gh_rw(
        dry,
        ["pr", "merge", &format!("#{}", nth.number), "--rebase"].as_slice(),
    )?;

    // Close others with a comment
    for pr in &segment[..take_n - 1] {
        gh_rw(
            dry,
            [
                "pr",
                "close",
                &format!("#{}", pr.number),
                "--comment",
                &format!("Merged as part of PR #{}", nth.number),
            ]
            .as_slice(),
        )?;
    }

    Ok(())
}
