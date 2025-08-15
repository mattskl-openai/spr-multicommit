use anyhow::Result;

use crate::git::git_ro;
use crate::git::normalize_branch_name;
use crate::github::PrInfo;
use crate::parsing::Group;

#[derive(Clone, Copy)]
pub enum Limit {
    ByPr(usize),
    ByCommits(usize),
}

pub fn apply_limit_groups(mut groups: Vec<Group>, limit: Option<Limit>) -> Result<Vec<Group>> {
    match limit {
        None => Ok(groups),
        Some(Limit::ByPr(n)) => Ok(groups.into_iter().take(n).collect()),
        Some(Limit::ByCommits(mut n)) => {
            let mut out = vec![];
            for mut g in groups.drain(..) {
                if n == 0 {
                    break;
                }
                let len = g.commits.len();
                if len <= n {
                    out.push(g);
                    n -= len;
                } else {
                    g.commits.truncate(n);
                    if !g.subjects.is_empty() {
                        g.subjects.truncate(g.commits.len().min(g.subjects.len()));
                    }
                    out.push(g);
                    n = 0;
                }
            }
            Ok(out)
        }
    }
}

pub fn apply_limit_prs_for_restack<'a>(
    base: &str,
    order: &'a Vec<&'a PrInfo>,
    limit: Option<Limit>,
) -> Result<Vec<&'a PrInfo>> {
    match limit {
        None => Ok(order.clone()),
        Some(Limit::ByPr(n)) => Ok(order.iter().take(n).cloned().collect()),
        Some(Limit::ByCommits(mut n)) => {
            // Keep adding PRs while cumulative unique commit count (over parent) <= n
            let mut out: Vec<&PrInfo> = vec![];
            for (i, pr) in order.iter().enumerate() {
                out.push(pr);
                if i == order.len() - 1 {
                    break;
                }
                let parent = if i == 0 {
                    normalize_branch_name(base)
                } else {
                    order[i].head.clone()
                };
                let child = &order[i + 1].head;
                let cnt_s =
                    git_ro(["rev-list", "--count", &format!("{}..{}", parent, child)].as_slice())?;
                let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
                if cnt > n {
                    break;
                }
                n = n.saturating_sub(cnt);
            }
            Ok(out)
        }
    }
}
