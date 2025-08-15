use anyhow::Result;

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
