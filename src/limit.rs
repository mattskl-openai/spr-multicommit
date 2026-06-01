use anyhow::Result;

use crate::parsing::Group;

#[derive(Clone, Copy)]
pub enum Limit {
    ByPr(usize),
}

pub fn apply_limit_groups(groups: Vec<Group>, limit: Option<Limit>) -> Result<Vec<Group>> {
    match limit {
        None => Ok(groups),
        Some(Limit::ByPr(n)) => Ok(groups.into_iter().take(n).collect()),
    }
}
