//! Typed parsing and resolution for PR-group selectors.
//!
//! These types separate user-facing selector syntax from the current local PR
//! numbering. Command entrypoints parse into these enums once, then resolve
//! them against the current `Vec<Group>` to recover the numeric boundary or
//! local ordinal that existing command logic already understands.

use std::fmt::{self, Display};
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};

use crate::parsing::Group;

/// An immutable `pr:<label>` handle that refers to one outstanding PR group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableHandle {
    pub tag: String,
}

impl Display for StableHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pr:{}", self.tag)
    }
}

/// A selector for a single current PR group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupSelector {
    LocalPr(usize),
    Stable(StableHandle),
}

impl Display for GroupSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalPr(n) => write!(f, "LPR #{}", n),
            Self::Stable(handle) => write!(f, "{handle}"),
        }
    }
}

/// A selector for "through this group" semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InclusiveSelector {
    All,
    Group(GroupSelector),
}

/// A selector for "after this group" semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterSelector {
    Bottom,
    Top,
    Group(GroupSelector),
}

/// A selector for moving either one group or an inclusive range of groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupRangeSelector {
    Single(GroupSelector),
    Inclusive {
        start: GroupSelector,
        end: GroupSelector,
    },
}

fn has_pr_prefix(input: &str) -> bool {
    if let Some(prefix) = input.get(..3) {
        prefix.eq_ignore_ascii_case("pr:")
    } else {
        false
    }
}

fn parse_handle(input: &str) -> std::result::Result<StableHandle, String> {
    let trimmed = input.trim();
    if !has_pr_prefix(trimmed) {
        Err(format!("`{trimmed}` must start with `pr:`"))
    } else if let Some(tag) = trimmed.get(3..) {
        if tag.is_empty() {
            Err(format!(
                "stable selector `{trimmed}` is missing the label after `pr:`"
            ))
        } else if tag
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            Ok(StableHandle {
                tag: tag.to_string(),
            })
        } else {
            Err(format!(
                "stable selector `{trimmed}` must use only [A-Za-z0-9._-] after `pr:`"
            ))
        }
    } else {
        Err(format!("`{trimmed}` must start with `pr:`"))
    }
}

fn parse_local_pr(input: &str) -> std::result::Result<usize, String> {
    let trimmed = input.trim();
    let parsed = trimmed
        .parse::<usize>()
        .map_err(|_| format!("`{trimmed}` is not a valid local PR number"))?;
    if parsed == 0 {
        Err("local PR numbers are 1-based; use 1 or greater".to_string())
    } else {
        Ok(parsed)
    }
}

impl FromStr for GroupSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        if has_pr_prefix(trimmed) {
            parse_handle(trimmed).map(Self::Stable)
        } else {
            parse_local_pr(trimmed).map(Self::LocalPr)
        }
    }
}

impl FromStr for InclusiveSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        if has_pr_prefix(trimmed) {
            parse_handle(trimmed)
                .map(GroupSelector::Stable)
                .map(Self::Group)
        } else {
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                format!("`{trimmed}` must be 0, a local PR number, or `pr:<label>`")
            })?;
            if parsed == 0 {
                Ok(Self::All)
            } else {
                Ok(Self::Group(GroupSelector::LocalPr(parsed)))
            }
        }
    }
}

impl FromStr for AfterSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        let lowered = trimmed.to_ascii_lowercase();
        if lowered == "bottom" {
            Ok(Self::Bottom)
        } else if lowered == "top" || lowered == "last" || lowered == "all" {
            Ok(Self::Top)
        } else if has_pr_prefix(trimmed) {
            parse_handle(trimmed)
                .map(GroupSelector::Stable)
                .map(Self::Group)
        } else {
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                format!(
                    "`{trimmed}` must be 0, a local PR number, `bottom`, `top`, `last`, `all`, or `pr:<label>`"
                )
            })?;
            if parsed == 0 {
                Ok(Self::Bottom)
            } else {
                Ok(Self::Group(GroupSelector::LocalPr(parsed)))
            }
        }
    }
}

impl FromStr for GroupRangeSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        if let Some((start, end)) = trimmed.split_once("..") {
            let start = start.parse::<GroupSelector>()?;
            let end = end.parse::<GroupSelector>()?;
            Ok(Self::Inclusive { start, end })
        } else {
            trimmed.parse::<GroupSelector>().map(Self::Single)
        }
    }
}

/// Resolve a selector to a 0-based index into the current local stack.
pub fn resolve_group_index(groups: &[Group], selector: &GroupSelector) -> Result<usize> {
    match selector {
        GroupSelector::LocalPr(n) => {
            if *n > 0 && *n <= groups.len() {
                Ok(*n - 1)
            } else if groups.is_empty() {
                bail!("No outstanding PR groups are available.")
            } else {
                bail!(
                    "{} is out of range for the current stack (1..={}).",
                    selector,
                    groups.len()
                )
            }
        }
        GroupSelector::Stable(handle) => groups
            .iter()
            .position(|group| group.tag == handle.tag)
            .ok_or_else(|| anyhow!("No outstanding PR group matches stable handle `{handle}`.")),
    }
}

/// Resolve a selector to the current 1-based local PR ordinal.
pub fn resolve_group_ordinal(groups: &[Group], selector: &GroupSelector) -> Result<usize> {
    resolve_group_index(groups, selector).map(|index| index + 1)
}

/// Resolve an inclusive boundary to the number of groups to include from the bottom.
pub fn resolve_inclusive_count(groups: &[Group], selector: &InclusiveSelector) -> Result<usize> {
    match selector {
        InclusiveSelector::All => Ok(groups.len()),
        InclusiveSelector::Group(selector) => resolve_group_ordinal(groups, selector),
    }
}

/// Resolve an after-boundary to the number of groups to keep in place.
pub fn resolve_after_count(groups: &[Group], selector: &AfterSelector) -> Result<usize> {
    match selector {
        AfterSelector::Bottom => Ok(0),
        AfterSelector::Top => Ok(groups.len()),
        AfterSelector::Group(selector) => resolve_group_ordinal(groups, selector),
    }
}

/// Resolve a move-range selector to inclusive 1-based local ordinals.
pub fn resolve_group_range(
    groups: &[Group],
    selector: &GroupRangeSelector,
) -> Result<(usize, usize)> {
    match selector {
        GroupRangeSelector::Single(selector) => {
            let ordinal = resolve_group_ordinal(groups, selector)?;
            Ok((ordinal, ordinal))
        }
        GroupRangeSelector::Inclusive { start, end } => {
            let start = resolve_group_ordinal(groups, start)?;
            let end = resolve_group_ordinal(groups, end)?;
            Ok((start, end))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_after_count, resolve_group_index, resolve_group_range, resolve_inclusive_count,
        AfterSelector, GroupRangeSelector, GroupSelector, InclusiveSelector, StableHandle,
    };
    use crate::parsing::Group;

    fn group(tag: &str) -> Group {
        Group {
            tag: tag.to_string(),
            subjects: vec![format!("feat: {tag}")],
            commits: vec![format!("{tag}1")],
            first_message: Some(format!("feat: {tag} pr:{tag}")),
            ignored_after: Vec::new(),
        }
    }

    fn groups(tags: &[&str]) -> Vec<Group> {
        tags.iter().map(|tag| group(tag)).collect()
    }

    #[test]
    fn group_selector_parses_numeric_and_stable_forms() {
        assert_eq!(
            "2".parse::<GroupSelector>().unwrap(),
            GroupSelector::LocalPr(2)
        );
        assert_eq!(
            "PR:Beta".parse::<GroupSelector>().unwrap(),
            GroupSelector::Stable(StableHandle {
                tag: "Beta".to_string()
            })
        );
    }

    #[test]
    fn inclusive_selector_parses_zero_as_all() {
        assert_eq!(
            "0".parse::<InclusiveSelector>().unwrap(),
            InclusiveSelector::All
        );
    }

    #[test]
    fn after_selector_parses_keywords_and_stable_handle() {
        assert_eq!(
            "bottom".parse::<AfterSelector>().unwrap(),
            AfterSelector::Bottom
        );
        assert_eq!("last".parse::<AfterSelector>().unwrap(), AfterSelector::Top);
        assert_eq!(
            "pr:beta".parse::<AfterSelector>().unwrap(),
            AfterSelector::Group(GroupSelector::Stable(StableHandle {
                tag: "beta".to_string()
            }))
        );
    }

    #[test]
    fn group_range_selector_parses_single_and_range_forms() {
        assert_eq!(
            "pr:beta..3".parse::<GroupRangeSelector>().unwrap(),
            GroupRangeSelector::Inclusive {
                start: GroupSelector::Stable(StableHandle {
                    tag: "beta".to_string()
                }),
                end: GroupSelector::LocalPr(3)
            }
        );
        assert_eq!(
            "2".parse::<GroupRangeSelector>().unwrap(),
            GroupRangeSelector::Single(GroupSelector::LocalPr(2))
        );
    }

    #[test]
    fn malformed_stable_handle_is_rejected() {
        let err = "pr:beta!oops".parse::<GroupSelector>().unwrap_err();
        assert!(err.contains("[A-Za-z0-9._-]"), "unexpected error: {err}");
    }

    #[test]
    fn stable_handle_resolution_survives_local_pr_renumbering() {
        let before = groups(&["alpha", "beta", "gamma"]);
        let after = groups(&["beta", "gamma"]);
        let selector = GroupSelector::Stable(StableHandle {
            tag: "beta".to_string(),
        });

        let before_index = resolve_group_index(&before, &selector).unwrap();
        let after_index = resolve_group_index(&after, &selector).unwrap();

        assert_eq!(before[before_index].tag, "beta");
        assert_eq!(after[after_index].tag, "beta");
        assert_eq!(before_index + 1, 2);
        assert_eq!(after_index + 1, 1);
    }

    #[test]
    fn resolution_helpers_map_boundaries_to_counts() {
        let groups = groups(&["alpha", "beta", "gamma"]);

        assert_eq!(
            resolve_inclusive_count(
                &groups,
                &InclusiveSelector::Group(GroupSelector::Stable(StableHandle {
                    tag: "beta".to_string()
                }))
            )
            .unwrap(),
            2
        );
        assert_eq!(
            resolve_after_count(
                &groups,
                &AfterSelector::Group(GroupSelector::Stable(StableHandle {
                    tag: "beta".to_string()
                }))
            )
            .unwrap(),
            2
        );
        assert_eq!(
            resolve_group_range(
                &groups,
                &GroupRangeSelector::Inclusive {
                    start: GroupSelector::Stable(StableHandle {
                        tag: "beta".to_string()
                    }),
                    end: GroupSelector::LocalPr(3)
                }
            )
            .unwrap(),
            (2, 3)
        );
    }
}
