//! Typed parsing and resolution for PR-group selectors.
//!
//! Explicit selectors use either `pr:<label>` or `branch:<branch-name>`.
//! Bare selectors are convenience syntax only: they resolve when exactly one
//! current-stack group has the matching bare marker payload.

use std::fmt::{self, Display};
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};

use crate::group_markers::GroupMarker;
use crate::parsing::Group;

/// An explicit selector that refers to one outstanding PR group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplicitGroupSelector {
    PrLabel(String),
    BranchName(String),
}

impl ExplicitGroupSelector {
    fn marker(&self) -> GroupMarker {
        match self {
            Self::PrLabel(label) => GroupMarker::PrLabel(label.clone()),
            Self::BranchName(branch_name) => GroupMarker::BranchName(branch_name.clone()),
        }
    }
}

impl Display for ExplicitGroupSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrLabel(label) => write!(f, "pr:{label}"),
            Self::BranchName(branch_name) => write!(f, "branch:{branch_name}"),
        }
    }
}

/// A selector for a single current PR group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupSelector {
    LocalPr(usize),
    Explicit(ExplicitGroupSelector),
    Bare(String),
}

impl Display for GroupSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalPr(n) => write!(f, "LPR #{n}"),
            Self::Explicit(selector) => write!(f, "{selector}"),
            Self::Bare(value) => write!(f, "{value}"),
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

impl Display for AfterSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bottom => write!(f, "bottom"),
            Self::Top => write!(f, "top"),
            Self::Group(selector) => write!(f, "{selector}"),
        }
    }
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

fn strip_ascii_prefix<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let head = input.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        input.get(prefix.len()..)
    } else {
        None
    }
}

fn is_digits_only(input: &str) -> bool {
    !input.is_empty() && input.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_pr_label(input: &str, whole: &str) -> std::result::Result<String, String> {
    if input.is_empty() {
        Err(format!(
            "explicit selector `{whole}` is missing the label after `pr:`"
        ))
    } else if let Err(err) = crate::pr_labels::validate_label(input) {
        match err {
            crate::pr_labels::LabelValidationError::MustStartWithLetter => Err(format!(
                "explicit selector `{whole}` must start with an ASCII letter after `pr:`"
            )),
            crate::pr_labels::LabelValidationError::InvalidCharacters => Err(format!(
                "explicit selector `{whole}` must use only ASCII letters, digits, `.`, `_`, or `-` after the first letter"
            )),
        }
    } else {
        Ok(input.to_string())
    }
}

fn parse_branch_name(input: &str, whole: &str) -> std::result::Result<String, String> {
    crate::git::validate_branch_name(input)
        .map(|()| input.to_string())
        .map_err(|err| format!("explicit selector `{whole}` has invalid branch name: {err:#}"))
}

fn parse_explicit_selector(
    input: &str,
) -> Option<std::result::Result<ExplicitGroupSelector, String>> {
    if let Some(label) = strip_ascii_prefix(input, "pr:") {
        Some(parse_pr_label(label, input).map(ExplicitGroupSelector::PrLabel))
    } else {
        strip_ascii_prefix(input, "branch:").map(|branch_name| {
            parse_branch_name(branch_name, input).map(ExplicitGroupSelector::BranchName)
        })
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
        if is_digits_only(trimmed) {
            parse_local_pr(trimmed).map(Self::LocalPr)
        } else if let Some(selector) = parse_explicit_selector(trimmed) {
            selector.map(Self::Explicit)
        } else {
            Ok(Self::Bare(trimmed.to_string()))
        }
    }
}

impl FromStr for InclusiveSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        if is_digits_only(trimmed) {
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                format!("`{trimmed}` must be 0, a local PR number, or a group selector")
            })?;
            if parsed == 0 {
                Ok(Self::All)
            } else {
                Ok(Self::Group(GroupSelector::LocalPr(parsed)))
            }
        } else {
            trimmed.parse::<GroupSelector>().map(Self::Group)
        }
    }
}

impl FromStr for AfterSelector {
    type Err = String;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = input.trim();
        let lowered = trimmed.to_ascii_lowercase();
        if is_digits_only(trimmed) {
            let parsed = trimmed.parse::<usize>().map_err(|_| {
                format!(
                    "`{trimmed}` must be 0, a local PR number, `bottom`, `top`, `last`, `all`, or a group selector"
                )
            })?;
            if parsed == 0 {
                Ok(Self::Bottom)
            } else {
                Ok(Self::Group(GroupSelector::LocalPr(parsed)))
            }
        } else if lowered == "bottom" {
            Ok(Self::Bottom)
        } else if lowered == "top" || lowered == "last" || lowered == "all" {
            Ok(Self::Top)
        } else {
            trimmed.parse::<GroupSelector>().map(Self::Group)
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

fn matching_bare_groups<'a>(groups: &'a [Group], bare: &str) -> Vec<(usize, &'a Group)> {
    groups
        .iter()
        .enumerate()
        .filter(|(_, group)| group.bare_selector_text() == bare)
        .collect()
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
        GroupSelector::Explicit(explicit) => {
            let marker = explicit.marker();
            groups
                .iter()
                .position(|group| group.marker == marker)
                .ok_or_else(|| anyhow!("No outstanding PR group matches selector `{explicit}`."))
        }
        GroupSelector::Bare(bare) => match matching_bare_groups(groups, bare).as_slice() {
            [] => Err(anyhow!(
                "No outstanding PR group matches bare selector `{bare}`."
            )),
            [(index, _)] => Ok(*index),
            matches => {
                let explicit = matches
                    .iter()
                    .map(|(_, group)| format!("`{}`", group.selector_text()))
                    .collect::<Vec<_>>()
                    .join(" and ");
                Err(anyhow!(
                    "Bare selector `{bare}` is ambiguous in the current stack: it matches {explicit}. Use an explicit selector."
                ))
            }
        },
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
        AfterSelector, ExplicitGroupSelector, GroupRangeSelector, GroupSelector, InclusiveSelector,
    };
    use crate::group_markers::GroupMarker;
    use crate::parsing::Group;

    fn pr_group(label: &str) -> Group {
        Group {
            marker: GroupMarker::PrLabel(label.to_string()),
            subjects: vec![format!("feat: {label}")],
            commits: vec![format!("{label}1")],
            first_message: Some(format!("feat: {label} pr:{label}")),
            ignored_after: Vec::new(),
        }
    }

    fn branch_group(branch_name: &str) -> Group {
        Group {
            marker: GroupMarker::BranchName(branch_name.to_string()),
            subjects: vec![format!("feat: {branch_name}")],
            commits: vec![format!("{branch_name}1")],
            first_message: Some(format!("feat: {branch_name} branch:{branch_name}")),
            ignored_after: Vec::new(),
        }
    }

    fn pr_groups(labels: &[&str]) -> Vec<Group> {
        labels.iter().map(|label| pr_group(label)).collect()
    }

    #[test]
    fn group_selector_parses_numeric_explicit_and_bare_forms() {
        assert_eq!(
            "2".parse::<GroupSelector>().unwrap(),
            GroupSelector::LocalPr(2)
        );
        assert_eq!(
            "PR:Beta".parse::<GroupSelector>().unwrap(),
            GroupSelector::Explicit(ExplicitGroupSelector::PrLabel("Beta".to_string()))
        );
        assert_eq!(
            "branch:feature/login".parse::<GroupSelector>().unwrap(),
            GroupSelector::Explicit(ExplicitGroupSelector::BranchName(
                "feature/login".to_string()
            ))
        );
        assert_eq!(
            "beta".parse::<GroupSelector>().unwrap(),
            GroupSelector::Bare("beta".to_string())
        );
    }

    #[test]
    fn inclusive_selector_parses_zero_as_all() {
        assert_eq!(
            "0".parse::<InclusiveSelector>().unwrap(),
            InclusiveSelector::All
        );
        assert_eq!(
            "beta".parse::<InclusiveSelector>().unwrap(),
            InclusiveSelector::Group(GroupSelector::Bare("beta".to_string()))
        );
    }

    #[test]
    fn after_selector_parses_keywords_and_explicit_selector() {
        assert_eq!(
            "bottom".parse::<AfterSelector>().unwrap(),
            AfterSelector::Bottom
        );
        assert_eq!("last".parse::<AfterSelector>().unwrap(), AfterSelector::Top);
        assert_eq!(
            "pr:beta".parse::<AfterSelector>().unwrap(),
            AfterSelector::Group(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
                "beta".to_string()
            )))
        );
    }

    #[test]
    fn group_range_selector_parses_single_and_range_forms() {
        assert_eq!(
            "beta..3".parse::<GroupRangeSelector>().unwrap(),
            GroupRangeSelector::Inclusive {
                start: GroupSelector::Bare("beta".to_string()),
                end: GroupSelector::LocalPr(3)
            }
        );
        assert_eq!(
            "2".parse::<GroupRangeSelector>().unwrap(),
            GroupRangeSelector::Single(GroupSelector::LocalPr(2))
        );
    }

    #[test]
    fn malformed_explicit_selectors_are_rejected() {
        assert!("pr:1beta".parse::<GroupSelector>().is_err());
        assert!("branch:bad..name".parse::<GroupSelector>().is_err());
    }

    #[test]
    fn prefixed_keyword_can_still_target_an_explicit_selector() {
        assert_eq!(
            "pr:all".parse::<AfterSelector>().unwrap(),
            AfterSelector::Group(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
                "all".to_string()
            )))
        );
    }

    #[test]
    fn digits_only_selectors_remain_local_pr_ordinals() {
        assert_eq!(
            "2".parse::<InclusiveSelector>().unwrap(),
            InclusiveSelector::Group(GroupSelector::LocalPr(2))
        );
        assert_eq!(
            "2".parse::<AfterSelector>().unwrap(),
            AfterSelector::Group(GroupSelector::LocalPr(2))
        );
    }

    #[test]
    fn bare_selector_resolution_survives_local_pr_renumbering() {
        let before = pr_groups(&["alpha", "beta", "gamma"]);
        let after = pr_groups(&["beta", "gamma"]);
        let selector = GroupSelector::Bare("beta".to_string());

        let before_index = resolve_group_index(&before, &selector).unwrap();
        let after_index = resolve_group_index(&after, &selector).unwrap();

        assert_eq!(before[before_index].bare_selector_text(), "beta");
        assert_eq!(after[after_index].bare_selector_text(), "beta");
        assert_eq!(before_index + 1, 2);
        assert_eq!(after_index + 1, 1);
    }

    #[test]
    fn bare_selector_rejects_ambiguity_between_marker_kinds() {
        let groups = vec![pr_group("beta"), branch_group("beta")];
        let selector = GroupSelector::Bare("beta".to_string());

        let err = resolve_group_index(&groups, &selector).unwrap_err();

        assert!(err
            .to_string()
            .contains("Bare selector `beta` is ambiguous"));
        assert!(err.to_string().contains("`pr:beta` and `branch:beta`"));
    }

    #[test]
    fn explicit_selector_resolution_remains_exact_case() {
        let groups = pr_groups(&["Alpha"]);
        let selector = GroupSelector::Explicit(ExplicitGroupSelector::PrLabel("alpha".to_string()));

        let err = resolve_group_index(&groups, &selector).unwrap_err();

        assert!(
            err.to_string()
                .contains("No outstanding PR group matches selector `pr:alpha`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolution_helpers_map_boundaries_to_counts() {
        let groups = pr_groups(&["alpha", "beta", "gamma"]);

        assert_eq!(
            resolve_inclusive_count(
                &groups,
                &InclusiveSelector::Group(GroupSelector::Bare("beta".to_string()))
            )
            .unwrap(),
            2
        );
        assert_eq!(
            resolve_after_count(
                &groups,
                &AfterSelector::Group(GroupSelector::Bare("beta".to_string()))
            )
            .unwrap(),
            2
        );
        assert_eq!(
            resolve_group_range(
                &groups,
                &GroupRangeSelector::Inclusive {
                    start: GroupSelector::Bare("beta".to_string()),
                    end: GroupSelector::LocalPr(3)
                }
            )
            .unwrap(),
            (2, 3)
        );
    }
}
