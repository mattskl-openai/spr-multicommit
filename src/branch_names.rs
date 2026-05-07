//! Shared group branch-name derivation and conflict checks.
//!
//! `pr:<label>` groups derive branch names by concatenating the configured
//! prefix and label, while `branch:<branch-name>` groups use their exact branch
//! name. On case-insensitive filesystems, two exact marker identities can still
//! collide once they become concrete branch names, so conflict decisions use a
//! canonicalized comparison key instead of raw string equality.

use anyhow::Result;
use std::collections::HashMap;

use crate::parsing::Group;

/// Canonical comparison key for concrete branch-name conflicts.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalBranchConflictKey(String);

impl CanonicalBranchConflictKey {
    pub fn new(branch_name: &str) -> Self {
        Self(branch_name.to_ascii_lowercase())
    }
}

/// Exact concrete branch name plus the canonical key used only for comparisons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBranchIdentity {
    pub exact: String,
    pub conflict_key: CanonicalBranchConflictKey,
}

/// One live PR group whose concrete branch name participates in a collision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBranchCollisionEntry {
    pub selector: String,
    pub head_branch: String,
}

/// Two live PR groups whose concrete branch names collide under case-folding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBranchNameCollision {
    pub first: GroupBranchCollisionEntry,
    pub second: GroupBranchCollisionEntry,
}

impl GroupBranchIdentity {
    pub fn new(branch_name: String) -> Self {
        let conflict_key = CanonicalBranchConflictKey::new(&branch_name);
        Self {
            exact: branch_name,
            conflict_key,
        }
    }
}

pub fn canonical_branch_conflict_key(branch_name: &str) -> CanonicalBranchConflictKey {
    CanonicalBranchConflictKey::new(branch_name)
}

pub fn group_branch_name(prefix: &str, group: &Group) -> String {
    group.concrete_branch_name(prefix)
}

impl std::fmt::Display for GroupBranchNameCollision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Refusing to operate on this stack because {} and {} derive conflicting branch names (`{}` and `{}`) under case-insensitive comparison. Group selectors remain exact-case, but these branch names are not safe to treat as distinct on a case-insensitive filesystem.",
            self.first.selector,
            self.second.selector,
            self.first.head_branch,
            self.second.head_branch
        )
    }
}

impl std::error::Error for GroupBranchNameCollision {}

/// Return the first case-folding concrete-branch collision in `groups`, if any.
pub fn find_group_branch_name_collision(
    groups: &[Group],
    prefix: &str,
) -> Option<GroupBranchNameCollision> {
    let mut seen: HashMap<CanonicalBranchConflictKey, GroupBranchCollisionEntry> = HashMap::new();
    for group in groups {
        let head_branch = group_branch_name(prefix, group);
        let selector = group.selector_text();
        let conflict_key = canonical_branch_conflict_key(&head_branch);
        let entry = GroupBranchCollisionEntry {
            selector,
            head_branch,
        };
        if let Some(previous) = seen.get(&conflict_key) {
            return Some(GroupBranchNameCollision {
                first: previous.clone(),
                second: entry,
            });
        } else {
            seen.insert(conflict_key, entry);
        }
    }
    None
}

/// Derive concrete branch identities for `groups` and reject canonical collisions.
pub fn group_branch_identities(groups: &[Group], prefix: &str) -> Result<Vec<GroupBranchIdentity>> {
    if let Some(collision) = find_group_branch_name_collision(groups, prefix) {
        Err(collision.into())
    } else {
        Ok(groups
            .iter()
            .map(|group| GroupBranchIdentity::new(group_branch_name(prefix, group)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_branch_conflict_key, find_group_branch_name_collision, group_branch_identities,
        GroupBranchIdentity,
    };
    use crate::group_markers::GroupMarker;
    use crate::parsing::Group;

    fn pr_group(label: &str) -> Group {
        Group {
            marker: GroupMarker::PrLabel(label.to_string()),
            subjects: vec![format!("feat: {label}")],
            commits: vec![format!("{label}1")],
            first_message: Some(format!("feat: {label}\n\npr:{label}")),
            ignored_after: Vec::new(),
        }
    }

    fn branch_group(branch_name: &str) -> Group {
        Group {
            marker: GroupMarker::BranchName(branch_name.to_string()),
            subjects: vec![format!("feat: {branch_name}")],
            commits: vec![format!("{branch_name}1")],
            first_message: Some(format!("feat: {branch_name}\n\nbranch:{branch_name}")),
            ignored_after: Vec::new(),
        }
    }

    #[test]
    fn branch_identity_preserves_exact_branch_name() {
        let identity = GroupBranchIdentity::new(
            GroupMarker::PrLabel("Alpha".to_string()).concrete_branch_name("dank-spr/"),
        );

        assert_eq!(identity.exact, "dank-spr/Alpha");
    }

    #[test]
    fn branch_identity_uses_exact_explicit_branch_name() {
        let identity = GroupBranchIdentity::new(
            GroupMarker::BranchName("feature/login".to_string()).concrete_branch_name("dank-spr/"),
        );

        assert_eq!(identity.exact, "feature/login");
    }

    #[test]
    fn canonical_key_collides_for_case_only_difference() {
        let lower = canonical_branch_conflict_key("dank-spr/alpha");
        let upper = canonical_branch_conflict_key("dank-spr/Alpha");

        assert_eq!(lower, upper);
    }

    #[test]
    fn canonical_key_distinguishes_non_colliding_branch_names() {
        let alpha = canonical_branch_conflict_key("dank-spr/alpha");
        let beta = canonical_branch_conflict_key("dank-spr/beta");

        assert_ne!(alpha, beta);
    }

    #[test]
    fn group_branch_identities_reject_case_only_collision() {
        let err = group_branch_identities(&[pr_group("alpha"), pr_group("Alpha")], "dank-spr/")
            .unwrap_err();

        assert!(err.to_string().contains("pr:alpha and pr:Alpha"));
        assert!(err.to_string().contains("dank-spr/alpha"));
        assert!(err.to_string().contains("dank-spr/Alpha"));
    }

    #[test]
    fn group_branch_identities_reject_explicit_collision() {
        let err = group_branch_identities(
            &[pr_group("beta"), branch_group("dank-spr/beta")],
            "dank-spr/",
        )
        .unwrap_err();

        assert!(err.to_string().contains("pr:beta and branch:dank-spr/beta"));
    }

    #[test]
    fn find_group_branch_name_collision_returns_typed_entries() {
        let collision =
            find_group_branch_name_collision(&[pr_group("alpha"), pr_group("Alpha")], "dank-spr/")
                .unwrap();

        assert_eq!(collision.first.selector, "pr:alpha");
        assert_eq!(collision.first.head_branch, "dank-spr/alpha");
        assert_eq!(collision.second.selector, "pr:Alpha");
        assert_eq!(collision.second.head_branch, "dank-spr/Alpha");
    }
}
