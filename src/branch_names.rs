//! Shared synthetic branch-name derivation and conflict checks.
//!
//! `spr` stores exact-case `pr:<tag>` handles in commit history, but most
//! commands also derive synthetic branch names such as `dank-spr/alpha` by
//! concatenating the configured prefix and the stored tag. On
//! case-insensitive filesystems, two exact handles can still collide once they
//! become branch names, so branch-conflict decisions must use a canonicalized
//! comparison key instead of raw string equality.

use anyhow::{anyhow, Result};
use std::collections::HashMap;

use crate::parsing::Group;

/// Canonical comparison key for synthetic branch-name conflicts.
///
/// The stored `pr:<tag>` grammar is ASCII-only, so ASCII lowercasing is enough
/// to catch the case-only collisions this rollout targets without changing the
/// user-visible exact tag spelling.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalBranchConflictKey(String);

impl CanonicalBranchConflictKey {
    pub fn new(branch_name: &str) -> Self {
        Self(branch_name.to_ascii_lowercase())
    }
}

/// Exact derived branch name plus the canonical key used only for comparisons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticBranchIdentity {
    pub exact: String,
    pub conflict_key: CanonicalBranchConflictKey,
}

impl SyntheticBranchIdentity {
    pub fn new(prefix: &str, tag: &str) -> Self {
        let exact = format!("{prefix}{tag}");
        let conflict_key = CanonicalBranchConflictKey::new(&exact);
        Self {
            exact,
            conflict_key,
        }
    }
}

pub fn canonical_branch_conflict_key(branch_name: &str) -> CanonicalBranchConflictKey {
    CanonicalBranchConflictKey::new(branch_name)
}

pub fn synthetic_branch_identity(prefix: &str, tag: &str) -> SyntheticBranchIdentity {
    SyntheticBranchIdentity::new(prefix, tag)
}

pub fn synthetic_branch_name(prefix: &str, tag: &str) -> String {
    synthetic_branch_identity(prefix, tag).exact
}

/// Derive synthetic branch identities for `groups` and reject canonical collisions.
///
/// The returned vector preserves the input order. On collision, the error names
/// both exact tags and both exact branch spellings so the user can fix history
/// without guessing which pair caused the problem.
pub fn group_branch_identities(
    groups: &[Group],
    prefix: &str,
) -> Result<Vec<SyntheticBranchIdentity>> {
    let mut identities = Vec::with_capacity(groups.len());
    let mut seen: HashMap<CanonicalBranchConflictKey, (String, String)> = HashMap::new();
    for group in groups {
        let identity = synthetic_branch_identity(prefix, &group.tag);
        if let Some((prior_tag, prior_branch)) = seen.get(&identity.conflict_key) {
            return Err(anyhow!(
                "Refusing to operate on this stack because pr:{} and pr:{} derive conflicting synthetic branch names (`{}` and `{}`) under case-insensitive comparison. Stable handles remain exact-case, but these branch names are not safe to treat as distinct on a case-insensitive filesystem.",
                prior_tag,
                group.tag,
                prior_branch,
                identity.exact
            ));
        } else {
            seen.insert(
                identity.conflict_key.clone(),
                (group.tag.clone(), identity.exact.clone()),
            );
            identities.push(identity);
        }
    }
    Ok(identities)
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_branch_conflict_key, group_branch_identities, synthetic_branch_identity,
    };
    use crate::parsing::Group;

    fn group(tag: &str) -> Group {
        Group {
            tag: tag.to_string(),
            subjects: vec![format!("feat: {tag}")],
            commits: vec![format!("{tag}1")],
            first_message: Some(format!("feat: {tag}\n\npr:{tag}")),
            ignored_after: Vec::new(),
        }
    }

    #[test]
    fn synthetic_branch_identity_preserves_exact_branch_name() {
        let identity = synthetic_branch_identity("dank-spr/", "Alpha");

        assert_eq!(identity.exact, "dank-spr/Alpha");
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
    fn group_branch_identities_keep_prefix_in_collision_domain() {
        let alpha = canonical_branch_conflict_key("dank-spr/alpha");
        let other = canonical_branch_conflict_key("other-spr/alpha");

        assert_ne!(alpha, other);
    }

    #[test]
    fn group_branch_identities_reject_case_only_collision() {
        let err =
            group_branch_identities(&[group("alpha"), group("Alpha")], "dank-spr/").unwrap_err();

        assert!(err.to_string().contains("pr:alpha and pr:Alpha"));
        assert!(err.to_string().contains("dank-spr/alpha"));
        assert!(err.to_string().contains("dank-spr/Alpha"));
    }
}
