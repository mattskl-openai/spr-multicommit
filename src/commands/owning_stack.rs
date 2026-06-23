//! Shared owning-stack resolution for commands invoked from stack prefixes.

use anyhow::{anyhow, bail, Result};
use std::collections::{BTreeMap, BTreeSet};

use crate::commands::{common, rewrite_resume};
use crate::git::{git_ref_exists_at, git_rev_parse};
use crate::parsing::{derive_groups_between_with_ignored, Group};
use crate::stack_metadata::{
    load_metadata_for_repo_path, verify_stack_branch_for_stack_id, PrBranchRecord,
    RefreshMetadataContext, StackId, StackMetadataFile,
};

#[derive(Debug, Clone)]
pub struct ResolvedOwningStack {
    #[allow(dead_code)]
    pub metadata: StackMetadataFile,
    #[allow(dead_code)]
    pub stack_id: StackId,
    pub stack_branch: String,
    pub stack_head: String,
}

pub fn selector_sequence(groups: &[Group]) -> Vec<String> {
    groups.iter().map(Group::selector_text).collect()
}

pub fn load_recorded_owning_stack_for_candidate_groups(
    metadata_context: &RefreshMetadataContext,
    candidate_groups: &[Group],
) -> Result<Option<ResolvedOwningStack>> {
    let repo_path = rewrite_resume::current_repo_root()?;
    let Some(metadata) = load_metadata_for_repo_path(&repo_path)? else {
        return Ok(None);
    };
    let (stack_id, stack_branch) = resolve_owning_stack_target(
        &repo_path,
        &metadata,
        metadata_context,
        &selector_sequence(candidate_groups),
    )?;
    let stack_head = git_rev_parse(&format!("refs/heads/{stack_branch}"))?;
    Ok(Some(ResolvedOwningStack {
        metadata,
        stack_id,
        stack_branch,
        stack_head,
    }))
}

pub fn ensure_branch_not_checked_out(branch: &str) -> Result<()> {
    if let Some(worktree) = common::checked_out_worktree_for_branch(branch)? {
        bail!(
            "owning full-stack branch {} is checked out in worktree {}; refusing to move it by ref",
            branch,
            worktree
        );
    }
    Ok(())
}

fn live_selector_owners(metadata: &StackMetadataFile) -> Result<BTreeMap<String, StackId>> {
    let mut owners = BTreeMap::new();
    for record in metadata.pr_branches.values() {
        if let PrBranchRecord::Live {
            stack_id, selector, ..
        } = record
        {
            if let Some(existing_owner) = owners.insert(selector.0.clone(), stack_id.clone()) {
                if existing_owner != *stack_id {
                    bail!(
                        "live explicit selector `{}` belongs to more than one live stack: {} and {}",
                        selector.0,
                        existing_owner.0,
                        stack_id.0
                    );
                }
            }
        }
    }
    Ok(owners)
}

fn verified_stack_branches(
    repo_path: &str,
    metadata: &StackMetadataFile,
    stack_id: &StackId,
    ignore_tag: &str,
) -> Result<BTreeSet<String>> {
    let stack_record = metadata
        .stacks
        .get(stack_id)
        .ok_or_else(|| anyhow!("recorded stack_id {} is missing stack metadata", stack_id.0))?;
    std::iter::once(&stack_record.preferred_branch)
        .chain(stack_record.known_branches.iter())
        .map(|candidate| -> Result<Option<String>> {
            if verify_stack_branch_for_stack_id(
                repo_path, metadata, candidate, stack_id, ignore_tag,
            )? {
                Ok(Some(candidate.0.clone()))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>>>()
        .map(|branches| branches.into_iter().flatten().collect())
}

fn recorded_stack_branch_aliases(
    metadata: &StackMetadataFile,
    stack_id: &StackId,
) -> Result<BTreeSet<String>> {
    let stack_record = metadata
        .stacks
        .get(stack_id)
        .ok_or_else(|| anyhow!("recorded stack_id {} is missing stack metadata", stack_id.0))?;
    Ok(std::iter::once(&stack_record.preferred_branch)
        .chain(stack_record.known_branches.iter())
        .map(|branch| branch.0.clone())
        .collect())
}

fn ensure_stack_context_matches(
    metadata: &StackMetadataFile,
    stack_id: &StackId,
    metadata_context: &RefreshMetadataContext,
) -> Result<()> {
    let stack_record = metadata
        .stacks
        .get(stack_id)
        .ok_or_else(|| anyhow!("recorded stack_id {} is missing stack metadata", stack_id.0))?;
    if stack_record.base != metadata_context.base || stack_record.prefix != metadata_context.prefix
    {
        bail!(
            "recorded live stack {} uses base `{}` and prefix `{}`, but absorb was invoked with base `{}` and prefix `{}`; refusing to recover stack ownership across contexts",
            stack_id.0,
            stack_record.base,
            stack_record.prefix,
            metadata_context.base,
            metadata_context.prefix
        );
    }
    Ok(())
}

fn matching_verified_stack_ids(
    repo_path: &str,
    metadata: &StackMetadataFile,
    metadata_context: &RefreshMetadataContext,
    candidate_selectors: &[String],
) -> Result<BTreeSet<StackId>> {
    let mut matching_stack_ids = BTreeSet::new();
    for stack_id in metadata.stacks.keys() {
        for branch in
            verified_stack_branches(repo_path, metadata, stack_id, &metadata_context.ignore_tag)?
        {
            let branch_ref = format!("refs/heads/{branch}");
            let (_merge_base, _leading_ignored, groups) = derive_groups_between_with_ignored(
                &metadata_context.base,
                &branch_ref,
                &metadata_context.ignore_tag,
            )?;
            if selector_sequence(&groups).starts_with(candidate_selectors) {
                matching_stack_ids.insert(stack_id.clone());
                break;
            }
        }
    }
    Ok(matching_stack_ids)
}

fn resolve_verified_owning_stack_target(
    repo_path: &str,
    metadata: &StackMetadataFile,
    metadata_context: &RefreshMetadataContext,
    candidate_selectors: &[String],
) -> Result<Option<(StackId, String)>> {
    let matching_stack_ids =
        matching_verified_stack_ids(repo_path, metadata, metadata_context, candidate_selectors)?;

    if matching_stack_ids.is_empty() {
        return Ok(None);
    }
    if matching_stack_ids.len() > 1 {
        bail!(
            "candidate selector prefix [{}] matches more than one verified stack history: {}",
            candidate_selectors.join(", "),
            matching_stack_ids
                .iter()
                .map(|stack_id| stack_id.0.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let stack_id = matching_stack_ids.into_iter().next().unwrap();
    let stack_branch =
        verified_owning_stack_branch(repo_path, metadata, &stack_id, &metadata_context.ignore_tag)?;
    Ok(Some((stack_id, stack_branch)))
}

fn recorded_owning_stack_id(
    owners: &BTreeMap<String, StackId>,
    candidate_selectors: &[String],
) -> Result<Option<StackId>> {
    let mut matching_stack_ids = BTreeSet::new();
    for selector in candidate_selectors {
        let Some(stack_id) = owners.get(selector) else {
            return Ok(None);
        };
        matching_stack_ids.insert(stack_id.clone());
    }

    if matching_stack_ids.len() > 1 {
        bail!(
            "candidate selector prefix [{}] belongs to more than one recorded live stack: {}",
            candidate_selectors.join(", "),
            matching_stack_ids
                .iter()
                .map(|stack_id| stack_id.0.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(matching_stack_ids.into_iter().next())
}

fn resolve_stale_compatible_owning_stack_target(
    repo_path: &str,
    metadata: &StackMetadataFile,
    metadata_context: &RefreshMetadataContext,
    selector_owners: &BTreeMap<String, StackId>,
    candidate_selectors: &[String],
) -> Result<(StackId, String)> {
    let Some(stack_id) = recorded_owning_stack_id(selector_owners, candidate_selectors)? else {
        bail!(
            "candidate selector prefix [{}] does not match any verified stack history",
            candidate_selectors.join(", ")
        );
    };
    ensure_stack_context_matches(metadata, &stack_id, metadata_context)?;

    let mut matching_branches = BTreeSet::new();
    for branch in recorded_stack_branch_aliases(metadata, &stack_id)? {
        let branch_ref = format!("refs/heads/{branch}");
        if !git_ref_exists_at(repo_path, &branch_ref)? {
            continue;
        }
        let (_merge_base, _leading_ignored, groups) = derive_groups_between_with_ignored(
            &metadata_context.base,
            &branch_ref,
            &metadata_context.ignore_tag,
        )?;
        if selector_sequence(&groups).starts_with(candidate_selectors) {
            matching_branches.insert(branch);
        }
    }

    if matching_branches.is_empty() {
        bail!(
            "recorded live stack {} has no stale-compatible full-stack branch for candidate selector prefix [{}]",
            stack_id.0,
            candidate_selectors.join(", ")
        );
    }
    if matching_branches.len() > 1 {
        bail!(
            "recorded live stack {} has multiple stale-compatible full-stack branch aliases for candidate selector prefix [{}]: {}",
            stack_id.0,
            candidate_selectors.join(", "),
            matching_branches.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    Ok((stack_id, matching_branches.into_iter().next().unwrap()))
}

fn resolve_owning_stack_target(
    repo_path: &str,
    metadata: &StackMetadataFile,
    metadata_context: &RefreshMetadataContext,
    candidate_selectors: &[String],
) -> Result<(StackId, String)> {
    if candidate_selectors.is_empty() {
        bail!("candidate history contains no explicit selectors to identify an owning stack");
    }
    let selector_owners = live_selector_owners(metadata)?;
    if let Some(target) = resolve_verified_owning_stack_target(
        repo_path,
        metadata,
        metadata_context,
        candidate_selectors,
    )? {
        return Ok(target);
    }
    resolve_stale_compatible_owning_stack_target(
        repo_path,
        metadata,
        metadata_context,
        &selector_owners,
        candidate_selectors,
    )
}

fn verified_owning_stack_branch(
    repo_path: &str,
    metadata: &StackMetadataFile,
    stack_id: &StackId,
    ignore_tag: &str,
) -> Result<String> {
    let verified_branches = verified_stack_branches(repo_path, metadata, stack_id, ignore_tag)?;
    if verified_branches.len() == 1 {
        Ok(verified_branches.into_iter().next().unwrap())
    } else if verified_branches.is_empty() {
        bail!(
            "recorded live stack {} has no currently verified full-stack branch",
            stack_id.0
        );
    } else {
        bail!(
            "recorded live stack {} has multiple verified full-stack branch aliases: {}",
            stack_id.0,
            verified_branches.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::live_selector_owners;
    use crate::stack_metadata::{
        GroupSelectorText, PrBranchName, PrBranchRecord, StackId, StackMetadataFile,
    };

    fn live_metadata(records: &[(&str, &str, &str)]) -> StackMetadataFile {
        StackMetadataFile {
            schema_version: crate::stack_metadata::STACK_METADATA_SCHEMA_VERSION,
            stacks: BTreeMap::new(),
            pr_branches: records
                .iter()
                .map(|(branch, selector, stack_id)| {
                    (
                        PrBranchName((*branch).to_string()),
                        PrBranchRecord::Live {
                            stack_id: StackId((*stack_id).to_string()),
                            selector: GroupSelectorText((*selector).to_string()),
                            last_group_seed: format!("{branch}-seed"),
                            last_group_tip: format!("{branch}-tip"),
                            last_stack_head: format!("{stack_id}-head"),
                            updated_at: "now".to_string(),
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn live_selector_owners_reject_duplicate_live_ownership() {
        let metadata = live_metadata(&[
            ("alpha-a", "pr:alpha", "stack-a"),
            ("alpha-b", "pr:alpha", "stack-b"),
        ]);

        let err = live_selector_owners(&metadata).unwrap_err();

        assert!(err.to_string().contains("pr:alpha"));
        assert!(err.to_string().contains("more than one live stack"));
    }
}
