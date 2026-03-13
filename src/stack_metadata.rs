//! Repo-local stack ownership metadata shared across worktrees.
//!
//! The metadata file lives under the repository common Git directory so every
//! linked worktree sees the same stack ownership state. Refreshing metadata is
//! intentionally strict: branch names are locators, `stack_id` is identity, and
//! branch ownership is derived only from the current local stack plus existing
//! metadata. The module keeps the persisted JSON schema, file locking, atomic
//! writes, and snapshot refresh logic in one place.

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use uuid::Uuid;

use crate::branch_names::group_branch_identities;
use crate::git::{
    git_common_dir, git_common_dir_at, git_current_branch, git_current_branch_at,
    git_ref_exists_at, git_rev_parse_at, git_ro_in, repo_root,
};
use crate::parsing::{parse_groups_with_ignored, split_groups_for_update};

pub const STACK_METADATA_SCHEMA_VERSION: u32 = 1;
const LOCK_RETRY_ATTEMPTS: usize = 100;
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const STACK_METADATA_FILE_NAME: &str = "stack_metadata_v1.json";
const STACK_METADATA_LOCK_NAME: &str = "stack_metadata_v1.lock";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StackId(pub String);

impl StackId {
    pub fn fresh() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StackBranchName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrBranchName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrTag(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneReason {
    RemovedFromLiveStack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackRecord {
    pub preferred_branch: StackBranchName,
    pub known_branches: Vec<StackBranchName>,
    pub base: String,
    pub prefix: String,
    pub last_seen_head: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PrBranchRecord {
    Live {
        stack_id: StackId,
        tag: PrTag,
        last_group_seed: String,
        last_group_tip: String,
        last_stack_head: String,
        updated_at: String,
    },
    Tombstoned {
        stack_id: StackId,
        tag: PrTag,
        last_group_seed: String,
        last_group_tip: String,
        last_stack_head: String,
        updated_at: String,
        tombstone_reason: TombstoneReason,
    },
}

impl PrBranchRecord {
    pub fn stack_id(&self) -> &StackId {
        match self {
            Self::Live { stack_id, .. } | Self::Tombstoned { stack_id, .. } => stack_id,
        }
    }

    pub fn tag(&self) -> &PrTag {
        match self {
            Self::Live { tag, .. } | Self::Tombstoned { tag, .. } => tag,
        }
    }

    pub fn last_group_seed(&self) -> &str {
        match self {
            Self::Live {
                last_group_seed, ..
            }
            | Self::Tombstoned {
                last_group_seed, ..
            } => last_group_seed,
        }
    }

    pub fn last_group_tip(&self) -> &str {
        match self {
            Self::Live { last_group_tip, .. } | Self::Tombstoned { last_group_tip, .. } => {
                last_group_tip
            }
        }
    }

    pub fn last_stack_head(&self) -> &str {
        match self {
            Self::Live {
                last_stack_head, ..
            }
            | Self::Tombstoned {
                last_stack_head, ..
            } => last_stack_head,
        }
    }

    pub fn as_live(&self) -> Option<LivePrBranchRecord<'_>> {
        match self {
            Self::Live {
                stack_id,
                tag,
                last_group_seed,
                last_group_tip,
                last_stack_head,
                updated_at,
            } => Some(LivePrBranchRecord {
                stack_id,
                tag,
                last_group_seed,
                last_group_tip,
                last_stack_head,
                updated_at,
            }),
            Self::Tombstoned { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePrBranchRecord<'a> {
    pub stack_id: &'a StackId,
    pub tag: &'a PrTag,
    pub last_group_seed: &'a str,
    pub last_group_tip: &'a str,
    pub last_stack_head: &'a str,
    pub updated_at: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackMetadataFile {
    pub schema_version: u32,
    pub stacks: BTreeMap<StackId, StackRecord>,
    pub pr_branches: BTreeMap<PrBranchName, PrBranchRecord>,
}

impl Default for StackMetadataFile {
    fn default() -> Self {
        Self {
            schema_version: STACK_METADATA_SCHEMA_VERSION,
            stacks: BTreeMap::new(),
            pr_branches: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackSnapshot {
    pub stack_branch: StackBranchName,
    pub stack_head: String,
    pub base: String,
    pub prefix: String,
    pub groups: Vec<StackSnapshotGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackSnapshotGroup {
    pub pr_branch: PrBranchName,
    pub tag: PrTag,
    pub last_group_seed: String,
    pub last_group_tip: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshMetadataContext {
    pub base: String,
    pub prefix: String,
    pub ignore_tag: String,
}

fn metadata_dir(git_common_dir: &Path) -> PathBuf {
    git_common_dir.join("spr")
}

pub fn metadata_path(git_common_dir: &Path) -> PathBuf {
    metadata_dir(git_common_dir).join(STACK_METADATA_FILE_NAME)
}

fn metadata_lock_path(git_common_dir: &Path) -> PathBuf {
    metadata_dir(git_common_dir).join(STACK_METADATA_LOCK_NAME)
}

fn now_rfc3339() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .context("failed to format RFC3339 timestamp")
}

fn load_metadata_from_path(path: &Path) -> Result<Option<StackMetadataFile>> {
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read stack metadata {}", path.display()))?;
    let metadata: StackMetadataFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse stack metadata {}", path.display()))?;
    if metadata.schema_version != STACK_METADATA_SCHEMA_VERSION {
        bail!(
            "stack metadata {} uses schema version {}, but this spr expects {}",
            path.display(),
            metadata.schema_version,
            STACK_METADATA_SCHEMA_VERSION
        );
    }
    Ok(Some(metadata))
}

pub fn load_metadata_for_repo_path(repo_path: &str) -> Result<Option<StackMetadataFile>> {
    let git_common_dir = git_common_dir_at(repo_path)?;
    load_metadata_from_path(&metadata_path(&git_common_dir))
}

fn ordered_known_branches(
    preferred_branch: &StackBranchName,
    known_branches: impl IntoIterator<Item = StackBranchName>,
) -> Vec<StackBranchName> {
    let mut ordered = vec![preferred_branch.clone()];
    let remainder = known_branches
        .into_iter()
        .filter(|branch| branch != preferred_branch)
        .collect::<BTreeSet<_>>();
    ordered.extend(remainder);
    ordered
}

fn resolve_stack_id_for_empty_snapshot(
    metadata: &StackMetadataFile,
    stack_branch: &StackBranchName,
) -> Result<StackId> {
    let matches = metadata
        .stacks
        .iter()
        .filter(|(_, record)| {
            record.preferred_branch == *stack_branch || record.known_branches.contains(stack_branch)
        })
        .map(|(stack_id, _)| stack_id.clone())
        .collect::<BTreeSet<_>>();
    if matches.len() > 1 {
        bail!(
            "empty stack snapshot for branch {} matches multiple recorded stack_ids",
            stack_branch.0
        );
    } else if let Some(existing_stack_id) = matches.into_iter().next() {
        Ok(existing_stack_id)
    } else {
        Ok(StackId::fresh())
    }
}

fn resolve_stack_id_for_snapshot(
    metadata: &StackMetadataFile,
    snapshot: &StackSnapshot,
) -> Result<StackId> {
    if snapshot.groups.is_empty() {
        return resolve_stack_id_for_empty_snapshot(metadata, &snapshot.stack_branch);
    }

    let matching_stack_ids = snapshot
        .groups
        .iter()
        .filter_map(|group| metadata.pr_branches.get(&group.pr_branch))
        .filter_map(PrBranchRecord::as_live)
        .map(|record| record.stack_id.clone())
        .collect::<BTreeSet<_>>();
    if matching_stack_ids.len() > 1 {
        bail!(
            "live PR branch records for stack {} disagree on stack ownership",
            snapshot.stack_branch.0
        );
    } else if let Some(existing_stack_id) = matching_stack_ids.into_iter().next() {
        Ok(existing_stack_id)
    } else {
        Ok(StackId::fresh())
    }
}

fn tombstone_record(record: &PrBranchRecord, updated_at: &str) -> PrBranchRecord {
    PrBranchRecord::Tombstoned {
        stack_id: record.stack_id().clone(),
        tag: record.tag().clone(),
        last_group_seed: record.last_group_seed().to_string(),
        last_group_tip: record.last_group_tip().to_string(),
        last_stack_head: record.last_stack_head().to_string(),
        updated_at: updated_at.to_string(),
        tombstone_reason: TombstoneReason::RemovedFromLiveStack,
    }
}

fn apply_snapshot(
    mut metadata: StackMetadataFile,
    snapshot: &StackSnapshot,
    updated_at: &str,
) -> Result<StackMetadataFile> {
    let stack_id = resolve_stack_id_for_snapshot(&metadata, snapshot)?;
    let existing_stack = metadata.stacks.get(&stack_id).cloned();
    let mut known_branches = existing_stack
        .as_ref()
        .map(|stack| {
            stack
                .known_branches
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if let Some(existing_stack) = &existing_stack {
        known_branches.insert(existing_stack.preferred_branch.clone());
    }
    known_branches.insert(snapshot.stack_branch.clone());

    metadata.stacks.insert(
        stack_id.clone(),
        StackRecord {
            preferred_branch: snapshot.stack_branch.clone(),
            known_branches: ordered_known_branches(&snapshot.stack_branch, known_branches),
            base: snapshot.base.clone(),
            prefix: snapshot.prefix.clone(),
            last_seen_head: snapshot.stack_head.clone(),
            updated_at: updated_at.to_string(),
        },
    );

    let live_pr_branches = snapshot
        .groups
        .iter()
        .map(|group| group.pr_branch.clone())
        .collect::<BTreeSet<_>>();

    let tombstoned_names = metadata
        .pr_branches
        .iter()
        .filter_map(|(branch_name, record)| match record {
            PrBranchRecord::Live {
                stack_id: owner, ..
            } if *owner == stack_id && !live_pr_branches.contains(branch_name) => {
                Some(branch_name.clone())
            }
            PrBranchRecord::Live { .. } | PrBranchRecord::Tombstoned { .. } => None,
        })
        .collect::<Vec<_>>();
    for branch_name in tombstoned_names {
        if let Some(record) = metadata.pr_branches.get(&branch_name).cloned() {
            metadata
                .pr_branches
                .insert(branch_name, tombstone_record(&record, updated_at));
        }
    }

    for group in &snapshot.groups {
        metadata.pr_branches.insert(
            group.pr_branch.clone(),
            PrBranchRecord::Live {
                stack_id: stack_id.clone(),
                tag: group.tag.clone(),
                last_group_seed: group.last_group_seed.clone(),
                last_group_tip: group.last_group_tip.clone(),
                last_stack_head: snapshot.stack_head.clone(),
                updated_at: updated_at.to_string(),
            },
        );
    }

    Ok(metadata)
}

struct MetadataLock {
    _file: File,
    path: PathBuf,
}

impl Drop for MetadataLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_lock(lock_path: &Path) -> Result<MetadataLock> {
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create metadata directory {}", parent.display()))?;
    }

    for attempt in 0..LOCK_RETRY_ATTEMPTS {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)
        {
            Ok(mut file) => {
                writeln!(file, "pid={}", std::process::id()).with_context(|| {
                    format!("failed to write lock file {}", lock_path.display())
                })?;
                return Ok(MetadataLock {
                    _file: file,
                    path: lock_path.to_path_buf(),
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if attempt + 1 == LOCK_RETRY_ATTEMPTS {
                    break;
                }
                thread::sleep(LOCK_RETRY_INTERVAL);
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to acquire metadata lock {}", lock_path.display())
                });
            }
        }
    }

    bail!(
        "timed out acquiring stack metadata lock {}",
        lock_path.display()
    );
}

fn write_metadata_atomically(path: &Path, metadata: &StackMetadataFile) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "stack metadata path {} has no parent directory",
            path.display()
        )
    })?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create metadata directory {}", parent.display()))?;

    let json = serde_json::to_string_pretty(metadata).context("failed to encode stack metadata")?;
    let temp_path = parent.join(format!(
        ".{}.{}.tmp",
        STACK_METADATA_FILE_NAME,
        std::process::id()
    ));
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temp_path).with_context(|| {
            format!(
                "failed to create temp metadata file {}",
                temp_path.display()
            )
        })?;
        file.write_all(json.as_bytes()).with_context(|| {
            format!("failed to write temp metadata file {}", temp_path.display())
        })?;
        file.sync_all().with_context(|| {
            format!("failed to sync temp metadata file {}", temp_path.display())
        })?;
        fs::rename(&temp_path, path).with_context(|| {
            format!(
                "failed to rename temp metadata file {} to {}",
                temp_path.display(),
                path.display()
            )
        })?;
        Ok(())
    })();
    if write_result.is_err() && temp_path.exists() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

pub fn build_snapshot_for_branch(
    repo_path: &str,
    stack_branch: &str,
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> Result<StackSnapshot> {
    let branch_reference = format!("refs/heads/{stack_branch}");
    let stack_head = git_rev_parse_at(repo_path, &branch_reference)?;
    let merge_base = git_ro_in(
        repo_path,
        ["merge-base", base, &branch_reference].as_slice(),
    )?
    .trim()
    .to_string();
    let range = format!("{merge_base}..{branch_reference}");
    let lines = git_ro_in(
        repo_path,
        [
            "log",
            "--first-parent",
            "--format=%H%x00%B%x1e",
            "--reverse",
            &range,
        ]
        .as_slice(),
    )?;
    let (leading_ignored, parsed_groups) = parse_groups_with_ignored(&lines, ignore_tag)?;
    let (groups, _skipped_handles) = split_groups_for_update(&leading_ignored, parsed_groups);
    let branch_identities = group_branch_identities(&groups, prefix)?;
    let groups = groups
        .iter()
        .zip(branch_identities.iter())
        .map(|(group, identity)| {
            let last_group_seed = group
                .commits
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("group pr:{} has no seed commit", group.tag))?;
            let last_group_tip = group
                .commits
                .last()
                .cloned()
                .ok_or_else(|| anyhow!("group pr:{} has no tip commit", group.tag))?;
            Ok(StackSnapshotGroup {
                pr_branch: PrBranchName(identity.exact.clone()),
                tag: PrTag(group.tag.clone()),
                last_group_seed,
                last_group_tip,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StackSnapshot {
        stack_branch: StackBranchName(stack_branch.to_string()),
        stack_head,
        base: base.to_string(),
        prefix: prefix.to_string(),
        groups,
    })
}

pub fn build_snapshot_for_current_checkout(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> Result<StackSnapshot> {
    let repo_path = repo_root()?.ok_or_else(|| anyhow!("`spr` must run inside a git worktree"))?;
    let stack_branch = git_current_branch()?;
    if stack_branch == "HEAD" {
        bail!("cannot refresh stack metadata from a detached HEAD checkout");
    }
    build_snapshot_for_branch(&repo_path, &stack_branch, base, prefix, ignore_tag)
}

pub fn refresh_metadata_for_current_checkout(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
) -> Result<()> {
    let snapshot = build_snapshot_for_current_checkout(base, prefix, ignore_tag)?;
    let git_common_dir = git_common_dir()?;
    let path = metadata_path(&git_common_dir);
    let lock_path = metadata_lock_path(&git_common_dir);
    let _lock = acquire_lock(&lock_path)?;
    let existing = load_metadata_from_path(&path)?.unwrap_or_default();
    let updated_at = now_rfc3339()?;
    let metadata = apply_snapshot(existing, &snapshot, &updated_at)?;
    write_metadata_atomically(&path, &metadata)
}

pub fn refresh_metadata_for_branch(
    repo_path: &str,
    stack_branch: &str,
    context: &RefreshMetadataContext,
    git_common_dir_override: Option<&Path>,
) -> Result<()> {
    let snapshot = build_snapshot_for_branch(
        repo_path,
        stack_branch,
        &context.base,
        &context.prefix,
        &context.ignore_tag,
    )?;
    let git_common_dir = if let Some(git_common_dir_override) = git_common_dir_override {
        git_common_dir_override.to_path_buf()
    } else {
        git_common_dir_at(repo_path)?
    };
    let path = metadata_path(&git_common_dir);
    let lock_path = metadata_lock_path(&git_common_dir);
    let _lock = acquire_lock(&lock_path)?;
    let existing = load_metadata_from_path(&path)?.unwrap_or_default();
    let updated_at = now_rfc3339()?;
    let metadata = apply_snapshot(existing, &snapshot, &updated_at)?;
    write_metadata_atomically(&path, &metadata)
}

pub fn stack_ids_for_branch(metadata: &StackMetadataFile, stack_branch: &str) -> Vec<StackId> {
    metadata
        .stacks
        .iter()
        .filter(|(_, record)| {
            record.preferred_branch.0 == stack_branch
                || record
                    .known_branches
                    .iter()
                    .any(|candidate| candidate.0 == stack_branch)
        })
        .map(|(stack_id, _)| stack_id.clone())
        .collect()
}

pub fn verify_stack_branch_for_pr_record(
    repo_path: &str,
    candidate_branch: &StackBranchName,
    stack_record: &StackRecord,
    pr_branch_name: &PrBranchName,
    pr_record: LivePrBranchRecord<'_>,
    ignore_tag: &str,
) -> Result<bool> {
    let branch_reference = format!("refs/heads/{}", candidate_branch.0);
    if !git_ref_exists_at(repo_path, &branch_reference)? {
        return Ok(false);
    }

    let snapshot = build_snapshot_for_branch(
        repo_path,
        &candidate_branch.0,
        &stack_record.base,
        &stack_record.prefix,
        ignore_tag,
    )?;
    let matching_groups = snapshot
        .groups
        .iter()
        .filter(|group| {
            group.pr_branch == *pr_branch_name
                && group.last_group_seed == pr_record.last_group_seed
                && group.last_group_tip == pr_record.last_group_tip
        })
        .count();
    Ok(matching_groups == 1)
}

pub fn verify_stack_branch_for_stack_id(
    repo_path: &str,
    metadata: &StackMetadataFile,
    candidate_branch: &StackBranchName,
    stack_id: &StackId,
    ignore_tag: &str,
) -> Result<bool> {
    let Some(stack_record) = metadata.stacks.get(stack_id) else {
        return Ok(false);
    };
    let branch_reference = format!("refs/heads/{}", candidate_branch.0);
    if !git_ref_exists_at(repo_path, &branch_reference)? {
        return Ok(false);
    }

    let snapshot = build_snapshot_for_branch(
        repo_path,
        &candidate_branch.0,
        &stack_record.base,
        &stack_record.prefix,
        ignore_tag,
    )?;
    let recorded_live_groups = metadata
        .pr_branches
        .iter()
        .filter_map(|(branch_name, record)| {
            record.as_live().and_then(|live_record| {
                if live_record.stack_id == stack_id {
                    Some((branch_name, live_record))
                } else {
                    None
                }
            })
        })
        .collect::<BTreeMap<_, _>>();
    if snapshot.groups.len() != recorded_live_groups.len() {
        return Ok(false);
    }

    Ok(snapshot.groups.iter().all(|group| {
        recorded_live_groups
            .get(&group.pr_branch)
            .map(|record| {
                record.last_group_seed == group.last_group_seed
                    && record.last_group_tip == group.last_group_tip
            })
            .unwrap_or(false)
    }))
}

pub fn current_branch_or_none(repo_path: &str) -> Result<Option<String>> {
    let branch = git_current_branch_at(repo_path)?;
    if branch == "HEAD" {
        Ok(None)
    } else {
        Ok(Some(branch))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        acquire_lock, apply_snapshot, load_metadata_from_path, metadata_lock_path,
        ordered_known_branches, write_metadata_atomically, PrBranchName, PrBranchRecord, PrTag,
        StackBranchName, StackId, StackMetadataFile, StackRecord, StackSnapshot,
        StackSnapshotGroup, TombstoneReason, STACK_METADATA_SCHEMA_VERSION,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;

    fn sample_snapshot(branch: &str, groups: &[(&str, &str, &str)]) -> StackSnapshot {
        StackSnapshot {
            stack_branch: StackBranchName(branch.to_string()),
            stack_head: "head123".to_string(),
            base: "origin/main".to_string(),
            prefix: "dank-spr/".to_string(),
            groups: groups
                .iter()
                .map(|(branch_name, seed, tip)| StackSnapshotGroup {
                    pr_branch: PrBranchName((*branch_name).to_string()),
                    tag: PrTag(branch_name.rsplit('/').next().unwrap().to_string()),
                    last_group_seed: (*seed).to_string(),
                    last_group_tip: (*tip).to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn ordered_known_branches_keeps_preferred_first() {
        let preferred = StackBranchName("dank/stack".to_string());
        let known = ordered_known_branches(
            &preferred,
            [
                StackBranchName("tmp/old".to_string()),
                preferred.clone(),
                StackBranchName("dank/stack-2".to_string()),
            ],
        );

        assert_eq!(known[0], preferred);
        assert_eq!(known.len(), 3);
    }

    #[test]
    fn apply_snapshot_promotes_branch_rename_and_tombstones_removed_prs() {
        let existing = StackMetadataFile {
            schema_version: STACK_METADATA_SCHEMA_VERSION,
            stacks: BTreeMap::from([(
                StackId("stack-1".to_string()),
                StackRecord {
                    preferred_branch: StackBranchName("dank/old-stack".to_string()),
                    known_branches: vec![StackBranchName("dank/old-stack".to_string())],
                    base: "origin/main".to_string(),
                    prefix: "dank-spr/".to_string(),
                    last_seen_head: "old-head".to_string(),
                    updated_at: "2026-03-05T00:00:00Z".to_string(),
                },
            )]),
            pr_branches: BTreeMap::from([
                (
                    PrBranchName("dank-spr/alpha".to_string()),
                    PrBranchRecord::Live {
                        stack_id: StackId("stack-1".to_string()),
                        tag: PrTag("alpha".to_string()),
                        last_group_seed: "a1".to_string(),
                        last_group_tip: "a2".to_string(),
                        last_stack_head: "old-head".to_string(),
                        updated_at: "2026-03-05T00:00:00Z".to_string(),
                    },
                ),
                (
                    PrBranchName("dank-spr/beta".to_string()),
                    PrBranchRecord::Live {
                        stack_id: StackId("stack-1".to_string()),
                        tag: PrTag("beta".to_string()),
                        last_group_seed: "b1".to_string(),
                        last_group_tip: "b2".to_string(),
                        last_stack_head: "old-head".to_string(),
                        updated_at: "2026-03-05T00:00:00Z".to_string(),
                    },
                ),
            ]),
        };
        let snapshot = sample_snapshot("dank/new-stack", &[("dank-spr/alpha", "a1", "a2")]);

        let updated = apply_snapshot(existing, &snapshot, "2026-03-05T01:00:00Z").unwrap();
        let stack = updated.stacks.get(&StackId("stack-1".to_string())).unwrap();
        assert_eq!(stack.preferred_branch.0, "dank/new-stack");
        assert!(stack
            .known_branches
            .contains(&StackBranchName("dank/old-stack".to_string())));

        match updated
            .pr_branches
            .get(&PrBranchName("dank-spr/beta".to_string()))
            .unwrap()
        {
            PrBranchRecord::Tombstoned {
                tombstone_reason, ..
            } => assert_eq!(*tombstone_reason, TombstoneReason::RemovedFromLiveStack),
            other => panic!("expected tombstone, got {other:?}"),
        }
    }

    #[test]
    fn apply_snapshot_reuses_empty_stack_by_known_branch() {
        let existing = StackMetadataFile {
            schema_version: STACK_METADATA_SCHEMA_VERSION,
            stacks: BTreeMap::from([(
                StackId("stack-1".to_string()),
                StackRecord {
                    preferred_branch: StackBranchName("dank/stack".to_string()),
                    known_branches: vec![StackBranchName("dank/stack".to_string())],
                    base: "origin/main".to_string(),
                    prefix: "dank-spr/".to_string(),
                    last_seen_head: "old-head".to_string(),
                    updated_at: "2026-03-05T00:00:00Z".to_string(),
                },
            )]),
            pr_branches: BTreeMap::from([(
                PrBranchName("dank-spr/alpha".to_string()),
                PrBranchRecord::Live {
                    stack_id: StackId("stack-1".to_string()),
                    tag: PrTag("alpha".to_string()),
                    last_group_seed: "a1".to_string(),
                    last_group_tip: "a2".to_string(),
                    last_stack_head: "old-head".to_string(),
                    updated_at: "2026-03-05T00:00:00Z".to_string(),
                },
            )]),
        };
        let snapshot = sample_snapshot("dank/stack", &[]);

        let updated = apply_snapshot(existing, &snapshot, "2026-03-05T02:00:00Z").unwrap();
        match updated
            .pr_branches
            .get(&PrBranchName("dank-spr/alpha".to_string()))
            .unwrap()
        {
            PrBranchRecord::Tombstoned { stack_id, .. } => {
                assert_eq!(*stack_id, StackId("stack-1".to_string()))
            }
            other => panic!("expected tombstone, got {other:?}"),
        }
    }

    #[test]
    fn load_metadata_rejects_future_schema_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("stack_metadata_v1.json");
        fs::write(
            &path,
            r#"{"schema_version":2,"stacks":{},"pr_branches":{}}"#,
        )
        .unwrap();

        let err = load_metadata_from_path(&path).unwrap_err();
        assert!(err.to_string().contains("schema version 2"));
    }

    #[test]
    fn write_metadata_atomically_replaces_file_and_cleans_temp_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("stack_metadata_v1.json");
        fs::write(&path, "old").unwrap();
        let metadata = StackMetadataFile::default();

        write_metadata_atomically(&path, &metadata).unwrap();

        let written = fs::read_to_string(&path).unwrap();
        assert!(written.contains("\"schema_version\": 1"));
        assert!(fs::read_dir(dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp")));
    }

    #[test]
    fn metadata_lock_file_is_removed_on_drop() {
        let dir = TempDir::new().unwrap();
        let git_common_dir = dir.path().join(".git");
        fs::create_dir_all(&git_common_dir).unwrap();
        let lock_path = metadata_lock_path(&git_common_dir);

        {
            let _lock = acquire_lock(&lock_path).unwrap();
            assert!(lock_path.exists());
        }

        assert!(!lock_path.exists());
    }
}
