//! Read-only stack discovery backed by repo-local stack metadata.

use anyhow::{anyhow, bail, Result};
use serde::Serialize;

use crate::git::{git_ref_exists_at, git_ro_in, repo_root};
use crate::github::resolve_pr_url_head_ref;
use crate::stack_metadata::{
    current_branch_or_none, load_metadata_for_repo_path, stack_ids_for_branch,
    verify_stack_branch_for_pr_record, verify_stack_branch_for_stack_id, PrBranchName,
    PrBranchRecord, StackBranchName, StackMetadataFile, TombstoneReason,
};

const RESOLVE_STACK_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveStackTargetKind {
    PrBranch,
    StackBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolveStackDiagnostic {
    DetachedHead,
    MissingRemoteTrackingRef {
        remote_ref: String,
    },
    MultipleMatchingStacks {
        branch: String,
        stack_ids: Vec<String>,
    },
    PreferredBranchDidNotVerify {
        branch: String,
    },
    ResolvedPrUrl {
        url: String,
        head_ref_name: String,
    },
    UnrecordedCurrentBranch {
        branch: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolveStackOutput {
    pub schema_version: u32,
    #[serde(flatten)]
    pub payload: ResolveStackPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ResolveStackPayload {
    Found {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        stack_id: String,
        stack_branch: String,
        preferred_branch: String,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    AlreadyStackBranch {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        stack_id: String,
        stack_branch: String,
        preferred_branch: String,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    MissingMetadata {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    StaleMetadata {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        stack_id: Option<String>,
        preferred_branch: Option<String>,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    Tombstoned {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        stack_id: String,
        preferred_branch: Option<String>,
        tombstone_reason: TombstoneReason,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    Ambiguous {
        target_kind: ResolveStackTargetKind,
        normalized_target: String,
        stack_id: Option<String>,
        preferred_branch: Option<String>,
        candidate_branches: Vec<String>,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
    InvalidTarget {
        target_kind: Option<ResolveStackTargetKind>,
        normalized_target: Option<String>,
        diagnostics: Vec<ResolveStackDiagnostic>,
    },
}

impl ResolveStackOutput {
    fn new(payload: ResolveStackPayload) -> Self {
        Self {
            schema_version: RESOLVE_STACK_SCHEMA_VERSION,
            payload,
        }
    }

    pub fn render_human(&self) -> String {
        match &self.payload {
            ResolveStackPayload::Found { stack_branch, .. }
            | ResolveStackPayload::AlreadyStackBranch { stack_branch, .. } => stack_branch.clone(),
            ResolveStackPayload::MissingMetadata {
                normalized_target, ..
            } => format!("No stack metadata recorded for {}.", normalized_target),
            ResolveStackPayload::StaleMetadata {
                normalized_target, ..
            } => format!("Stack metadata for {} is stale.", normalized_target),
            ResolveStackPayload::Tombstoned {
                normalized_target, ..
            } => format!("{} is tombstoned in stack metadata.", normalized_target),
            ResolveStackPayload::Ambiguous {
                normalized_target,
                candidate_branches,
                ..
            } => format!(
                "Stack metadata for {} is ambiguous: {}",
                normalized_target,
                candidate_branches.join(", ")
            ),
            ResolveStackPayload::InvalidTarget {
                normalized_target, ..
            } => {
                if let Some(normalized_target) = normalized_target {
                    format!(
                        "{} is not a supported stack-discovery target.",
                        normalized_target
                    )
                } else {
                    "The current checkout is not a supported stack-discovery target.".to_string()
                }
            }
        }
    }
}

fn configured_remote_names(repo_path: &str) -> Result<Vec<String>> {
    Ok(git_ro_in(repo_path, ["remote"].as_slice())?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn looks_like_pr_url(target: &str) -> bool {
    (target.starts_with("https://github.com/") || target.starts_with("http://github.com/"))
        && target.contains("/pull/")
}

fn resolve_stack_branch_target(
    repo_path: &str,
    metadata: &StackMetadataFile,
    branch_name: &str,
    ignore_tag: &str,
) -> Result<ResolveStackOutput> {
    let stack_ids = stack_ids_for_branch(metadata, branch_name);
    if stack_ids.len() > 1 {
        return Ok(ResolveStackOutput::new(ResolveStackPayload::Ambiguous {
            target_kind: ResolveStackTargetKind::StackBranch,
            normalized_target: branch_name.to_string(),
            stack_id: None,
            preferred_branch: None,
            candidate_branches: vec![branch_name.to_string()],
            diagnostics: vec![ResolveStackDiagnostic::MultipleMatchingStacks {
                branch: branch_name.to_string(),
                stack_ids: stack_ids.into_iter().map(|stack_id| stack_id.0).collect(),
            }],
        }));
    }

    let Some(stack_id) = stack_ids.into_iter().next() else {
        bail!("resolve_stack_branch_target called without a matching recorded stack");
    };
    let stack_record = metadata
        .stacks
        .get(&stack_id)
        .ok_or_else(|| anyhow!("recorded stack_id {} is missing stack metadata", stack_id.0))?;
    if verify_stack_branch_for_stack_id(
        repo_path,
        metadata,
        &StackBranchName(branch_name.to_string()),
        &stack_id,
        ignore_tag,
    )? {
        Ok(ResolveStackOutput::new(
            ResolveStackPayload::AlreadyStackBranch {
                target_kind: ResolveStackTargetKind::StackBranch,
                normalized_target: branch_name.to_string(),
                stack_id: stack_id.0.clone(),
                stack_branch: branch_name.to_string(),
                preferred_branch: stack_record.preferred_branch.0.clone(),
                diagnostics: Vec::new(),
            },
        ))
    } else {
        Ok(ResolveStackOutput::new(
            ResolveStackPayload::StaleMetadata {
                target_kind: ResolveStackTargetKind::StackBranch,
                normalized_target: branch_name.to_string(),
                stack_id: Some(stack_id.0.clone()),
                preferred_branch: Some(stack_record.preferred_branch.0.clone()),
                diagnostics: Vec::new(),
            },
        ))
    }
}

fn resolve_pr_branch_target(
    repo_path: &str,
    metadata: Option<&StackMetadataFile>,
    pr_branch_name: &str,
    ignore_tag: &str,
    mut diagnostics: Vec<ResolveStackDiagnostic>,
) -> Result<ResolveStackOutput> {
    let Some(metadata) = metadata else {
        return Ok(ResolveStackOutput::new(
            ResolveStackPayload::MissingMetadata {
                target_kind: ResolveStackTargetKind::PrBranch,
                normalized_target: pr_branch_name.to_string(),
                diagnostics,
            },
        ));
    };
    let branch_name = PrBranchName(pr_branch_name.to_string());
    let Some(record) = metadata.pr_branches.get(&branch_name) else {
        return Ok(ResolveStackOutput::new(
            ResolveStackPayload::MissingMetadata {
                target_kind: ResolveStackTargetKind::PrBranch,
                normalized_target: pr_branch_name.to_string(),
                diagnostics,
            },
        ));
    };

    match record {
        PrBranchRecord::Tombstoned {
            stack_id,
            tombstone_reason,
            ..
        } => {
            let preferred_branch = metadata
                .stacks
                .get(stack_id)
                .map(|stack_record| stack_record.preferred_branch.0.clone());
            Ok(ResolveStackOutput::new(ResolveStackPayload::Tombstoned {
                target_kind: ResolveStackTargetKind::PrBranch,
                normalized_target: pr_branch_name.to_string(),
                stack_id: stack_id.0.clone(),
                preferred_branch,
                tombstone_reason: tombstone_reason.clone(),
                diagnostics,
            }))
        }
        PrBranchRecord::Live { stack_id, .. } => {
            let stack_record = metadata.stacks.get(stack_id).ok_or_else(|| {
                anyhow!(
                    "recorded stack_id {} is missing stack metadata for {}",
                    stack_id.0,
                    pr_branch_name
                )
            })?;
            let live_record = record.as_live().ok_or_else(|| {
                anyhow!(
                    "live PR branch record unexpectedly downgraded for {}",
                    pr_branch_name
                )
            })?;
            if verify_stack_branch_for_pr_record(
                repo_path,
                &stack_record.preferred_branch,
                stack_record,
                &branch_name,
                live_record.clone(),
                ignore_tag,
            )? {
                return Ok(ResolveStackOutput::new(ResolveStackPayload::Found {
                    target_kind: ResolveStackTargetKind::PrBranch,
                    normalized_target: pr_branch_name.to_string(),
                    stack_id: stack_id.0.clone(),
                    stack_branch: stack_record.preferred_branch.0.clone(),
                    preferred_branch: stack_record.preferred_branch.0.clone(),
                    diagnostics,
                }));
            }

            diagnostics.push(ResolveStackDiagnostic::PreferredBranchDidNotVerify {
                branch: stack_record.preferred_branch.0.clone(),
            });
            let verified_aliases = stack_record
                .known_branches
                .iter()
                .filter(|candidate| **candidate != stack_record.preferred_branch)
                .map(|candidate| -> Result<Option<String>> {
                    if verify_stack_branch_for_pr_record(
                        repo_path,
                        candidate,
                        stack_record,
                        &branch_name,
                        live_record.clone(),
                        ignore_tag,
                    )? {
                        Ok(Some(candidate.0.clone()))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            if verified_aliases.len() == 1 {
                Ok(ResolveStackOutput::new(ResolveStackPayload::Found {
                    target_kind: ResolveStackTargetKind::PrBranch,
                    normalized_target: pr_branch_name.to_string(),
                    stack_id: stack_id.0.clone(),
                    stack_branch: verified_aliases[0].clone(),
                    preferred_branch: stack_record.preferred_branch.0.clone(),
                    diagnostics,
                }))
            } else if verified_aliases.len() > 1 {
                Ok(ResolveStackOutput::new(ResolveStackPayload::Ambiguous {
                    target_kind: ResolveStackTargetKind::PrBranch,
                    normalized_target: pr_branch_name.to_string(),
                    stack_id: Some(stack_id.0.clone()),
                    preferred_branch: Some(stack_record.preferred_branch.0.clone()),
                    candidate_branches: verified_aliases,
                    diagnostics,
                }))
            } else {
                Ok(ResolveStackOutput::new(
                    ResolveStackPayload::StaleMetadata {
                        target_kind: ResolveStackTargetKind::PrBranch,
                        normalized_target: pr_branch_name.to_string(),
                        stack_id: Some(stack_id.0.clone()),
                        preferred_branch: Some(stack_record.preferred_branch.0.clone()),
                        diagnostics,
                    },
                ))
            }
        }
    }
}

fn resolve_branch_target(
    repo_path: &str,
    metadata: Option<&StackMetadataFile>,
    branch_name: &str,
    ignore_tag: &str,
) -> Result<ResolveStackOutput> {
    if let Some(metadata) = metadata {
        if !stack_ids_for_branch(metadata, branch_name).is_empty() {
            return resolve_stack_branch_target(repo_path, metadata, branch_name, ignore_tag);
        }
    }
    resolve_pr_branch_target(repo_path, metadata, branch_name, ignore_tag, Vec::new())
}

fn resolve_explicit_target(
    repo_path: &str,
    metadata: Option<&StackMetadataFile>,
    target: &str,
    ignore_tag: &str,
) -> Result<ResolveStackOutput> {
    if looks_like_pr_url(target) {
        let head_ref_name = resolve_pr_url_head_ref(target)?;
        let diagnostics = vec![ResolveStackDiagnostic::ResolvedPrUrl {
            url: target.to_string(),
            head_ref_name: head_ref_name.clone(),
        }];
        return resolve_pr_branch_target(
            repo_path,
            metadata,
            &head_ref_name,
            ignore_tag,
            diagnostics,
        );
    }

    let remote_names = configured_remote_names(repo_path)?;
    if let Some((remote_name, branch_name)) = target.split_once('/') {
        if remote_names
            .iter()
            .any(|candidate| candidate == remote_name)
        {
            let remote_reference = format!("refs/remotes/{target}");
            if !git_ref_exists_at(repo_path, &remote_reference)? {
                return Ok(ResolveStackOutput::new(
                    ResolveStackPayload::InvalidTarget {
                        target_kind: Some(ResolveStackTargetKind::PrBranch),
                        normalized_target: Some(target.to_string()),
                        diagnostics: vec![ResolveStackDiagnostic::MissingRemoteTrackingRef {
                            remote_ref: target.to_string(),
                        }],
                    },
                ));
            }
            return resolve_branch_target(repo_path, metadata, branch_name, ignore_tag);
        }
    }

    resolve_branch_target(repo_path, metadata, target, ignore_tag)
}

fn resolve_current_branch_target(
    repo_path: &str,
    metadata: Option<&StackMetadataFile>,
    ignore_tag: &str,
) -> Result<ResolveStackOutput> {
    let Some(current_branch) = current_branch_or_none(repo_path)? else {
        return Ok(ResolveStackOutput::new(
            ResolveStackPayload::InvalidTarget {
                target_kind: None,
                normalized_target: None,
                diagnostics: vec![ResolveStackDiagnostic::DetachedHead],
            },
        ));
    };

    if let Some(metadata) = metadata {
        if !stack_ids_for_branch(metadata, &current_branch).is_empty() {
            return resolve_stack_branch_target(repo_path, metadata, &current_branch, ignore_tag);
        }
        if metadata
            .pr_branches
            .contains_key(&PrBranchName(current_branch.clone()))
        {
            return resolve_pr_branch_target(
                repo_path,
                Some(metadata),
                &current_branch,
                ignore_tag,
                Vec::new(),
            );
        }
    }

    Ok(ResolveStackOutput::new(
        ResolveStackPayload::InvalidTarget {
            target_kind: Some(ResolveStackTargetKind::PrBranch),
            normalized_target: Some(current_branch.clone()),
            diagnostics: vec![ResolveStackDiagnostic::UnrecordedCurrentBranch {
                branch: current_branch,
            }],
        },
    ))
}

pub fn resolve_stack(target: Option<String>, ignore_tag: &str) -> Result<ResolveStackOutput> {
    let repo_path = repo_root()?.ok_or_else(|| anyhow!("`spr` must run inside a git worktree"))?;
    let metadata = load_metadata_for_repo_path(&repo_path)?;
    if let Some(target) = target {
        resolve_explicit_target(&repo_path, metadata.as_ref(), &target, ignore_tag)
    } else {
        resolve_current_branch_target(&repo_path, metadata.as_ref(), ignore_tag)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        looks_like_pr_url, resolve_stack, ResolveStackDiagnostic, ResolveStackOutput,
        ResolveStackPayload,
    };
    use crate::stack_metadata::{
        load_metadata_for_repo_path, metadata_path, refresh_metadata_for_branch,
        RefreshMetadataContext, StackBranchName,
    };
    use crate::test_support::{commit_file, git, lock_cwd, DirGuard};
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    const STACK_BRANCH: &str = "dank/stack";
    const PREFIX: &str = "dank-spr/";
    const IGNORE_TAG: &str = "ignore";

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: String) -> Self {
            let original = env::var(key).ok();
            env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    struct TestRepo {
        _dir: TempDir,
        repo: PathBuf,
        alpha_tip: String,
    }

    impl TestRepo {
        fn init() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let repo = dir.path().join("repo");
            fs::create_dir_all(&repo).unwrap();
            git(&repo, ["init", "-b", "main"].as_slice());
            git(
                &repo,
                ["config", "user.email", "spr@example.com"].as_slice(),
            );
            git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
            commit_file(&repo, "story.txt", "base\n", "init");
            git(&repo, ["checkout", "-b", STACK_BRANCH].as_slice());
            commit_file(&repo, "story.txt", "alpha-1\n", "feat: alpha\n\npr:alpha");
            let alpha_tip = commit_file(&repo, "story.txt", "alpha-2\n", "feat: alpha follow-up");
            commit_file(&repo, "story.txt", "beta-1\n", "feat: beta\n\npr:beta");
            Self {
                _dir: dir,
                repo,
                alpha_tip,
            }
        }

        fn repo_path(&self) -> &Path {
            &self.repo
        }

        fn refresh_metadata(&self, branch: &str) {
            refresh_metadata_for_branch(
                self.repo.to_str().unwrap(),
                branch,
                &RefreshMetadataContext {
                    base: "main".to_string(),
                    prefix: PREFIX.to_string(),
                    ignore_tag: IGNORE_TAG.to_string(),
                },
                None,
            )
            .unwrap();
        }

        fn metadata_path(&self) -> PathBuf {
            let git_common_dir =
                crate::git::git_common_dir_at(self.repo.to_str().unwrap()).unwrap();
            metadata_path(&git_common_dir)
        }

        fn rewrite_stack_branch(&self, commits: &[(&str, &str)]) {
            git(
                &self.repo,
                ["checkout", "-B", STACK_BRANCH, "main"].as_slice(),
            );
            for (contents, message) in commits {
                commit_file(&self.repo, "story.txt", contents, message);
            }
        }

        fn with_cwd<T>(&self, f: impl FnOnce() -> T) -> T {
            let _lock = lock_cwd();
            let _guard = DirGuard::change_to(&self.repo);
            f()
        }
    }

    fn install_gh_wrapper(script_body: &str) -> (TempDir, EnvVarGuard) {
        let wrapper_dir = tempfile::tempdir().unwrap();
        let script_path = wrapper_dir.path().join("gh");
        fs::write(&script_path, script_body).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).unwrap();

        let original_path = env::var("PATH").unwrap_or_default();
        let path_guard = EnvVarGuard::set(
            "PATH",
            format!("{}:{}", wrapper_dir.path().display(), original_path),
        );

        (wrapper_dir, path_guard)
    }

    fn read_metadata(repo: &TestRepo) -> crate::stack_metadata::StackMetadataFile {
        load_metadata_for_repo_path(repo.repo.to_str().unwrap())
            .unwrap()
            .unwrap()
    }

    fn write_metadata(repo: &TestRepo, metadata: &crate::stack_metadata::StackMetadataFile) {
        fs::write(
            repo.metadata_path(),
            serde_json::to_string_pretty(metadata).unwrap(),
        )
        .unwrap();
    }

    fn assert_status(output: ResolveStackOutput) -> ResolveStackPayload {
        output.payload
    }

    #[test]
    fn resolve_stack_returns_found_for_pr_branch() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/alpha".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::Found {
                stack_branch,
                preferred_branch,
                ..
            } => {
                assert_eq!(stack_branch, STACK_BRANCH);
                assert_eq!(preferred_branch, STACK_BRANCH);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_already_stack_branch_without_target() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);

        let payload = repo.with_cwd(|| assert_status(resolve_stack(None, IGNORE_TAG).unwrap()));

        match payload {
            ResolveStackPayload::AlreadyStackBranch { stack_branch, .. } => {
                assert_eq!(stack_branch, STACK_BRANCH);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_invalid_target_for_detached_head() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        git(
            repo.repo_path(),
            ["checkout", "--detach", "HEAD"].as_slice(),
        );

        let payload = repo.with_cwd(|| assert_status(resolve_stack(None, IGNORE_TAG).unwrap()));

        match payload {
            ResolveStackPayload::InvalidTarget {
                diagnostics,
                normalized_target,
                ..
            } => {
                assert!(normalized_target.is_none());
                assert_eq!(diagnostics, vec![ResolveStackDiagnostic::DetachedHead]);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_missing_metadata_for_unknown_pr_branch() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/gamma".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::MissingMetadata {
                normalized_target, ..
            } => assert_eq!(normalized_target, "dank-spr/gamma"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_tombstoned_for_retired_pr_branch() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        git(
            repo.repo_path(),
            ["reset", "--hard", &repo.alpha_tip].as_slice(),
        );
        repo.refresh_metadata(STACK_BRANCH);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/beta".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::Tombstoned {
                normalized_target, ..
            } => assert_eq!(normalized_target, "dank-spr/beta"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_stale_metadata_when_recorded_owner_no_longer_verifies() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        repo.rewrite_stack_branch(&[
            ("alpha-new-1\n", "feat: alpha rewritten\n\npr:alpha"),
            ("beta-new-1\n", "feat: beta rewritten\n\npr:beta"),
        ]);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/alpha".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::StaleMetadata {
                normalized_target, ..
            } => assert_eq!(normalized_target, "dank-spr/alpha"),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_returns_ambiguous_when_multiple_recorded_aliases_verify() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        git(
            repo.repo_path(),
            ["branch", "dank/alias-one", "HEAD"].as_slice(),
        );
        git(
            repo.repo_path(),
            ["branch", "dank/alias-two", "HEAD"].as_slice(),
        );

        let mut metadata = read_metadata(&repo);
        let stack_id = metadata.stacks.keys().next().unwrap().clone();
        let stack = metadata.stacks.get_mut(&stack_id).unwrap();
        stack.preferred_branch = StackBranchName("dank/stale".to_string());
        stack.known_branches = vec![
            StackBranchName("dank/stale".to_string()),
            StackBranchName("dank/alias-one".to_string()),
            StackBranchName("dank/alias-two".to_string()),
        ];
        write_metadata(&repo, &metadata);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/alpha".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::Ambiguous {
                candidate_branches, ..
            } => assert_eq!(
                candidate_branches,
                vec!["dank/alias-one".to_string(), "dank/alias-two".to_string()]
            ),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_does_not_scan_unrecorded_alias_branches() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        git(
            repo.repo_path(),
            ["branch", "dank/unrelated", "HEAD"].as_slice(),
        );

        let mut metadata = read_metadata(&repo);
        let stack_id = metadata.stacks.keys().next().unwrap().clone();
        let stack = metadata.stacks.get_mut(&stack_id).unwrap();
        stack.preferred_branch = StackBranchName("dank/stale".to_string());
        stack.known_branches = vec![StackBranchName("dank/stale".to_string())];
        write_metadata(&repo, &metadata);

        let payload = repo.with_cwd(|| {
            assert_status(resolve_stack(Some("dank-spr/alpha".to_string()), IGNORE_TAG).unwrap())
        });

        match payload {
            ResolveStackPayload::StaleMetadata { .. } => {}
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn resolve_stack_resolves_pr_url_only_to_head_ref_name() {
        let repo = TestRepo::init();
        repo.refresh_metadata(STACK_BRANCH);
        let log_path = repo.repo.join("gh.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"$1\" = \"pr\" ] && [ \"$2\" = \"view\" ]; then\n  echo '{{\"headRefName\":\"dank-spr/alpha\"}}'\n  exit 0\nfi\necho \"unexpected gh invocation: $*\" >&2\nexit 1\n",
            log_path.display()
        );
        let (_wrapper_dir, _path_guard) = install_gh_wrapper(&script);

        let payload = repo.with_cwd(|| {
            assert_status(
                resolve_stack(
                    Some("https://github.com/o/r/pull/17".to_string()),
                    IGNORE_TAG,
                )
                .unwrap(),
            )
        });

        match payload {
            ResolveStackPayload::Found { diagnostics, .. } => assert_eq!(
                diagnostics,
                vec![ResolveStackDiagnostic::ResolvedPrUrl {
                    url: "https://github.com/o/r/pull/17".to_string(),
                    head_ref_name: "dank-spr/alpha".to_string(),
                }]
            ),
            other => panic!("unexpected payload: {other:?}"),
        }

        let log = fs::read_to_string(log_path).unwrap();
        assert!(log.contains("pr view https://github.com/o/r/pull/17 --json headRefName"));
    }

    #[test]
    fn looks_like_pr_url_requires_github_pull_path() {
        assert!(looks_like_pr_url("https://github.com/o/r/pull/17"));
        assert!(!looks_like_pr_url("https://github.com/o/r/issues/17"));
        assert!(!looks_like_pr_url("dank-spr/alpha"));
    }
}
