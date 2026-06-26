use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::info;
use uuid::Uuid;

use crate::branch_names::group_branch_identities;
use crate::git::{git_common_dir, git_rev_parse, git_ro, repo_root};
use crate::parsing::{derive_groups_between_with_ignored, split_groups_for_update, Group};
use crate::selectors::{resolve_inclusive_count, InclusiveSelector};

const VALIDATION_PROTOCOL_VERSION: u32 = 2;
const RECEIPT_DIR_NAME: &str = "validation_receipts_v2";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookFingerprint {
    pub path: String,
    pub present: bool,
    pub executable: bool,
    pub content_hash: Option<String>,
}

impl HookFingerprint {
    fn effective(&self) -> bool {
        self.present && self.executable
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationDescriptor {
    pub protocol_version: u32,
    pub base_ref: String,
    pub base_sha: String,
    pub merge_base: String,
    pub prefix: String,
    pub ignore_tag: String,
    pub origin_push_url: String,
    pub pre_push_hook: HookFingerprint,
    pub stable_handle: String,
    pub head_branch: String,
    pub previous_tip_sha: String,
    pub tip_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReceipt {
    pub digest: String,
    pub validated_at: String,
    pub descriptor: ValidationDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidationSummaryData {
    pub success: bool,
    pub validated_group_count: usize,
    pub recorded_group_count: usize,
    pub reused_group_count: usize,
    pub skipped_groups: Vec<String>,
    pub receipts: Vec<ValidationReceiptSummaryData>,
    pub pre_push_hook_present: bool,
    pub pre_push_hook_executable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationReceiptAction {
    Recorded,
    Reused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidationReceiptSummaryData {
    pub local_pr_number: usize,
    pub stable_handle: String,
    pub head_branch: String,
    pub previous_tip_sha: String,
    pub tip_sha: String,
    pub action: ValidationReceiptAction,
    pub digest: String,
    pub path: String,
}

fn run_git(args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("failed to spawn git {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "git {} failed\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_git_in(path: &Path, args: &[&str]) -> Result<String> {
    let path = path
        .to_str()
        .ok_or_else(|| anyhow!("non-UTF-8 worktree path {}", path.display()))?;
    let mut argv = vec!["-C", path];
    argv.extend_from_slice(args);
    run_git(&argv)
}

fn hash_bytes(bytes: &[u8]) -> Result<String> {
    let mut child = Command::new("git")
        .args(["hash-object", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to spawn git hash-object --stdin")?;
    child
        .stdin
        .take()
        .context("git hash-object stdin unavailable")?
        .write_all(bytes)
        .context("failed to write git hash-object stdin")?;
    let output = child
        .wait_with_output()
        .context("failed to wait for git hash-object")?;
    if !output.status.success() {
        bail!(
            "git hash-object --stdin failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn now_rfc3339() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .context("failed to format RFC3339 timestamp")
}

fn origin_push_url() -> Result<String> {
    let output = git_ro(["remote", "get-url", "--push", "--all", "origin"].as_slice())?;
    let urls = output
        .lines()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();
    match urls.as_slice() {
        [url] => Ok((*url).to_string()),
        [] => bail!("origin has no configured push URL"),
        _ => bail!(
            "validation receipts require exactly one origin push URL, but found {}: {}",
            urls.len(),
            urls.join(", ")
        ),
    }
}

fn pre_push_hook_fingerprint(repo_root: &Path) -> Result<HookFingerprint> {
    let reported = git_ro(["rev-parse", "--git-path", "hooks/pre-push"].as_slice())?;
    let reported = reported.trim();
    let path = if Path::new(reported).is_absolute() {
        PathBuf::from(reported)
    } else {
        repo_root.join(reported)
    };
    let present = path.is_file();
    let executable = if present {
        fs::metadata(&path)
            .with_context(|| format!("failed to inspect hook {}", path.display()))?
            .permissions()
            .mode()
            & 0o111
            != 0
    } else {
        false
    };
    let content_hash = if present {
        Some(hash_bytes(&fs::read(&path).with_context(|| {
            format!("failed to read hook {}", path.display())
        })?)?)
    } else {
        None
    };
    Ok(HookFingerprint {
        path: path.display().to_string(),
        present,
        executable,
        content_hash,
    })
}

pub fn build_descriptors(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    merge_base: &str,
    groups: &[Group],
) -> Result<Vec<ValidationDescriptor>> {
    let root = repo_root()?.ok_or_else(|| anyhow!("`spr` must run inside a git worktree"))?;
    let base_sha = git_rev_parse(base)?;
    let origin_push_url = origin_push_url()?;
    let pre_push_hook = pre_push_hook_fingerprint(Path::new(&root))?;
    let branch_identities = group_branch_identities(groups, prefix)?;
    let mut previous_tip_sha = merge_base.to_string();
    groups
        .iter()
        .zip(branch_identities)
        .map(|(group, identity)| {
            let tip_sha = group
                .commits
                .last()
                .cloned()
                .ok_or_else(|| anyhow!("group {} has no commits", group.selector_text()))?;
            let descriptor = ValidationDescriptor {
                protocol_version: VALIDATION_PROTOCOL_VERSION,
                base_ref: base.to_string(),
                base_sha: base_sha.clone(),
                merge_base: merge_base.to_string(),
                prefix: prefix.to_string(),
                ignore_tag: ignore_tag.to_string(),
                origin_push_url: origin_push_url.clone(),
                pre_push_hook: pre_push_hook.clone(),
                stable_handle: group.selector_text(),
                head_branch: identity.exact,
                previous_tip_sha: previous_tip_sha.clone(),
                tip_sha: tip_sha.clone(),
            };
            previous_tip_sha = tip_sha;
            Ok(descriptor)
        })
        .collect()
}

pub fn descriptor_digest(descriptor: &ValidationDescriptor) -> Result<String> {
    hash_bytes(&serde_json::to_vec(descriptor).context("failed to encode validation descriptor")?)
}

pub fn receipt_path_for_digest(digest: &str) -> Result<PathBuf> {
    Ok(git_common_dir()?
        .join("spr")
        .join(RECEIPT_DIR_NAME)
        .join(format!("{digest}.json")))
}

fn matching_receipt_path(descriptor: &ValidationDescriptor) -> Result<Option<PathBuf>> {
    let digest = descriptor_digest(descriptor)?;
    let path = receipt_path_for_digest(&digest)?;
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to read validation receipt {}", path.display()));
        }
    };
    let receipt: ValidationReceipt = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse validation receipt {}", path.display()))?;
    if receipt.digest != digest || receipt.descriptor != *descriptor {
        return Ok(None);
    }
    Ok(Some(path))
}

pub fn require_matching_receipts(descriptors: &[ValidationDescriptor]) -> Result<Vec<PathBuf>> {
    descriptors
        .iter()
        .map(|descriptor| {
            let digest = descriptor_digest(descriptor)?;
            let expected_path = receipt_path_for_digest(&digest)?;
            matching_receipt_path(descriptor)?.ok_or_else(|| {
                anyhow!(
                    "missing or stale `spr validate` receipt for {}; run `spr validate` or rerun `spr update --skip-validation` to bypass validation ({})",
                    descriptor.stable_handle,
                    expected_path.display()
                )
            })
        })
        .collect()
}

fn write_receipt(descriptor: ValidationDescriptor) -> Result<(String, PathBuf)> {
    let digest = descriptor_digest(&descriptor)?;
    let path = receipt_path_for_digest(&digest)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("validation receipt path {} has no parent", path.display()))?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create validation receipt directory {}",
            parent.display()
        )
    })?;
    let receipt = ValidationReceipt {
        digest: digest.clone(),
        validated_at: now_rfc3339()?,
        descriptor,
    };
    let temp_path = parent.join(format!(".{digest}.{}.tmp", Uuid::new_v4()));
    fs::write(
        &temp_path,
        serde_json::to_vec_pretty(&receipt).context("failed to encode validation receipt")?,
    )
    .with_context(|| format!("failed to write validation receipt {}", temp_path.display()))?;
    fs::rename(&temp_path, &path).with_context(|| {
        format!(
            "failed to rename validation receipt {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok((digest, path))
}

fn worktree_path() -> PathBuf {
    std::env::temp_dir().join(format!("spr-validate-{}", Uuid::new_v4()))
}

fn hook_input_path() -> PathBuf {
    std::env::temp_dir().join(format!("spr-validate-hook-input-{}", Uuid::new_v4()))
}

fn run_pre_push_hook(
    worktree: &Path,
    pre_push_hook: &HookFingerprint,
    origin_push_url: &str,
    branch: &str,
    previous_tip: &str,
    current_tip: &str,
) -> Result<()> {
    let input_path = hook_input_path();
    let ref_name = format!("refs/heads/{branch}");
    fs::write(
        &input_path,
        format!("{ref_name} {current_tip} {ref_name} {previous_tip}\n"),
    )
    .with_context(|| format!("failed to write hook input {}", input_path.display()))?;
    let hooks_path = Path::new(&pre_push_hook.path)
        .parent()
        .ok_or_else(|| anyhow!("pre-push hook path {} has no parent", pre_push_hook.path))?;
    let status = Command::new("git")
        .current_dir(worktree)
        .args([
            "-c",
            &format!("core.hooksPath={}", hooks_path.display()),
            "hook",
            "run",
            "--ignore-missing",
            &format!("--to-stdin={}", input_path.display()),
            "pre-push",
            "--",
            "origin",
            origin_push_url,
        ])
        .status()
        .context("failed to spawn git hook run pre-push")?;
    let _ = fs::remove_file(&input_path);
    if !status.success() {
        bail!("pre-push hook failed");
    }
    Ok(())
}

fn tracked_changes(worktree: &Path) -> Result<String> {
    run_git_in(
        worktree,
        ["status", "--porcelain", "--untracked-files=no"].as_slice(),
    )
}

fn cleanup_worktree(worktree: &Path) -> Result<()> {
    run_git(
        [
            "worktree",
            "remove",
            "--force",
            &worktree.display().to_string(),
        ]
        .as_slice(),
    )?;
    Ok(())
}

#[cfg(test)]
pub fn validate_current_stack(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    until: &InclusiveSelector,
) -> Result<ValidationSummaryData> {
    validate_stack(base, prefix, ignore_tag, "HEAD", until)
}

pub fn validate_stack(
    base: &str,
    prefix: &str,
    ignore_tag: &str,
    from: &str,
    until: &InclusiveSelector,
) -> Result<ValidationSummaryData> {
    let (merge_base, leading_ignored, all_groups) =
        derive_groups_between_with_ignored(base, from, ignore_tag)?;
    let (mut groups, skipped_groups) = split_groups_for_update(&leading_ignored, all_groups);
    let group_count = resolve_inclusive_count(&groups, until)?;
    groups.truncate(group_count);
    let descriptors = build_descriptors(base, prefix, ignore_tag, &merge_base, &groups)?;
    let pre_push_hook = if let Some(descriptor) = descriptors.first() {
        descriptor.pre_push_hook.clone()
    } else {
        let root = repo_root()?.ok_or_else(|| anyhow!("`spr` must run inside a git worktree"))?;
        pre_push_hook_fingerprint(Path::new(&root))?
    };
    let missing_indices = descriptors
        .iter()
        .enumerate()
        .filter_map(
            |(index, descriptor)| match matching_receipt_path(descriptor) {
                Ok(Some(_)) => None,
                Ok(None) => Some(Ok(index)),
                Err(err) => Some(Err(err)),
            },
        )
        .collect::<Result<Vec<_>>>()?;
    if pre_push_hook.effective() && !missing_indices.is_empty() {
        let worktree = worktree_path();
        run_git(
            [
                "worktree",
                "add",
                "--detach",
                &worktree.display().to_string(),
                &merge_base,
            ]
            .as_slice(),
        )?;
        let validation_result = (|| {
            for group_idx in &missing_indices {
                let descriptor = &descriptors[*group_idx];
                info!(
                    "Validating local PR #{} / {} at {}",
                    group_idx + 1,
                    descriptor.stable_handle,
                    descriptor.tip_sha
                );
                run_git_in(
                    &worktree,
                    ["checkout", "--detach", "--force", &descriptor.tip_sha].as_slice(),
                )?;
                run_git_in(&worktree, ["clean", "-fdx"].as_slice())?;
                run_pre_push_hook(
                    &worktree,
                    &descriptor.pre_push_hook,
                    &descriptor.origin_push_url,
                    &descriptor.head_branch,
                    &descriptor.previous_tip_sha,
                    &descriptor.tip_sha,
                )
                .with_context(|| {
                    format!(
                        "validation failed for local PR #{} / {} in preserved worktree {}",
                        group_idx + 1,
                        descriptor.stable_handle,
                        worktree.display()
                    )
                })?;
                let changes = tracked_changes(&worktree)?;
                if !changes.trim().is_empty() {
                    bail!(
                        "validation hook modified tracked files for local PR #{} / {}; inspect preserved worktree {}\n{}",
                        group_idx + 1,
                        descriptor.stable_handle,
                        worktree.display(),
                        changes
                    );
                }
                write_receipt(descriptor.clone())?;
            }
            Ok(())
        })();
        validation_result?;
        cleanup_worktree(&worktree)?;
    }
    if !pre_push_hook.effective() {
        for group_idx in &missing_indices {
            write_receipt(descriptors[*group_idx].clone())?;
        }
    }
    let receipts = descriptors
        .iter()
        .enumerate()
        .map(|(group_idx, descriptor)| {
            let digest = descriptor_digest(descriptor)?;
            let path = matching_receipt_path(descriptor)?.ok_or_else(|| {
                anyhow!(
                    "validation receipt for {} was not recorded",
                    descriptor.stable_handle
                )
            })?;
            Ok(ValidationReceiptSummaryData {
                local_pr_number: group_idx + 1,
                stable_handle: descriptor.stable_handle.clone(),
                head_branch: descriptor.head_branch.clone(),
                previous_tip_sha: descriptor.previous_tip_sha.clone(),
                tip_sha: descriptor.tip_sha.clone(),
                action: if missing_indices.contains(&group_idx) {
                    ValidationReceiptAction::Recorded
                } else {
                    ValidationReceiptAction::Reused
                },
                digest,
                path: path.display().to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let pre_push_hook_present = pre_push_hook.present;
    let pre_push_hook_executable = pre_push_hook.executable;
    let validated_group_count = descriptors.len();
    let recorded_group_count = missing_indices.len();
    let reused_group_count = validated_group_count - recorded_group_count;
    Ok(ValidationSummaryData {
        success: true,
        validated_group_count,
        recorded_group_count,
        reused_group_count,
        skipped_groups,
        receipts,
        pre_push_hook_present,
        pre_push_hook_executable,
    })
}

pub fn print_validation_summary(summary: &ValidationSummaryData) {
    println!("{}", validation_status_line(summary));
    if !summary.pre_push_hook_executable {
        if summary.pre_push_hook_present {
            println!("The configured pre-push hook is not executable");
        } else {
            println!("No pre-push hook is installed");
        }
    }
    for receipt in &summary.receipts {
        if receipt.action == ValidationReceiptAction::Recorded {
            println!("Recorded validation receipt: {}", receipt.path);
        }
    }
    if !summary.skipped_groups.is_empty() {
        eprintln!("{}", validation_skipped_groups_warning(summary).unwrap());
    }
}

fn validation_status_line(summary: &ValidationSummaryData) -> String {
    if summary.pre_push_hook_executable {
        format!(
            "Validated {} local PR boundary/boundaries ({} recorded, {} reused)",
            summary.validated_group_count, summary.recorded_group_count, summary.reused_group_count
        )
    } else {
        format!(
            "Recorded receipts for {} local PR boundary/boundaries without running a pre-push hook ({} recorded, {} reused)",
            summary.validated_group_count, summary.recorded_group_count, summary.reused_group_count
        )
    }
}

fn validation_skipped_groups_warning(summary: &ValidationSummaryData) -> Option<String> {
    (!summary.skipped_groups.is_empty())
        .then(|| crate::commands::update::ignored_boundary_warning(&summary.skipped_groups))
}

#[cfg(test)]
mod tests {
    use super::{
        build_descriptors, require_matching_receipts, validate_current_stack, validate_stack,
        validation_skipped_groups_warning, validation_status_line, ValidationReceiptAction,
        ValidationSummaryData,
    };
    use crate::commands::{build_from_groups_with_validation, UpdatePushValidation};
    use crate::config::{ListOrder, LocalPrBranchSyncPolicy, PrDescriptionMode};
    use crate::execution::ExecutionMode;
    use crate::parsing::{derive_local_groups_with_ignored, split_groups_for_update};
    use crate::selectors::{ExplicitGroupSelector, GroupSelector, InclusiveSelector};
    use crate::test_support::{commit_file, git, lock_cwd, write_file, DirGuard};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    struct ValidationRepo {
        _dir: TempDir,
        repo: PathBuf,
        log: PathBuf,
        main_sha: String,
        alpha_tip: String,
        beta_tip: String,
    }

    fn init_validation_repo() -> ValidationRepo {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git(&repo, ["init", "-b", "main"].as_slice());
        git(
            &repo,
            ["config", "user.email", "spr@example.com"].as_slice(),
        );
        git(&repo, ["config", "user.name", "SPR Tests"].as_slice());
        git(&repo, ["config", "core.hooksPath", ".git/hooks"].as_slice());
        write_file(&repo, "README.md", "init\n");
        git(&repo, ["add", "README.md"].as_slice());
        git(&repo, ["commit", "-m", "init"].as_slice());
        let main_sha = git(&repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();
        let origin = dir.path().join("origin.git");
        git(
            &repo,
            ["init", "--bare", origin.to_str().unwrap()].as_slice(),
        );
        git(
            &repo,
            ["remote", "add", "origin", origin.to_str().unwrap()].as_slice(),
        );
        git(&repo, ["push", "-u", "origin", "main"].as_slice());
        git(&repo, ["checkout", "-b", "stack"].as_slice());
        commit_file(&repo, "alpha.txt", "alpha seed\n", "alpha pr:alpha");
        let alpha_tip = commit_file(&repo, "alpha.txt", "alpha tip\n", "alpha follow-up");
        let beta_tip = commit_file(&repo, "beta.txt", "beta tip\n", "beta pr:beta");
        ValidationRepo {
            log: dir.path().join("hook.log"),
            _dir: dir,
            repo,
            main_sha,
            alpha_tip,
            beta_tip,
        }
    }

    fn install_hook(repo: &ValidationRepo, body: &str) {
        let hook = repo.repo.join(".git/hooks/pre-push");
        fs::write(&hook, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&hook).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&hook, permissions).unwrap();
    }

    fn descriptors(_repo: &ValidationRepo) -> Vec<super::ValidationDescriptor> {
        let (merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored("main", "ignore").unwrap();
        let (groups, _) = split_groups_for_update(&leading_ignored, groups);
        build_descriptors("main", "spr/", "ignore", &merge_base, &groups).unwrap()
    }

    fn groups_and_descriptors(
        repo: &ValidationRepo,
    ) -> (Vec<crate::parsing::Group>, Vec<super::ValidationDescriptor>) {
        let (merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored("main", "ignore").unwrap();
        let (groups, _) = split_groups_for_update(&leading_ignored, groups);
        let descriptors =
            build_descriptors("main", "spr/", "ignore", &merge_base, &groups).unwrap();
        let _ = repo;
        (groups, descriptors)
    }

    fn update_no_pr(
        groups: Vec<crate::parsing::Group>,
        push_validation: UpdatePushValidation,
    ) -> anyhow::Result<()> {
        build_from_groups_with_validation(
            "main",
            "spr/",
            &[],
            true,
            ExecutionMode::Apply,
            PrDescriptionMode::Overwrite,
            None,
            groups,
            ListOrder::RecentOnTop,
            true,
            0,
            LocalPrBranchSyncPolicy::Off,
            push_validation,
        )
    }

    fn remove_preserved_worktree(repo: &ValidationRepo) {
        let worktrees = git(&repo.repo, ["worktree", "list", "--porcelain"].as_slice());
        for line in worktrees.lines() {
            if let Some(path) = line.strip_prefix("worktree /tmp/spr-validate-") {
                let path = format!("/tmp/spr-validate-{path}");
                git(
                    &repo.repo,
                    ["worktree", "remove", "--force", &path].as_slice(),
                );
            }
        }
    }

    fn assert_receipt(summary: &ValidationSummaryData) {
        assert!(summary.success);
        assert!(!summary.receipts.is_empty());
        for receipt in &summary.receipts {
            assert!(Path::new(&receipt.path).is_file());
            assert!(!receipt.digest.is_empty());
        }
    }

    fn first_group() -> InclusiveSelector {
        InclusiveSelector::Group(GroupSelector::LocalPr(1))
    }

    fn beta_group() -> InclusiveSelector {
        InclusiveSelector::Group(GroupSelector::Explicit(ExplicitGroupSelector::PrLabel(
            "beta".to_string(),
        )))
    }

    fn append_gamma(repo: &ValidationRepo) -> String {
        commit_file(&repo.repo, "gamma.txt", "gamma tip\n", "gamma pr:gamma")
    }

    fn validation_summary(
        pre_push_hook_present: bool,
        pre_push_hook_executable: bool,
        skipped_groups: Vec<String>,
    ) -> ValidationSummaryData {
        ValidationSummaryData {
            success: true,
            validated_group_count: 2,
            recorded_group_count: 2,
            reused_group_count: 0,
            skipped_groups,
            receipts: Vec::new(),
            pre_push_hook_present,
            pre_push_hook_executable,
        }
    }

    #[test]
    fn validates_each_pr_tip_with_pr_local_ranges_and_historical_contents() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(
            &repo,
            &format!(
                "printf 'alpha=' >> '{}'\ntr '\\n' ' ' < alpha.txt >> '{}' 2>/dev/null || true\nprintf '\\n' >> '{}'\ncat >> '{}'",
                repo.log.display(),
                repo.log.display(),
                repo.log.display(),
                repo.log.display()
            ),
        );

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_receipt(&summary);
        assert_eq!(summary.validated_group_count, 2);
        let log = fs::read_to_string(&repo.log).unwrap();
        assert!(log.contains("alpha=alpha tip "));
        assert!(log.contains(&format!(
            "refs/heads/spr/alpha {} refs/heads/spr/alpha {}",
            repo.alpha_tip, repo.main_sha
        )));
        assert!(log.contains(&format!(
            "refs/heads/spr/beta {} refs/heads/spr/beta {}",
            repo.beta_tip, repo.alpha_tip
        )));
    }

    #[test]
    fn missing_hook_succeeds_and_writes_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_receipt(&summary);
        assert!(!summary.pre_push_hook_present);
        assert_eq!(summary.recorded_group_count, 2);
        assert_eq!(summary.reused_group_count, 0);
        assert!(summary
            .receipts
            .iter()
            .all(|receipt| receipt.path.contains("validation_receipts_v2")));
    }

    #[test]
    fn validation_fixture_overrides_machine_global_hooks_path() {
        let repo = init_validation_repo();

        assert_eq!(
            git(
                &repo.repo,
                ["config", "--local", "--get", "core.hooksPath"].as_slice()
            )
            .trim(),
            ".git/hooks"
        );
    }

    #[test]
    fn validate_from_ref_records_receipts_for_selected_source_stack() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        git(
            &repo.repo,
            ["branch", "old-stack", &repo.alpha_tip].as_slice(),
        );

        let summary = validate_stack(
            "main",
            "spr/",
            "ignore",
            "old-stack",
            &InclusiveSelector::All,
        )
        .unwrap();

        assert_eq!(summary.validated_group_count, 1);
        assert_eq!(summary.receipts[0].stable_handle, "pr:alpha");
        assert_eq!(summary.receipts[0].tip_sha, repo.alpha_tip);
    }

    #[test]
    fn relative_hooks_path_executes_the_fingerprinted_hook_for_every_boundary() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        git(&repo.repo, ["checkout", "main"].as_slice());
        git(&repo.repo, ["checkout", "-b", "relative-hooks"].as_slice());
        git(
            &repo.repo,
            ["config", "core.hooksPath", "hooksdir"].as_slice(),
        );
        let hook = repo.repo.join("hooksdir/pre-push");
        fs::create_dir_all(hook.parent().unwrap()).unwrap();
        fs::write(
            &hook,
            format!("#!/bin/sh\nprintf a >> '{}'\n", repo.log.display()),
        )
        .unwrap();
        let mut permissions = fs::metadata(&hook).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&hook, permissions).unwrap();
        git(&repo.repo, ["add", "hooksdir/pre-push"].as_slice());
        git(
            &repo.repo,
            ["commit", "-m", "alpha hook pr:alpha"].as_slice(),
        );
        fs::write(
            &hook,
            format!("#!/bin/sh\nprintf b >> '{}'\n", repo.log.display()),
        )
        .unwrap();
        git(&repo.repo, ["add", "hooksdir/pre-push"].as_slice());
        git(&repo.repo, ["commit", "-m", "beta hook pr:beta"].as_slice());

        validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_eq!(fs::read_to_string(&repo.log).unwrap(), "bb");
    }

    #[test]
    fn non_executable_hook_succeeds_and_writes_receipts_without_running_hook() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        let hook = repo.repo.join(".git/hooks/pre-push");
        fs::write(
            &hook,
            format!("#!/bin/sh\nprintf x >> '{}'\n", repo.log.display()),
        )
        .unwrap();

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_receipt(&summary);
        assert!(summary.pre_push_hook_present);
        assert!(!summary.pre_push_hook_executable);
        assert!(!repo.log.exists());
    }

    #[test]
    fn non_executable_hook_summary_says_receipts_were_recorded_without_running_hook() {
        let line = validation_status_line(&validation_summary(true, false, Vec::new()));

        assert!(line.contains("Recorded receipts"));
        assert!(line.contains("without running a pre-push hook"));
        assert!(!line.starts_with("Validated"));
    }

    #[test]
    fn validation_summary_warns_about_skipped_ignored_groups() {
        let warning = validation_skipped_groups_warning(&validation_summary(
            false,
            false,
            vec!["pr:beta".to_string()],
        ))
        .unwrap();

        assert!(warning.contains("Skipping PR groups above the ignored block"));
        assert!(warning.contains("pr:beta"));
    }

    #[test]
    fn validate_until_limits_receipts_by_ordinal_and_stable_selector() {
        let _cwd_lock = lock_cwd();
        let ordinal_repo = init_validation_repo();
        let _ordinal_cwd = DirGuard::change_to(&ordinal_repo.repo);

        let ordinal_summary =
            validate_current_stack("main", "spr/", "ignore", &first_group()).unwrap();

        assert_eq!(ordinal_summary.validated_group_count, 1);
        assert_eq!(ordinal_summary.receipts[0].stable_handle, "pr:alpha");
        drop(_ordinal_cwd);

        let selector_repo = init_validation_repo();
        let _selector_cwd = DirGuard::change_to(&selector_repo.repo);
        let selector_summary =
            validate_current_stack("main", "spr/", "ignore", &beta_group()).unwrap();

        assert_eq!(selector_summary.validated_group_count, 2);
        assert_eq!(selector_summary.receipts[1].stable_handle, "pr:beta");
    }

    #[test]
    fn appended_pr_records_only_new_boundary_and_reuses_lower_receipts() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(
            &repo,
            &format!("printf x >> '{}'\nexit 0", repo.log.display()),
        );
        validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();
        assert_eq!(fs::read_to_string(&repo.log).unwrap(), "xx");
        let gamma_tip = append_gamma(&repo);

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_eq!(fs::read_to_string(&repo.log).unwrap(), "xxx");
        assert_eq!(summary.validated_group_count, 3);
        assert_eq!(summary.recorded_group_count, 1);
        assert_eq!(summary.reused_group_count, 2);
        assert_eq!(
            summary
                .receipts
                .iter()
                .map(|receipt| receipt.action)
                .collect::<Vec<_>>(),
            vec![
                ValidationReceiptAction::Reused,
                ValidationReceiptAction::Reused,
                ValidationReceiptAction::Recorded,
            ]
        );
        assert_eq!(summary.receipts[2].stable_handle, "pr:gamma");
        assert_eq!(summary.receipts[2].tip_sha, gamma_tip);
        assert_eq!(summary.receipts[2].previous_tip_sha, repo.beta_tip);
    }

    #[test]
    fn validation_removes_ignored_artifacts_between_boundaries() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        write_file(&repo.repo, ".gitignore", "boundary-cache\n");
        git(&repo.repo, ["add", ".gitignore"].as_slice());
        git(
            &repo.repo,
            ["commit", "-m", "ignore validation cache"].as_slice(),
        );
        install_hook(
            &repo,
            "read local_ref _ _ _\ncase \"$local_ref\" in\n  refs/heads/spr/alpha) touch boundary-cache ;;\n  refs/heads/spr/beta) [ ! -e boundary-cache ] || exit 1 ;;\nesac\nexit 0",
        );

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_eq!(summary.recorded_group_count, 2);
        assert!(summary.success);
    }

    #[test]
    fn validation_rejects_multiple_origin_push_urls() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        git(
            &repo.repo,
            ["config", "--add", "remote.origin.pushurl", "first"].as_slice(),
        );
        git(
            &repo.repo,
            ["config", "--add", "remote.origin.pushurl", "second"].as_slice(),
        );
        let (merge_base, leading_ignored, groups) =
            derive_local_groups_with_ignored("main", "ignore").unwrap();
        let (groups, _) = split_groups_for_update(&leading_ignored, groups);

        let error = build_descriptors("main", "spr/", "ignore", &merge_base, &groups).unwrap_err();

        assert!(error
            .to_string()
            .contains("require exactly one origin push URL"));
        assert!(error.to_string().contains("first, second"));
    }

    #[test]
    fn successful_boundaries_are_reused_after_later_failure() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        let allow_beta = repo.repo.parent().unwrap().join("allow-beta");
        install_hook(
            &repo,
            &format!(
                "read local_ref _ _ _\nprintf '%s\\n' \"$local_ref\" >> '{}'\nif [ \"$local_ref\" = refs/heads/spr/beta ] && [ ! -f '{}' ]; then exit 1; fi\nexit 0",
                repo.log.display(),
                allow_beta.display()
            ),
        );

        let err =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap_err();
        assert!(format!("{err:#}").contains("pr:beta"));
        fs::write(&allow_beta, "").unwrap();

        let summary =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();

        assert_eq!(
            fs::read_to_string(&repo.log).unwrap(),
            "refs/heads/spr/alpha\nrefs/heads/spr/beta\nrefs/heads/spr/beta\n"
        );
        assert_eq!(summary.recorded_group_count, 1);
        assert_eq!(summary.reused_group_count, 1);
        assert_eq!(summary.receipts[0].action, ValidationReceiptAction::Reused);
        assert_eq!(
            summary.receipts[1].action,
            ValidationReceiptAction::Recorded
        );
        remove_preserved_worktree(&repo);
    }

    #[test]
    fn hook_content_change_invalidates_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(&repo, "exit 0");
        validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();
        require_matching_receipts(&descriptors(&repo)).unwrap();
        install_hook(&repo, "printf changed\\n >/dev/null\nexit 0");

        let err = require_matching_receipts(&descriptors(&repo)).unwrap_err();

        assert!(format!("{err:#}").contains("missing or stale"));
    }

    #[test]
    fn descriptor_field_changes_invalidate_receipts() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();
        let descriptors = descriptors(&repo);
        let descriptor = descriptors[1].clone();
        let mut changed_descriptors = Vec::new();

        let mut changed = descriptor.clone();
        changed.base_sha = "changed-base".to_string();
        changed_descriptors.push(changed);
        let mut changed = descriptor.clone();
        changed.origin_push_url = "changed-url".to_string();
        changed_descriptors.push(changed);
        let mut changed = descriptor.clone();
        changed.stable_handle = "pr:changed".to_string();
        changed_descriptors.push(changed);
        let mut changed = descriptor.clone();
        changed.head_branch = "spr/changed".to_string();
        changed_descriptors.push(changed);
        let mut changed = descriptor.clone();
        changed.previous_tip_sha = "changed-previous".to_string();
        changed_descriptors.push(changed);
        let mut changed = descriptor;
        changed.tip_sha = "changed-tip".to_string();
        changed_descriptors.push(changed);

        for changed in changed_descriptors {
            let err = require_matching_receipts(&[changed]).unwrap_err();
            assert!(format!("{err:#}").contains("missing or stale"));
        }
    }

    #[test]
    fn failing_hook_stops_at_first_boundary_and_preserves_worktree() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(
            &repo,
            &format!("printf x >> '{}'\nexit 1", repo.log.display()),
        );

        let err =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap_err();

        assert!(format!("{err:#}").contains("preserved worktree"));
        assert_eq!(fs::read_to_string(&repo.log).unwrap(), "x");
        remove_preserved_worktree(&repo);
    }

    #[test]
    fn tracked_hook_edits_fail_and_preserve_worktree() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(&repo, "printf reformatted >> alpha.txt\nexit 0");

        let err =
            validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap_err();

        assert!(format!("{err:#}").contains("modified tracked files"));
        remove_preserved_worktree(&repo);
    }

    #[test]
    fn required_update_blocks_before_push_without_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        let (groups, descriptors) = groups_and_descriptors(&repo);

        let err = update_no_pr(groups, UpdatePushValidation::Required(descriptors)).unwrap_err();

        assert!(format!("{err:#}").contains("missing or stale"));
        assert!(git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/alpha"].as_slice()
        )
        .trim()
        .is_empty());
    }

    #[test]
    fn required_update_uses_receipt_and_skips_push_hook() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(
            &repo,
            &format!("printf x >> '{}'\nexit 0", repo.log.display()),
        );
        validate_current_stack("main", "spr/", "ignore", &InclusiveSelector::All).unwrap();
        let validated_hook_runs = fs::read_to_string(&repo.log).unwrap();
        let (groups, descriptors) = groups_and_descriptors(&repo);

        update_no_pr(groups, UpdatePushValidation::Required(descriptors)).unwrap();

        assert_eq!(fs::read_to_string(&repo.log).unwrap(), validated_hook_runs);
    }

    #[test]
    fn required_partial_prefix_update_uses_lower_receipt_without_higher_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        validate_current_stack("main", "spr/", "ignore", &first_group()).unwrap();
        let (groups, descriptors) = groups_and_descriptors(&repo);

        update_no_pr(
            groups[..1].to_vec(),
            UpdatePushValidation::Required(descriptors[..1].to_vec()),
        )
        .unwrap();

        assert!(!git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/alpha"].as_slice()
        )
        .trim()
        .is_empty());
        assert!(git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/beta"].as_slice()
        )
        .trim()
        .is_empty());
    }

    #[test]
    fn required_full_update_rejects_missing_higher_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        validate_current_stack("main", "spr/", "ignore", &first_group()).unwrap();
        let (groups, descriptors) = groups_and_descriptors(&repo);

        let err = update_no_pr(groups, UpdatePushValidation::Required(descriptors)).unwrap_err();

        assert!(format!("{err:#}").contains("pr:beta"));
        assert!(git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/alpha"].as_slice()
        )
        .trim()
        .is_empty());
    }

    #[test]
    fn legacy_update_preserves_push_hook_execution() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(&repo, "exit 1");
        let (groups, _) = groups_and_descriptors(&repo);

        let err = update_no_pr(groups, UpdatePushValidation::Legacy).unwrap_err();

        assert!(format!("{err:#}").contains("git"));
    }

    #[test]
    fn skipped_validation_bypasses_receipts_and_push_hooks() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        install_hook(&repo, "exit 1");
        let (groups, _) = groups_and_descriptors(&repo);

        update_no_pr(groups, UpdatePushValidation::Skip).unwrap();

        assert!(!git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/beta"].as_slice()
        )
        .trim()
        .is_empty());
    }

    #[test]
    fn required_update_without_branch_changes_does_not_need_receipt() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        let (groups, _) = groups_and_descriptors(&repo);
        update_no_pr(groups, UpdatePushValidation::Skip).unwrap();
        let (groups, descriptors) = groups_and_descriptors(&repo);

        update_no_pr(groups, UpdatePushValidation::Required(descriptors)).unwrap();
    }

    #[test]
    fn skipped_validation_bypasses_push_hooks_for_force_updates() {
        let _cwd_lock = lock_cwd();
        let repo = init_validation_repo();
        let _cwd = DirGuard::change_to(&repo.repo);
        let (groups, _) = groups_and_descriptors(&repo);
        update_no_pr(groups, UpdatePushValidation::Skip).unwrap();
        git(&repo.repo, ["reset", "--hard", "HEAD~1"].as_slice());
        commit_file(
            &repo.repo,
            "beta.txt",
            "rewritten beta\n",
            "beta rewritten pr:beta",
        );
        install_hook(&repo, "exit 1");
        let (groups, _) = groups_and_descriptors(&repo);

        update_no_pr(groups, UpdatePushValidation::Skip).unwrap();

        let remote_tip = git(
            &repo.repo,
            ["ls-remote", "--heads", "origin", "spr/beta"].as_slice(),
        )
        .split_whitespace()
        .next()
        .unwrap()
        .to_string();
        let local_tip = git(&repo.repo, ["rev-parse", "HEAD"].as_slice())
            .trim()
            .to_string();
        assert_eq!(remote_tip, local_tip);
    }
}
