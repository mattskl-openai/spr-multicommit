//! Repository and user configuration for `spr`.
//!
//! Configuration is loaded from `$HOME/.spr_multicommit_cfg.yml` and then
//! overridden by `<git-main-worktree-root>/.spr_multicommit_cfg.yml` when present.

use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum PrDescriptionMode {
    /// Overwrite the entire PR body from commit messages + stack block.
    Overwrite,
    /// Only update the stack block; preserve the rest of the PR body.
    StackOnly,
}

/// Opt-in policy for keeping local per-PR branches aligned with stack group tips.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum LocalPrBranchSyncPolicy {
    /// Preserve existing behavior: do not create or move local per-PR branches.
    Off,
    /// Move local per-PR branches that already exist, but do not create missing branches.
    UpdateExisting,
    /// Create missing local per-PR branches and move existing ones.
    CreateOrUpdate,
}

/// Behavior when `spr restack` encounters a cherry-pick conflict.
///
/// This is YAML-deserializable and avoids stringly-typed policy handling.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RestackConflictPolicy {
    /// Abort and clean up temp restack state.
    Rollback,
    /// Suspend, leave the temp worktree in place, and resume with `spr resume`.
    Halt,
}

/// Behavior when a branch-rewriting command sees local changes.
///
/// This applies to commands that rebuild the checked-out branch and then move
/// it to a rewritten tip, such as `spr restack`, `spr move`, `spr fix-pr`, and
/// `spr absorb`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DirtyWorktreePolicy {
    /// Preserve the historical behavior: proceed and let the rewrite replace
    /// tracked changes in the checked-out worktree.
    Discard,
    /// Stash tracked, staged, and untracked changes before the rewrite, then
    /// reapply them with `git stash apply --index`.
    Stash,
    /// Refuse to rewrite until the worktree is clean.
    Halt,
}

/// How `spr update` handles pre-push validation.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateValidationPolicy {
    /// Preserve Git's normal pre-push hook behavior.
    Legacy,
    /// Require matching `spr validate` receipts and skip push-time hooks.
    Required,
}

/// Output ordering for list-style displays.
///
/// The local stack order remains bottom-up and continues to define local PR numbers and
/// commit indices; this enum only selects which entries are shown first. If a caller
/// repurposes this for stack ordering, users will see renumbered or shuffled output that
/// no longer matches the underlying commit chain.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListOrder {
    /// Most recent items appear at the top (reverse of local stack order).
    RecentOnTop,
    /// Most recent items appear at the bottom (local stack order).
    RecentOnBottom,
}

impl ListOrder {
    /// Return 0-based group indices in the configured display order.
    ///
    /// The indices always refer to the local stack ordering (bottom-up). This means
    /// `RecentOnTop` simply reverses iteration without renumbering. If a caller uses these
    /// indices to renumber local PRs or commit indices, the visible order will disagree
    /// with the bottom-up numbering.
    pub fn display_indices(self, len: usize) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..len).collect();
        if self == ListOrder::RecentOnTop {
            indices.reverse();
        }
        indices
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    /// Root base branch for the stack, e.g. `origin/main`.
    ///
    /// When this is `None` and the CLI does not supply `--base`, the caller
    /// attempts to discover the base via `origin/HEAD` and will error loudly if
    /// discovery fails.
    pub base: Option<String>,
    pub prefix: Option<String>,
    pub land: Option<String>,
    /// Optional `pr:<tag>` value that starts an ignore block during group parsing.
    pub ignore_tag: Option<String>,
    /// How `spr update` should manage PR descriptions from commit messages.
    pub pr_description_mode: Option<PrDescriptionMode>,
    /// Order for printing PR/commit lists and update progress output.
    pub list_order: Option<ListOrder>,
    /// Whether to synchronize local per-PR branches named like synthetic PR branches.
    pub local_pr_branches: Option<LocalPrBranchSyncPolicy>,
    /// Behavior when `spr restack` encounters a cherry-pick conflict.
    ///
    /// Supported values:
    /// - `halt` (default): suspend, leave the temp worktree in place, and use `spr resume`
    /// - `rollback`: abort and clean up temp restack state
    pub restack_conflict: Option<RestackConflictPolicy>,
    /// Behavior when a branch-rewriting command sees local changes.
    ///
    /// Supported values:
    /// - `discard`: preserve current behavior and continue the rewrite
    /// - `stash`: stash tracked, staged, and untracked changes and reapply them
    /// - `halt`: refuse to rewrite until the worktree is clean
    pub dirty_worktree: Option<DirtyWorktreePolicy>,
    /// Block `spr update` from recreating a PR when the same branch name had a recently merged
    /// or closed PR within this many days.
    ///
    /// `0` effectively disables the guard for past terminal PRs.
    pub branch_reuse_guard_days: Option<u32>,
    /// How `spr update` handles explicit per-PR validation receipts.
    pub update_validation: Option<UpdateValidationPolicy>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: String,
    pub prefix: String,
    pub land: String,
    /// Optional `pr:<tag>` value that starts an ignore block during group parsing.
    pub ignore_tag: String,
    /// How `spr update` should manage PR descriptions from commit messages.
    pub pr_description_mode: PrDescriptionMode,
    /// Order for printing PR/commit lists and update progress output.
    pub list_order: ListOrder,
    /// Whether to synchronize local per-PR branches named like synthetic PR branches.
    pub local_pr_branches: LocalPrBranchSyncPolicy,
    /// Behavior when `spr restack` encounters a cherry-pick conflict.
    pub restack_conflict: RestackConflictPolicy,
    /// Behavior when a branch-rewriting command sees local changes.
    pub dirty_worktree: DirtyWorktreePolicy,
    /// Threshold in days for blocking `spr update` from recreating a PR on a branch name that
    /// already had a recently merged or closed PR.
    ///
    /// `0` effectively disables the guard for past terminal PRs.
    pub branch_reuse_guard_days: u32,
    /// How `spr update` handles explicit per-PR validation receipts.
    pub update_validation: UpdateValidationPolicy,
}

/// Normalize a configured branch prefix and reject values outside the ASCII-only conflict domain.
pub fn normalize_prefix(prefix: &str) -> Result<String> {
    let mut normalized = prefix.trim_end_matches('/').to_string();
    if !normalized.is_ascii() {
        return Err(anyhow!(
            "Branch prefix must be ASCII because synthetic branch conflict checks only support ASCII prefixes: {:?}",
            prefix
        ));
    }
    normalized.push('/');
    Ok(normalized)
}

fn read_config_file(path: &PathBuf) -> Result<Option<FileConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    let cfg: FileConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
    Ok(Some(cfg))
}

fn default_config() -> Config {
    let user = std::env::var("USER").unwrap_or_else(|_| "".to_string());
    Config {
        base: String::new(),
        prefix: format!("{}-spr/", user),
        land: "flatten".to_string(),
        ignore_tag: "ignore".to_string(),
        pr_description_mode: PrDescriptionMode::Overwrite,
        list_order: ListOrder::RecentOnTop,
        local_pr_branches: LocalPrBranchSyncPolicy::Off,
        restack_conflict: RestackConflictPolicy::Halt,
        dirty_worktree: DirtyWorktreePolicy::Halt,
        branch_reuse_guard_days: 180,
        update_validation: UpdateValidationPolicy::Legacy,
    }
}

fn apply_overrides(config: &Config, overrides: FileConfig) -> Config {
    let mut merged = config.clone();
    if let Some(base) = overrides.base {
        merged.base = base;
    }
    if let Some(prefix) = overrides.prefix {
        merged.prefix = prefix;
    }
    if let Some(land) = overrides.land {
        merged.land = land;
    }
    if let Some(ignore_tag) = overrides.ignore_tag {
        merged.ignore_tag = ignore_tag;
    }
    if let Some(pr_description_mode) = overrides.pr_description_mode {
        merged.pr_description_mode = pr_description_mode;
    }
    if let Some(list_order) = overrides.list_order {
        merged.list_order = list_order;
    }
    if let Some(local_pr_branches) = overrides.local_pr_branches {
        merged.local_pr_branches = local_pr_branches;
    }
    if let Some(restack_conflict) = overrides.restack_conflict {
        merged.restack_conflict = restack_conflict;
    }
    if let Some(dirty_worktree) = overrides.dirty_worktree {
        merged.dirty_worktree = dirty_worktree;
    }
    if let Some(branch_reuse_guard_days) = overrides.branch_reuse_guard_days {
        merged.branch_reuse_guard_days = branch_reuse_guard_days;
    }
    if let Some(update_validation) = overrides.update_validation {
        merged.update_validation = update_validation;
    }
    merged
}

fn normalize_config(config: &mut Config) -> Result<()> {
    config.prefix = normalize_prefix(&config.prefix)?;
    if config.ignore_tag.trim().is_empty() {
        config.ignore_tag = "ignore".to_string();
    }
    Ok(())
}

pub fn load_config() -> Result<Config> {
    // Home config
    let mut merged = default_config();
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".spr_multicommit_cfg.yml");
        if let Some(home_cfg) = read_config_file(&p)? {
            merged = apply_overrides(&merged, home_cfg);
        }
    }

    // Repo config overrides home
    if let Some(root) = crate::git::main_worktree_root()? {
        let mut p = PathBuf::from(root);
        p.push(".spr_multicommit_cfg.yml");
        if let Some(repo_cfg) = read_config_file(&p)? {
            merged = apply_overrides(&merged, repo_cfg);
        }
    }

    normalize_config(&mut merged)?;
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_overrides, default_config, load_config, normalize_config, normalize_prefix,
        read_config_file, DirtyWorktreePolicy, FileConfig, LocalPrBranchSyncPolicy,
        PrDescriptionMode, RestackConflictPolicy, UpdateValidationPolicy,
    };
    use crate::test_support::{git, init_repo, lock_cwd, DirGuard};
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    struct EnvVarGuard {
        key: &'static str,
        old: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: String) -> Self {
            let old = env::var(key).ok();
            env::set_var(key, value);
            Self { key, old }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(old) = &self.old {
                env::set_var(self.key, old);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn add_linked_worktree(repo: &Path) -> (tempfile::TempDir, PathBuf) {
        let linked_parent = tempdir().unwrap();
        let linked_path = linked_parent.path().join("linked-worktree");
        git(repo, ["branch", "linked-config-test", "HEAD"].as_slice());
        git(
            repo,
            [
                "worktree",
                "add",
                linked_path.to_str().unwrap(),
                "linked-config-test",
            ]
            .as_slice(),
        );
        (linked_parent, linked_path)
    }

    #[test]
    fn read_config_file_allows_yaml_comments() {
        let dir = tempdir().expect("tempdir");
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(
            &path,
            r#"
# top-level comment
base: origin/main # trailing comment
pr_description_mode: stack_only
"#,
        )
        .expect("write config");

        let cfg = read_config_file(&path)
            .expect("parse config")
            .expect("config exists");
        assert_eq!(cfg.base.as_deref(), Some("origin/main"));
        assert_eq!(cfg.pr_description_mode, Some(PrDescriptionMode::StackOnly));
    }

    #[test]
    fn read_config_file_rejects_unknown_key() {
        let dir = tempdir().expect("tempdir");
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(
            &path,
            r#"
overwrite_pr_description: false
"#,
        )
        .expect("write config");

        let err = read_config_file(&path).expect_err("unknown key should fail");
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("unknown field `overwrite_pr_description`"),
            "unexpected error: {err_msg}"
        );
        assert!(
            err_msg.contains(path.to_string_lossy().as_ref()),
            "error should include config path: {err_msg}"
        );
    }

    #[test]
    fn read_config_file_parses_dirty_worktree_policy() {
        let dir = tempdir().expect("tempdir");
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(
            &path,
            r#"
dirty_worktree: stash
"#,
        )
        .expect("write config");

        let cfg = read_config_file(&path)
            .expect("parse config")
            .expect("config exists");
        assert_eq!(cfg.dirty_worktree, Some(DirtyWorktreePolicy::Stash));
    }

    #[test]
    fn read_config_file_parses_restack_conflict_policy() {
        let dir = tempdir().expect("tempdir");
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(
            &path,
            r#"
restack_conflict: rollback
"#,
        )
        .expect("write config");

        let cfg = read_config_file(&path)
            .expect("parse config")
            .expect("config exists");
        assert_eq!(cfg.restack_conflict, Some(RestackConflictPolicy::Rollback));
    }

    #[test]
    fn default_config_uses_halt_for_restack_conflict_policy() {
        let cfg = default_config();

        assert_eq!(cfg.restack_conflict, RestackConflictPolicy::Halt);
    }

    #[test]
    fn default_config_uses_halt_for_dirty_worktree_policy() {
        let cfg = default_config();

        assert_eq!(cfg.dirty_worktree, DirtyWorktreePolicy::Halt);
    }

    #[test]
    fn default_config_leaves_local_pr_branch_sync_off() {
        let cfg = default_config();

        assert_eq!(cfg.local_pr_branches, LocalPrBranchSyncPolicy::Off);
    }

    #[test]
    fn load_config_reads_main_worktree_repo_config_from_linked_worktree() {
        let _lock = lock_cwd();
        let home = tempdir().unwrap();
        let _home_guard = EnvVarGuard::set("HOME", home.path().display().to_string());
        let repo_dir = init_repo();
        let repo = repo_dir.path().to_path_buf();
        fs::write(
            repo.join(".spr_multicommit_cfg.yml"),
            "base: origin/release-candidate\n",
        )
        .unwrap();
        let (_linked_parent, linked_path) = add_linked_worktree(&repo);
        fs::write(
            linked_path.join(".spr_multicommit_cfg.yml"),
            "base: origin/linked-worktree-only\n",
        )
        .unwrap();
        let _guard = DirGuard::change_to(&linked_path);

        let cfg = load_config().unwrap();

        assert_eq!(cfg.base, "origin/release-candidate");
    }

    #[test]
    fn load_config_ignores_linked_worktree_repo_config_without_main_repo_config() {
        let _lock = lock_cwd();
        let home = tempdir().unwrap();
        let _home_guard = EnvVarGuard::set("HOME", home.path().display().to_string());
        let repo_dir = init_repo();
        let repo = repo_dir.path().to_path_buf();
        let (_linked_parent, linked_path) = add_linked_worktree(&repo);
        fs::write(
            linked_path.join(".spr_multicommit_cfg.yml"),
            "base: origin/linked-worktree-only\n",
        )
        .unwrap();
        let _guard = DirGuard::change_to(&linked_path);

        let cfg = load_config().unwrap();

        assert_eq!(cfg.base, "");
    }

    #[test]
    fn default_config_preserves_legacy_update_validation() {
        let cfg = default_config();

        assert_eq!(cfg.update_validation, UpdateValidationPolicy::Legacy);
    }

    #[test]
    fn read_config_file_parses_required_update_validation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".spr_multicommit_cfg.yml");
        fs::write(&path, "update_validation: required\n").unwrap();

        let cfg = read_config_file(&path).unwrap().unwrap();
        assert_eq!(
            cfg.update_validation,
            Some(UpdateValidationPolicy::Required)
        );
    }

    #[test]
    fn apply_overrides_updates_validation_policy() {
        let merged = apply_overrides(
            &default_config(),
            FileConfig {
                base: None,
                prefix: None,
                land: None,
                ignore_tag: None,
                pr_description_mode: None,
                list_order: None,
                local_pr_branches: None,
                restack_conflict: None,
                dirty_worktree: None,
                branch_reuse_guard_days: None,
                update_validation: Some(UpdateValidationPolicy::Required),
            },
        );

        assert_eq!(merged.update_validation, UpdateValidationPolicy::Required);
    }

    #[test]
    fn read_config_file_parses_local_pr_branch_sync_policy() {
        let dir = tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(&path, "local_pr_branches: create-or-update\n").unwrap();

        let cfg = read_config_file(&path).unwrap().unwrap();
        assert_eq!(
            cfg.local_pr_branches,
            Some(LocalPrBranchSyncPolicy::CreateOrUpdate)
        );
    }

    #[test]
    // Verifies: YAML config parsing accepts an integer value for branch_reuse_guard_days.
    // Catches: regressions where the new config key is rejected or parsed with the wrong type.
    fn read_config_file_parses_branch_reuse_guard_days_integer() {
        let dir = tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push(".spr_multicommit_cfg.yml");
        fs::write(&path, "branch_reuse_guard_days: 0\n").unwrap();

        let cfg = read_config_file(&path).unwrap().unwrap();
        assert_eq!(cfg.branch_reuse_guard_days, Some(0));
    }

    #[test]
    // Verifies: file-config overrides replace the default branch reuse guard threshold.
    // Catches: regressions where the new config key is ignored during config merge.
    fn apply_overrides_updates_branch_reuse_guard_days() {
        let base = default_config();
        let merged = apply_overrides(
            &base,
            FileConfig {
                base: None,
                prefix: None,
                land: None,
                ignore_tag: None,
                pr_description_mode: None,
                list_order: None,
                local_pr_branches: None,
                restack_conflict: None,
                dirty_worktree: None,
                branch_reuse_guard_days: Some(30),
                update_validation: None,
            },
        );

        assert_eq!(merged.branch_reuse_guard_days, 30);
    }

    #[test]
    fn apply_overrides_updates_local_pr_branch_sync_policy() {
        let merged = apply_overrides(
            &default_config(),
            FileConfig {
                base: None,
                prefix: None,
                land: None,
                ignore_tag: None,
                pr_description_mode: None,
                list_order: None,
                local_pr_branches: Some(LocalPrBranchSyncPolicy::UpdateExisting),
                restack_conflict: None,
                dirty_worktree: None,
                branch_reuse_guard_days: None,
                update_validation: None,
            },
        );

        assert_eq!(
            merged.local_pr_branches,
            LocalPrBranchSyncPolicy::UpdateExisting
        );
    }

    #[test]
    fn normalize_config_rejects_non_ascii_prefix() {
        let mut cfg = default_config();
        cfg.prefix = "dank-spr/".to_string();
        normalize_config(&mut cfg).unwrap();

        cfg.prefix = "dänk-spr".to_string();
        let err = normalize_config(&mut cfg).unwrap_err();

        assert!(err.to_string().contains("Branch prefix must be ASCII"));
    }

    #[test]
    fn normalize_prefix_adds_one_trailing_slash() {
        assert_eq!(normalize_prefix("dank-spr").unwrap(), "dank-spr/");
        assert_eq!(normalize_prefix("dank-spr/").unwrap(), "dank-spr/");
    }
}
