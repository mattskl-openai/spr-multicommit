//! Repository and user configuration for `spr`.
//!
//! Configuration is loaded from `$HOME/.spr_multicommit_cfg.yml` and then
//! overridden by `<repo-root>/.spr_multicommit_cfg.yml` when present.

use anyhow::Result;
use clap::ValueEnum;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum PrDescriptionMode {
    /// Overwrite the entire PR body from commit messages + stack block.
    Overwrite,
    /// Only update the stack block; preserve the rest of the PR body.
    StackOnly,
}

/// Behavior when `spr restack` encounters a cherry-pick conflict.
///
/// This is YAML-deserializable and avoids stringly-typed policy handling.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RestackConflictPolicy {
    /// Abort and clean up temp restack state (default).
    Rollback,
    /// Halt and leave the temp worktree/branch for manual resolution.
    Halt,
}

impl Default for RestackConflictPolicy {
    fn default() -> Self {
        Self::Rollback
    }
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
    /// Behavior when `spr restack` encounters a cherry-pick conflict.
    ///
    /// Supported values:
    /// - `rollback` (default): abort and clean up temp restack state
    /// - `halt`: stop and leave the temp worktree for manual resolution
    pub restack_conflict: Option<RestackConflictPolicy>,
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
    /// Behavior when `spr restack` encounters a cherry-pick conflict.
    pub restack_conflict: RestackConflictPolicy,
}

fn read_config_file(path: &PathBuf) -> Result<Option<FileConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)?;
    let cfg: FileConfig = serde_yaml::from_str(&content)?;
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
        restack_conflict: RestackConflictPolicy::Rollback,
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
    if let Some(restack_conflict) = overrides.restack_conflict {
        merged.restack_conflict = restack_conflict;
    }
    merged
}

fn normalize_config(config: &mut Config) {
    let mut prefix = config.prefix.trim_end_matches('/').to_string();
    prefix.push('/');
    config.prefix = prefix;
    if config.ignore_tag.trim().is_empty() {
        config.ignore_tag = "ignore".to_string();
    }
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
    if let Ok(Some(root)) = crate::git::repo_root() {
        let mut p = PathBuf::from(root);
        p.push(".spr_multicommit_cfg.yml");
        if let Some(repo_cfg) = read_config_file(&p)? {
            merged = apply_overrides(&merged, repo_cfg);
        }
    }

    normalize_config(&mut merged);
    Ok(merged)
}
