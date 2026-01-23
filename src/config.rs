use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Default, Deserialize, Clone)]
pub struct FileConfig {
    pub base: Option<String>,
    pub prefix: Option<String>,
    pub land: Option<String>,
    /// Optional `pr:<tag>` value that starts an ignore block during group parsing.
    pub ignore_tag: Option<String>,
    /// Whether `spr update` should overwrite PR descriptions from commit messages.
    pub overwrite_pr_description: Option<bool>,
}

fn read_config_file(path: &PathBuf) -> Result<Option<FileConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)?;
    let cfg: FileConfig = serde_yaml::from_str(&content)?;
    Ok(Some(cfg))
}

pub fn load_config() -> Result<FileConfig> {
    // Home config
    let mut merged = FileConfig::default();
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".spr_multicommit_cfg.yml");
        if let Some(home_cfg) = read_config_file(&p)? {
            if let Some(b) = home_cfg.base {
                merged.base = Some(b);
            }
            if let Some(pfx) = home_cfg.prefix {
                merged.prefix = Some(pfx);
            }
            if let Some(mode) = home_cfg.land {
                merged.land = Some(mode);
            }
            if let Some(ignore_tag) = home_cfg.ignore_tag {
                merged.ignore_tag = Some(ignore_tag);
            }
            if let Some(overwrite_pr_description) = home_cfg.overwrite_pr_description {
                merged.overwrite_pr_description = Some(overwrite_pr_description);
            }
        }
    }

    // Repo config overrides home
    if let Ok(Some(root)) = crate::git::repo_root() {
        let mut p = PathBuf::from(root);
        p.push(".spr_multicommit_cfg.yml");
        if let Some(repo_cfg) = read_config_file(&p)? {
            if repo_cfg.base.is_some() {
                merged.base = repo_cfg.base;
            }
            if repo_cfg.prefix.is_some() {
                merged.prefix = repo_cfg.prefix;
            }
            if repo_cfg.land.is_some() {
                merged.land = repo_cfg.land;
            }
            if repo_cfg.ignore_tag.is_some() {
                merged.ignore_tag = repo_cfg.ignore_tag;
            }
            if repo_cfg.overwrite_pr_description.is_some() {
                merged.overwrite_pr_description = repo_cfg.overwrite_pr_description;
            }
        }
    }

    Ok(merged)
}
