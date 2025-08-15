use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Default, Deserialize, Clone)]
pub struct FileConfig {
    pub base: Option<String>,
    pub prefix: Option<String>,
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
            if let Some(b) = home_cfg.base { merged.base = Some(b); }
            if let Some(pfx) = home_cfg.prefix { merged.prefix = Some(pfx); }
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
        }
    }

    Ok(merged)
}


