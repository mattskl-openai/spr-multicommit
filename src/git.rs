//! Thin wrappers around `git`/`gh` commands plus repository-specific helpers.
//!
//! This module centralizes command execution, dry-run logging, and small
//! normalization utilities used across commands. When no base branch is
//! configured, callers rely on [`discover_origin_head_base`] to resolve the
//! default base via `origin/HEAD`.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::process::{Command, Stdio};
use tracing::{error, info};

pub fn ensure_tool(name: &str) -> Result<()> {
    let status = Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("{} not found in PATH", name))?;
    if !status.success() {
        bail!("{} appears to be installed but not runnable", name);
    }
    Ok(())
}

/* ------------------ command runners ------------------ */

pub fn git_ro(args: &[&str]) -> Result<String> {
    if std::env::var_os("SPR_DRY_RUN").is_some() {
        info!("DRY-RUN: git {}", shellish(args));
    }
    verbose_log_cmd("git", args);
    run("git", args)
}

pub fn git_rw(dry: bool, args: &[&str]) -> Result<String> {
    if dry {
        // Allow executing safe local ops in dry-run to mimic real flow closely
        let mut idx = 0;
        let mut in_tmp = false;
        if let Some(first) = args.first() {
            if *first == "-C" && args.len() >= 2 {
                idx = 2;
                in_tmp = args[1].starts_with("/tmp/spr-");
            }
        }
        let sub = args.get(idx).copied().unwrap_or("");
        let is_push = sub == "push";
        let is_worktree = sub == "worktree";
        let allow = (in_tmp && !is_push) || is_worktree;
        if allow {
            info!("DRY-RUN (exec): git {}", shellish(args));
            return run("git", args);
        }
        info!("DRY-RUN: git {}", shellish(args));
        return Ok(String::new());
    }
    verbose_log_cmd("git", args);
    run("git", args)
}

pub fn gh_ro(args: &[&str]) -> Result<String> {
    if std::env::var_os("SPR_DRY_RUN").is_some() {
        info!("DRY-RUN: gh {}", shellish(args));
    }
    verbose_log_cmd("gh", args);
    run("gh", args)
}

pub fn gh_rw(dry: bool, args: &[&str]) -> Result<String> {
    if dry {
        let printable = if args.contains(&"--body") {
            let mut v = args.to_vec();
            if let Some(i) = v.iter().position(|a| *a == "--body") {
                if i + 1 < v.len() {
                    v[i + 1] = "<elided-body>";
                }
            }
            v
        } else {
            args.to_vec()
        };
        info!("DRY-RUN: gh {}", shellish(&printable));
        Ok(String::new())
    } else {
        verbose_log_cmd("gh", args);
        run("gh", args)
    }
}

pub fn run(bin: &str, args: &[&str]) -> Result<String> {
    let out = Command::new(bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to spawn {}", bin))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        error!(
            "{} {:?} failed\nstdout:\n{}\nstderr:\n{}",
            bin, args, stdout, stderr
        );
        bail!("command failed: {} {:?}", bin, args);
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

pub fn shellish(args: &[&str]) -> String {
    args.iter()
        .map(|a| {
            if a.chars()
                .any(|c| c.is_whitespace() || c == '"' || c == '\'')
            {
                format!("{:?}", a)
            } else {
                a.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn sanitize_gh_base_ref(base: &str) -> String {
    if let Some(stripped) = base.strip_prefix("origin/") {
        return stripped.to_string();
    }
    base.to_string()
}

pub fn normalize_branch_name(name: &str) -> String {
    let mut out = name.strip_prefix("refs/heads/").unwrap_or(name);
    out = out.strip_prefix("origin/").unwrap_or(out);
    out.to_string()
}

pub fn repo_root() -> Result<Option<String>> {
    match git_ro(["rev-parse", "--show-toplevel"].as_slice()) {
        Ok(path) => Ok(Some(path.trim().to_string())),
        Err(_) => Ok(None),
    }
}

/// Discover the repository's default branch via `origin/HEAD`.
///
/// This runs `git symbolic-ref --short refs/remotes/origin/HEAD` and expects
/// output like `origin/main`. If `origin/HEAD` is unset or the command fails,
/// callers should surface the error loudly and instruct users to set `base`
/// explicitly in `.spr_multicommit_cfg.yml`.
///
/// This helper assumes the primary remote is named `origin` and that the
/// local `origin/HEAD` symbolic ref is up to date. In repositories that use a
/// different remote name or do not track `origin/HEAD`, explicit configuration
/// is more reliable than discovery.
pub fn discover_origin_head_base() -> Result<String> {
    let out = git_ro(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"].as_slice())
        .with_context(|| {
            "failed to discover default branch from origin/HEAD; set `base` in .spr_multicommit_cfg.yml or run `git remote set-head origin -a`"
        })?;
    let base = out.trim();
    if base.is_empty() {
        bail!(
            "origin/HEAD resolved to an empty ref; set `base` in .spr_multicommit_cfg.yml or run `git remote set-head origin -a`"
        );
    }
    Ok(base.to_string())
}

pub fn verbose_log_cmd(tool: &str, args: &[&str]) {
    if std::env::var_os("SPR_VERBOSE").is_some() {
        info!("{} {}", tool, shellish(args));
    }
}

pub fn to_remote_ref(name: &str) -> String {
    let name = name.strip_prefix("refs/heads/").unwrap_or(name);
    let name = name.strip_prefix("origin/").unwrap_or(name);
    format!("origin/{}", name)
}

pub fn get_remote_branches_sha(branches: &[String]) -> Result<HashMap<String, String>> {
    let mut out_map: HashMap<String, String> = HashMap::new();
    if branches.is_empty() {
        return Ok(out_map);
    }
    let mut args: Vec<&str> = vec!["ls-remote", "--heads", "origin"];
    let owned: Vec<String> = branches.iter().map(|b| b.to_string()).collect();
    let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    args.extend(refs);
    let out = git_ro(&args)?;
    for line in out.lines() {
        let mut parts = line.split_whitespace();
        let sha = parts.next().unwrap_or("").trim();
        let r = parts.next().unwrap_or("").trim();
        if sha.is_empty() || r.is_empty() {
            continue;
        }
        let name = r.strip_prefix("refs/heads/").unwrap_or(r).to_string();
        out_map.insert(name, sha.to_string());
    }
    Ok(out_map)
}

pub fn git_is_ancestor(ancestor: &str, descendant: &str) -> Result<bool> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .with_context(|| "failed to run git merge-base --is-ancestor")?;
    Ok(status.success())
}

pub fn list_remote_branches_with_prefix(prefix: &str) -> Result<Vec<String>> {
    // List all remote heads and filter by prefix
    let out = git_ro(["ls-remote", "--heads", "origin"].as_slice())?;
    let mut names: Vec<String> = vec![];
    for line in out.lines() {
        let mut parts = line.split_whitespace();
        let _sha = parts.next().unwrap_or("").trim();
        let r = parts.next().unwrap_or("").trim();
        if r.is_empty() {
            continue;
        }
        let name = r.strip_prefix("refs/heads/").unwrap_or(r).to_string();
        if name.starts_with(prefix) {
            names.push(name);
        }
    }
    Ok(names)
}
