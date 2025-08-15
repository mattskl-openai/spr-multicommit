use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::process::Command;
use tracing::{error, info};

pub fn ensure_tool(name: &str) -> Result<()> {
    let status = Command::new(name)
        .arg("--version")
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
        if let Some(first) = args.get(0) {
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

pub fn safe_checkout_reset(dry: bool, branch: &str, start_point: &str) -> Result<()> {
    // If branch exists, back it up to avoid losing local commits
    let exists = git_ro(["rev-parse", "--verify", branch].as_slice()).is_ok();
    if exists {
        let sha = git_ro(["rev-parse", branch].as_slice())?.trim().to_string();
        let backup = format!(
            "spr-backup/{}-{}",
            branch.replace('/', "_"),
            &sha[..8.min(sha.len())]
        );
        info!("Backing up existing branch {} to {}", branch, backup);
        if !dry {
            git_ro(["branch", &backup, branch].as_slice())?;
        }
    }
    git_rw(dry, ["checkout", "-B", branch, start_point].as_slice())?;
    Ok(())
}

pub fn verbose_log_cmd(tool: &str, args: &[&str]) {
    if std::env::var_os("SPR_VERBOSE").is_some() {
        info!("{} {}", tool, shellish(args));
    }
}

pub fn git_rw_in(dry: bool, dir: &str, args: &[&str]) -> Result<String> {
    let mut v: Vec<String> = Vec::with_capacity(args.len() + 2);
    v.push("-C".to_string());
    v.push(dir.to_string());
    for a in args {
        v.push((*a).to_string());
    }
    let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
    git_rw(dry, &refs)
}

pub fn git_ro_in(dir: &str, args: &[&str]) -> Result<String> {
    let mut v: Vec<String> = Vec::with_capacity(args.len() + 2);
    v.push("-C".to_string());
    v.push(dir.to_string());
    for a in args {
        v.push((*a).to_string());
    }
    let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
    git_ro(&refs)
}

pub fn to_remote_ref(name: &str) -> String {
    let name = name.strip_prefix("refs/heads/").unwrap_or(name);
    let name = name.strip_prefix("origin/").unwrap_or(name);
    format!("origin/{}", name)
}

pub fn get_remote_branch_sha(branch: &str) -> Result<Option<String>> {
    let out = git_ro(["ls-remote", "--heads", "origin", branch].as_slice())?;
    let sha = out.split_whitespace().next().unwrap_or("").trim();
    if sha.is_empty() {
        Ok(None)
    } else {
        Ok(Some(sha.to_string()))
    }
}

pub fn get_remote_branches_sha(branches: &Vec<String>) -> Result<HashMap<String, String>> {
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

pub fn git_is_ancestor_in(dir: &str, ancestor: &str, descendant: &str) -> Result<bool> {
    let status = Command::new("git")
        .args([
            "-C",
            dir,
            "merge-base",
            "--is-ancestor",
            ancestor,
            descendant,
        ])
        .status()
        .with_context(|| format!("failed to run git -C {} merge-base --is-ancestor", dir))?;
    Ok(status.success())
}

pub fn git_is_ancestor(ancestor: &str, descendant: &str) -> Result<bool> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .with_context(|| "failed to run git merge-base --is-ancestor")?;
    Ok(status.success())
}
