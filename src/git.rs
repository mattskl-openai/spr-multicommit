//! Thin wrappers around `git`/`gh` commands plus repository-specific helpers.
//!
//! This module centralizes command execution, dry-run logging, and small
//! normalization utilities used across commands. When no base branch is
//! configured, callers rely on [`discover_origin_head_base`] to resolve the
//! default base via `origin/HEAD`.

use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::io::Write;
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
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let stderr = dedupe_prefixed_lines(bin, &stderr);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        error!(
            "{} {:?} failed\nstdout:\n{}\nstderr:\n{}",
            bin, args, stdout, stderr
        );
        bail!(
            "command failed: {} {:?}\nstdout:\n{}\nstderr:\n{}",
            bin,
            args,
            stdout,
            stderr
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn dedupe_prefixed_lines(bin: &str, stderr: &str) -> String {
    let prefix = format!("{}:", bin);
    let prefix_sp = format!("{}: ", bin);
    let mut prefixed: HashSet<String> = HashSet::new();
    for line in stderr.lines() {
        if let Some(rest) = line
            .strip_prefix(&prefix_sp)
            .or_else(|| line.strip_prefix(&prefix))
        {
            prefixed.insert(rest.trim().to_string());
        }
    }
    if prefixed.is_empty() {
        return stderr.to_string();
    }
    let mut out: Vec<&str> = Vec::new();
    for line in stderr.lines() {
        if line
            .strip_prefix(&prefix_sp)
            .or_else(|| line.strip_prefix(&prefix))
            .is_some()
        {
            out.push(line);
            continue;
        }
        if prefixed.contains(line.trim()) {
            continue;
        }
        out.push(line);
    }
    out.join("\n")
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

/// Resolves a revision to its full object id.
pub fn git_rev_parse(revision: &str) -> Result<String> {
    Ok(git_ro(["rev-parse", revision].as_slice())?
        .trim()
        .to_string())
}

/// Resolves the tree object id at a commit or tree-ish revision.
pub fn git_commit_tree(revision: &str) -> Result<String> {
    let tree_revision = format!("{revision}^{{tree}}");
    git_rev_parse(&tree_revision)
}

/// Returns the merge-base object id of two revisions.
pub fn git_merge_base(left: &str, right: &str) -> Result<String> {
    Ok(git_ro(["merge-base", left, right].as_slice())?
        .trim()
        .to_string())
}

/// Returns the tip SHA of an exact local branch name, if it exists.
pub fn git_local_branch_tip(branch: &str) -> Result<Option<String>> {
    let reference = format!("refs/heads/{branch}^{{commit}}");
    let out = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", &reference])
        .output()
        .with_context(|| format!("failed to inspect local branch {}", branch))?;
    if out.status.success() {
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(Some(sha))
    } else if out.status.code() == Some(1) {
        Ok(None)
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        bail!(
            "failed to inspect local branch {} via {}: {}",
            branch,
            reference,
            stderr
        );
    }
}

/// Returns the commits in `from_exclusive..to_inclusive`, oldest first.
pub fn git_rev_list_range(from_exclusive: &str, to_inclusive: &str) -> Result<Vec<String>> {
    let range = format!("{from_exclusive}..{to_inclusive}");
    let out = git_ro(["rev-list", "--reverse", &range].as_slice())?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// Returns the number of parents on the given commit.
pub fn git_commit_parent_count(sha: &str) -> Result<usize> {
    let out = git_ro(["rev-list", "--parents", "-n", "1", sha].as_slice())?;
    let count = out.split_whitespace().count().saturating_sub(1);
    Ok(count)
}

/// Returns the full commit message for `sha`.
pub fn git_commit_message(sha: &str) -> Result<String> {
    git_ro(["log", "-n", "1", "--format=%B", sha].as_slice())
}

/// Returns a verbatim patch fingerprint for each commit, keyed by commit SHA.
///
/// The fingerprint matches clean cherry-picks and rebases of the same patch
/// even when the commit SHA differs. Commits that produce no patch output get
/// a synthetic per-commit fallback so callers can still classify them without
/// failing the whole lookup.
pub fn git_patch_ids_for_commits(commits: &[String]) -> Result<HashMap<String, String>> {
    if commits.is_empty() {
        return Ok(HashMap::new());
    }

    let mut diff_tree = Command::new("git")
        .args(["diff-tree", "--stdin", "-p", "--root"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| "failed to spawn git diff-tree --stdin -p --root")?;
    {
        let mut stdin = diff_tree
            .stdin
            .take()
            .with_context(|| "failed to open git diff-tree stdin")?;
        for commit in commits {
            writeln!(stdin, "{commit}")
                .with_context(|| format!("failed to queue commit {commit} for patch-id lookup"))?;
        }
    }
    let diff_output = diff_tree
        .wait_with_output()
        .with_context(|| "failed to collect git diff-tree output")?;
    if !diff_output.status.success() {
        let stdout = String::from_utf8_lossy(&diff_output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&diff_output.stderr).to_string();
        bail!(
            "command failed: git diff-tree --stdin -p --root\nstdout:\n{}\nstderr:\n{}",
            stdout,
            stderr
        );
    }

    let mut patch_id = Command::new("git")
        .args(["patch-id", "--verbatim"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| "failed to spawn git patch-id --verbatim")?;
    {
        let mut stdin = patch_id
            .stdin
            .take()
            .with_context(|| "failed to open git patch-id stdin")?;
        stdin
            .write_all(&diff_output.stdout)
            .with_context(|| "failed to feed patch data into git patch-id")?;
    }
    let patch_output = patch_id
        .wait_with_output()
        .with_context(|| "failed to collect git patch-id output")?;
    if !patch_output.status.success() {
        let stdout = String::from_utf8_lossy(&patch_output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&patch_output.stderr).to_string();
        bail!(
            "command failed: git patch-id --verbatim\nstdout:\n{}\nstderr:\n{}",
            stdout,
            stderr
        );
    }

    let mut patch_ids = HashMap::new();
    let patch_stdout = String::from_utf8_lossy(&patch_output.stdout);
    for line in patch_stdout.lines() {
        let mut fields = line.split_whitespace();
        let Some(patch_id) = fields.next() else {
            continue;
        };
        let Some(commit) = fields.next() else {
            continue;
        };
        patch_ids.insert(commit.to_string(), patch_id.to_string());
    }
    for commit in commits {
        patch_ids
            .entry(commit.clone())
            .or_insert_with(|| format!("empty:{commit}"));
    }
    Ok(patch_ids)
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
