use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use regex::Regex;
use serde::Deserialize;
use std::process::Command;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "spr",
    version,
    about = "Stacked PRs from commit tags or existing spr/* branches"
)]
struct Cli {
    /// Verbose output for underlying git/gh commands
    #[arg(long)]
    verbose: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Build/refresh stacked PRs
    Update {
        /// Base branch to stack onto (root PR bases on this)
        #[arg(short = 'b', long, default_value = "main")]
        base: String,

        /// Branch prefix for per-PR branches
        #[arg(long, default_value = "spr/")]
        prefix: String,

        /// Source ref to read commits from (if building from tags)
        #[arg(long, default_value = "HEAD")]
        from: String,

        /// Don’t create PRs, only (re)create branches
        #[arg(long)]
        no_pr: bool,

        /// If set, always restack existing spr/* PR branches (skip tag parsing)
        #[arg(long)]
        restack: bool,

        /// Print all state-changing git/gh commands instead of executing them
        #[arg(long)]
        dry_run: bool,

        /// In --dry-run, assume PRs already exist for branches (so we print 'edit' instead of 'create')
        #[arg(long)]
        assume_existing_prs: bool,

        /// Limit how much to update (optional sub-mode)
        #[command(subcommand)]
        extent: Option<Extent>,
    },

    /// Prepare PRs for landing (e.g., squash)
    Prep {
        #[command(subcommand)]
        what: PrepWhat,

        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long, default_value = "main")]
        base: String,

        /// Branch prefix for per-PR branches
        #[arg(long, default_value = "spr/")]
        prefix: String,

        /// Print state-changing commands instead of executing
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug, Clone, Copy)]
enum Extent {
    /// Update the first N PRs (bottom-up)
    Pr { n: usize },
    /// Update only the first N commits from base..from (push partial groups if needed)
    Commits { n: usize },
}

#[derive(Subcommand, Debug)]
enum PrepWhat {
    /// Squash the first N PRs (bottom-up) into a single commit each and force-push
    Pr { n: usize },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    if cli.verbose {
        std::env::set_var("SPR_VERBOSE", "1");
    }
    match cli.cmd {
        Cmd::Update {
            base,
            prefix,
            from,
            no_pr,
            restack,
            dry_run,
            assume_existing_prs,
            extent,
        } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            if dry_run {
                std::env::set_var("SPR_DRY_RUN", "1");
            }
            if dry_run && assume_existing_prs {
                std::env::set_var("SPR_DRY_ASSUME_EXISTING", "1");
            }
            let limit = extent.map(|e| match e {
                Extent::Pr { n } => Limit::ByPr(n),
                Extent::Commits { n } => Limit::ByCommits(n),
            });
            if restack {
                restack_existing(&base, &prefix, no_pr, dry_run, limit)?;
            } else if has_tagged_commits(&base, &from)? {
                build_from_tags(&base, &from, &prefix, no_pr, dry_run, limit)?;
            } else {
                info!(
                    "No pr:<tag> markers found between {} and {}. Falling back to --restack.",
                    base, from
                );
                restack_existing(&base, &prefix, no_pr, dry_run, limit)?;
            }
        }
        Cmd::Prep {
            what,
            base,
            prefix,
            dry_run,
        } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            if dry_run {
                std::env::set_var("SPR_DRY_RUN", "1");
            }
            match what {
                PrepWhat::Pr { n } => prep_squash_first_n_prs(&base, &prefix, n, dry_run)?,
            }
        }
    }
    Ok(())
}

/* ------------------ update (from tags) ------------------ */

/// Bootstrap/refresh stack from pr:<tag> markers on `from` vs merge-base(base, from).
fn build_from_tags(
    base: &str,
    from: &str,
    prefix: &str,
    no_pr: bool,
    dry: bool,
    limit: Option<Limit>,
) -> Result<()> {
    let merge_base = git_ro(&["merge-base", base, from])?.trim().to_string();
    let lines = git_ro(&[
        "log",
        "--format=%H%x00%B%x1e",
        "--reverse",
        &format!("{merge_base}..{from}"),
    ])?;
    let mut groups: Vec<Group> = parse_groups(&lines)?;

    if groups.is_empty() {
        info!("No groups discovered; nothing to do.");
        return Ok(());
    }

    // Apply extent limits
    groups = apply_limit_groups(groups, limit)?;
    let total_groups = groups.len();

    info!(
        "Preparing {} group(s): {}",
        groups.len(),
        groups
            .iter()
            .map(|g| g.tag.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Build bottom→top and collect PR refs for the visual update pass.
    let mut parent_branch = base.to_string();
    let mut stack: Vec<PrRef> = vec![];
    let mut force_from_now = false; // flip to true on first divergence with remote

    for (idx, g) in groups.iter_mut().enumerate() {
        let branch = format!("{}{}", prefix, g.tag);
        info!(
            "({}/{}) Rebuilding branch {}",
            idx + 1,
            total_groups,
            branch
        );

        // Use local commit SHA as source of truth; avoid rewriting commits when possible
        let remote_head = get_remote_branch_sha(&branch)?;
        let target_sha = g
            .commits
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("Group {} has no commits", g.tag))?;
        if remote_head.as_deref() == Some(target_sha.as_str()) {
            info!("No changes for {}; skipping push", branch);
        } else {
            // If remote exists but is not an ancestor of our target, switch to force-with-lease for current and subsequent pushes
            if let Some(ref remote_sha) = remote_head {
                let ff_ok = git_is_ancestor(remote_sha, &target_sha)?;
                if !ff_ok {
                    force_from_now = true;
                }
            }
            if force_from_now {
                git_rw(
                    dry,
                    &[
                        "push",
                        "--force-with-lease",
                        "origin",
                        &format!("{}:refs/heads/{}", &target_sha, &branch),
                    ],
                )?;
            } else {
                git_rw(
                    dry,
                    &[
                        "push",
                        "origin",
                        &format!("{}:refs/heads/{}", &target_sha, &branch),
                    ],
                )?;
            }
        }

        if !no_pr {
            let num = upsert_pr(
                &branch,
                &sanitize_gh_base_ref(&parent_branch),
                &g.pr_title()?,
                &g.pr_body()?,
                dry,
            )?;
            stack.push(PrRef {
                number: num,
                head: branch.clone(),
                base: parent_branch.clone(),
            });
        }

        parent_branch = branch;
    }

    if !no_pr && !dry {
        update_stack_bodies(&stack, dry)?;
    } else if !no_pr && dry {
        info!(
            "DRY-RUN: would update PR descriptions with stack visual for {} PRs",
            stack.len()
        );
    }

    Ok(())
}

/* ------------------ update (restack existing) ------------------ */

/// Restack existing spr/* PRs by rebase --onto Parent → Child, bottom→top.
fn restack_existing(
    base: &str,
    prefix: &str,
    no_pr: bool,
    dry: bool,
    limit: Option<Limit>,
) -> Result<()> {
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Build linear chains for each root (baseRefName == base)
    let mut roots: Vec<&PrInfo> = prs.iter().filter(|p| p.base == base).collect();
    if roots.is_empty() {
        bail!("Could not find a root PR basing on `{base}`. Ensure one PR has base `{base}`.");
    }
    if roots.len() > 1 {
        warn!(
            "Found {} roots basing on `{base}`; processing each chain independently.",
            roots.len()
        );
    }

    let mut overall_stack: Vec<PrRef> = vec![];

    for root in roots {
        let mut order: Vec<&PrInfo> = vec![];
        let mut cur = root;
        loop {
            order.push(cur);
            if let Some(next) = prs.iter().find(|p| p.base == cur.head) {
                cur = next;
            } else {
                break;
            }
        }

        // Apply extent limits for restack
        let order = apply_limit_prs_for_restack(base, &order, limit)?;

        info!(
            "Restacking {} PR(s): {}",
            order.len(),
            order
                .iter()
                .map(|p| p.head.as_str())
                .collect::<Vec<_>>()
                .join(" → ")
        );

        for win in order.windows(2) {
            let parent = &win[0].head;
            let child = &win[1].head;

            info!("Rebasing {child} onto {parent}");
            git_rw(dry, &["fetch", "origin"])?; // state-changing, print in dry-run
            git_ro(&["checkout", child])?;
            git_rw(
                dry,
                &[
                    "merge",
                    "--no-ff",
                    parent,
                    "-m",
                    &format!("spr: merge {} into {}", parent, child),
                ],
            )?;
            git_rw(dry, &["push", "origin", child])?;

            if !no_pr {
                gh_rw(
                    dry,
                    &["pr", "edit", child, "--base", &sanitize_gh_base_ref(parent)],
                )?;
            }
        }

        // Collect for the visual pass (bottom→top order)
        for pr in &order {
            overall_stack.push(PrRef {
                number: pr.number,
                head: pr.head.clone(),
                base: pr.base.clone(),
            });
        }
    }

    if !no_pr && !dry {
        update_stack_bodies(&overall_stack, dry)?;
    } else if !no_pr && dry {
        info!(
            "DRY-RUN: would update PR descriptions with stack visual for {} PRs",
            overall_stack.len()
        );
    }

    Ok(())
}

/* ------------------ prep (squash) ------------------ */

/// Squash the first N PRs (bottom-up) into a single commit each and force-push.
fn prep_squash_first_n_prs(base: &str, prefix: &str, n: usize, dry: bool) -> Result<()> {
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Build the single chain from root basing on `base`
    let root = prs
        .iter()
        .find(|p| p.base == base)
        .ok_or_else(|| anyhow!("No root PR with base `{base}`"))?;
    let mut order: Vec<&PrInfo> = vec![];
    let mut cur = root;
    loop {
        order.push(cur);
        if let Some(next) = prs.iter().find(|p| p.base == cur.head) {
            cur = next;
        } else {
            break;
        }
    }

    let to_prep = order.into_iter().take(n).collect::<Vec<_>>();
    if to_prep.is_empty() {
        info!("Nothing to prep");
        return Ok(());
    }

    info!(
        "Squashing first {} PR(s): {}",
        to_prep.len(),
        to_prep
            .iter()
            .map(|p| format!("#{}", p.number))
            .collect::<Vec<_>>()
            .join(", ")
    );

    for (i, pr) in to_prep.iter().enumerate() {
        // Parent is base for first element, else previous head
        let parent = if i == 0 {
            base.to_string()
        } else {
            to_prep[i - 1].head.clone()
        };

        // Count unique commits to know if there is anything to squash
        let cnt_s = git_ro(&["rev-list", "--count", &format!("{}..{}", parent, pr.head)])?;
        let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
        if cnt == 0 {
            info!(
                "PR #{} has no unique commits over {}; skipping",
                pr.number, parent
            );
            continue;
        }

        // Get PR title for commit message
        let meta_json = gh_ro(&[
            "pr",
            "view",
            &format!("#{}", pr.number),
            "--json",
            "title,number",
        ])?;
        #[derive(Deserialize)]
        struct Meta {
            title: String,
            number: u64,
        }
        let meta: Meta = serde_json::from_str(&meta_json)?;
        let msg = format!("{} (#{})", meta.title.trim(), meta.number);

        info!("Squashing PR #{} ({} commits) into one", pr.number, cnt);
        git_ro(&["checkout", &pr.head])?;
        // Create single commit preserving tree of HEAD
        git_rw(dry, &["reset", "--soft", &parent])?;
        // Ensure there is something staged; if dry-run, just print commit
        if dry {
            info!("DRY-RUN: git commit -m {:?}", msg);
        } else {
            // If nothing staged (shouldn't happen when cnt>0), skip commit gracefully
            let diff = Command::new("git")
                .args(["diff", "--cached", "--quiet"])
                .status()?;
            if !diff.success() {
                git_rw(false, &["commit", "-m", &msg])?;
            } else {
                info!(
                    "Nothing staged after reset; skipping commit for #{}",
                    pr.number
                );
            }
        }
        git_rw(dry, &["push", "--force-with-lease", "origin", &pr.head])?;
    }

    Ok(())
}

/* ------------------ data & helpers ------------------ */

#[derive(Debug, Default, Clone)]
struct Group {
    tag: String,
    subjects: Vec<String>,
    commits: Vec<String>, // SHAs oldest→newest
}

impl Group {
    fn pr_title(&self) -> Result<String> {
        if let Some(s) = self.subjects.first() {
            let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
            let t = re.replace_all(s, "").trim().to_string();
            if !t.is_empty() {
                return Ok(t);
            }
        }
        Ok(self.tag.clone())
    }
    fn pr_body(&self) -> Result<String> {
        // Use only the body (drop the subject/title line); remove pr:<tag> markers
        let base_body = if let Some(full) = &self.first_message {
            let mut it = full.lines();
            let _ = it.next();
            it.collect::<Vec<_>>().join("\n")
        } else { String::new() };
        let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
        let cleaned = re.replace_all(&base_body, "").to_string().trim().to_string();
        let sep = if cleaned.is_empty() { "" } else { "\n\n" };
        Ok(format!(
            "{}{}<!-- spr-stack:start -->\n(placeholder; will be filled by spr)\n<!-- spr-stack:end -->",
            cleaned, sep,
        ))
    }
}

/// Parse commit stream from `git log --format=%H%x00%B%x1e --reverse <range>`
fn parse_groups(raw: &str) -> Result<Vec<Group>> {
    let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
    let mut groups: Vec<Group> = vec![];
    let mut current: Option<Group> = None;

    for chunk in raw.split('\u{001e}') {
        let chunk = chunk.trim_end_matches('\n');
        if chunk.trim().is_empty() {
            continue;
        }
        let mut parts = chunk.splitn(2, '\0');
        let sha = parts.next().unwrap_or_default().trim().to_string();
        let message = parts.next().unwrap_or_default().to_string();
        let subj = message.lines().next().unwrap_or_default().to_string();

        let tag_matches = re.captures_iter(&message).count();
        if tag_matches > 1 {
            bail!("Multiple pr:<tag> markers found in commit {sha}");
        }

        if tag_matches == 1 {
            let cap = re.captures(&message).unwrap();
            if let Some(g) = current.take() {
                if !g.commits.is_empty() {
                    groups.push(g);
                }
            }
            let tag = cap.get(1).unwrap().as_str().to_string();
            current = Some(Group {
                tag,
                subjects: vec![subj.clone()],
                commits: vec![sha],
            });
        } else if let Some(g) = current.as_mut() {
            g.subjects.push(subj);
            g.commits.push(sha);
        } else {
            warn!("Untagged commit before first pr:<tag>; ignored");
        }
    }
    if let Some(g) = current.take() {
        if !g.commits.is_empty() {
            groups.push(g);
        }
    }
    Ok(groups)
}

#[derive(Clone, Copy)]
enum Limit {
    ByPr(usize),
    ByCommits(usize),
}

fn apply_limit_groups(mut groups: Vec<Group>, limit: Option<Limit>) -> Result<Vec<Group>> {
    match limit {
        None => Ok(groups),
        Some(Limit::ByPr(n)) => Ok(groups.into_iter().take(n).collect()),
        Some(Limit::ByCommits(mut n)) => {
            let mut out = vec![];
            for mut g in groups.drain(..) {
                if n == 0 {
                    break;
                }
                let len = g.commits.len();
                if len <= n {
                    out.push(g);
                    n -= len;
                } else {
                    g.commits.truncate(n);
                    if !g.subjects.is_empty() {
                        g.subjects.truncate(g.commits.len().min(g.subjects.len()));
                    }
                    out.push(g);
                    n = 0;
                }
            }
            Ok(out)
        }
    }
}

fn apply_limit_prs_for_restack<'a>(
    base: &str,
    order: &'a Vec<&'a PrInfo>,
    limit: Option<Limit>,
) -> Result<Vec<&'a PrInfo>> {
    match limit {
        None => Ok(order.clone()),
        Some(Limit::ByPr(n)) => Ok(order.iter().take(n).cloned().collect()),
        Some(Limit::ByCommits(mut n)) => {
            // Keep adding PRs while cumulative unique commit count (over parent) <= n
            let mut out: Vec<&PrInfo> = vec![];
            for (i, pr) in order.iter().enumerate() {
                out.push(pr);
                if i == order.len() - 1 {
                    break;
                }
                let parent = if i == 0 {
                    base.to_string()
                } else {
                    order[i].head.clone()
                };
                let child = &order[i + 1].head;
                let cnt_s = git_ro(&["rev-list", "--count", &format!("{}..{}", parent, child)])?;
                let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
                if cnt > n {
                    break;
                }
                n = n.saturating_sub(cnt);
            }
            Ok(out)
        }
    }
}

fn has_tagged_commits(base: &str, from: &str) -> Result<bool> {
    let merge_base = git_ro(&["merge-base", base, from])?.trim().to_string();
    let out = git_ro(&["log", "--format=%B", &format!("{merge_base}..{from}")])?;
    Ok(out.lines().any(|s| s.contains("pr:")))
}

fn ensure_tool(name: &str) -> Result<()> {
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

fn git_ro(args: &[&str]) -> Result<String> {
    if std::env::var_os("SPR_DRY_RUN").is_some() {
        info!("DRY-RUN: git {}", shellish(args));
    }
    verbose_log_cmd("git", args);
    run("git", args)
}
fn git_rw(dry: bool, args: &[&str]) -> Result<String> {
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
fn gh_ro(args: &[&str]) -> Result<String> {
    if std::env::var_os("SPR_DRY_RUN").is_some() {
        info!("DRY-RUN: gh {}", shellish(args));
    }
    verbose_log_cmd("gh", args);
    run("gh", args)
}
fn gh_rw(dry: bool, args: &[&str]) -> Result<String> {
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

fn run(bin: &str, args: &[&str]) -> Result<String> {
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

fn shellish(args: &[&str]) -> String {
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

fn sanitize_gh_base_ref(base: &str) -> String {
    if let Some(stripped) = base.strip_prefix("origin/") {
        return stripped.to_string();
    }
    base.to_string()
}

fn safe_checkout_reset(dry: bool, branch: &str, start_point: &str) -> Result<()> {
    // If branch exists, back it up to avoid losing local commits
    let exists = git_ro(&["rev-parse", "--verify", branch]).is_ok();
    if exists {
        let sha = git_ro(&["rev-parse", branch])?.trim().to_string();
        let backup = format!(
            "spr-backup/{}-{}",
            branch.replace('/', "_"),
            &sha[..8.min(sha.len())]
        );
        info!("Backing up existing branch {} to {}", branch, backup);
        if !dry {
            git_ro(&["branch", &backup, branch])?;
        }
    }
    git_rw(dry, &["checkout", "-B", branch, start_point])?;
    Ok(())
}

fn verbose_log_cmd(tool: &str, args: &[&str]) {
    if std::env::var_os("SPR_VERBOSE").is_some() {
        info!("{} {}", tool, shellish(args));
    }
}

fn git_rw_in(dry: bool, dir: &str, args: &[&str]) -> Result<String> {
    let mut v: Vec<String> = Vec::with_capacity(args.len() + 2);
    v.push("-C".to_string());
    v.push(dir.to_string());
    for a in args {
        v.push((*a).to_string());
    }
    let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
    git_rw(dry, &refs)
}

fn git_ro_in(dir: &str, args: &[&str]) -> Result<String> {
    let mut v: Vec<String> = Vec::with_capacity(args.len() + 2);
    v.push("-C".to_string());
    v.push(dir.to_string());
    for a in args {
        v.push((*a).to_string());
    }
    let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
    git_ro(&refs)
}

fn to_remote_ref(name: &str) -> String {
    let name = name.strip_prefix("refs/heads/").unwrap_or(name);
    let name = name.strip_prefix("origin/").unwrap_or(name);
    format!("origin/{}", name)
}

fn get_remote_branch_sha(branch: &str) -> Result<Option<String>> {
    let out = git_ro(&["ls-remote", "--heads", "origin", branch])?;
    let sha = out.split_whitespace().next().unwrap_or("").trim();
    if sha.is_empty() {
        Ok(None)
    } else {
        Ok(Some(sha.to_string()))
    }
}

fn git_is_ancestor_in(dir: &str, ancestor: &str, descendant: &str) -> Result<bool> {
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

fn git_is_ancestor(ancestor: &str, descendant: &str) -> Result<bool> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .with_context(|| "failed to run git merge-base --is-ancestor")?;
    Ok(status.success())
}

fn fake_pr_number(head: &str) -> u64 {
    let mut sum: u64 = 0;
    for b in head.bytes() {
        sum = sum.wrapping_add(b as u64);
    }
    1000 + (sum % 900_000)
}

/* ------------------ GitHub PR helpers ------------------ */

#[derive(Debug, Deserialize, Clone)]
struct PrInfo {
    number: u64,
    head: String,
    base: String,
}

#[derive(Debug, Clone)]
struct PrRef {
    number: u64,
    head: String,
    base: String,
}

/// List open PRs, filtering to those whose head starts with `prefix`
fn list_spr_prs(prefix: &str) -> Result<Vec<PrInfo>> {
    let json = gh_ro(&[
        "pr",
        "list",
        "--state",
        "open",
        "--limit",
        "200",
        "--json",
        "number,headRefName,baseRefName",
    ])?;
    #[derive(Deserialize)]
    struct Raw {
        number: u64,
        #[serde(rename = "headRefName")]
        headRefName: String,
        #[serde(rename = "baseRefName")]
        baseRefName: String,
    }
    let raws: Vec<Raw> = serde_json::from_str(&json)?;
    let mut out = vec![];
    for r in raws {
        if r.headRefName.starts_with(prefix) {
            out.push(PrInfo {
                number: r.number,
                head: r.headRefName,
                base: r.baseRefName,
            });
        }
    }
    if out.is_empty() {
        warn!("No open PRs with head starting with `{}` found.", prefix);
    }
    Ok(out)
}

/// Create or update a PR for `branch` with base `parent`. Returns PR number.
fn upsert_pr(branch: &str, parent: &str, title: &str, body: &str, dry: bool) -> Result<u64> {
    if dry {
        if std::env::var_os("SPR_DRY_ASSUME_EXISTING").is_some() {
            let n = fake_pr_number(branch);
            gh_rw(
                dry,
                &[
                    "pr",
                    "edit",
                    &format!("#{}", n),
                    "--title",
                    title,
                    "--base",
                    &sanitize_gh_base_ref(parent),
                    "--body",
                    body,
                ],
            )?;
            return Ok(n);
        } else {
            // In dry-run default, pretend PR does not exist and show create command
            gh_rw(
                dry,
                &[
                    "pr",
                    "create",
                    "--head",
                    branch,
                    "--base",
                    &sanitize_gh_base_ref(parent),
                    "--title",
                    title,
                    "--body",
                    body,
                ],
            )?;
            return Ok(0);
        }
    }
    // Check for existing open PR by head
    let json = gh_ro(&[
        "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number",
    ])?;
    #[derive(Deserialize)]
    struct V {
        number: u64,
    }
    let existing: Vec<V> = serde_json::from_str(&json)?;
    if let Some(v) = existing.get(0) {
        gh_rw(
            dry,
            &[
                "pr",
                "edit",
                &format!("#{}", v.number),
                "--title",
                title,
                "--base",
                &sanitize_gh_base_ref(parent),
                "--body",
                body,
            ],
        )?;
        return Ok(v.number);
    }

    // Create new PR
    gh_rw(
        dry,
        &[
            "pr",
            "create",
            "--head",
            branch,
            "--base",
            &sanitize_gh_base_ref(parent),
            "--title",
            title,
            "--body",
            body,
        ],
    )?;

    // Fetch created PR number
    let json2 = gh_ro(&[
        "pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number",
    ])?;
    let created: Vec<V> = serde_json::from_str(&json2)?;
    let num = created
        .get(0)
        .map(|v| v.number)
        .ok_or_else(|| anyhow!("Failed to determine PR number for {}", branch))?;
    Ok(num)
}

/// Update the stack visual in each PR body (only for the set we touched).
fn update_stack_bodies(stack: &Vec<PrRef>, dry: bool) -> Result<()> {
    if stack.is_empty() {
        return Ok(());
    }

    let numbers: Vec<u64> = stack.iter().map(|p| p.number).collect();
    let numbers_rev: Vec<u64> = numbers.iter().cloned().rev().collect();

    for (idx, pr) in stack.iter().enumerate() {
        let json = gh_ro(&["pr", "view", &format!("#{}", pr.number), "--json", "body"])?;
        #[derive(Deserialize)]
        struct B {
            body: String,
        }
        let mut b: B = serde_json::from_str(&json)?;

        let start = "<!-- spr-stack:start -->";
        let end = "<!-- spr-stack:end -->";
        let re = Regex::new(&format!(
            r"(?s){}.*?{}",
            regex::escape(start),
            regex::escape(end)
        ))?;
        b.body = re.replace(&b.body, "").trim().to_string();

        let em_space = "\u{2003}"; // U+2003 EM SPACE for indentation
        let mut lines = String::new();
        for n in &numbers_rev {
            let marker = if *n == pr.number { "➡" } else { em_space };
            lines.push_str(&format!("- {} #{}\n", marker, n));
        }
        let block = format!(
            "\n\n{}\n**Stack**:\n{}\n\n⚠️ *Part of a stack created by [spr-multicommit](https://github.com/mattskl-openai/spr-multicommit). Do not merge manually using the UI - doing so may have unexpected results.*\n{}\n",
            start,
            lines.trim_end(),
            end,
        );
        let new_body = if b.body.is_empty() {
            block.clone()
        } else {
            format!("{}\n\n{}", b.body, block)
        };

        gh_rw(
            dry,
            &[
                "pr",
                "edit",
                &format!("#{}", pr.number),
                "--body",
                &new_body,
            ],
        )?;
        info!("Updated stack visual in PR #{}", pr.number);
    }
    Ok(())
}
