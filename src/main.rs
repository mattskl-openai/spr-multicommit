use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
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
        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long, default_value = "main")]
        base: String,

        /// Branch prefix for per-PR branches
        #[arg(long, default_value = "spr/")]
        prefix: String,

        /// Prep N PRs from bottom of stack (use 0 for all)
        #[arg(long, conflicts_with = "exact")]
        until: Option<usize>,

        /// Prep exactly this PR index (1-based from bottom)
        #[arg(long, conflicts_with = "until")]
        exact: Option<usize>,

        /// Print state-changing commands instead of executing
        #[arg(long)]
        dry_run: bool,
    },

    

    /// List entities
    List {
        #[command(subcommand)]
        what: ListWhat,

        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long, default_value = "main")]
        base: String,

        /// Branch prefix for per-PR branches
        #[arg(long, default_value = "spr/")]
        prefix: String,
    },

    /// Merge PRs from the bottom of the stack
    Merge {
        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long, default_value = "main")]
        base: String,

        /// Branch prefix for per-PR branches
        #[arg(long, default_value = "spr/")]
        prefix: String,

        /// Merge the first N PRs (bottom-up)
        #[arg(long, value_name = "N")]
        until: usize,

        /// Print state-changing commands instead of executing
        #[arg(long)]
        dry_run: bool,
    },

    /// Fix PR base connectivity to match local commit stack
    FixChain {
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

#[derive(Clone, Copy, Debug)]
enum PrepSelection { Until(usize), Exact(usize), All }

#[derive(Subcommand, Debug, Clone, Copy)]
enum ListWhat {
    /// List PRs in the stack (bottom-up)
    Pr,
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
        Cmd::Prep { base, prefix, until, exact, dry_run } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            if dry_run {
                std::env::set_var("SPR_DRY_RUN", "1");
            }
            let selection = if let Some(n) = until { if n == 0 { PrepSelection::All } else { PrepSelection::Until(n) } }
                            else if let Some(i) = exact { PrepSelection::Exact(i) }
                            else { PrepSelection::All };
            prep_squash(&base, &prefix, selection, dry_run)?;
        }
        Cmd::List { what, base, prefix } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            match what { ListWhat::Pr => list_prs_display(&base, &prefix)? }
        }
        Cmd::Merge { base, prefix, until, dry_run } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            merge_prs_until(&base, &prefix, until, dry_run)?;
        }
        Cmd::FixChain { base, prefix, dry_run } => {
            ensure_tool("git")?;
            ensure_tool("gh")?;
            fix_chain(&base, &prefix, dry_run)?;
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
    // Prefetch open PRs to reduce per-branch lookups
    let mut prs_by_head: HashMap<String, u64> =
        list_spr_prs(prefix)?.into_iter().map(|p| (p.head, p.number)).collect();
    let mut force_from_now = false; // flip to true on first divergence with remote

    // Batch fetch remote SHAs for all target branches
    let branch_names: Vec<String> = groups
        .iter()
        .map(|g| format!("{}{}", prefix, g.tag))
        .collect();
    let remote_map = get_remote_branches_sha(&branch_names)?; // branch -> sha

    for (idx, g) in groups.iter_mut().enumerate() {
        let branch = format!("{}{}", prefix, g.tag);
        info!(
            "({}/{}) Rebuilding branch {}",
            idx + 1,
            total_groups,
            branch
        );

        // Use local commit SHA as source of truth; avoid rewriting commits when possible
        let remote_head = remote_map.get(&branch).cloned();
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
            let num = upsert_pr_cached(
                &branch,
                &sanitize_gh_base_ref(&parent_branch),
                &g.pr_title()?,
                &g.pr_body()?,
                dry,
                &mut prs_by_head,
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
    let base_n = normalize_branch_name(base);
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() {
        bail!("No open PRs with head starting with `{prefix}`.");
    }

    // Build linear chains for each root (baseRefName == base)
    let mut roots: Vec<&PrInfo> = prs.iter().filter(|p| p.base == base_n).collect();
    if roots.is_empty() {
        bail!(
            "Could not find a root PR basing on `{}`. Ensure one PR has base `{}`.",
            base_n,
            base_n
        );
    }
    if roots.len() > 1 {
        warn!(
            "Found {} roots basing on `{}`; processing each chain independently.",
            roots.len(),
            base_n
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
        let order = apply_limit_prs_for_restack(&base_n, &order, limit)?;

        info!(
            "Restacking {} PR(s): {}",
            order.len(),
            order
                .iter()
                .map(|p| p.head.as_str())
                .collect::<Vec<_>>()
                .join(" → ")
        );

        // Ensure we only fetch once per chain
        git_rw(dry, &["fetch", "origin"])?;
        for win in order.windows(2) {
            let parent = &win[0].head;
            let child = &win[1].head;

            info!("Rebasing {child} onto {parent}");
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

/// Squash PRs according to selection; operate locally then run update for the affected groups.
fn prep_squash(base: &str, prefix: &str, selection: PrepSelection, dry: bool) -> Result<()> {
    // Work purely on local commit stack: build groups from base..HEAD
    let merge_base = git_ro(&["merge-base", base, "HEAD"])?.trim().to_string();
    let lines = git_ro(&["log", "--format=%H%x00%B%x1e", "--reverse", &format!("{}..HEAD", merge_base)])?;
    let groups = parse_groups(&lines)?;
    if groups.is_empty() { info!("Nothing to prep"); return Ok(()); }

    let total_groups = groups.len();
    let num_to_prep = match selection { PrepSelection::All => total_groups, PrepSelection::Until(n) => n.min(total_groups), PrepSelection::Exact(i) => { if i==0 || i>total_groups { bail!("--exact out of range (1..={})", total_groups); } 1 } };
    let to_prep = match selection { PrepSelection::Exact(i) => groups.iter().skip(i-1).take(1).cloned().collect::<Vec<_>>(), _ => groups.iter().take(num_to_prep).cloned().collect::<Vec<_>>() };
    info!("Locally squashing {} group(s)", to_prep.len());
    for (i, g) in to_prep.iter().enumerate() {
        info!("Group {}: tag = {}", i + 1, g.tag);
        for (j, sha) in g.commits.iter().enumerate() {
            info!("  Commit {}: {}", j + 1, sha);
        }
    }
      
    // Build a new chain: starting parent depends on selection
    let mut parent_sha = match selection { PrepSelection::Exact(i) => { if i==1 { merge_base.clone() } else { groups[i-2].commits.last().unwrap().clone() } }, _ => merge_base.clone() };
    // Batch tip trees
    if !to_prep.is_empty() {
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for g in &to_prep {
            let tip = g.commits.last().ok_or_else(|| anyhow!("Empty group {}", g.tag))?;
            args.push(format!("{}^{{tree}}", tip));
        }
        let ref_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let trees_out = git_ro(&ref_args)?;
        let trees: Vec<&str> = trees_out.lines().collect();
        for (idx, g) in to_prep.iter().enumerate() {
            let tree = trees.get(idx).copied().unwrap_or("");
            let msg = g.squash_commit_message()?;
            let new_commit = git_rw(dry, &["commit-tree", tree, "-p", &parent_sha, "-m", &msg])?.trim().to_string();
            parent_sha = new_commit;
        }
    }

    // Replay the remaining commits (not prepped) on top to preserve the rest of the stack
    let skip_after = match selection { PrepSelection::Exact(i) => i, _ => num_to_prep };
    let remainder: Vec<String> = groups.iter().skip(skip_after).flat_map(|g| g.commits.iter().cloned()).collect();
    if !remainder.is_empty() {
        // Batch trees
        let mut args: Vec<String> = vec!["rev-parse".into()];
        for sha in &remainder { args.push(format!("{}^{{tree}}", sha)); }
        let ref_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let trees_out = git_ro(&ref_args)?;
        let trees: Vec<&str> = trees_out.lines().collect();
        // Batch bodies
        let mut log_args: Vec<&str> = vec!["log", "-1", "--format=%B%x1e"]; // RS-separated
        let rem_refs: Vec<&str> = remainder.iter().map(|s| s.as_str()).collect();
        log_args.extend(rem_refs);
        let bodies_raw = git_ro(&log_args)?;
        let bodies: Vec<&str> = bodies_raw.split('\u{001e}').map(|s| s.trim_end_matches('\n')).filter(|s| !s.is_empty()).collect();
        for i in 0..remainder.len() {
            let tree = trees.get(i).copied().unwrap_or("");
            let msg = bodies.get(i).copied().unwrap_or("");
            let new_commit = git_rw(dry, &["commit-tree", tree, "-p", &parent_sha, "-m", msg])?.trim().to_string();
            parent_sha = new_commit;
        }
    }

    // Move current branch to new head (includes squashed N groups + unchanged remainder)
    let cur_branch = git_ro(&["symbolic-ref", "--quiet", "--short", "HEAD"])?.trim().to_string();
    git_rw(dry, &["update-ref", &format!("refs/heads/{}", cur_branch), &parent_sha])?;

    // Immediately run update for affected groups
    let limit = match selection { PrepSelection::All => None, PrepSelection::Until(_) => Some(Limit::ByPr(num_to_prep)), PrepSelection::Exact(_) => None };
    build_from_tags(base, "HEAD", prefix, false, dry, limit)
}

/* ------------------ data & helpers ------------------ */

#[derive(Debug, Default, Clone)]
struct Group {
    tag: String,
    subjects: Vec<String>,
    commits: Vec<String>, // SHAs oldest→newest
    first_message: Option<String>,
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
    fn squash_commit_message(&self) -> Result<String> {
        if let Some(full) = &self.first_message {
            // Validate the first commit contains the expected pr:<tag> marker
            let re = Regex::new(r"(?i)\bpr:([A-Za-z0-9._\-]+)\b")?;
            if let Some(cap) = re.captures(full) {
                let found = cap.get(1).unwrap().as_str();
                if !found.eq_ignore_ascii_case(&self.tag) {
                    bail!("First commit tag mismatch for group `{}`: expected `pr:{}`, found `pr:{}`", self.tag, self.tag, found);
                }
            } else {
                bail!("First commit is missing required `pr:{}` tag for group `{}`.", self.tag, self.tag);
            }
            return Ok(full.trim_end().to_string());
        }
        bail!("First commit message missing for group `{}`", self.tag)
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
                first_message: Some(message.clone()),
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

fn list_prs_display(base: &str, prefix: &str) -> Result<()> {
    // Derive stack from local commits (source of truth)
    let merge_base = git_ro(&["merge-base", base, "HEAD"])?.trim().to_string();
    let lines = git_ro(&["log", "--format=%H%x00%B%x1e", "--reverse", &format!("{}..HEAD", merge_base)])?;
    let groups = parse_groups(&lines)?;
    if groups.is_empty() { info!("No groups discovered; nothing to list."); return Ok(()); }

    // Fetch PRs to annotate with numbers when available
    let prs = list_spr_prs(prefix)?; // may be empty; that's fine

    for (i, g) in groups.iter().enumerate() {
        let head_branch = format!("{}{}", prefix, g.tag);
        let num = prs.iter().find(|p| p.head == head_branch).map(|p| p.number);
        let count = g.commits.len();
        match num {
            Some(n) => info!("{}: {} (#{}) - {} commit(s)", i+1, head_branch, n, count),
            None => info!("{}: {} - {} commit(s)", i+1, head_branch, count),
        }
    }
    Ok(())
}

#[derive(Clone)]
struct PrBodyInfo { id: String, body: String }

fn fetch_pr_bodies_graphql(numbers: &Vec<u64>) -> Result<HashMap<u64, PrBodyInfo>> {
    let mut out = HashMap::new();
    if numbers.is_empty() { return Ok(out); }
    let (owner, name) = get_repo_owner_name()?;
    let mut q = String::from("query($owner:String!,$name:String!){ repository(owner:$owner,name:$name){ ");
    for (i, n) in numbers.iter().enumerate() {
        q.push_str(&format!("pr{}: pullRequest(number: {}) {{ id body }} ", i, n));
    }
    q.push_str("} }");
    let json = gh_ro(&["api", "graphql", "-f", &format!("query={}", q), "-F", &format!("owner={}", owner), "-F", &format!("name={}", name)])?;
    let v: serde_json::Value = serde_json::from_str(&json)?;
    let repo = &v["data"]["repository"];
    for (i, n) in numbers.iter().enumerate() {
        let key = format!("pr{}", i);
        let id = repo[&key]["id"].as_str().unwrap_or("").to_string();
        let body = repo[&key]["body"].as_str().unwrap_or("").to_string();
        out.insert(*n, PrBodyInfo { id, body });
    }
    Ok(out)
}

fn get_repo_owner_name() -> Result<(String, String)> {
    let url = git_ro(&["config", "--get", "remote.origin.url"])?.trim().to_string();
    if let Some(idx) = url.find("://") {
        let rest = &url[idx+3..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 3 {
            let owner = parts[1].to_string();
            let mut name = parts[2].to_string();
            if let Some(s) = name.strip_suffix(".git") { name = s.to_string(); }
            return Ok((owner, name));
        }
    } else if let Some(pos) = url.find(":") {
        // git@github.com:owner/name.git
        let rest = &url[pos+1..];
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() >= 2 {
            let owner = parts[0].to_string();
            let mut name = parts[1].to_string();
            if let Some(s) = name.strip_suffix(".git") { name = s.to_string(); }
            return Ok((owner, name));
        }
    }
    bail!("Unable to parse remote.origin.url: {}", url)
}

fn graphql_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

fn merge_prs_until(base: &str, prefix: &str, n: usize, dry: bool) -> Result<()> {
    if n == 0 { bail!("--until must be >= 1"); }
    let base_n = normalize_branch_name(base);
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() { bail!("No open PRs with head starting with `{prefix}`."); }
    let root = prs.iter().find(|p| p.base == base_n).ok_or_else(|| anyhow!("No root PR with base `{}`", base_n))?;

    // Build ordered chain bottom-up
    let mut ordered: Vec<&PrInfo> = vec![]; let mut cur = root;
    loop { ordered.push(cur); if let Some(next) = prs.iter().find(|p| p.base == cur.head) { cur = next; } else { break; } }
    if ordered.is_empty() { bail!("No PR chain found"); }

    let take_n = n.min(ordered.len());
    let segment = &ordered[..take_n];

    // Verify each has exactly one unique commit over its parent
    git_rw(dry, &["fetch", "origin"])?; // ensure remotes up to date
    let mut offenders: Vec<u64> = vec![];
    for (i, pr) in segment.iter().enumerate() {
        let parent = if i == 0 { base_n.clone() } else { segment[i-1].head.clone() };
        let parent_ref = to_remote_ref(&parent);
        let child_ref = to_remote_ref(&pr.head);
        let cnt_s = git_ro(&["rev-list", "--count", &format!("{}..{}", parent_ref, child_ref)])?;
        let cnt: usize = cnt_s.trim().parse().unwrap_or(0);
        if cnt != 1 { offenders.push(pr.number); }
    }
    if !offenders.is_empty() {
        warn!("The following PRs have != 1 commit: {}", offenders.iter().map(|x| format!("#{}", x)).collect::<Vec<_>>().join(", "));
        bail!("Run `spr prep` to squash them first");
    }

    // Change base of Nth PR to actual base and merge it with rebase
    let nth = segment[take_n-1];
    gh_rw(dry, &["pr", "edit", &format!("#{}", nth.number), "--base", &sanitize_gh_base_ref(base)])?;
    gh_rw(dry, &["pr", "merge", &format!("#{}", nth.number), "--rebase"])?;

    // Close others with a comment
    for pr in &segment[..take_n-1] {
        gh_rw(dry, &["pr", "close", &format!("#{}", pr.number), "--comment", &format!("Merged as part of PR #{}", nth.number)])?;
    }

    Ok(())
}

fn fix_chain(base: &str, prefix: &str, dry: bool) -> Result<()> {
    let base_n = normalize_branch_name(base);
    // Build local expected chain from base..HEAD
    let merge_base = git_ro(&["merge-base", base, "HEAD"])?.trim().to_string();
    let lines = git_ro(&["log", "--format=%H%x00%B%x1e", "--reverse", &format!("{}..HEAD", merge_base)])?;
    let groups = parse_groups(&lines)?;
    if groups.is_empty() { info!("No local groups found; nothing to fix."); return Ok(()); }

    // Existing PRs map by head
    let prs = list_spr_prs(prefix)?;
    if prs.is_empty() { bail!("No open PRs with head starting with `{prefix}`."); }

    // Expected connectivity bottom-up
    let mut expected: Vec<(String, String)> = vec![]; // (head, base)
    let mut parent = base_n.clone();
    for g in &groups {
        let head = format!("{}{}", prefix, g.tag);
        expected.push((head.clone(), parent.clone()));
        parent = head;
    }

    // Apply base edits where needed
    for (head, want_base) in expected {
        if let Some(pr) = prs.iter().find(|p| p.head == head) {
            if pr.base != want_base {
                info!("Updating base of {} (#{}) from {} to {}", head, pr.number, pr.base, want_base);
                gh_rw(dry, &["pr", "edit", &format!("#{}", pr.number), "--base", &sanitize_gh_base_ref(&want_base)])?;
            } else {
                info!("{} (#{}) already basing on {}", head, pr.number, want_base);
            }
        } else {
            warn!("No open PR found for {}; skipping", head);
        }
    }
    Ok(())
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
                    normalize_branch_name(base)
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

fn normalize_branch_name(name: &str) -> String {
    let mut out = name.strip_prefix("refs/heads/").unwrap_or(name);
    out = out.strip_prefix("origin/").unwrap_or(out);
    out.to_string()
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

fn get_remote_branches_sha(branches: &Vec<String>) -> Result<HashMap<String, String>> {
    let mut out_map: HashMap<String, String> = HashMap::new();
    if branches.is_empty() { return Ok(out_map); }
    let mut args: Vec<&str> = vec!["ls-remote", "--heads", "origin"];
    let owned: Vec<String> = branches.iter().map(|b| b.to_string()).collect();
    let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    args.extend(refs);
    let out = git_ro(&args)?;
    for line in out.lines() {
        let mut parts = line.split_whitespace();
        let sha = parts.next().unwrap_or("").trim();
        let r = parts.next().unwrap_or("").trim();
        if sha.is_empty() || r.is_empty() { continue; }
        let name = r.strip_prefix("refs/heads/").unwrap_or(r).to_string();
        out_map.insert(name, sha.to_string());
    }
    Ok(out_map)
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
        // Defer edits (title/body) to final update pass; just return the number
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

fn upsert_pr_cached(
    branch: &str,
    parent: &str,
    title: &str,
    body: &str,
    dry: bool,
    prs_by_head: &mut HashMap<String, u64>,
) -> Result<u64> {
    if let Some(&num) = prs_by_head.get(branch) {
        // Defer edits to the final pass
        return Ok(num);
    }
    gh_rw(dry, &["pr", "create", "--head", branch, "--base", parent, "--title", title, "--body", body])?;
    let json = gh_ro(&["pr", "list", "--state", "open", "--head", branch, "--limit", "1", "--json", "number"])?;
    #[derive(Deserialize)] struct V { number: u64 }
    let arr: Vec<V> = serde_json::from_str(&json)?;
    let num = arr.get(0).map(|v| v.number).ok_or_else(|| anyhow!("Failed to determine PR number for {}", branch))?;
    prs_by_head.insert(branch.to_string(), num);
    Ok(num)
}

/// Update the stack visual in each PR body (only for the set we touched).
fn update_stack_bodies(stack: &Vec<PrRef>, dry: bool) -> Result<()> {
    if stack.is_empty() {
        return Ok(());
    }

    let numbers: Vec<u64> = stack.iter().map(|p| p.number).collect();
    let numbers_rev: Vec<u64> = numbers.iter().cloned().rev().collect();
    let bodies_by_number = fetch_pr_bodies_graphql(&numbers)?;
    let mut to_update: Vec<(u64, String, String)> = vec![]; // (number, id, new_body)

    for (idx, pr) in stack.iter().enumerate() {
        let mut body = bodies_by_number.get(&pr.number).map(|x| x.body.clone()).unwrap_or_default();

        let start = "<!-- spr-stack:start -->";
        let end = "<!-- spr-stack:end -->";
        let re = Regex::new(&format!(
            r"(?s){}.*?{}",
            regex::escape(start),
            regex::escape(end)
        ))?;
        body = re.replace(&body, "").trim().to_string();

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
        let new_body = if body.is_empty() { block.clone() } else { format!("{}\n\n{}", body, block) };

        if new_body.trim() == body.trim() {
            info!("PR #{} body unchanged; skipping edit", pr.number);
        } else {
            let id = bodies_by_number.get(&pr.number).map(|x| x.id.clone()).unwrap_or_default();
            if !id.is_empty() { to_update.push((pr.number, id, new_body)); }
        }
    }
    if !to_update.is_empty() {
        let mut m = String::from("mutation {");
        for (i, (_num, id, body)) in to_update.iter().enumerate() {
            m.push_str(&format!("m{}: updatePullRequest(input:{{pullRequestId:\"{}\", body:\"{}\"}}){{ clientMutationId }} ", i, id, graphql_escape(body)));
        }
        m.push_str("}");
        gh_rw(dry, &["api", "graphql", "-f", &format!("query={}", m)])?;
        for (num, _, _) in to_update { info!("Updated stack visual in PR #{}", num); }
    }
    Ok(())
}
