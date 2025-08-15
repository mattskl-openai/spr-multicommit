use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug, Clone, Copy)]
pub enum Extent {
    /// Update the first N PRs (bottom-up)
    Pr { n: usize },
    /// Update only the first N commits from base..from (push partial groups if needed)
    Commits { n: usize },
}

#[derive(Clone, Copy, Debug)]
pub enum PrepSelection {
    Until(usize),
    Exact(usize),
    All,
}

#[derive(Subcommand, Debug, Clone, Copy)]
pub enum ListWhat {
    /// List PRs in the stack (bottom-up)
    Pr,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Build/refresh stacked PRs
    Update {
        /// Source ref to read commits from (if building from tags)
        #[arg(long, default_value = "HEAD")]
        from: String,

        /// Don’t create PRs, only (re)create branches
        #[arg(long)]
        no_pr: bool,

        /// If set, always restack existing spr/* PR branches (skip tag parsing)
        #[arg(long)]
        restack: bool,

        /// In --dry-run, assume PRs already exist for branches (so we print 'edit' instead of 'create')
        #[arg(long)]
        assume_existing_prs: bool,

        /// Rewrite PR descriptions (bodies) even when content would be unchanged
        #[arg(long, default_value_t = false)]
        update_pr_body: bool,

        /// Limit how much to update (optional sub-mode)
        #[command(subcommand)]
        extent: Option<Extent>,
    },

    /// Restack PRs by rebasing the top commits after the bottom N PR groups onto the latest base
    Restack {
        /// Ignore the bottom N PRs; rebase the remaining commits onto base
        #[arg(long, value_name = "N")]
        after: usize,
    },

    /// Prepare PRs for landing (e.g., squash)
    Prep {
        // selection is provided via global --until/--exact flags
    },

    /// List entities
    List {
        #[command(subcommand)]
        what: ListWhat,
    },

    /// Land PRs (merge variants)
    Land {
        // Target PR index is provided via global --until. For `flatten`, 0 means the top PR. For `per-pr`, 0 means all
        #[command(subcommand)]
        which: Option<LandCmd>,
    },

    /// Fix PR stack connectivity to match local commit stack
    FixStack {
        // dry-run is provided via global --dry-run
    },
}

#[derive(Subcommand, Debug, Clone, Copy)]
pub enum LandCmd {
    /// Flatten PRs from the bottom up to N (0 means all): set base to actual base then squash-merge each
    Flatten,
    /// Prior behavior: rebase-merge Nth and close previous with comments
    PerPr,
}

#[derive(Parser, Debug)]
#[command(
    name = "spr",
    version,
    about = "Stacked PRs from commit tags or existing spr/* branches"
)]
pub struct Cli {
    /// Verbose output for underlying git/gh commands
    #[arg(long, global = true)]
    pub verbose: bool,
    /// Global base branch (root of stack)
    #[arg(short = 'b', long, global = true)]
    pub base: Option<String>,
    /// Global branch prefix for per-PR branches
    #[arg(long, global = true)]
    pub prefix: Option<String>,
    /// Global dry-run flag (applies to all subcommands)
    #[arg(long, global = true)]
    pub dry_run: bool,
    /// Global until (used by prep/land). 0 means all
    #[arg(long, global = true)]
    pub until: Option<usize>,
    /// Global exact (used by prep)
    #[arg(long, global = true)]
    pub exact: Option<usize>,
    #[command(subcommand)]
    pub cmd: Cmd,
}
