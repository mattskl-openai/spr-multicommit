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
    #[command(alias = "p")]
    Pr,
    /// List commits in the stack (bottom-up)
    #[command(alias = "c")]
    Commit,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Build/refresh stacked PRs
    #[command(alias = "u")]
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
        /// Ignore the bottom N PRs; rebase the remaining commits onto base. Accepts a number, or keywords: bottom|top|last
        #[arg(long, value_name = "N|bottom|top|last")]
        after: String,

        /// Create a local backup branch at current HEAD before rebasing
        #[arg(long)]
        safe: bool,
    },

    /// Prepare PRs for landing (e.g., squash)
    Prep {
        // selection is provided via global --until/--exact flags
    },

    /// List entities
    #[command(alias = "ls")]
    List {
        #[command(subcommand)]
        what: ListWhat,
    },

    /// Status overview (alias for `list pr`)
    #[command(alias = "stat")]
    Status {
        // no options; uses global flags
    },

    /// Land PRs (merge variants)
    Land {
        // Target PR index is provided via global --until. For `flatten`, 0 means the top PR. For `per-pr`, 0 means all
        #[command(subcommand)]
        which: Option<LandCmd>,
        /// Allow bypassing safety validations (CI/review checks)
        #[arg(long = "unsafe", visible_alias = "force", visible_short_alias = 'f')]
        r#unsafe: bool,
    },

    /// Fix PR stack connectivity to match local commit stack
    FixStack {
        // dry-run is provided via global --dry-run
    },

    /// Delete remote branches with the configured prefix whose PRs are all closed
    #[command(alias = "clean")]
    Cleanup {
        // dry-run is provided via global --dry-run
    },

    /// Reorder local PR groups by moving one or a range to come after a target PR
    #[command(alias = "mv")]
    Move {
        /// Position or range to move: either `A` or `A..B` (1-based, bottom→top)
        range: String,
        /// Target PR position to come after: number (0..=N), or one of: bottom, top. Must not be in [A..B]
        #[arg(long, value_name = "C|bottom|top")]
        after: String,
        /// Create a local backup branch at current HEAD before rewriting
        #[arg(long)]
        safe: bool,
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
    #[arg(long, global = true, visible_alias = "dr")]
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
