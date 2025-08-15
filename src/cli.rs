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
        /// Base branch to stack onto (root PR bases on this)
        #[arg(short = 'b', long)]
        base: Option<String>,

        /// Branch prefix for per-PR branches
        #[arg(long)]
        prefix: Option<String>,

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

        /// Rewrite PR descriptions (bodies) even when content would be unchanged
        #[arg(long, default_value_t = false)]
        update_pr_body: bool,

        /// Limit how much to update (optional sub-mode)
        #[command(subcommand)]
        extent: Option<Extent>,
    },

    /// Prepare PRs for landing (e.g., squash)
    Prep {
        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long)]
        base: Option<String>,

        /// Branch prefix for per-PR branches
        #[arg(long)]
        prefix: Option<String>,

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
        #[arg(short = 'b', long)]
        base: Option<String>,

        /// Branch prefix for per-PR branches
        #[arg(long)]
        prefix: Option<String>,
    },

    /// Land PRs (merge variants)
    Land {
        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long)]
        base: Option<String>,

        /// Branch prefix for per-PR branches
        #[arg(long)]
        prefix: Option<String>,

        /// Print state-changing commands instead of executing
        #[arg(long)]
        dry_run: bool,

        /// Target PR index. For `flatten`, 0 means the top PR. For `per-pr`, 0 means all
        #[arg(long, value_name = "N")]
        until: usize,

        #[command(subcommand)]
        which: Option<LandCmd>,
    },

    /// Fix PR base connectivity to match local commit stack
    FixChain {
        /// Base branch to locate the root of the stack
        #[arg(short = 'b', long)]
        base: Option<String>,

        /// Branch prefix for per-PR branches
        #[arg(long)]
        prefix: Option<String>,

        /// Print state-changing commands instead of executing
        #[arg(long)]
        dry_run: bool,
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
    #[command(subcommand)]
    pub cmd: Cmd,
}
