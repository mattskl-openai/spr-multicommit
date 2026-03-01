use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug, Clone)]
pub enum Extent {
    /// Update the first N PRs (bottom-up)
    Pr {
        /// Limit updates through this local PR number or stable handle
        #[arg(long, value_name = "N|pr:<label>", conflicts_with_all = ["n", "legacy_n"])]
        to: Option<crate::selectors::GroupSelector>,
        /// Legacy numeric-only limit
        #[arg(long, value_name = "N", conflicts_with_all = ["to", "legacy_n"])]
        n: Option<usize>,
        /// Backward-compatible positional numeric limit
        #[arg(value_name = "N", hide = true, conflicts_with_all = ["to", "n"])]
        legacy_n: Option<usize>,
    },
    /// Update only the first N commits from base..from (push partial groups if needed)
    Commits { n: usize },
}

#[derive(Clone, Debug)]
pub enum PrepSelection {
    Until(crate::selectors::InclusiveSelector),
    Exact(crate::selectors::GroupSelector),
    All,
}

#[derive(Subcommand, Debug, Clone, Copy)]
pub enum ListWhat {
    /// List PRs in the stack (order via list_order config)
    #[command(alias = "p")]
    Pr,
    /// List commits in the stack (order via list_order config)
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

        /// How to manage PR descriptions (overrides pr_description_mode config)
        #[arg(long, value_enum)]
        pr_description_mode: Option<crate::config::PrDescriptionMode>,

        /// Bypass the recent branch-name reuse guard and allow creating a new PR even when the
        /// same branch name had a recently closed or merged PR.
        #[arg(long)]
        allow_branch_reuse: bool,

        /// Limit how much to update (optional sub-mode)
        #[command(subcommand)]
        extent: Option<Extent>,
    },

    /// Restack PRs by rebasing the top commits after the bottom N PR groups onto the latest base
    Restack {
        /// Keep groups through this selector in place and rebuild only the groups above it
        #[arg(long, value_name = "N|0|bottom|top|last|all|pr:<label>")]
        after: crate::selectors::AfterSelector,

        /// Create a local backup tag at current HEAD before rebasing
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
        /// Skip automatic restack after landing (default: restack remaining commits with `--after N`)
        #[arg(long = "no-restack")]
        no_restack: bool,
    },

    /// Relink PR stack to match local commit stack
    RelinkPrs {
        // dry-run is provided via global --dry-run
    },

    /// Delete remote branches with the configured prefix whose PRs are all closed
    #[command(alias = "clean")]
    Cleanup {
        // dry-run is provided via global --dry-run
    },

    /// Move the last M commits (top of stack) to the tail of PR N (1-based, bottom→top)
    #[command(visible_alias = "fix")]
    FixPr {
        /// Target local PR number or stable handle
        target: crate::selectors::GroupSelector,
        /// Number of top commits to move to PR N's tail
        #[arg(short = 't', long = "tail", default_value_t = 1)]
        tail: usize,
        /// Create a local backup tag at current HEAD before rewriting
        #[arg(long)]
        safe: bool,
    },

    /// Reorder local PR groups by moving one or a range to come after a target PR
    #[command(alias = "mv")]
    Move {
        /// Position or range to move: either `A`, `A..B`, `pr:<label>`, or `pr:<a>..pr:<b>`
        range: crate::selectors::GroupRangeSelector,
        /// Target PR position to come after: number, stable handle, or one of bottom/top/last/all
        #[arg(long, value_name = "C|bottom|top|last|all|pr:<label>")]
        after: crate::selectors::AfterSelector,
        /// Create a local backup tag at current HEAD before rewriting
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
    /// Global until (used by prep/land). Accepts 0, a local PR number, or a stable handle
    #[arg(long, global = true, value_name = "N|0|pr:<label>")]
    pub until: Option<crate::selectors::InclusiveSelector>,
    /// Global exact (used by prep). Accepts a local PR number or a stable handle
    #[arg(long, global = true, value_name = "I|pr:<label>")]
    pub exact: Option<crate::selectors::GroupSelector>,
    #[command(subcommand)]
    pub cmd: Cmd,
}
