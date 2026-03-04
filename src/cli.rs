use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Subcommand, Debug, Clone)]
pub enum Extent {
    /// Update the first N PRs (bottom-up)
    Pr {
        /// Limit updates through this local PR number or stable handle
        #[arg(long, value_name = "N|label|pr:<label>", conflicts_with_all = ["n", "legacy_n"])]
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
    /// List PRs in the stack (halts early if live groups derive case-colliding synthetic branch names)
    #[command(alias = "p")]
    Pr,
    /// List commits in the stack (halts early if live groups derive case-colliding synthetic branch names)
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
        /// same synthetic branch name, including case-only variants, had a recently closed or
        /// merged PR.
        #[arg(long)]
        allow_branch_reuse: bool,

        /// Limit how much to update (optional sub-mode)
        #[command(subcommand)]
        extent: Option<Extent>,
    },

    /// Restack PRs by rebasing the top commits after the bottom N PR groups onto the latest base
    #[command(
        long_about = "Restack PRs by rebasing the top commits after the bottom N PR groups onto the latest base.\n\nWhen `restack_conflict` is `halt`, `spr restack` leaves the temp rewrite worktree in place on conflict, writes a resume file under the repository common Git directory, and prints `spr resume <path>`. Resolve conflicts in that temp worktree, stage the resolution, and hand control back to `spr` with the printed resume command.\n\nWhen `restack_conflict` is `rollback`, `spr restack` preserves the historical cleanup-on-conflict behavior and removes the temp rewrite state instead."
    )]
    Restack {
        /// Keep groups through this selector in place and rebuild only the groups above it
        #[arg(long, value_name = "N|0|bottom|top|last|all|label|pr:<label>")]
        after: crate::selectors::AfterSelector,

        /// Create a local backup tag at current HEAD before rebasing
        #[arg(long)]
        safe: bool,

        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },

    /// Absorb commits appended to canonical local per-PR branches back into the checked-out stack branch
    #[command(
        long_about = "Absorb commits appended to canonical local per-PR branches back into the checked-out stack branch.\n\nIf you append commits to the end of a local PR branch such as `user-spr/alpha`, run `spr absorb` while the stack branch is checked out. `spr` rebuilds the local stack so the new commits become part of the matching PR group. The PR-group order stays the same.\n\nThis command is local-only: it rewrites the current stack branch, creates a backup tag, and does not update GitHub. After checking the result, run `spr update`.\n\nOnly exact local branches named `prefix + tag` are considered. If one of those branches still points at rewritten-equivalent stack commits, `spr absorb` accepts that prefix only when the branch still descends from the same stack merge-base and the matched pre-tail commit ends at the same tree as the canonical stack prefix. A no-op rewritten match is reported as `skip (rewritten-equivalent prefix)`, and only commits appended above that proven prefix are absorbed. `spr absorb` also refuses to operate when two live PR groups would derive synthetic branch names that differ only by case.\n\nExample:\n- The current stack has three PR groups: `pr:alpha`, `pr:beta`, and `pr:gamma`.\n- Check out `user-spr/alpha` and append 2 commits.\n- Check out the stack branch.\n- Run `spr absorb`.\n- Result: the 2 new commits are folded into the `pr:alpha` group, and the PR-group order stays the same.\n- Then run `spr update`.\n\nOn cherry-pick conflict, `spr absorb` leaves the temp rewrite worktree in place, writes a resume file under the repository common Git directory, and prints `spr resume <path>`. Resolve conflicts in that temp worktree, stage the resolution, and run the printed resume command.\n\nAdvanced:\n- By default, absorb blocks copied later commits when replaying the stack would become empty or ambiguous.\n- `--allow-replayed-duplicates` allows an earlier copied non-seed follow-up commit to coexist with its later replayed copy by keeping both commits in the rewritten stack."
    )]
    Absorb {
        /// Allow replayed duplicates and keep both copies when the later replay is non-seed
        #[arg(long)]
        allow_replayed_duplicates: bool,

        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },

    /// Prepare PRs for landing (e.g., squash) and halt early on case-colliding synthetic branch names
    Prep {
        // selection is provided via global --until/--exact flags
    },

    /// Resume a suspended local rewrite from a resume-state file
    #[command(
        long_about = "Resume a suspended local rewrite from a resume-state file.\n\nRun `spr resume <path>` with the exact path printed by `spr restack`, `spr absorb`, `spr move`, or `spr fix-pr` after a cherry-pick conflict. The supported workflow is: resolve the conflict in the printed temp rewrite worktree, stage the resolution, and then run the printed `spr resume <path>` command from any worktree in the same repository.\n\nThe resume file lives under the repository common Git directory, usually `.git/spr/resume/`. `spr resume` tolerates one accidental manual `git cherry-pick --continue` for the paused step, but broader manual replay edits are rejected."
    )]
    Resume {
        /// Explicit path to the suspended rewrite's resume-state JSON file
        #[arg(value_name = "PATH")]
        path: PathBuf,

        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },

    /// List entities and halt early on case-colliding synthetic branch names
    #[command(alias = "ls")]
    List {
        #[command(subcommand)]
        what: ListWhat,
    },

    /// Status overview (alias for `list pr`) with the same early synthetic branch-collision guard
    #[command(alias = "stat")]
    Status {
        // no options; uses global flags
    },

    /// Land PRs (merge variants) and halt early on case-colliding synthetic branch names
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
        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },

    /// Relink PR stack to match local commit stack and halt early on case-colliding synthetic branch names
    RelinkPrs {
        // dry-run is provided via global --dry-run
    },

    /// Delete remote branches with the configured prefix whose PRs are all closed
    #[command(alias = "clean")]
    Cleanup {
        // dry-run is provided via global --dry-run
    },

    /// Move the last M commits (top of stack) to the tail of a selected PR group
    #[command(visible_alias = "fix")]
    FixPr {
        /// Target local PR number or stable handle
        target: crate::selectors::GroupSelector,
        /// Number of top commits to move to the selected PR group's tail
        #[arg(short = 't', long = "tail", default_value_t = 1)]
        tail: usize,
        /// Create a local backup tag at current HEAD before rewriting
        #[arg(long)]
        safe: bool,
        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },

    /// Reorder local PR groups by moving one or a range to come after a target PR, halting early on case-colliding synthetic branch names
    #[command(alias = "mv")]
    Move {
        /// Position or range to move: `A`, `A..B`, `label`, `pr:<label>`, `a..b`, or `pr:<a>..pr:<b>`
        range: crate::selectors::GroupRangeSelector,
        /// Target PR position to come after: number, stable handle, or one of bottom/top/last/all
        #[arg(long, value_name = "C|bottom|top|last|all|label|pr:<label>")]
        after: crate::selectors::AfterSelector,
        /// Create a local backup tag at current HEAD before rewriting
        #[arg(long)]
        safe: bool,
        /// Emit machine-readable JSON to stdout and keep stderr quiet unless the underlying tools leak output unexpectedly
        #[arg(long)]
        json: bool,
    },
}

impl Cmd {
    pub fn json_mode(&self) -> bool {
        match self {
            Self::Restack { json, .. }
            | Self::Absorb { json, .. }
            | Self::Resume { json, .. }
            | Self::Land { json, .. }
            | Self::FixPr { json, .. }
            | Self::Move { json, .. } => *json,
            Self::Update { .. }
            | Self::Prep {}
            | Self::List { .. }
            | Self::Status {}
            | Self::RelinkPrs {}
            | Self::Cleanup {} => false,
        }
    }
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
    /// Change to PATH before loading repo config or running git/gh commands
    #[arg(long, global = true, value_name = "PATH")]
    pub cd: Option<PathBuf>,
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
    #[arg(long, global = true, value_name = "N|0|label|pr:<label>")]
    pub until: Option<crate::selectors::InclusiveSelector>,
    /// Global exact (used by prep). Accepts a local PR number or a stable handle
    #[arg(long, global = true, value_name = "I|label|pr:<label>")]
    pub exact: Option<crate::selectors::GroupSelector>,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Cmd};
    use clap::{CommandFactory, Parser};
    use std::path::PathBuf;

    #[test]
    fn absorb_override_flag_parses() {
        let cli = Cli::try_parse_from(["spr", "absorb", "--allow-replayed-duplicates"]).unwrap();

        match cli.cmd {
            Cmd::Absorb {
                allow_replayed_duplicates,
                json: _,
            } => assert!(allow_replayed_duplicates),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn absorb_help_text_mentions_pr_groups_and_example_flow() {
        let mut cli = Cli::command();
        let absorb = cli.find_subcommand_mut("absorb").unwrap();
        let long_about = absorb.get_long_about().unwrap().to_string();

        assert!(long_about.contains(
            "The current stack has three PR groups: `pr:alpha`, `pr:beta`, and `pr:gamma`."
        ));
        assert!(long_about.contains("Check out `user-spr/alpha` and append 2 commits."));
        assert!(long_about.contains("Check out the stack branch."));
        assert!(long_about.contains("Run `spr absorb`."));
        assert!(long_about.contains(
            "Result: the 2 new commits are folded into the `pr:alpha` group, and the PR-group order stays the same."
        ));
        assert!(long_about.contains("spr resume <path>"));
        assert!(long_about.contains("skip (rewritten-equivalent prefix)"));
    }

    #[test]
    fn resume_command_parses_explicit_path() {
        let cli = Cli::try_parse_from([
            "spr",
            "resume",
            "--json",
            ".git/spr/resume/restack-example.json",
        ])
        .unwrap();

        match cli.cmd {
            Cmd::Resume { path, json } => {
                assert_eq!(path, PathBuf::from(".git/spr/resume/restack-example.json"));
                assert!(json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn resume_help_text_mentions_supported_workflow() {
        let mut cli = Cli::command();
        let resume = cli.find_subcommand_mut("resume").unwrap();
        let long_about = resume.get_long_about().unwrap().to_string();

        assert!(long_about.contains("resolve the conflict"));
        assert!(long_about.contains("spr resume <path>"));
        assert!(long_about.contains("common Git directory"));
    }

    #[test]
    fn rewrite_commands_report_json_mode() {
        let restack = Cli::try_parse_from(["spr", "restack", "--after", "1", "--json"]).unwrap();
        assert!(restack.cmd.json_mode());

        let land = Cli::try_parse_from(["spr", "land", "--json"]).unwrap();
        assert!(land.cmd.json_mode());

        let list = Cli::try_parse_from(["spr", "list", "pr"]).unwrap();
        assert!(!list.cmd.json_mode());
    }

    #[test]
    fn global_cd_flag_parses_after_subcommand() {
        let cli = Cli::try_parse_from(["spr", "status", "--cd", "/tmp/example"]).unwrap();

        assert_eq!(cli.cd, Some(PathBuf::from("/tmp/example")));
        assert!(matches!(cli.cmd, Cmd::Status {}));
    }

    #[test]
    fn global_cd_flag_parses_before_subcommand() {
        let cli = Cli::try_parse_from(["spr", "--cd", "/tmp/example", "status"]).unwrap();

        assert_eq!(cli.cd, Some(PathBuf::from("/tmp/example")));
        assert!(matches!(cli.cmd, Cmd::Status {}));
    }
}
