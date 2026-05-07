use clap::{Args, Parser, Subcommand};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Args, Debug, Clone, Copy, Default)]
pub struct OutputArgs {
    /// Emit a single machine-readable JSON object to stdout
    #[arg(long = "json")]
    json_requested: bool,
}

impl OutputArgs {
    #[cfg(test)]
    pub fn human() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn json() -> Self {
        Self {
            json_requested: true,
        }
    }

    pub fn format(self) -> OutputFormat {
        if self.json_requested {
            OutputFormat::Json
        } else {
            OutputFormat::Human
        }
    }
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

        #[command(flatten)]
        output: OutputArgs,

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

        /// Print the resolved high-level restack plan and do not fetch, rewrite, or publish
        #[arg(long)]
        preview: bool,

        #[command(flatten)]
        output: OutputArgs,
    },

    /// Drop bottom PR groups whose GitHub PRs already merged, without landing or mutating PRs
    #[command(
        long_about = "Drop bottom PR groups whose GitHub PRs already merged, without landing or mutating PRs.\n\n`spr drop-merged-prefix` is local post-merge maintenance. It reads GitHub PR state, verifies each dropped PR's GitHub merge commit is contained in the configured SPR base, and then rewrites only the checked-out local stack.\n\nIt does not merge, close, retarget, comment on, or push GitHub PRs. After inspecting the rewritten stack, run `spr update` to publish remaining PR branch updates."
    )]
    DropMergedPrefix {
        /// Create a local backup tag at current HEAD before rewriting
        #[arg(long)]
        safe: bool,

        #[command(flatten)]
        output: OutputArgs,
    },

    /// Absorb commits appended to canonical local per-PR branches back into the checked-out stack branch
    #[command(
        long_about = "Absorb commits appended to canonical local per-PR branches back into the checked-out stack branch.\n\nIf you append commits to the end of a local PR branch such as `user-spr/alpha`, run `spr absorb` while the stack branch is checked out. `spr` rebuilds the local stack so the new commits become part of the matching PR group. The PR-group order stays the same.\n\nThis command is local-only: it rewrites the current stack branch, creates a backup tag, and does not update GitHub. After checking the result, run `spr update`.\n\nOnly exact local branches named `prefix + tag` are considered. If one of those branches still points at rewritten-equivalent stack commits, `spr absorb` accepts that prefix only when the branch still descends from the same stack merge-base and the matched pre-tail commit ends at the same tree as the canonical stack prefix. A no-op rewritten match is reported as `skip (rewritten-equivalent prefix)`, and only commits appended above that proven prefix are absorbed. `spr absorb` also refuses to operate when two live PR groups would derive synthetic branch names that differ only by case.\n\nExample:\n- The current stack has three PR groups: `pr:alpha`, `pr:beta`, and `pr:gamma`.\n- Check out `user-spr/alpha` and append 2 commits.\n- Check out the stack branch.\n- Run `spr absorb`.\n- Result: the 2 new commits are folded into the `pr:alpha` group, and the PR-group order stays the same.\n- Then run `spr update`.\n\nOn cherry-pick conflict, `spr absorb` leaves the temp rewrite worktree in place, writes a resume file under the repository common Git directory, and prints `spr resume <path>`. Resolve conflicts in that temp worktree, stage the resolution, and run the printed resume command.\n\nAdvanced:\n- By default, absorb blocks copied later commits when replaying the stack would become empty or ambiguous.\n- `--allow-replayed-duplicates` allows an earlier copied non-seed follow-up commit to coexist with its later replayed copy by keeping both commits in the rewritten stack."
    )]
    Absorb {
        /// Allow replayed duplicates and keep both copies when the later replay is non-seed
        #[arg(long)]
        allow_replayed_duplicates: bool,

        #[command(flatten)]
        output: OutputArgs,
    },

    /// Prepare PRs for landing (e.g., squash) and halt early on case-colliding synthetic branch names
    Prep {
        // selection is provided via global --until/--exact flags
        #[command(flatten)]
        output: OutputArgs,
    },

    /// Resume a suspended local rewrite from a resume-state file
    #[command(
        long_about = "Resume a suspended local rewrite from a resume-state file.\n\nRun `spr resume <path>` with the exact path printed by `spr restack`, `spr absorb`, `spr move`, or `spr fix-pr` after a cherry-pick conflict. The supported workflow is: resolve the conflict in the printed temp rewrite worktree, stage the resolution, and then run the printed `spr resume <path>` command from any worktree in the same repository.\n\nThe resume file lives under the repository common Git directory, usually `.git/spr/resume/`. `spr resume` tolerates one accidental manual `git cherry-pick --continue` for the paused step, but broader manual replay edits are rejected."
    )]
    Resume {
        /// Explicit path to the suspended rewrite's resume-state JSON file
        #[arg(value_name = "PATH")]
        path: PathBuf,

        #[command(flatten)]
        output: OutputArgs,
    },

    /// List entities and halt early on case-colliding synthetic branch names
    #[command(alias = "ls")]
    List {
        #[command(flatten)]
        output: OutputArgs,
        #[command(subcommand)]
        what: ListWhat,
    },

    /// Status overview (alias for `list pr`) with the same early synthetic branch-collision guard
    #[command(alias = "stat")]
    Status {
        #[command(flatten)]
        output: OutputArgs,
    },

    /// Find the owning stack branch for a PR branch or report that the target is already a stack branch
    #[command(
        long_about = "Find the owning stack branch for a PR branch using repo-local stack metadata.\n\nTargets may be omitted (use the current branch), a local branch name, a remote-qualified branch name such as `origin/dank-spr/alpha`, or a GitHub PR URL. This command is strict and metadata-backed: it does not scan unrelated branches or guess a likely owner."
    )]
    ResolveStack {
        /// Optional target: current branch, local branch, remote-qualified branch, or PR URL
        target: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
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
        #[command(flatten)]
        output: OutputArgs,
    },

    /// Relink PR stack to match local commit stack and halt early on case-colliding synthetic branch names
    RelinkPrs {
        // dry-run is provided via global --dry-run
        #[command(flatten)]
        output: OutputArgs,
    },

    /// Delete remote branches with the configured prefix whose PRs are all closed
    #[command(alias = "clean")]
    Cleanup {
        // dry-run is provided via global --dry-run
        #[command(flatten)]
        output: OutputArgs,
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
        #[command(flatten)]
        output: OutputArgs,
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
        #[command(flatten)]
        output: OutputArgs,
    },
}

impl Cmd {
    pub fn output_format(&self) -> OutputFormat {
        match self {
            Self::Restack { output, .. }
            | Self::DropMergedPrefix { output, .. }
            | Self::Absorb { output, .. }
            | Self::ResolveStack { output, .. }
            | Self::Resume { output, .. }
            | Self::Land { output, .. }
            | Self::FixPr { output, .. }
            | Self::Status { output, .. }
            | Self::Prep { output }
            | Self::RelinkPrs { output }
            | Self::Cleanup { output }
            | Self::Move { output, .. } => output.format(),
            Self::List { output, .. } => output.format(),
            Self::Update { output, .. } => output.format(),
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
    use super::{Cli, Cmd, OutputFormat};
    use clap::{CommandFactory, Parser};
    use std::path::PathBuf;

    #[test]
    fn absorb_override_flag_parses() {
        let cli = Cli::try_parse_from(["spr", "absorb", "--allow-replayed-duplicates"]).unwrap();

        match cli.cmd {
            Cmd::Absorb {
                allow_replayed_duplicates,
                output: _,
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
    fn drop_merged_prefix_help_text_says_local_post_merge_maintenance() {
        let mut cli = Cli::command();
        let command = cli.find_subcommand_mut("drop-merged-prefix").unwrap();
        let long_about = command.get_long_about().unwrap().to_string();

        assert!(long_about.contains("local post-merge maintenance"));
        assert!(long_about.contains("without landing or mutating PRs"));
        assert!(long_about.contains("does not merge, close, retarget, comment on, or push"));
        assert!(long_about.contains("run `spr update`"));
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
            Cmd::Resume { path, output } => {
                assert_eq!(path, PathBuf::from(".git/spr/resume/restack-example.json"));
                assert_eq!(output.format(), OutputFormat::Json);
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
        assert_eq!(restack.cmd.output_format(), OutputFormat::Json);

        let drop_merged = Cli::try_parse_from(["spr", "drop-merged-prefix", "--json"]).unwrap();
        assert_eq!(drop_merged.cmd.output_format(), OutputFormat::Json);

        let land = Cli::try_parse_from(["spr", "land", "--json"]).unwrap();
        assert_eq!(land.cmd.output_format(), OutputFormat::Json);

        let list = Cli::try_parse_from(["spr", "list", "--json", "pr"]).unwrap();
        assert_eq!(list.cmd.output_format(), OutputFormat::Json);
    }

    #[test]
    fn restack_preview_flag_parses_with_json_and_safe() {
        let cli = Cli::try_parse_from([
            "spr",
            "restack",
            "--after",
            "pr:alpha",
            "--safe",
            "--preview",
            "--json",
        ])
        .unwrap();

        match cli.cmd {
            Cmd::Restack {
                after,
                safe,
                preview,
                output,
            } => {
                assert_eq!(after.to_string(), "pr:alpha");
                assert!(safe);
                assert!(preview);
                assert_eq!(output.format(), OutputFormat::Json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn update_json_flag_enables_json_mode() {
        let cli = Cli::try_parse_from(["spr", "update", "--json"]).unwrap();

        assert_eq!(cli.cmd.output_format(), OutputFormat::Json);
    }

    #[test]
    fn update_json_flag_parses_with_extent_subcommand() {
        let cli = Cli::try_parse_from(["spr", "update", "--json", "pr", "1"]).unwrap();

        match cli.cmd {
            Cmd::Update {
                output,
                extent: Some(super::Extent::Pr { legacy_n, .. }),
                ..
            } => {
                assert_eq!(output.format(), OutputFormat::Json);
                assert_eq!(legacy_n, Some(1));
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn list_pr_json_flag_parses_before_leaf_subcommand() {
        let cli = Cli::try_parse_from(["spr", "list", "--json", "pr"]).unwrap();

        match cli.cmd {
            Cmd::List {
                output,
                what: super::ListWhat::Pr,
            } => {
                assert_eq!(output.format(), OutputFormat::Json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn list_command_parses_json_flag() {
        let cli = Cli::try_parse_from(["spr", "list", "--json", "commit"]).unwrap();

        match cli.cmd {
            Cmd::List {
                output,
                what: super::ListWhat::Commit,
            } => {
                assert_eq!(output.format(), OutputFormat::Json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn list_json_flag_after_leaf_subcommand_is_rejected() {
        let err = Cli::try_parse_from(["spr", "list", "pr", "--json"]).unwrap_err();

        assert!(err.to_string().contains("unexpected argument '--json'"));
    }

    #[test]
    fn status_command_parses_json_flag() {
        let cli = Cli::try_parse_from(["spr", "status", "--json"]).unwrap();

        match cli.cmd {
            Cmd::Status { output } => {
                assert_eq!(output.format(), OutputFormat::Json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn update_help_includes_json_flag() {
        let mut cli = Cli::command();
        let update = cli.find_subcommand_mut("update").unwrap();

        assert!(update
            .get_arguments()
            .any(|argument| argument.get_long() == Some("json")));
    }

    #[test]
    fn global_cd_flag_parses_after_subcommand() {
        let cli = Cli::try_parse_from(["spr", "status", "--cd", "/tmp/example"]).unwrap();

        assert_eq!(cli.cd, Some(PathBuf::from("/tmp/example")));
        assert!(matches!(
            cli.cmd,
            Cmd::Status { output } if output.format() == OutputFormat::Human
        ));
    }

    #[test]
    fn global_cd_flag_parses_before_subcommand() {
        let cli = Cli::try_parse_from(["spr", "--cd", "/tmp/example", "status"]).unwrap();

        assert_eq!(cli.cd, Some(PathBuf::from("/tmp/example")));
        assert!(matches!(
            cli.cmd,
            Cmd::Status { output } if output.format() == OutputFormat::Human
        ));
    }
}
