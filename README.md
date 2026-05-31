spr-multicommit
================

Stack-friendly CLI to manage, update, and land stacked GitHub pull requests.

- Builds a stack of PRs from commit markers
- PRs in the stack can have multiple commits, to better show diffs while being reviewed
- Keeps PR bases and bodies in sync with your local stack
- Supports two landing strategies: flatten (squash) and per-pr (rebase)

Installation
------------

- Requires `git` in `PATH`. GitHub CLI `gh` (authenticated) is required for GitHub-backed commands such as `spr update` without `--no-pr`, `spr list`, and `spr land`, and also for `spr move` because it checks the bottom PR's auto-merge state before rewriting the stack.
- Build from source:

```bash
cargo install --path .
```

Quick start
-----------

1. Create commits with `pr:<tag>` or `branch:<branch-name>` markers to define PR groups, bottom → top. Use `pr:ignore` to skip commits until the next group marker. Example:

```bash
git commit -m "feat: parser groundwork pr:alpha"
git commit -m "feat: alpha follow-ups"
git commit -m "chore: local experiments pr:ignore"
git commit -m "wip: spike cleanup"
git commit -m "feat: new API pr:beta" -m "Body explaining the change"
```

Ignored blocks are local-only. If an ignored block sits below later group
markers, `spr update` warns and leaves those later groups local-only instead of
publishing PRs whose GitHub diffs would include the ignored commits.

2. Configure defaults (optional) in `.spr_multicommit_cfg.yml` (see below), then:

```bash
# Build/refresh the stack (creates/updates branches and PRs)
spr update

# Inspect the stack
spr list pr
spr list commit

# Discover which stack branch owns a canonical PR branch
spr resolve-stack dank-spr/alpha

# After appending commits directly to canonical local per-PR branches,
# fold those tails back into the checked-out stack branch.
spr absorb

# Inspect the rewritten stack, then republish the per-PR branches.
spr list commit
spr update
```

Each PR group has exactly one marker. `pr:<tag>` derives `prefix + tag`, while
`branch:<branch-name>` preserves that exact Git branch name. `pr:` labels must
start with an ASCII letter; `branch:` payloads use Git branch-name validation.
Commands that target groups accept explicit selectors such as `pr:beta` or
`branch:feature/login`, plus bare selectors such as `beta` only when exactly one
current-stack group matches. `LPR #N` is only the current local position in the
outstanding stack and may renumber after lower groups land or are removed.
Concrete branch-name conflict checks are separate and fold case on the resolved
branch name.
Any command that derives concrete branch names from the live stack may halt
before doing command-specific work if two outstanding groups would collide
under that case-insensitive branch-name comparison.

Configuration
-------------

spr reads configuration from YAML in two locations (repo overrides home):

1. `$HOME/.spr_multicommit_cfg.yml`
2. `<git-main-worktree-root>/.spr_multicommit_cfg.yml`

When `spr` runs from a linked worktree, repository config still comes from Git's main worktree.
A `.spr_multicommit_cfg.yml` that exists only in the linked worktree is ignored.

Supported keys:

```yaml
# Base branch used as the root of the stack
# If omitted, spr discovers origin/HEAD and errors if discovery fails
base: origin/main

# Branch prefix used for per-PR branches
# Trailing slashes are normalized to exactly one
prefix: mattskl-spr/

# Default land mode when not specified on the CLI
# one of: "flatten" (default) or "per-pr"
land: flatten

# Tag used to ignore commits between PR groups
# Commit with pr:ignore_tag starts ignore mode until the next group marker
# Must start with an ASCII letter
ignore_tag: ignore

# How `spr update` manages PR descriptions from commit messages
# This setting affects how existing PR bodies are updated after creation.
# Initial PR creation always uses the bottom commit in the PR group:
# the title comes from the first line of that commit message, and the
# PR description comes from the rest of that same commit message.
pr_description_mode: overwrite | stack_only

# Order for printing PR/commit lists
# one of: "recent_on_bottom" (default) or "recent_on_top"
list_order: recent_on_bottom

# Optional local synchronization for each group's resolved concrete branch.
# - `off` (default): do not create or move local per-PR branches
# - `update-existing`: move matching local branches that already exist
# - `create-or-update`: create missing matching local branches and move existing ones
# Read-only `list` / `status` commands surface any drift this policy would reconcile.
local_pr_branches: off

# How `spr restack` behaves on cherry-pick conflicts
# - `halt` (default): suspend, leave the temp worktree in place, and resume
#   with `spr resume <path>`
# - `rollback`: abort and clean up temp restack state
restack_conflict: halt

# How branch-rewriting commands handle local changes in the checked-out worktree
# This applies to `spr restack`, `spr move`, `spr fix-pr`, and `spr absorb`.
# - `discard` preserves the historical behavior: tracked changes may be lost,
#   while untracked files remain in place
# - `stash` stashes tracked, staged, and untracked changes and reapplies them
#   with `git stash apply --index`
# - `halt` (default) refuses to rewrite until the worktree is clean
dirty_worktree: halt

# Blocks PR recreation when the same synthetic head branch, including
# case-only variants, had a recently merged or closed PR within the configured
# window. Set to 0 to disable the guard.
branch_reuse_guard_days: 180

# How `spr update` handles pre-push validation.
# - `legacy` (default): preserve Git's normal batched pre-push hook execution
# - `required`: require matching `spr validate` receipts, then push with
#   `--no-verify` to avoid rerunning hooks
update_validation: legacy
```

Precedence for defaults:

- CLI flag > repo YAML > home YAML > git discovery (`origin/HEAD`)
- Base has no built-in fallback; if discovery fails, set `base` explicitly
- Built-in defaults still apply for non-base keys: `prefix = "${USER}-spr/"`, `land = flatten`, `ignore_tag = "ignore"`, `pr_description_mode = overwrite`, `list_order = recent_on_bottom`, `local_pr_branches = off`, `restack_conflict = halt`, `dirty_worktree = halt`, `update_validation = legacy`

Global flags
------------

- `--cd <PATH>`: change to `PATH` before loading repo config or running git/gh commands
- `--base, -b <BRANCH>`: root base branch (default from config)
- `--prefix <PREFIX>`: per-PR branch prefix (default from config, normalized to a single trailing `/`)
- `--local-pr-branches <off|update-existing|create-or-update>`: override local per-PR branch synchronization for this run
- `--until <N|0|name|pr:<label>|branch:<branch-name>>`: target range used by `update`, `validate`, `prep`, and `land` (`0` means all)
- `--exact <I|name|pr:<label>|branch:<branch-name>>`: used by `prep` to select exactly one PR group
- `--verbose`: enable verbose logging of underlying git/gh commands

Example:

```bash
spr --cd /path/to/repo status
```

Commands
--------

### spr update

Build/refresh the stack from commit markers, or restack existing branches.

Aliases:

- `u`

Key options:

- `--from <REF>`: commit range upper bound when parsing tags (default `HEAD`) (untested)
- `--no-pr`: only (re)create branches; skip PR creation/updates; this path stays Git-only in `--json` mode
- `--pr-description-mode <overwrite|stack_only>`: override `pr_description_mode` for this update run
- `--allow-branch-reuse`: bypass the recent closed-or-merged branch-name reuse guard
- `--skip-validation`: bypass receipt enforcement and push with `--no-verify`, intentionally skipping Git pre-push hooks
- `--json`: write exactly one update summary object to stdout
- `--until <N|0|name|pr:<label>|branch:<branch-name>>`: update only the lower prefix through one PR boundary (`0` means all)

Behavior:

- Parses group markers from `merge-base(base, from)..from` (commits between `pr:ignore` and the next group marker are ignored)
- Creates/updates per-PR branches and GitHub PRs
- Warns and skips any PR groups above an ignored block, because GitHub would include the ignored commits in those higher PRs
- When a PR is first created, `spr` always seeds it from the bottom commit in that PR group:
  the PR title comes from the first line of that commit message, and the PR description comes
  from the rest of that same commit message, regardless of `pr_description_mode`
- Before creating a PR for a branch head without an open PR, checks whether the same concrete
  branch name, including case-only variants, had a recently merged or closed PR and halts within
  `branch_reuse_guard_days`
- Refuses to operate when two live PR groups would derive concrete branch names that differ only
  by case, because those names are unsafe on case-insensitive filesystems
- Updates PR bodies with a visualized stack block and correct `baseRefName`
  - When `pr_description_mode` is `stack_only`, only the stack block (between markers) is updated; the rest of the body is preserved
  - After publishing branch heads, reconciles each PR base directly to the local stack chain
- When `local_pr_branches` is enabled, synchronizes local branches named exactly like each group's resolved concrete branch after the update succeeds. `update-existing` only moves existing local branches; `create-or-update` also creates missing ones.
- In the default `update_validation: legacy` mode, retains Git's normal batched pre-push behavior. A pre-commit-managed hook may select only one cumulative ref from each push batch.
- In `update_validation: required` mode, requires matching successful `spr validate` receipts for every selected PR boundary before changing remote branches, then pushes with `--no-verify`.
- `--skip-validation` bypasses both receipt enforcement and Git pre-push hooks.

### spr validate

Run configured Git pre-push hooks at every selected publishable PR boundary and record reusable
local receipts for the exact PR-local ranges.

Aliases:

- `v`

Behavior:

- Validates the full publishable prefix by default after applying the same ignored-block rule as `spr update`.
- Honors global `--until <N|0|name|pr:<label>|branch:<branch-name>>` to validate only the lower prefix through one PR boundary.
- Reuses one detached temporary worktree and checks out the final commit in each PR group, bottom → top.
- Runs the configured `pre-push` hook with a synthetic PR-local range: merge-base → PR 1 tip, then previous PR tip → current PR tip.
- Reuses matching receipts for unchanged PR-local ranges and runs hooks only for missing or stale boundaries. Extending a validated stack by one PR validates only the new boundary.
- Stops at the first hook failure and leaves the temporary worktree in place for inspection.
- Keeps receipts already recorded for successful lower boundaries when a later boundary fails, so retries resume incrementally.
- Refuses to issue a receipt when a successful hook rewrites tracked files.
- Records receipts under the repository common Git directory at `.git/spr/validation_receipts_v2/`.
- Succeeds with a receipt when no pre-push hook is installed.
- `--json` writes validated, recorded, and reused counts; ordered per-boundary receipt details; hook state; and success status.

Incremental workflow:

```bash
spr v --until 9
spr u pr --to 9
spr v --until 10  # reuses boundaries 1..=9 and validates only boundary 10
```

### spr restack

Restack the local stack by rebuilding commits after the bottom N PR groups onto the latest base.
Use this for local open-stack reshaping, such as reordering PR groups or rebuilding the remaining
open groups onto base. If bottom PRs already merged on GitHub and you want the local stack to catch
up, prefer `spr drop-merged-prefix`.

Options:

- `--after <N|0|bottom|top|last|all|name|pr:<label>|branch:<branch-name>>`: keep groups through this selector in place, then rebase only the groups above it onto `--base`
  - `0` or `bottom`: restack all groups (moves everything after merge-base)
  - `top` or `last` or `all`: skip all PRs; ignored commits (pr:ignore blocks) are preserved, so the branch may remain ahead of base
  - bare name, `pr:<label>`, or `branch:<branch-name>`: keep that group and everything below it in place even if local PR numbers renumber
- `--safe`: create a local backup tag at current `HEAD` before rebasing
- `--preview`: print the resolved high-level plan and stop before fetch, backup tags, temp worktrees, resume files, cherry-picks, branch resets, metadata writes, pushes, or GitHub calls
- `--json`: with `--preview`, write exactly one preview object to stdout

Behavior:

- Computes PR groups from `merge-base(base, HEAD)..HEAD` using group markers (oldest → newest; ignore blocks are preserved but excluded from grouping)
- `spr restack --after ... --preview` uses local refs only and reports that remote freshness, replay conflicts, tests, hooks, GitHub mergeability, and update/push results have not been validated
- Normal human `spr restack --after ...` prints the same high-level plan immediately before it starts the rewrite phase
- Drops the first N groups, then rebuilds the remaining commits onto `--base`
  - Ignored commits attached to dropped groups are kept before the remaining stack
  - Ignored commits attached to kept groups move with those groups
- Uses an in-place Git rebase for clean contiguous-suffix plans.
- Uses the existing temp-worktree cherry-pick executor when the plan is not one contiguous suffix,
  or when the fast rebase conflicts and can be aborted cleanly.
- Updates the current branch to the rebuilt tip
- When `local_pr_branches` is enabled, synchronizes local resolved PR branches to the final rewritten group tips after a successful rewrite.
- With `--safe`, a backup tag named like `backup/restack/<current-branch>-<short-sha>` is created first
- Conflict handling is controlled by `restack_conflict` in config.
- Before rewriting the checked-out branch, `spr restack` follows the `dirty_worktree` config.
- `halt` (default) suspends on conflict, leaves the temp restack worktree and branch in place, writes a resume file under the repository common Git directory, and prints `spr resume <path>`.
- `rollback` preserves the historical cleanup-on-conflict behavior and attempts to remove the temp restack worktree and branch (cleanup failures may require manual cleanup).
- When restack suspends, resolve conflicts inside the printed temp worktree path, stage the resolution, and run the printed `spr resume <path>` command. Resolving in your original worktree does not advance the suspended cherry-pick.

### spr adopt-prefix

Use `spr adopt-prefix` after rewriting an already-live bottom prefix locally while keeping its explicit `pr:` and `branch:` selectors intact. Run it from the rewritten candidate checkout. SPR resolves the owning live stack from those selectors, verifies that the candidate is still the exact live bottom sequence, preserves the existing stack merge base, and rebuilds the remaining raw history above the adopted prefix.

- The current checkout is the prefix source. The verified owning full-stack branch is the destination branch that gets moved.
- The candidate checkout may be detached. The owning full-stack branch must not be checked out in another worktree while SPR moves it.
- The selector sequence through `HEAD` must match an existing live bottom sequence exactly. This command does not also drop merged groups, reorder groups, or change the stack base.
- Ignored blocks are allowed only when adoption leaves the publishable selector sequence unchanged.
- `--preview` resolves and prints the plan without rewriting, creating backup tags, or touching GitHub.
- With `--safe`, SPR creates a backup tag named like `backup/adopt-prefix/<owning-stack-branch>-<short-sha>` at the old owning stack tip first.
- Before rewriting, `spr adopt-prefix` follows the shared `dirty_worktree` policy.
- On cherry-pick conflict, `spr adopt-prefix` suspends the rewrite, leaves the temp worktree in place, and prints `spr resume <path>`.
- This command is local-only. Inspect the rebuilt stack and run `spr update` when you are ready to publish the rewritten stack.

### spr drop-merged-prefix

Drop the bottom local PR groups whose GitHub PRs already merged, without landing or updating any
GitHub PR.
Prefer this when one or more bottom PRs already merged on GitHub and your local stack still
contains them. This is post-merge local cleanup; `spr restack` remains the general local rewrite
tool for reshaping the remaining open stack.

Typical workflow:

```bash
# After GitHub auto-merges the bottom PR:
spr drop-merged-prefix --safe
spr status
spr update
```

Behavior:

- Reads open/merged GitHub PR state for the local stack's synthetic PR branches
- Selects only the contiguous bottom prefix whose PRs are `MERGED`; if the bottom PR is still open, nothing is rewritten
- Refreshes `origin`, then fetches each dropped PR's GitHub merge commit OID and verifies that commit is already an ancestor of the configured SPR base, such as `origin/main`
- Uses a fast native Git rewrite for the common case, so the checked-out stack branch starts at the first still-open PR group on top of the updated base
- Falls back to the existing `spr restack` replay when it must preserve ignored local commits from the dropped prefix, or when native rebase cannot complete cleanly
- With `--safe`, creates a local backup tag named like `backup/drop-merged-prefix/<current-branch>-<short-sha>` before moving the branch
- Does not merge, close, retarget, comment on, push, or otherwise mutate GitHub PRs
- Does not run `spr update`; run it afterwards to publish the remaining open PR branch updates
- When `local_pr_branches` is enabled, synchronizes local resolved PR branches for the remaining stack after the local rewrite succeeds.

### spr absorb

Absorb commits appended to canonical local per-PR branches back into the owning stack branch.

Behavior:

- If you append commits to the end of a local PR branch such as `user-spr/alpha`, `spr absorb` may be run from that branch or from the owning stack branch; when the invoking checkout's selector sequence identifies one verified live stack, `spr absorb` rebuilds that owning stack so new commits from every absorbable local PR branch become part of their matching PR groups while preserving PR-group order
- Inspects only each group's exact resolved local branch
- Skips missing branches, branches whose rewritten-equivalent prefix still descends from the same stack merge-base and ends at the same pre-tail tree as the current stack, and branches that are behind the current stack
- Refuses to operate when two live PR groups would derive concrete branch names that differ only by case
- Refuses to guess through divergence, source branches that incorporated later stack commits, merge commits, or absorbed commits that contain group markers
- By default, also blocks copied later stack commits whose original replay would otherwise become empty or ambiguous
- `--allow-replayed-duplicates` overrides that copied-commit blocker for safe cases by absorbing the copied commit and keeping its later non-seed replay in the rebuilt stack
- Rebuilds the owning stack from its existing `merge-base(base, owning-stack-tip)` rather than restacking onto the latest base tip
- Inserts absorbed commits after the group's real commits and before that group's trailing ignored block
- Creates a local backup tag before rewriting the stack
- Before rewriting from the invoking checkout, `spr absorb` follows the `dirty_worktree` config. If the owning stack branch is checked out in another worktree, absorb refuses to move it by ref.
- On cherry-pick conflict, `spr absorb` suspends the rewrite, leaves the temp worktree in place, and prints `spr resume <path>`
- Does not update GitHub; inspect the rewritten stack first, then run `spr update`
- When `local_pr_branches` is enabled, synchronizes local resolved PR branches to the absorbed stack's final group tips after the local rewrite succeeds. A branch checked out in any worktree is reported as blocked and is not moved.
- `--from <N|name|pr:<label>|branch:<branch-name>>` constrains absorb to one explicit selected-and-higher suffix.
- `--query-changed-branches --json` reports the stack branch plus every resolved PR branch whose logical tip would change if the absorb rewrite ran, without creating backup tags, temp worktrees, or rewrite side effects. When paired with `--from`, it reports the full selected suffix even when only some branches currently have absorbable tails, so callers can query and apply one stable transaction boundary.

Typical workflow:

```bash
# The current stack has three PR groups: pr:alpha, pr:beta, and pr:gamma.
git checkout user-spr/alpha
git commit -m "feat: alpha branch tail"
git commit -m "feat: alpha branch tail 2"

spr absorb
spr list commit
spr update
```

Override example for intentionally keeping both an earlier copied follow-up commit and its later replay:

```bash
spr absorb --allow-replayed-duplicates

# Query the branch names that an absorb rewrite would change without rewriting anything.
spr absorb --query-changed-branches --json

# For a planned lower-group fix, query and apply the same explicit suffix.
spr absorb --from pr:beta --query-changed-branches --json
spr absorb --from pr:beta --json
```

### spr resolve-stack

Resolve a canonical PR branch back to its owning stack branch using repo-local
metadata stored under the repository common Git directory:

- Metadata path: `<git-common-dir>/spr/stack_metadata_v1.json`
- The filename is a stable historical path; the JSON `schema_version` inside the file is the
  authoritative format version
- Metadata is refreshed after successful `spr update`, `spr restack`,
  `spr adopt-prefix`, `spr absorb`, `spr move`, `spr fix-pr`, `spr resume`, and `spr land` when it
  also finishes the local follow-on restack
- Supported targets:
  - no argument: current branch
  - local branch name such as `dank-spr/alpha`
  - remote-qualified branch name such as `origin/dank-spr/alpha`
  - GitHub PR URL
- JSON mode returns typed states such as `found`, `already_stack_branch`,
  `missing_metadata`, `stale_metadata`, `tombstoned`, `ambiguous`, and
  `invalid_target`
- This command is strict and local-only after target normalization: it does not
  scan unrelated branches or repair stale metadata

Examples:

```bash
# PR branch -> owning stack branch
spr resolve-stack dank-spr/alpha

# Current branch
spr resolve-stack

# Automation-friendly JSON
spr resolve-stack --json https://github.com/org/repo/pull/123
```

### spr sync-local-branches

Reconcile local resolved PR branches with the checked-out stack without publishing or rewriting
the stack itself.

Behavior:

- Uses the same `local_pr_branches` policy as automatic post-command sync
- `off` is a no-op unless you override it with `--local-pr-branches`
- `update-existing` moves only already-present local branches
- `create-or-update` also creates missing local branches
- Refuses to move a branch that is checked out in any worktree
- Useful after manual Git surgery outside `spr`, such as an interactive rebase or manual commit
  reorder, when the next read-only `spr list` / `spr status` output reports local branch drift
- Does not touch GitHub or rewrite the checked-out stack branch

### spr resume

Resume a suspended local rewrite from the exact path printed by `spr restack`,
`spr adopt-prefix`, `spr absorb`, `spr move`, or `spr fix-pr`.

Behavior:

- Accepts one explicit resume-file path under the repository common Git directory, usually `.git/spr/resume/`
- Validates that the resume file belongs to the current repository and that the recorded temp worktree still exists
- The suspend output prints the temp worktree path, temp branch, original branch, and resume-file path so the caller knows exactly which rewrite is paused
- Supported workflow: resolve conflicts in the printed temp rewrite worktree, stage the resolution, then run the printed `spr resume <path>`
- Tolerates one accidental manual `git cherry-pick --continue` for the paused step, then resumes the remaining replay under `spr`
- Rejects broader manual replay edits, unknown resume-file schema versions, missing temp worktrees, or unresolved conflicts that are still staged as unmerged

Machine-readable `--json` mode:

- `--json` is a global output mode. It can appear before the command, after the command, or
  between a parent command and leaf subcommand, as long as it appears before a literal `--`
  passthrough marker. For example, `spr --json status`, `spr status --json`,
  `spr --json list commit`, `spr list --json commit`, and `spr list commit --json` are
  equivalent. A `--json` token after `--` is preserved as payload and does not change `spr`
  output mode.
- Supported on operational commands including `spr list pr`, `spr list commit`, `spr status`,
  `spr sync-local-branches`, `spr update`, `spr prep`, `spr relink-prs`, `spr cleanup`,
  `spr restack`, `spr adopt-prefix`, `spr absorb`, `spr move`, `spr fix-pr`, `spr land`, `spr resume`, and
  `spr resolve-stack`
- Also supported for display output: `spr --json --help`, `spr --help --json`,
  `spr --json help list commit`, `spr list commit --help --json`, `spr --json --version`, and
  `spr --version --json` each emit one structured JSON object
- In `--json` mode, stdout is exactly one JSON object and stderr is normally empty
- Summary-style commands (`list pr`, `list commit`, `status`, `sync-local-branches`, `update`,
  `prep`, `relink-prs`, and `cleanup`) share the same top-level shape: `schema_version`, `command`,
  `result: "summary"`, and `data`
- JSON help uses `result: "help"` and includes the resolved command path, usage, options,
  positionals, subcommands, aliases, and `rendered_text` containing Clap's normal human help
- JSON version uses `result: "version"` and includes the binary name and Cargo package version
- `spr list --json pr`, `spr list --json commit`, and `spr status --json` always emit canonical
  bottom-up stack order, even when `list_order: recent_on_top` changes the human display order.
  Each group's `remote.kind` encodes whether there is no matching PR, a PR without CI/review
  data, or a PR with typed CI/review status. When `local_pr_branches` is enabled, those read-only
  payloads also include `local_pr_branch_drift`, which lists the local branch actions a
  follow-on `spr sync-local-branches` would need to perform.
- `spr sync-local-branches --json` writes the effective local branch sync policy plus
  `local_pr_branch_actions` for the reconciliation it just performed.
- `spr update --json` writes repo context, resolved extent metadata, warnings, skipped groups,
  per-group branch or PR actions, and `local_pr_branch_actions`
- `spr prep --json` writes the resolved selection, selected-group rewrite actions, replay counts,
  next-child warning action, and the nested update summary for the rewritten tip
- `spr relink-prs --json` writes the expected local head/base chain plus one decision per PR head
- `spr cleanup --json` writes remote candidates, open-PR heads, per-branch decisions, and the
  delete batch
- `spr restack --preview --json` writes one preview object with `result: "preview"` and
  a `data` object containing the local base ref/SHA, current branch/HEAD, selected dropped groups,
  remaining groups, ignored-segment count, planned cherry-pick operation count, operations that a
  real run would perform, and the checks not validated by preview
- `spr adopt-prefix --preview --json` writes one preview object with `result: "preview"` and
  a `data` object containing candidate HEAD/groups, owning stack ID/branch/old head, shared merge
  base, replaced raw boundary, replay suffix groups, planned cherry-pick operation count,
  publishable selectors before/after, execution side-effect booleans, and the checks not validated
  by preview
- `spr restack --json` without `--preview` keeps the rewrite lifecycle contract:
  it writes one completed or suspended object, not a preview object. Completed rewrite JSON includes
  `local_pr_branch_actions`, which is empty unless local per-PR branch sync is enabled.
- `spr adopt-prefix --json` uses the same completed rewrite envelope and also includes
  `destination_branch` on successful apply, naming the owning stack branch that was moved.
- JSON errors across commands use one typed error envelope with `result: "error"` plus
  `error_kind` values such as `synthetic_branch_name_collision`, `invalid_arguments`, and
  `internal`
- Exit codes are:
  - `0` for completed
  - `1` for hard error, including CLI parse failures when `--json` was requested
  - `2` for suspended rewrite awaiting conflict resolution
- The suspended JSON payload includes the fields an agent needs to resolve and resume:
  - `original_worktree_root`
  - `original_branch`
  - `temp_branch`
  - `temp_worktree`
  - `resume_file`
  - `resume_argv`
  - `paused_source_sha`
  - `conflicted_paths`
  - `post_success_hint`

Example suspend payload:

```json
{
  "schema_version": 1,
  "result": "suspended",
  "command": "restack",
  "rewrite_command_kind": "restack",
  "original_worktree_root": "/path/to/repo",
  "original_branch": "stack",
  "temp_branch": "spr/tmp-restack-717b9d8",
  "temp_worktree": "/tmp/spr-restack-717b9d8",
  "resume_file": "/path/to/repo/.git/spr/resume/restack-stack-717b9d8.json",
  "resume_argv": [
    "spr",
    "--cd",
    "/path/to/repo",
    "resume",
    "--json",
    "/path/to/repo/.git/spr/resume/restack-stack-717b9d8.json"
  ],
  "paused_source_sha": "717b9d83bcdbea33286496800c76e65c62f795ed",
  "conflicted_paths": [
    "story.txt"
  ],
  "post_success_hint": null
}
```

Fallback rewrite mental model:

- `spr` uses the temp rewrite executor when a rewrite cannot run as a native Git operation
- The temp rewrite executor replays planned commits as individual cherry-picks in a temp worktree
- The temp rewrite worktree is the execution sandbox
- The resume file is a checkpoint for a paused temp-worktree rewrite
- During a temp-worktree rewrite, the original checked-out branch is not updated until the entire replay finishes successfully

Suspend/resume flow:

1. The original command (`spr restack`, `spr adopt-prefix`, `spr absorb`, `spr move`, or `spr fix-pr`) computes a replay plan for the rewritten stack.
2. If that command uses the temp rewrite executor, `spr` creates a temp branch and temp worktree at the right base commit.
3. `spr` starts replaying the plan as individual cherry-picks in that temp worktree.
4. If Git reports a cherry-pick conflict, `spr` records the paused rewrite state in the resume file, including the temp worktree path, the original branch identity, the paused temp-worktree `HEAD`, and the index of the failed replay step.
5. `spr` prints the temp worktree path, temp branch, original branch, and `spr resume <path>`, then leaves the temp worktree in place. The recorded rewrite destination has still not moved.
6. The user resolves only the current conflict in the printed temp worktree and stages the resolution with `git add`.
7. The user runs `spr resume <path>` from any worktree in the same repository.
8. `spr resume` reloads the checkpoint, validates that it still belongs to the current repository, validates that the temp worktree still exists, and then reconciles the paused step:
   - If `CHERRY_PICK_HEAD` is still present, `spr` continues that paused cherry-pick itself.
   - If `CHERRY_PICK_HEAD` is gone and the temp worktree advanced by exactly one commit, `spr` treats that as one accidental manual `git cherry-pick --continue` and resumes the remaining replay.
9. `spr` then continues the remaining replay steps under `spr` control. If another conflict happens, it rewrites the same resume file with the new paused step and suspends again.
10. Only after all replay steps succeed does `spr` finalize the recorded rewrite destination:
   - Checked-out-branch rewrites reset the original branch to the rebuilt temp tip.
   - Unchecked-out branch-ref rewrites such as `spr adopt-prefix` move that destination ref to the rebuilt temp tip while leaving the candidate checkout where it started.
   In both cases, `spr` then removes the temp worktree and temp branch and deletes the resume file.

Operator rules:

- Resolve conflicts in the printed temp worktree, not in your original worktree.
- Stage the resolution before running `spr resume <path>`.
- Hand control back to `spr` after resolving the current conflict instead of manually replaying the rest of the rewrite.
- One accidental manual `git cherry-pick --continue` is recoverable. Broader manual replay edits are intentionally rejected instead of being guessed through.

### spr list pr

Lists PRs in the current stack for the configured prefix. Display order is controlled by `list_order` (default `recent_on_bottom`); local PR numbers remain bottom → top, and the human output shows both the current `LPR #N` and each group's explicit selector. The derived concrete head branch is omitted because it is redundant with the selector.

Aliases:

- `p`

Legend:

- CI ✓/✗/◐ indicates passing/failing/pending CI status when available.
- Review ✓/✗/◐ indicates passing/failing/pending review status when available.
- `⑃M` indicates the PR is already merged (open PRs take precedence when a branch has both open and historical merged PRs).

Example summary line:

```text
✓✓ LPR #2 / pr:beta - abcdef12 (#17) - 3 commits
```

Before listing, `spr list pr` validates that no two live PR groups derive
concrete branch names that collide under case-insensitive comparison. If they
do, it halts before loading GitHub PR state.

`spr list --json pr` emits one read-only JSON object instead of human-formatted lines.
The payload always uses canonical bottom-up group order, includes remote PR metadata plus explicit
CI/review state when available, retains both `stable_handle` and `head_branch`, and reports
case-colliding concrete branch failures as a typed
`synthetic_branch_name_collision` error payload.

### spr status

Aliases:

- `stat`

Alias for `spr list pr`.

`spr status` runs the same early concrete branch-collision validation as
`spr list pr` before printing anything.

`spr status --json` emits the same read-only payload shape as `spr list --json pr`, but keeps the
top-level command identity as `status`.

### spr list commit

Lists commits in the current stack, grouped by local PR. Display order is controlled by `list_order` (default `recent_on_bottom`); local PR numbers and commit indices remain bottom → top, and each human group header shows its explicit selector without repeating the derived concrete head branch.

Before listing, `spr list commit` validates that no two live PR groups derive
concrete branch names that collide under case-insensitive comparison. If they
do, it halts before loading GitHub PR state.

`spr list --json commit` emits one read-only JSON object instead of human-formatted lines.
The payload uses the same canonical bottom-up group order as `spr list --json pr`, keeps canonical
global commit indices, retains both `stable_handle` and `head_branch`, and ignores `list_order` in
JSON mode.

Aliases:

- `c`

### spr move

Reorder local PR groups by moving one or a range to come after a target PR.

Requires authenticated GitHub CLI `gh` because `spr move` checks whether the
current bottom PR has auto-merge enabled before allowing a rewrite that would
move another PR below it.

Before planning the move, `spr move` validates that no two live PR groups
derive concrete branch names that collide under case-insensitive comparison.
If they do, it halts before the GitHub auto-merge lookup or any rewrite
planning.

Aliases:

- `mv`

- `spr move A --after C`: move PR at position A to come after PR C (C ∈ [0..N])
- `spr move A..B --after C`: move PRs A..B to come after PR C (requires A < B and C ∉ [A..B]; C ∈ [0..N])
- `spr move beta --after gamma`: move a bare selector without caring what its current local PR number is
- `spr move beta..gamma --after delta`: bare-selector range form
  - `--after bottom` is the same as `--after 0`
  - `--after top` is the same as `--after N`
- `--safe`: create a local backup tag at current `HEAD` before rewriting
- Ignore blocks (`pr:ignore`) stay attached to the preceding PR group and move with it
- Before rewriting the checked-out branch, `spr move` follows the `dirty_worktree` config.
- On cherry-pick conflict, `spr move` suspends the rewrite, leaves the temp worktree in place, and prints `spr resume <path>`

Prints an explicit plan, e.g.: `2..3→4: [1,2,3,4,5,6] → [1,4,2,3,5,6]`.

### spr land

Land PRs using either flatten or per-pr strategy.

Shared options (global):

- `--until <N|0|name|pr:<label>|branch:<branch-name>>`: land bottom-up through this local position or selector (`0` means all)
- `--no-restack`: do not automatically restack after landing

Before any GitHub land work, `spr land` validates that no two live PR groups
derive concrete branch names that collide under case-insensitive comparison.
If they do, it halts before resolving the PR segment to land.

Safety checks:

- Requires CI status SUCCESS and review APPROVED for PRs being landed.
- Override with `--unsafe` (aliases: `--force`, `-f`).

Mode selection:

- If `spr land <mode>` is specified, that mode is used
- If no `<mode>` is provided, the mode is read from config `land` (default `flatten`)

Default follow-up behavior:

- After a successful land, `spr` will automatically run `spr restack --after N` using the resolved group count from `--until`, so `spr land --until pr:beta` still restacks the correct remaining groups after `beta` disappears from the outstanding stack. Pass `--no-restack` to skip this.
- If that follow-on restack suspends, the GitHub land already succeeded. Resolve the local restack conflict and run the printed `spr resume <path>` command instead of rerunning `spr land`.

#### Mode: flatten

- For PRs 1..=N (or all when N==0):
  - Sets the N-th PR’s `baseRefName` to the actual base and squash-merges it
  - Adds a comment to and closes the previous PRs in the landed set

#### Mode: per-pr

- Validates that each PR in 1..=N has exactly one unique commit over its parent (abort if not)
- Use in conjunction with `spr prep`
- For the N-th PR:
  - Sets `baseRefName` to the actual base and rebase-merges it
- For PRs 1..=N-1:
  - Adds a comment linking to the N-th PR and closes them

### spr prep

Prepare PRs for landing per-PR - squashes each PR's commits into a single commit.

- Uses global `--until <N|0|name|pr:<label>|branch:<branch-name>>`, global
  `--exact <I|name|pr:<label>|branch:<branch-name>>`, or local
  `--from <N|name|pr:<label>|branch:<branch-name>>`; these selectors are mutually exclusive
- Before rewriting or pushing, `spr prep` validates that no two live PR groups
  derive concrete branch names that collide under case-insensitive
  comparison. If they do, it halts before the local squash or follow-on
  `spr update`.

Behavior:

- Rewrites local history to ensure selected PRs become single-commit groups
- Squashes each selected PR group independently; it does not combine commits
  across PR-group boundaries. For ordinary non-empty groups, that preserves the
  selected PRs' net diffs relative to their parent groups.
- Empty selected groups keep the existing `skipped_empty` behavior when their
  tip tree already matches the parent tree.
- Pushes branches (respects `--dry-run`)
- Adds a warning to the next PR not included in the push
- When `local_pr_branches` is enabled, the nested update path also synchronizes local resolved PR
  branches after the prepared rewrite succeeds.
- `--json` writes the typed prep summary instead of human log lines
- In `--dry-run`, prep still computes the hypothetical rewritten commit chain so the JSON summary
  reflects the rewritten stack instead of the current branch tip

### spr fix-pr

Move the tail M commits (top of stack) to the tail of a PR group selected by local PR number or selector.

Aliases:

- `spr fix N -t M`
- `spr fix N` (equivalent to `spr fix N -t 1`)
- `spr fix-pr beta --tail M`

Usage:

```bash
# Move the top commit to the tail of PR 3
spr fix-pr 3

# Move the top commit to the tail of the stable beta group
spr fix-pr beta

# Move the last 2 commits to the tail of PR 1
spr fix-pr 1 --tail 2
```

Behavior:

- Rewrites local history to move the tail M commits after the selected PR group's tail commit
- `--safe`: create a local backup tag at current `HEAD` before executing
- Ignore blocks (`pr:ignore`) are preserved and cannot be moved; the command aborts if the tail intersects an ignore block
- Before rewriting the checked-out branch, `spr fix-pr` follows the `dirty_worktree` config.
- On cherry-pick conflict, `spr fix-pr` suspends the rewrite, leaves the temp worktree in place, and prints `spr resume <path>`

### spr cleanup

Aliases:

- `clean`

Delete remote branches that match your configured `--prefix` and have no open PRs.

Behavior:

- Lists remote branches once and filters by `prefix`
- Batches GitHub lookups for open PRs
- Deletes all eligible branches in a single `git push --delete` call
- Respects `--dry-run`
- `--json` writes the typed cleanup summary instead of human log lines

Examples:

```bash
# Preview what would be deleted
spr cleanup --dry-run

# Delete branches safely (no open PRs on any of them)
spr cleanup
```

### spr relink-prs

Fix (GitHub) PR stack connectivity to match the local commit stack.

Behavior:

- Computes the expected bottom → top chain from local commits and updates each PR’s base to match.
- Skips PRs that are already correct; warns for missing PRs.
- Before comparing local and remote connectivity, `spr relink-prs` validates
  that no two live PR groups derive concrete branch names that collide under
  case-insensitive comparison. If they do, it halts before any GitHub edit.
- `--json` writes the typed relink summary instead of human log lines

Dry run behavior
----------------

- `--dry-run` (alias: `--dr`) is a command-local option accepted only by commands that can change
  local or remote state. Read-only commands such as `status`, `list`, and `resolve-stack` reject it.
- `--dry-run` prints most state-changing `git`/`gh` commands instead of executing
- For safety, some local operations may still execute in temporary worktrees to better mirror behavior
- In dry-run, set `--assume-existing-prs` with `spr update` to show `gh pr edit` instead of `gh pr create`
- `spr restack --preview` is not a dry-run rewrite: it only prints the resolved plan and does not
  fetch, create rewrite state, or exercise cherry-pick conflict handling

Notes
-----

- `.spr_multicommit_cfg.yml` is ignored by Git (see `.gitignore`)
- Branch prefix is normalized to exactly one trailing `/`
- Progress messages are shown before potentially slow GraphQL operations, even without `--verbose`

Examples
--------
![demo](./demo.gif)

```bash
# Build/refresh using defaults from config
spr update

# Update only through the stable beta group
spr update --until beta

# Prep the stack through the stable beta group
spr prep --until beta

# Prep exactly the stable gamma group
spr prep --exact gamma

# Prep the stable gamma group and every group above it
spr prep --from gamma

# Prep the first 3 PRs from the bottom
spr prep --until 3

# Restack everything onto the latest base
spr restack --after 0

# Restack everything above the first 2 PRs ('drops' the first 2 PRs)
spr restack --after 2

# Restack safely (creates a backup tag before rebase)
spr restack --after 2 --safe

# Restack only the groups above the stable beta group
spr restack --after beta

# Preview that restack plan without fetching or rewriting
spr restack --after beta --preview

# Emit the restack preview as one JSON object
spr restack --after beta --preview --json

# Land top PR only using config default mode (flatten by default)
spr land --until 1

# Land through the stable beta group
spr land flatten --until beta

# Explicitly land first 2 PRs via flatten
spr land flatten --until 2

# Explicitly land first 2 PRs via per-pr
spr land per-pr --until 2

# Reorder local PR groups 2..3 to come after PR 4 (creates a backup if desired)
spr move 2..3 --after 4 --safe

# Reorder stable groups without depending on local PR renumbering
spr move beta..gamma --after delta

# Fix PR base chain on GitHub to reflect local stack
spr relink-prs
```
