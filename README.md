spr-multicommit
================

Stack-friendly CLI to manage, update, and land stacked GitHub pull requests.

- Builds a stack of PRs from commit markers
- PRs in the stack can have multiple commits, to better show diffs while being reviewed
- Keeps PR bases and bodies in sync with your local stack
- Supports two landing strategies: flatten (squash) and per-pr (rebase)

Installation
------------

- Requires `git` and GitHub CLI `gh` (authenticated) in `PATH`.
- Build from source:

```bash
cargo install --path .
```

Quick start
-----------

1. Create commits with `pr:<tag>` markers to define PR groups, bottom → top. Use `pr:ignore` to skip commits until the next `pr:<tag>`. Example:

```bash
git commit -m "feat: parser groundwork pr:alpha"
git commit -m "feat: alpha follow-ups"
git commit -m "chore: local experiments pr:ignore"
git commit -m "wip: spike cleanup"
git commit -m "feat: new API pr:beta" -m "Body explaining the change"
```

2. Configure defaults (optional) in `.spr_multicommit_cfg.yml` (see below), then:

```bash
# Build/refresh the stack (creates/updates branches and PRs)
spr update

# Inspect the stack
spr list pr
spr list commit
```

Configuration
-------------

spr reads configuration from YAML in two locations (repo overrides home):

1. `$HOME/.spr_multicommit_cfg.yml`
2. `<repo-root>/.spr_multicommit_cfg.yml`

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
# Commit with pr:ignore_tag starts ignore mode until the next pr:<tag>
ignore_tag: ignore

# How `spr update` manages PR descriptions from commit messages
pr_description_mode: overwrite | stack_only

# Order for printing PR/commit lists
# one of: "recent_on_bottom" (default) or "recent_on_top"
list_order: recent_on_bottom

# How `spr restack` behaves on cherry-pick conflicts: "rollback" (default) or "halt"
restack_conflict: rollback
```

Precedence for defaults:

- CLI flag > repo YAML > home YAML > git discovery (`origin/HEAD`)
- Base has no built-in fallback; if discovery fails, set `base` explicitly
- Built-in defaults still apply for non-base keys: `prefix = "${USER}-spr/"`, `land = flatten`, `ignore_tag = "ignore"`, `pr_description_mode = overwrite`, `list_order = recent_on_bottom`, `restack_conflict = rollback`

Global flags
------------

 - `--base, -b <BRANCH>`: root base branch (default from config)
- `--prefix <PREFIX>`: per-PR branch prefix (default from config, normalized to a single trailing `/`)
- `--dry-run` (alias: `--dr`): print state-changing commands instead of executing
- `--until <N>`: target range used by `prep` and `land` (0 means all)
- `--exact <I>`: used by `prep` to select exactly the I-th PR (1-based)
- `--verbose`: enable verbose logging of underlying git/gh commands

Commands
--------

### spr update

Build/refresh the stack from commit markers, or restack existing branches.

Aliases:

- `u`

Key options:

- `--from <REF>`: commit range upper bound when parsing tags (default `HEAD`) (untested)
- `--no-pr`: only (re)create branches; skip PR creation/updates (untested)
- `--pr-description-mode <overwrite|stack_only>`: override `pr_description_mode` for this update run
- Extent (optional subcommand):
  - `pr --n <N>`: limit to first N PRs from the bottom
  - `commits --n <N>`: limit to first N commits (untested)

Behavior:

- Parses `pr:<tag>` markers from `merge-base(base, from)..from` (commits between `pr:ignore` and the next `pr:<tag>` are ignored)
- Creates/updates per-PR branches and GitHub PRs
- Updates PR bodies with a visualized stack block and correct `baseRefName`
  - When `pr_description_mode` is `stack_only`, only the stack block (between markers) is updated; the rest of the body is preserved
  - May temporarily set existing PR bases to the repo base while pushing, then re-chain them to match the local stack

### spr restack

Restack the local stack by rebuilding commits after the bottom N PR groups onto the latest base.

Options:

- `--after <N|bottom|top|last|all>`: 'drop' the first N PR groups; rebase the remaining commits onto `--base`
  - `0` or `bottom`: restack all groups (moves everything after merge-base)
  - `top` or `last` or `all`: skip all PRs; ignored commits (pr:ignore blocks) are preserved, so the branch may remain ahead of base
- `--safe`: create a local backup branch at current `HEAD` before rebasing

Behavior:

- Computes PR groups from `merge-base(base, HEAD)..HEAD` using `pr:<tag>` markers (oldest → newest; ignore blocks are preserved but excluded from grouping)
- Drops the first N groups, then rebuilds the remaining commits onto `--base` via a temp worktree
  - Ignored commits attached to dropped groups are kept before the remaining stack
  - Ignored commits attached to kept groups move with those groups
- Updates the current branch to the rebuilt tip
- With `--safe`, a backup branch named like `backup/restack/<current-branch>-<short-sha>` is created first
- Conflict handling is controlled by `restack_conflict` in config.
- `rollback` (default) aborts the restack and attempts to clean up the temp restack worktree and branch (cleanup failures may require manual cleanup).
- `halt` stops on conflict, leaves the temp restack worktree and branch in place, and prints manual rollback/continue instructions.
- When using `halt`, resolve conflicts inside the printed temp worktree path; resolving in your original worktree will not advance the halted cherry-pick.

### spr list pr

Lists PRs in the current stack for the configured prefix. Display order is controlled by `list_order` (default `recent_on_bottom`); local PR numbers remain bottom → top.

Aliases:

- `p`

Legend: CI ✓/✗/◐ and Review ✓/✗/◐ indicate passing/failing/pending states when available. `⑃M` indicates the PR is already merged (open PRs take precedence when a branch has both open and historical merged PRs).

### spr status

Aliases:

- `stat`

Alias for `spr list pr`.

### spr list commit

Lists commits in the current stack, grouped by local PR. Display order is controlled by `list_order` (default `recent_on_bottom`); local PR numbers and commit indices remain bottom → top.

Aliases:

- `c`

### spr move

Reorder local PR groups by moving one or a range to come after a target PR.

Aliases:

- `mv`

- `spr move A --after C`: move PR at position A to come after PR C (C ∈ [0..N])
- `spr move A..B --after C`: move PRs A..B to come after PR C (requires A < B and C ∉ [A..B]; C ∈ [0..N])
  - `--after bottom` is the same as `--after 0`
  - `--after top` is the same as `--after N`
- `--safe`: create a local backup branch at current `HEAD` before rewriting
 - Ignore blocks (`pr:ignore`) stay attached to the preceding PR group and move with it

Prints an explicit plan, e.g.: `2..3→4: [1,2,3,4,5,6] → [1,4,2,3,5,6]`.

### spr land

Land PRs using either flatten or per-pr strategy.

Shared options (global):

- `--until <N>`: land first N PRs bottom-up (0 means all)
 - `--no-restack`: do not automatically restack after landing

Safety checks:

- Requires CI status SUCCESS and review APPROVED for PRs being landed.
- Override with `--unsafe` (aliases: `--force`, `-f`).

Mode selection:

- If `spr land <mode>` is specified, that mode is used
- If no `<mode>` is provided, the mode is read from config `land` (default `flatten`)

Default follow-up behavior:

- After a successful land, `spr` will automatically run `spr restack --after N` (using the same `N` from `--until`) to rebase the remaining commits onto the latest base. Pass `--no-restack` to skip this.

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

- Uses global `--until` / `--exact`

Behavior:

- Rewrites local history to ensure selected PRs become single-commit groups
- Pushes branches (respects `--dry-run`)
- Adds a warning to the next PR not included in the push

### spr fix-pr

Move the tail M commits (top of stack) to the tail of PR N (1-based, bottom→top).

Aliases:

- `spr fix N -t M`
- `spr fix N` (equivalent to `spr fix N -t 1`)

Usage:

```bash
# Move the top commit to the tail of PR 3
spr fix-pr 3

# Move the last 2 commits to the tail of PR 1
spr fix-pr 1 --tail 2
```

Behavior:

- Rewrites local history to move the tail M commits after PR N’s tail commit
- `--safe`: create a local backup branch at current `HEAD` before executing
- Ignore blocks (`pr:ignore`) are preserved and cannot be moved; the command aborts if the tail intersects an ignore block

### spr cleanup

Aliases:

- `clean`

Delete remote branches that match your configured `--prefix` and have no open PRs.

Behavior:

- Lists remote branches once and filters by `prefix`
- Batches GitHub lookups for open PRs
- Deletes all eligible branches in a single `git push --delete` call
- Respects `--dry-run`

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

Dry run behavior
----------------

- `--dry-run` prints most state-changing `git`/`gh` commands instead of executing
- For safety, some local operations may still execute in temporary worktrees to better mirror behavior
- In dry-run, set `--assume-existing-prs` with `spr update` to show `gh pr edit` instead of `gh pr create`

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

# Prep the first 3 PRs from the bottom
spr prep --until 3

# Restack everything onto the latest base
spr restack --after 0

# Restack everything above the first 2 PRs ('drops' the first 2 PRs)
spr restack --after 2

# Restack safely (creates a backup branch before rebase)
spr restack --after 2 --safe

# Land top PR only using config default mode (flatten by default)
spr land --until 1

# Explicitly land first 2 PRs via flatten
spr land flatten --until 2

# Explicitly land first 2 PRs via per-pr
spr land per-pr --until 2

# Reorder local PR groups 2..3 to come after PR 4 (creates a backup if desired)
spr move 2..3 --after 4 --safe

# Fix PR base chain on GitHub to reflect local stack
spr relink-prs
```
