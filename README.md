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

Configuration
-------------

spr reads configuration from YAML in two locations (repo overrides home):

1. `$HOME/.spr_multicommit_cfg.yml`
2. `<repo-root>/.spr_multicommit_cfg.yml`

Supported keys:

```yaml
# Default base branch used when not provided via CLI
base: origin/oai-main  # example; use your repo's default (e.g., main)

# Branch prefix used for per-PR branches
# Trailing slashes are normalized to exactly one
prefix: mattskl-spr/

# Default land mode when not specified on the CLI
# one of: "flatten" (default) or "per-pr"
land: flatten
```

Precedence for defaults:

- CLI flag > repo YAML > home YAML > built-in defaults
- Built-in defaults: `base = origin/oai-main`, `prefix = "${USER}-spr/"`, `land = flatten`

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

Key options:

- `--from <REF>`: commit range upper bound when parsing tags (default `HEAD`) (untested)
- `--no-pr`: only (re)create branches; skip PR creation/updates (untested)
- Extent (optional subcommand):
  - `pr --n <N>`: limit to first N PRs from the bottom
  - `commits --n <N>`: limit to first N commits (untested)

Behavior:

- Parses `pr:<tag>` markers from `merge-base(base, from)..from`
- Creates/updates per-PR branches and GitHub PRs
- Updates PR bodies with a visualized stack block and correct `baseRefName`

### spr restack

Restack the local stack by rebasing commits after the bottom N PR groups onto the latest base.

Options:

- `--after <N|bottom|top|last>`: 'drop' the first N PR groups; rebase the remaining commits onto `--base`
  - `0` or `bottom`: restack all groups (moves everything after merge-base)
  - `top` or `last`: skip all PRs
 - `--safe`: create a local backup branch at current `HEAD` before rebasing

Behavior:

- Computes PR groups from `merge-base(base, HEAD)..HEAD` using `pr:<tag>` markers (oldest → newest)
- For `--after 0`: upstream is `merge-base(base, HEAD)`
- For `--after N>0`: upstream is the parent of the first commit of group N+1
- Runs: `git rebase --onto <base> <upstream> <current-branch>`
 - With `--safe`, a backup branch named like `backup/restack/<current-branch>-<short-sha>` is created first

### spr list pr

Lists PRs in the current stack (bottom → top) for the configured prefix.

### spr list commit (alias: c)

Lists commits in the current stack (bottom → top), grouped by local PR. Each group header shows the local PR number and branch (and remote PR number when available). Within each group, each line shows the bottom-up commit index (1-based) and the short SHA.

### spr move

Reorder local PR groups by moving one or a range to come after a target PR.

- `spr move A --after C`: move PR at position A to come after PR C (C ∈ [0..N])
- `spr move A..B --after C`: move PRs A..B to come after PR C (requires A < B and C ∉ [A..B]; C ∈ [0..N])
  - `--after bottom` is the same as `--after 0`
  - `--after top` is the same as `--after N`
- `--safe`: create a local backup branch at current `HEAD` before rewriting

Prints an explicit plan, e.g.: `2..3→4: [1,2,3,4,5,6] → [1,4,2,3,5,6]`.

### spr land

Land PRs using either flatten or per-pr strategy.

Shared options (global):

- `--until <N>`: land first N PRs bottom-up (0 means all)

Mode selection:

- If `spr land <mode>` is specified, that mode is used
- If no `<mode>` is provided, the mode is read from config `land` (default `flatten`)

#### Mode: flatten

- For PRs 1..=N (or all when N==0):
  - Sets each PR’s `baseRefName` to the actual base
  - Squash-merges each PR
  - Does not close other PRs explicitly (they’ll be closed by GitHub on merge)

#### Mode: per-pr

- Validates that each PR in 1..=N has exactly one unique commit over its parent (abort if not)
- Use in conjuction with `spr prep`
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

### spr cleanup (alias: clean)

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

### spr fix-stack

Fix PR stack connectivity to match the local commit stack.

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
```
