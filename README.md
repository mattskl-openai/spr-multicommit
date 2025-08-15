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

- `--verbose` (global): enable verbose logging of underlying git/gh commands
- Without `--verbose`, logs are compact and without timestamps; long GitHub operations show progress messages

Commands
--------

### spr update

Build/refresh the stack from commit markers, or restack existing branches.

Key options:

- `--base, -b <BRANCH>`: root base branch (default from config)
- `--prefix <PREFIX>`: per-PR branch prefix (default from config, normalized to a single trailing `/`)
- `--dry-run`: print state-changing commands instead of executing
- Extent (optional subcommand):
  - `pr --n <N>`: limit to first N PRs from the bottom
  - `commits --n <N>`: limit to first N commits (untested)

Behavior:

- Parses `pr:<tag>` markers from `merge-base(base, from)..from`
- Creates/updates per-PR branches and GitHub PRs
- Updates PR bodies with a visualized stack block and correct `baseRefName`

### spr list pr

Lists PRs in the current stack (bottom → top) for the configured prefix.

- `--base, -b <BRANCH>`
- `--prefix <PREFIX>`

### spr land

Land PRs using either flatten or per-pr strategy.

Shared options:

- `--base, -b <BRANCH>`
- `--prefix <PREFIX>`
- `--dry-run`
- `--until <N>`: target range
  - land first N PRs bottom-up (0 means all)

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

- `--base, -b <BRANCH>`
- `--prefix <PREFIX>`
- `--until <N>`: prep first N PRs (0 for all)
- `--exact <I>`: prep exactly the I-th PR from the bottom (1-based)
- `--dry-run`

Behavior:

- Rewrites local history to ensure selected PRs become single-commit groups
- Pushes branches (respects `--dry-run`)
- Adds a warning to the next PR not included in the push

### spr fix-stack

Fix PR stack connectivity to match the local commit stack.

- `--base, -b <BRANCH>`
- `--prefix <PREFIX>`
- `--dry-run`

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

```bash
# Build/refresh using defaults from config
spr update

# Prep the first 3 PRs from the bottom
spr prep --until 3

# Land top PR only using config default mode (flatten by default)
spr land --until 1

# Explicitly land first 2 PRs via flatten
spr land flatten --until 2

# Explicitly land first 2 PRs via per-pr
spr land per-pr --until 2
```
