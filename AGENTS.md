# Repository Guidelines

## Purpose and ownership

- This repository owns the `finresearch` Python package and CLI for
  deterministic investment-research data, modeling, validation, and reporting
  workflows.
- Keep research artifacts outside this repository. Commands that read or write
  research state must require an explicit `--workspace PATH` argument.
- Build the command surface with Typer and keep reusable behavior outside CLI
  callback functions.
- Do not add workspace discovery, user configuration, or implicit home/current
  directory fallbacks without explicit approval.

## Project layout

- `src/finresearch/`: reusable package code and CLI entry point.
- `tests/`: automated tests matching package behavior.
- `docs/case-contract.md`: normative versioned case and artifact contract;
  read it before changing manifest fields, paths, or case command semantics.
- `docs/data-ingestion.md`: raw snapshot semantics, dataset contracts, and
  provider ownership boundaries.
- `README.md`: user-facing setup and command usage.
- `pyproject.toml` and `uv.lock`: Python metadata and resolved dependencies;
  update them together through `uv`.

## Setup and validation

Run setup with:

```bash
uv sync --group dev
```

Install the hook runner once per machine, then install the repository's
configured `pre-commit` and `pre-push` hooks:

```bash
uv tool install prek
prek install
```

`.pre-commit-config.yaml` is the authoritative definition of mechanically
checkable validation and its scope. Run both configured stages outside Git
with:

```bash
prek run --all-files --stage pre-commit
prek run --all-files --stage pre-push
```

Run targeted tests during development with
`uv run pytest <path-or-node-id>`.

## Git policy

- Use `main` as the base branch.
- Use `prefix: concise imperative summary` with no trailing period. Allowed
  prefixes are: `feat` for functionality, `fix` for correctness, `docs` for
  documentation, `refactor` for behavior-preserving restructuring, `test` for
  tests, `build` for dependencies or packaging, `ci` for automation, `chore`
  for maintenance, and `revert` for reversals. Scopes are allowed when useful.
- Read-only work may use any safe checkout. Trivial isolated edits may be made
  on `main`, but commit only after explicit approval. Substantial, issue-bound,
  reviewable, or concurrent work must use a sibling worktree, a
  `<prefix>/<lowercase-task>` branch, and a pull request targeting `main`.
- Treat branching, committing, publishing, merging, cleanup, tagging, and
  releasing as separate approval gates. Preserve unrelated user changes and
  stage only intended files.
- Do not create a remote or publish without explicit approval. If publication
  is approved without a specified target, use a private Gitea repository;
  publish to public GitHub only when explicitly requested.

## Versioning and releases

- This repository ships a versioned CLI, versioned on-disk case and artifact
  contracts, deterministic report formats, and the repository-root
  `finresearch-skill`. Use Semantic Versioning and annotated `vX.Y.Z` tags.
- The public compatibility surfaces are CLI commands, options, exit behavior,
  stable printed identifiers, manifest and dataset contracts, model and report
  schemas, and the root skill's declared usage contract. Internal Python
  modules are not a supported public API beyond `finresearch.__version__`.
- Before `1.0.0`, use a minor bump for a breaking change to a public surface and
  a patch bump for compatible fixes or additions. Treat `1.0.0` as a separate
  explicit stability commitment.
- From `1.0.0` onward, use a major bump for breaking public changes, a minor
  bump for backward-compatible functionality, and a patch bump for compatible
  fixes. Prefer deprecating and introducing a replacement for at least one
  minor release before removal; break directly only when the old surface is
  unsafe, semantically wrong, or cannot be migrated in place.
- Adding a new schema version is compatible while older inputs remain readable;
  removing support for an existing schema version is breaking.
- `pyproject.toml` is the single package-version source. Refresh `uv.lock` when
  it changes; `finresearch.__version__` must read the installed distribution
  metadata rather than duplicate the version.
- Keep a version bump separate from the changes it releases, using a focused
  `build: bump version to X.Y.Z` commit. Group GitHub Release notes under Added,
  Changed, Fixed, Deprecated, and Breaking as applicable, and name every
  compatibility or migration requirement.
- Publish only an immutable tag and GitHub Release in this public repository.
  Do not publish to PyPI or another package index without an explicit policy
  change. Keep version history in GitHub Release notes rather than a parallel
  changelog.
- Before creating the first release tag, protect `refs/tags/v*` with an active
  GitHub tag ruleset that blocks deletion, updates, and non-fast-forward changes
  without a routine bypass. Treat every published release tag as immutable.
- Prepare a release from a clean `main` synchronized with `origin/main`. Require
  successful CI, `uv build --no-sources`, compatible wheel metadata, and an
  isolated wheel installation before presenting the exact version, target
  commit, and release notes for approval.
- Tagging and publishing the GitHub Release form one release action requiring
  explicit approval immediately before the annotated tag is created and
  pushed. Approval for editing, committing, or pushing ordinary commits is not
  release approval.
