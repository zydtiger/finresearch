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
