# finresearch

`finresearch` is a self-contained Typer CLI for deterministic,
programmatic investment-research workflows. It is intended to own reusable
data contracts, transformations, financial models, validation, and HTML report
generation while keeping research judgment and workflow guidance outside the
codebase.

The CLI does not discover or configure an artifact root. Every command that
uses research state receives the workspace explicitly with `--workspace`.

## Setup

```bash
uv sync --group dev
```

## Usage

The initial `root` command validates and prints the explicit workspace path:

```bash
uv run finresearch --workspace /path/to/research-artifacts root
```

Run the package directly with the same interface:

```bash
uv run python -m finresearch --workspace /path/to/research-artifacts root
```

## Development

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
uv run pytest
```
