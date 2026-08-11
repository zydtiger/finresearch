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

Initialize a case with a stable ID:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  case init aapl-2026-08-11 \
  --title "Apple valuation update"
```

Inspect or validate it:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  case status aapl-2026-08-11

uv run finresearch \
  --workspace /path/to/research-artifacts \
  case validate aapl-2026-08-11
```

The case directory and `manifest.toml` contract are documented in
[`docs/case-contract.md`](docs/case-contract.md). The same CLI is available
through `uv run python -m finresearch`.

Append an immutable raw daily-price snapshot from yfinance:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data ingest-yfinance-prices aapl-2026-08-11 AAPL \
  --start 2025-01-01 \
  --end 2026-01-01
```

Raw provider snapshots use versioned Polars schemas and Zstandard-compressed
Parquet. See [`docs/data-ingestion.md`](docs/data-ingestion.md) for the raw-data
contract and provider boundaries.

## Development

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
uv run pytest
```
