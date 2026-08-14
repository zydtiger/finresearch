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

Append SEC filing metadata and reported XBRL facts using the requester identity
required by SEC fair-access policy:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data ingest-sec-submissions aapl-2026-08-11 320193 \
  --user-agent "Researcher Name researcher@example.com"

uv run finresearch \
  --workspace /path/to/research-artifacts \
  data ingest-sec-companyfacts aapl-2026-08-11 320193 \
  --user-agent "Researcher Name researcher@example.com"
```

Derive deterministic normalized artifacts (instrument master and unadjusted
daily price bars) from one raw yfinance snapshot, carrying the raw artifact id
as lineage:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data normalize-daily-prices aapl-2026-08-11 AAPL [--raw-artifact-id ID]
```

Parse one raw SEC companyfacts snapshot into structured fundamental facts:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data normalize-fundamental-facts aapl-2026-08-11 320193 [--raw-artifact-id ID]
```

Deep-validate declared snapshots against their registered contracts, or
inspect one artifact's file, schema, provenance, and preview:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data validate aapl-2026-08-11 [ARTIFACT_ID]

uv run finresearch \
  --workspace /path/to/research-artifacts \
  data inspect aapl-2026-08-11 ARTIFACT_ID --limit 5
```

## Development

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
uv run pytest
```
