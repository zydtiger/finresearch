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

New cases use manifest v2. Existing v1 cases remain readable; migrate one
explicitly without rewriting its artifacts when v2 lineage is needed:

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  case migrate aapl-2026-08-11
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

Derive deterministic provider-scoped instrument-master observations,
unadjusted daily-price bars, and separate corporate actions from one raw
yfinance snapshot. V2 manifests record ordered input artifact lineage, input
file hashes, and producer metadata:

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

Import an explicit local CSV or Parquet projection without guessing its schema,
provider, or retrieval time. The command preserves the source bytes below the
case raw directory and writes a deterministic canonical normalized Parquet:

```bash
uv run finresearch --workspace /path/to/research-artifacts \
  data import-csv aapl-2026-08-11 ./prices.csv \
  --schema daily-prices.v2 --provider manual --retrieved-at 2026-08-11T04:00:00Z

uv run finresearch --workspace /path/to/research-artifacts \
  data import-parquet aapl-2026-08-11 ./facts.parquet \
  --schema fundamental-facts.v2 --provider manual --retrieved-at 2026-08-11T04:00:00Z
```

Build an explicit as-of current-state master only when the provider-scoped v2
observations are already present:

```bash
uv run finresearch --workspace /path/to/research-artifacts \
  data reconcile-instrument-master aapl-2026-08-11 --as-of 2026-08-11
```

Run an auditable case-backed DCF or declared comparable-company analysis:

```bash
uv run finresearch --workspace /path/to/research-artifacts \
  model dcf aapl-2026-08-11 --input analysis/dcf-inputs.toml --scenario all

uv run finresearch --workspace /path/to/research-artifacts \
  model comps aapl-2026-08-11 --input ARTIFACT_ID --as-of 2026-08-11 \
  --metrics ev_revenue,ev_ebitda
```

See [the modeling contract](docs/modeling.md) for strict source-provenance,
input, and projection-gate requirements.

Validate and summarize the human-edited research registers (evidence,
assumptions, scenarios, catalysts, open questions):

```bash
uv run finresearch \
  --workspace /path/to/research-artifacts \
  data registers status aapl-2026-08-11
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
