---
name: finresearch-skill
description: Use the finresearch CLI to build, validate, model, audit, and report deterministic investment-research cases. Invoke for case-backed data ingestion, DCF, comparable-company analysis, or point-in-time reporting; do not use for generic market commentary or spreadsheet modeling.
---

# Use finresearch

Use the installed `finresearch` executable. Run `finresearch --help` and the
relevant subcommand `--help` before relying on remembered syntax. The required
global `--workspace` option precedes the command, including for group help:

```text
finresearch --workspace WORKSPACE data --help
```

Do not assume a source checkout or substitute `uv run finresearch` unless the
user is explicitly working inside the finresearch repository.

## Establish explicit scope

Require the caller to identify an existing workspace directory. Never discover
or infer it from the current directory, home directory, environment, or a
previous case. Keep research workspaces outside the finresearch source tree.
The CLI rejects a missing workspace path, so create a newly named workspace
explicitly at the caller's request before running.

Use the caller's stable case ID exactly. Case IDs are 1-64 lowercase ASCII
letters, digits, or hyphens and start and end with a letter or digit.
Initialize a new case with `case init`; do not reuse an existing ID or
silently normalize it. New cases use manifest v2. Run `case migrate` only as
an explicit v1-to-v2 migration when a v2-only model, audit, or report
workflow requires it.

Treat `manifest.toml` and files registered below `data/raw`, `data/normalized`,
`data/derived`, and `reports` as CLI-owned immutable state. Do not edit, rename,
replace, or delete them manually. Human or agent inputs belong in the documented
register CSVs and case-relative `analysis/` files; validate them through the CLI
before modeling.

## Build the evidence base

Choose only inputs and point-in-time dates supplied or supported by the research
task. Do not invent providers, schemas, retrieval timestamps, identifiers,
financial values, peers, evidence, assumptions, or source IDs.

- Use `data ingest-yfinance-prices` for an immutable unadjusted daily-price
  snapshot. Its `--start` date is inclusive and `--end` is exclusive.
- SEC ingestion has no default identity. Every `ingest-sec-submissions` and
  `ingest-sec-companyfacts` call requires a truthful caller-supplied
  `--user-agent "NAME EMAIL"`. Never fabricate a name or contact email; ask for
  it when absent.
- Use `data import-csv` or `data import-parquet` only with an explicit named
  schema, provider, and RFC 3339 UTC `--retrieved-at`. Imports preserve the
  original bytes and publish deterministic canonical Parquet. Do not coerce a
  near-matching file or guess its projection.
- Use `data normalize-daily-prices` and
  `data normalize-fundamental-facts` only after the corresponding raw snapshot
  exists. When several eligible raw artifacts exist, select a specific
  `--raw-artifact-id` from CLI output or inspection; never guess one.
- Use `data reconcile-instrument-master --as-of YYYY-MM-DD` only for explicit
  current-state reconciliation from v2 observations.

Capture every returned artifact ID, path, row count, and checksum. Use
`data inspect CASE_ID ARTIFACT_ID` for schema, provenance, date ranges, nulls,
and a bounded preview. Use `data validate CASE_ID` for deep validation of all
declared artifacts; selecting one artifact narrows that check.

Registers are strict human-auditable CSVs below `registers/`: `evidence.csv`,
`assumptions.csv`, `scenarios.csv`, `catalysts.csv`, and
`open_questions.csv`. Missing registers are allowed. Validate every present
register with `data registers status CASE_ID`. Keep reported facts, estimates,
external evidence, and analyst assumptions explicitly classified, dated, and
linked; do not manufacture rows merely to satisfy a model. Each file has one
exact ordered header:

```text
evidence.csv       id,claim,source_type,source_ref,observed_at,notes
assumptions.csv    id,parameter,value,unit,rationale,source_evidence,updated_at
scenarios.csv      scenario,parameter,value,unit,rationale
catalysts.csv      id,event,expected_date,impact,notes
open_questions.csv id,question,context,importance,status,answered_at
```

Enums and reference rules: `source_type` is `filing`/`estimate`/`external`/
`assumption`; `assumptions.source_evidence` must reference an existing
evidence id; `scenarios.scenario` is `bear`/`base`/`bull` and every parameter
must define all three with pairwise-distinct values; `impact` is
`positive`/`negative`/`neutral`; `importance` is `high`/`medium`/`low`;
`status` is `open`/`answered`.

Registers freeze at the first model run in a case: model runs and reports pin
the byte hashes of `evidence.csv` and `assumptions.csv`, so any later edit to
those two files — including appending a row — invalidates every prior run's
identity and fails `case audit --verify-hashes`. Rerun freely over unchanged
registers; changed evidence or assumptions open a new dated case in the same
workspace, carrying the prior register content forward as explicitly dated
rows.

## Run auditable models

DCF economic inputs belong in the case-relative strict TOML file, normally
`analysis/dcf-inputs.toml`; they are not CLI flags. Require an explicit model
`as_of`, currency and value/share units, discount convention, terminal method,
WACC components, capitalization, and complete bear/base/bull scenarios. Every
economic leaf must contain `value`, controlled `unit`, and a `source_id` that
resolves to valid `evidence.csv` or `assumptions.csv` content dated no later
than the model cutoff. A minimal complete input (scenario blocks repeat for
bear, base, and bull):

```toml
version = 1
as_of = "2026-06-30"
currency = "USD"
value_unit = "USDm"
share_unit = "shares_m"
discount_convention = "year_end"   # or mid_year
terminal_method = "gordon_growth"  # or exit_multiple
projection_needs = []
[wacc]
cost_equity = { value = 0.10, unit = "ratio", source_id = "a1" }
cost_debt = { value = 0.05, unit = "ratio", source_id = "a2" }
tax_rate = { value = 0.25, unit = "ratio", source_id = "a3" }
debt_weight = { value = 0.20, unit = "ratio", source_id = "a4" }
[capitalization]
market_cap = { value = 1000, unit = "USDm", source_id = "e1" }
debt = { value = 200, unit = "USDm", source_id = "e2" }
cash = { value = 50, unit = "USDm", source_id = "e3" }
diluted_shares = { value = 100, unit = "shares_m", source_id = "e4" }
[scenario.base]
forecast = [{ period_end = "2027-12-31", free_cash_flow = { value = 80, unit = "USDm", source_id = "a5" } }]
terminal = { terminal_growth = { value = 0.02, unit = "ratio", source_id = "a6" } }
```

Units are controlled: amounts use `CURRENCY`/`CURRENCYk`/`CURRENCYm`/
`CURRENCYb`, shares use `shares`/`shares_k`/`shares_m`/`shares_b`, plus
`ratio`, `multiple`, and `CURRENCY/share`; output per-share values normalize
to `CURRENCY/share`. An `exit_multiple` terminal requires separately sourced
`terminal_metric` and `exit_multiple` leaves — never the final FCF.
`projection_needs` is a gate recording disclosed structural needs, not a
three-statement model; its values are `working-capital`, `capex-depreciation`,
`tax`, `cash-debt-interest`, `dilution`, `liquidity-covenant`, and
`balance-sheet-reconciliation`.

Run:

```text
finresearch --workspace WORKSPACE model projection-assess CASE_ID \
  --input analysis/dcf-inputs.toml
finresearch --workspace WORKSPACE model dcf CASE_ID \
  --input analysis/dcf-inputs.toml --scenario all
```

Add `--sensitivity 'WACCS;GROWTHS'` only when the task provides the grid. Do not
infer sensitivity values. Preserve the returned `run_id` and every typed model
artifact ID.

Comparable-company analysis requires a validated
`model.comps-observations.v1` artifact plus an explicit cutoff, requested
metrics, and optional declared target. finresearch does not discover peers,
invert FX, convert fiscal periods, or default missing net debt. Do not proceed
until the peer observations and requested multiples are explicitly supported.

Observation files use the exact ordered header `company_id,company_name,role,
metric,period_basis,period_end,knowledge_date,as_of,value,unit,currency,
source_id`, and every row's `source_id` must already resolve to an
`evidence.csv` or `assumptions.csv` row before `model comps` runs — write that
register first. All selected observations must come from one common snapshot
no later than the cutoff; the run selects the latest `knowledge_date` per
company/metric and treats tied latest observations as conflicts; companies
sharing one requested multiple must share the same basis and period end; EV
multiples require both `market_cap` and `net_debt`; every requested metric
needs at least one valid peer, and missing, zero, or negative denominators
become explicit exclusions.

```text
finresearch --workspace WORKSPACE model comps CASE_ID --input ARTIFACT_ID \
  --as-of YYYY-MM-DD --metrics ev_revenue,ev_ebitda --target COMPANY_ID
```

## Report and close the audit loop

Render a report only from a complete registered DCF or comps `run_id`:

```text
finresearch --workspace WORKSPACE report markdown CASE_ID --model-run-id RUN_ID
finresearch --workspace WORKSPACE report html CASE_ID --model-run-id RUN_ID
```

Reports are immutable and idempotent. Do not patch their generated Markdown or
HTML. Fix the registered inputs or model run and render again under the CLI's
deterministic identity rules.

Distinguish the validation gates:

- `case validate` checks the manifest, required directories, and declared-file
  presence.
- `data validate` checks registered bytes, lineage, row counts, and dataset
  contracts.
- `case audit` adds point-in-time, stale-price, model/report reconstruction, and
  provenance gates. Use `--verify-hashes` for the final full-byte audit.

Before delivery, run a cutoff-appropriate audit with an explicit acceptable
price age:

```text
finresearch --workspace WORKSPACE case audit CASE_ID --as-of YYYY-MM-DD \
  --max-price-age-days N --verify-hashes
```

Stop on any nonzero exit. Do not bypass validation or repair registered state by
hand. Report the workspace, case ID, cutoff, important input artifact IDs,
model run ID, generated report artifact and path, and final audit result.
`--max-price-age-days` is required even when a case declares no price
artifacts; the stale-price gate is vacuous then.
