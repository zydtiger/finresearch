# Data Ingestion Contract v1

## Scope

The first ingestion contract stores immutable, provider-faithful snapshots as
typed Parquet. It separates retrieval from later normalization: raw storage may
reshape a provider response into rows and columns, but it does not adjust
prices, convert currencies, reconcile identifiers, or calculate research
metrics.

The shared contract registry lives in `src/finresearch/data_contracts.py`.
Ingestion, validation, and future inspection commands must import the same
`DatasetContract` instead of declaring parallel schemas.

## Raw snapshot rules

- Each network retrieval creates a new Parquet file and a new manifest artifact.
- Existing raw snapshots are never overwritten by normal ingestion commands.
- Parquet uses Zstandard compression and includes statistics.
- Every snapshot records provider-specific request identity and parameters,
  retrieval time, and an independent dataset schema version. For example,
  yfinance records symbol/period/interval while SEC records CIK and endpoint.
- In v1 manifests, the manifest records artifact ID, kind, schema version,
  relative path, provider (`source`), retrieval time, row count, and SHA-256
  checksum. New v2 cases instead record deterministic producer metadata and an
  empty ordered parent list for raw snapshots; v2 has no legacy `source` field.
- A synchronous failed Parquet write or manifest update removes only output
  created by that invocation. A rerun may atomically recover matching,
  undeclared local-import outputs after verifying their expected bytes; a
  mismatched or partially declared output is an integrity failure and is never
  overwritten or removed.
- Per-case file and process locks serialize snapshot publication with manifest
  updates; the persistent `.finresearch.lock` file is internal workspace state.
- Provider adapters may use pandas at their external boundary; internal
  contracts and storage use Polars.

Raw yfinance prices are stored below:

```text
data/raw/yfinance/daily-prices/<symbol-key>/<retrieved-at>.parquet
```

The retrieval timestamp makes the path append-only. Provider symbols that are
not portable path components receive a readable slug plus a short hash.

SEC snapshots are stored below:

```text
data/raw/sec/submissions/<10-digit-cik>/<retrieved-at>.parquet
data/raw/sec/companyfacts/<10-digit-cik>/<retrieved-at>.parquet
```

## Raw yfinance daily-prices v1

Contract identifier: `raw.yfinance.daily-prices.v1`

| Field | Polars dtype | Meaning |
| --- | --- | --- |
| `schema_version` | `UInt16` | Independent dataset contract version |
| `provider` | `String` | Always `yfinance` |
| `provider_symbol` | `String` | Symbol sent to yfinance |
| `currency` | `String` | Trading currency reported by yfinance, e.g. `USD`, `HKD` |
| `retrieved_at` | `Datetime(us, UTC)` | Snapshot retrieval time |
| `requested_start` | `Date` | Inclusive request start |
| `requested_end` | `Date` | Exclusive request end |
| `interval` | `String` | Always `1d` in v1 |
| `provider_timezone` | `String` | Timezone attached by yfinance |
| `session_date` | `Date` | Provider-local trading date |
| `timestamp` | `Datetime(us, UTC)` | Provider timestamp converted to UTC |
| `open`, `high`, `low`, `close` | `Float64` | Unadjusted provider OHLC values |
| `adj_close` | `Float64` | Provider adjusted close when available |
| `volume` | `Int64` | Provider volume when available |
| `dividends` | `Float64` | Provider cash distribution value |
| `stock_splits` | `Float64` | Provider split ratio event value |
| `capital_gains` | `Float64` | Provider capital-gain event when available |

The metadata, currency, and timestamp fields are non-null. Numeric provider
values remain nullable so raw storage can preserve an incomplete response for
later quality assessment. `(provider_symbol, interval, timestamp)` is unique
within one snapshot.

The adapter reads the trading currency from yfinance `fast_info` at retrieval
time and stores it in the snapshot, so currency is a point-in-time provider
observation rather than a later inference. A symbol whose currency cannot be
reported fails ingestion instead of storing an unknown value.

## Command

```text
finresearch --workspace PATH data ingest-yfinance-prices \
  CASE_ID SYMBOL --start YYYY-MM-DD --end YYYY-MM-DD
```

The command requests unadjusted daily OHLC with actions enabled. The end date is
exclusive, matching yfinance semantics.

## Raw SEC submissions v1

Contract identifier: `raw.sec.submissions.v1`

The contract flattens the parallel arrays under `filings.recent` into one row
per accession number. Each row preserves CIK, entity metadata, tickers,
exchanges, accession number, filing and report dates, provider acceptance-time
text, Act and form, file and film numbers, items, filing size, XBRL flags, and
primary-document metadata. `(cik, accession_number)` is unique within a
snapshot.

The v1 command intentionally does not follow the supplemental historical files
listed under `filings.files`. A later explicit history command can add that
behavior with bounded requests and its own tests; ordinary ingestion must not
silently fan out across the full EDGAR archive.

## Raw SEC companyfacts v1

Contract identifier: `raw.sec.companyfacts.v1`

The contract flattens `facts.<taxonomy>.<concept>.units.<unit>` into one row per
reported observation. It preserves taxonomy, concept, label, description,
unit, start and end dates, accession number, fiscal year and period, form,
filed date, frame, and retrieval provenance.

Reported values remain lossless JSON-scalar text with an explicit
`value_type`. Raw ingestion therefore does not force large integers, monetary
values, shares, ratios, booleans, and textual DEI facts through one Float64
column. A normalized fundamentals contract will parse values according to unit
and concept semantics.

SEC can publish duplicate-looking observations. Raw companyfacts ingestion
preserves them; filing-aware deduplication belongs in normalization.

## SEC commands and fair access

```text
finresearch --workspace PATH data ingest-sec-submissions \
  CASE_ID CIK --user-agent "NAME EMAIL"

finresearch --workspace PATH data ingest-sec-companyfacts \
  CASE_ID CIK --user-agent "NAME EMAIL"
```

CIKs may be supplied with or without leading zeros and are stored in the SEC
API's ten-digit form. `--user-agent` is required on every SEC command and must
contain a contact email. The adapter sends the declared identity, requests
JSON with compression, uses a finite timeout, rejects redirects, and surfaces
HTTP or schema failures without writing a partial artifact. A host-shared file
lock spaces request starts by at least 110 milliseconds across provider
instances and CLI processes.

## Deep validation and inspection

`case validate` only checks that the manifest is well-formed and that declared
files exist. `data validate` performs common integrity checks for every
declared artifact, then re-reads Parquet artifacts for dataset validation. It
verifies, per artifact as applicable:

- the manifest SHA-256 matches the file bytes;
- for v2 artifacts, every declared input-file hash resolves to existing bytes
  with the recorded SHA-256;
- the manifest row count matches the Parquet height;
- the artifact `kind` and `schema_version` resolve to a registered
  `DatasetContract` in `data_contracts.py`;
- the Parquet schema, required non-null fields, and unique key satisfy that
  contract;
- the manifest `retrieved_at` matches the snapshot's own `retrieved_at`
  provenance column; and
- for a v2 Parquet contract with `source_artifact_id`, every non-null observed
  source is in the artifact's authoritative `input_artifact_ids`; tables may
  carry multiple valid source ids when they reconcile multiple inputs.

```text
finresearch --workspace PATH data validate CASE_ID [ARTIFACT_ID]
finresearch --workspace PATH data inspect CASE_ID ARTIFACT_ID [--limit N]
```

Without `ARTIFACT_ID`, `data validate` checks every declared artifact,
including non-Parquet outputs such as reports. Exact selection of a non-Parquet
artifact performs the same integrity checks. `data inspect` remains limited to
Parquet artifacts and reports a clear unsupported-type error otherwise; for a
Parquet artifact it runs the same deep validation first, then reports the
contract identifier, relative path, size, SHA-256,
row count, columns and dtypes, date and timestamp ranges, per-column null
counts, unique-key violations, constant provenance fields (`provider`,
`provider_symbol`, `cik`, and `source_url` when the snapshot declares them), and a
deterministic JSON-record preview of the first `N` rows (default 5, maximum
100). Invalid or unknown-contract artifacts are not inspected.

Both commands share the `DatasetContract` registry so schema, validation, and
inspection cannot drift; adding a new raw or normalized table means registering
one contract.

## Normalized daily prices v1 (legacy compatibility)

The historical v1 normalization was a deterministic, offline transformation
from one immutable raw yfinance snapshot. Existing v1 artifacts remain
readable; the current v2 provider output is specified below. Neither form
rewrites raw files or fetches from the network.

```text
finresearch --workspace PATH data normalize-daily-prices \
  CASE_ID SYMBOL [--raw-artifact-id ARTIFACT_ID]
```

The v1 form wrote two registered artifacts below `data/normalized/`. In a v2
manifest, each records the raw artifact in `input_artifact_ids` plus its
case-relative input-file hash, so every normalized row can be traced to its
exact input bytes. A v1 manifest retains its legacy `source` declaration. When
a symbol has several raw snapshots, `--raw-artifact-id` selects one; otherwise
normalization fails with the candidate ids.

### normalized.instrument-master.v1

One row per raw snapshot with the stable `instrument_id` (the portable symbol
key), observed `provider_symbol`, trading `currency` captured at retrieval,
provider timezone, first and last session dates, observation count, and
lineage. The unique key is `(instrument_id, source_artifact_id)`: a
current-state master that reconciles snapshots is a later workflow. Venue and
asset class are not derivable from yfinance snapshots and are deliberately
absent from v1.

### normalized.daily-prices.v1

One row per session date with UTC timestamps, session dates, unadjusted OHLCV,
explicit `price_basis = "unadjusted"`, trading currency, provider timezone,
lineage, and `normalized_at`. The unique key is `(instrument_id, session_date)`.
Rules:

- the raw snapshot must have interval `1d` and no duplicate session dates;
- OHLC and volume must be present; a raw bar with missing prices fails
  normalization with an actionable error instead of silently dropping it;
- absent dividends and splits fill to an explicit `0.0`;
- `adj_close` stays in raw; adjusted-price output is a future contract with its
  own `price_basis`, keeping any single table single-basis.

Normalization derives its transform timestamp from explicit `normalized_at`
input or the raw artifact's immutable `retrieved_at` metadata; for a valid
legacy v1 declaration that lacks manifest `retrieved_at`, it uses the single
contract-validated `retrieved_at` value in the raw Parquet snapshot. It never
reads the wall clock. The resulting identity, Parquet bytes, and manifest
declaration are therefore stable for the same source artifact, contract, and
producer version. Re-running that exact transformation returns the existing
pair without adding artifacts. The instrument master registers before the
price bars; if publication is interrupted between them, a rerun reuses the
master receipt and completes the pair. Existing v1 cases remain readable with
their original artifact files.

Both legacy normalized tables share the same `DatasetContract` registry, so
`data validate` and `data inspect` cover them without extra code.

## Canonical contracts and explicit local import

Current provider normalization writes immutable `normalized.instrument-master.v2`
and `normalized.daily-prices.v2` artifacts, plus
`normalized.corporate-actions.v1`. The master uses a provider-scoped stable
`instrument_id`; yfinance-derived fields that the raw snapshot cannot prove
remain NULL and `asset_class = "unknown"`. Price bars remain a single
`unadjusted` basis and actions are separate rows (`dividend`, `capital-gain`,
or `split`) with either a positive cash amount or a positive split ratio.

`normalized.fundamental-facts.v2` adds deterministic `fact_id`, nullable
entity/instrument mappings, metric/category fields, controlled unit and
currency values, accession and knowledge provenance, and an optional
`available_at`. SEC normalization records a safe hard-coded canonical mapping
only for known concepts; it does not invent an instrument mapping or filing
acceptance time. Exact duplicate facts collapse, while restatements remain
distinct because their fact-defining provenance differs.

The registry also defines strict canonical `normalized.estimates.v1` (explicit
estimate-as-of, availability, and retrieval timestamps; entity/instrument are
part of its unique identity),
`normalized.fx-rates.v1`, and the action contract above. Controlled values are:
currencies `AUD,CAD,CHF,CNY,EUR,GBP,HKD,JPY,KRW,SGD,USD`; price bases
`unadjusted,split-adjusted,total-return`; asset classes
`commodity,crypto,derivative,equity,etf,fixed-income,fund,fx,index,other,unknown`;
and fact categories `balance-sheet,cash-flow,entity,income-statement,other,share-data`.
Canonical publication applies each contract's declared deterministic sort key;
contracts enforce exact schemas,
nullability, unique keys, UTC timestamps, and semantic invariants such as
OHLC ordering, positive FX rates, valid action amounts, and PIT timestamp order.

```text
finresearch --workspace PATH data import-csv CASE_ID FILE \
  --schema instrument-master.v2|daily-prices.v2|fundamental-facts.v2|estimates.v1|corporate-actions.v1|fx-rates.v1|model.comps-observations.v1 \
  --provider PROVIDER --retrieved-at RFC3339_UTC

finresearch --workspace PATH data import-parquet CASE_ID FILE \
  --schema NAME --provider PROVIDER --retrieved-at RFC3339_UTC
```

CSV imports require UTF-8, the exact ordered header, exact `YYYY-MM-DD` dates,
`Z`-terminated RFC3339 UTC timestamps, decimal-dot numeric values, and empty
strings only for nullable fields. No NA aliases, missing/extra columns, best
effort casts, mtime, or local-time defaults are accepted. Input Parquet must
match its named projection exactly. The original byte stream is stored as a
non-Parquet raw source so validation can verify its own SHA without treating a
user projection as a dataset contract; absolute input paths are never put in
the manifest. Canonical parsing consumes that already-read byte stream, so a
source-path change during import cannot create mismatched raw and normalized
lineage.

The raw source and canonical output have identities from source bytes, schema,
provider, retrieval time, and the explicit import producer name and version.
An identical rerun reuses both; changing that producer version or other
identity metadata appends a separate pair. Import validation occurs before
publication, and a publication error removes any new raw/canonical files and
leaves no manifest declaration.

```text
finresearch --workspace PATH data reconcile-instrument-master CASE_ID \
  --as-of YYYY-MM-DD [--source-artifact-id ID ...]
```

Reconciliation writes `derived.instrument-master-current.v1` from v2 master
observations valid at the cutoff. It selects the latest `observed_at` without
guessing identifiers; equal-time conflicting non-null values fail explicitly.
Its ordered lineage includes the observed master artifacts and their raw source
artifacts in canonical artifact-id order; repeated source filters are treated
as one sorted set. `data validate` also compares every non-null price/action
currency by provider and instrument against a single
matching v2 master currency when one is unambiguous.

## Normalized fundamental-facts v1 (legacy compatibility)

```text
finresearch --workspace PATH data normalize-fundamental-facts \
  CASE_ID CIK [--raw-artifact-id ARTIFACT_ID]
```

The historical v1 form parses one immutable raw SEC companyfacts snapshot into
a long-form table with one row per reported observation. Existing artifacts
remain valid under this immutable contract; current normalization writes the v2
form documented above. Like the other normalized tables it is a deterministic,
offline transformation; v2 manifests record the raw artifact id in
`input_artifact_ids` and v1 manifests retain the legacy `source` field.

Contract identifier: `normalized.fundamental-facts.v1`

| Field | Meaning |
| --- | --- |
| `cik` | Ten-digit SEC CIK; the entity key until a cross-provider instrument registry exists |
| `taxonomy`, `concept`, `label` | XBRL classification and human label |
| `unit` | Reported unit, e.g. `USD`, `shares`, `USD/shares`, `pure` |
| `value_type` | JSON scalar kind from raw (`integer`, `number`, `string`, `boolean`, `null`) |
| `value_text` | Lossless original scalar text |
| `value` | Parsed `Float64` for `integer`/`number` facts; NULL otherwise |
| `period_type` | `duration` when `start_date` is present, else `instant` |
| `start_date`, `end_date` | Reporting period bounds; instant facts have NULL start |
| `fiscal_year`, `fiscal_period`, `form`, `filed_date`, `frame` | Filing context; `filed_date` is the point-in-time anchor; `fiscal_year`/`fiscal_period` are NULL for non-periodic filings such as DEF 14A |
| `source_artifact_id`, `normalized_at` | Lineage and transformation time |

Rules:

- numeric values are parsed with exact string preservation in `value_text`;
  `Float64` rounding for very large integers is a documented v1 precision
  tradeoff;
- an `integer`/`number` fact that cannot be parsed fails normalization
  instead of silently producing NULL;
- `string`/`boolean` facts stay unparsed with NULL `value`;
- exact duplicate rows are removed; distinct restatements of the same fact
  are preserved for analyst judgment, so the table declares no unique key;
- currency is carried by the reported `unit` (`USD`, `USD/shares`); an
  explicit currency column is deferred until a mixed-unit normalization is
  needed.

## Research registers

Registers store the explicit judgments and open items of a research process in
small CSV files under `<case>/registers/`; they are human-auditable and edited
with ordinary CSV tooling, not through the CLI.

```text
finresearch --workspace PATH data registers status CASE_ID
```

Five versioned register contracts are validated and summarized:

| File | Rows | Key rules |
| --- | --- | --- |
| `evidence.csv` | one claim per row | unique `id`; `source_type` is `filing`/`estimate`/`external`/`assumption`; `observed_at` date |
| `assumptions.csv` | one analyst assumption per row | unique `id`; optional `source_evidence` must reference an existing evidence id |
| `scenarios.csv` | one scenario parameter value per row | `scenario` is `bear`/`base`/`bull`; every parameter must define all three with pairwise-distinct values |
| `catalysts.csv` | one event per row | unique `id`; `impact` is `positive`/`negative`/`neutral`; optional `expected_date` |
| `open_questions.csv` | one open item per row | unique `id`; `importance` `high`/`medium`/`low`; `status` `open`/`answered` |

Missing registers are not errors; a case may have none yet. The command
reports per-register row counts and exits nonzero on any schema, enum, date,
uniqueness, reference, or scenario-completeness violation. Reported facts,
estimates, and analyst assumptions stay explicitly classified through
`source_type` and the evidence reference, so every assumption can be traced
to its support.

## Provider ownership

`yfinance` is a convenient market-data adapter for personal research and
prototyping. It is not an authoritative financial-reporting source and its own
documentation states that it is an unofficial Yahoo integration intended for
research and educational use:

- <https://ranaroussi.github.io/yfinance/index.html>
- <https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html>

The minimum provider stack for auditable U.S. equity research is:

1. **yfinance** for prototype prices, distributions, splits, and convenient
   market metadata.
2. **SEC EDGAR** for filing metadata and reported XBRL facts. The current
   adapters ingest `filings.recent` and companyfacts from `data.sec.gov`;
   clients must declare a user agent and respect the current fair-access limit.
3. **FRED or U.S. Treasury** for macro series and risk-free-rate inputs. FRED is
   useful when vintage-aware ALFRED observations matter; Treasury is the direct
   source for daily Treasury curve data.

Official references:

- <https://www.sec.gov/search-filings/edgar-application-programming-interfaces>
- <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data>
- <https://fred.stlouisfed.org/docs/api/fred/overview.html>
- <https://home.treasury.gov/treasury-daily-interest-rate-xml-feed>

SEC is the authoritative input for the later normalized reported-fundamentals
contract. FRED/Treasury can wait until the first DCF or macro-sensitive
workflow. Free consensus-estimate data has no equivalent authoritative source;
any later estimate adapter must preserve provider, `estimate_as_of`, retrieval
time, and licensing constraints rather than treating current yfinance
estimates as point-in-time history.
