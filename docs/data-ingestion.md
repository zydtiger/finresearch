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
- The manifest records artifact ID, kind, schema version, relative path,
  provider, retrieval time, row count, and SHA-256 checksum.
- A failed Parquet write or manifest update removes only the new partial output.
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
files exist. `data validate` re-reads each declared Parquet snapshot and
verifies, per artifact:

- the manifest SHA-256 matches the file bytes;
- the manifest row count matches the Parquet height;
- the artifact `kind` and `schema_version` resolve to a registered
  `DatasetContract` in `data_contracts.py`;
- the Parquet schema, required non-null fields, and unique key satisfy that
  contract; and
- the manifest `retrieved_at` matches the snapshot's own `retrieved_at`
  provenance column.

```text
finresearch --workspace PATH data validate CASE_ID [ARTIFACT_ID]
finresearch --workspace PATH data inspect CASE_ID ARTIFACT_ID [--limit N]
```

Without `ARTIFACT_ID`, `data validate` deep-checks every declared `.parquet`
artifact and ignores declared non-Parquet outputs such as reports. Exact
selection of a non-Parquet artifact fails as unsupported. `data inspect` runs
the same deep validation first, then reports the contract identifier, relative
path, size, SHA-256,
row count, columns and dtypes, date and timestamp ranges, per-column null
counts, unique-key violations, constant provenance fields (`provider`,
`provider_symbol`, `cik`, and `source_url` when the snapshot declares them), and a
deterministic JSON-record preview of the first `N` rows (default 5, maximum
100). Invalid or unknown-contract artifacts are not inspected.

Both commands share the `DatasetContract` registry so schema, validation, and
inspection cannot drift; adding a new raw or normalized table means registering
one contract.

## Normalized daily prices v1

Normalization is a deterministic, offline transformation from one immutable raw
yfinance snapshot. It never rewrites raw files and never fetches from the
network; the same snapshot and code version always produce the same output.

```text
finresearch --workspace PATH data normalize-daily-prices \
  CASE_ID SYMBOL [--raw-artifact-id ARTIFACT_ID]
```

The command writes two registered artifacts below `data/normalized/` and
records each manifest `source` as the raw artifact id, so every normalized row
can be traced to its exact raw snapshot. When a symbol has several raw
snapshots, `--raw-artifact-id` selects one; otherwise normalization fails with
the candidate ids.

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

Re-running normalization appends a fresh immutable pair (no overwrite). The
instrument master registers before the price bars; if the process is
interrupted between the two writes, `case validate` flags the orphaned master
and a re-run produces a complete pair.

Both normalized tables share the same `DatasetContract` registry, so
`data validate` and `data inspect` cover them without extra code.

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
