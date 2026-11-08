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

## Raw yfinance daily-prices v2

Contract identifier: `raw.yfinance.daily-prices.v2`

The v1 fields plus a non-null `quote_type` (`String`, e.g. `EQUITY`,
`CRYPTOCURRENCY`, `ETF`, `CURRENCY`, `INDEX`), placed after `currency`. The
adapter reads it from yfinance `fast_info` at retrieval time, like currency; a
symbol whose quote type cannot be reported fails ingestion. `data
ingest-yfinance-prices` writes v2 snapshots. v1 snapshots stay valid and
readable, and normalize as before (`asset_class = "unknown"`).

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
  CASE_ID SYMBOL [--raw-artifact-id ARTIFACT_ID] [--null-bars fail|drop]
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

Normalization reads raw v1 and v2 snapshots. A v2 snapshot writes a
`normalized.instrument-master.v3` row whose `asset_class` maps the raw
`quote_type` (`CRYPTOCURRENCY` to `crypto`, `EQUITY` to `equity`, `ETF` to
`etf`, `CURRENCY` to `fx`, `INDEX` to `index`, anything else to `unknown`) and
whose `asset_class_source` is `provider`; its token fields and `provider_ids`
are NULL. A v1 snapshot still writes the unchanged v2 master with
`asset_class = "unknown"`. `case audit` uses the provider class when no other
master observation resolves the price's class.

### Null provider bars and normalized.price-gaps.v1

`--null-bars fail` (default) keeps the rule above: a bar with a NULL open,
high, low, or close fails normalization. `--null-bars drop` removes those bars
from `normalized.daily-prices.v2` and publishes a companion
`normalized.price-gaps.v1` artifact, always, even when no bar was dropped
(`allow_empty`). The policy is a producer parameter of the prices and gaps
artifacts, so `drop` output has a distinct artifact id from `fail` output;
`fail` output keeps its existing identity. Bars with present but inconsistent
OHLC still fail under `drop`. The corporate-actions table is unaffected.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | `UInt16` | `1` |
| `instrument_id` | `String` | Instrument |
| `session_date` | `Date` | Dropped session |
| `reason` | `String` | `null_ohlc` |
| `null_fields` | `String` | Sorted comma list of the NULL fields among `open`, `high`, `low`, `close`, e.g. `close,high,low,open` |
| `source_artifact_id` | `String` | Raw snapshot |

Unique key `(instrument_id, session_date, source_artifact_id)`.

Both legacy normalized tables share the same `DatasetContract` registry, so
`data validate` and `data inspect` cover them without extra code.

### FX rates

```text
finresearch --workspace PATH data normalize-fx-rates CASE_ID SYMBOL \
  --base CCC --quote CCC [--raw-artifact-id ID]
```

Reads one raw yfinance daily snapshot (v1 or v2) and writes the existing
`normalized.fx-rates.v1` from `close` only, with `rate_kind = "close"`,
`rate_date = session_date`, and the raw `timestamp`. `SYMBOL` must be
`<BASE><QUOTE>=X` for the declared controlled ISO codes, which must differ. OHLC
ordering is not checked because no OHLC field is published, so yfinance FX bars
with `low` above `open` normalize. A NULL `close` drops the row; every dropped
session is recorded in `normalized.price-gaps.v1` (`null_fields = close`), which
is always published, possibly empty. A nonpositive `close`, duplicate session
dates, or no remaining row fails. Both artifacts carry the `null_bars = drop`
policy in their identity and descend from the raw snapshot. Pass the printed
`normalized.fx-rates` artifact id to `[translation]` in `dcf-inputs.toml`
(see `modeling.md`).

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
and fact categories
`balance-sheet,cash-flow,entity,income-statement,other,protocol-metric,share-data,token-supply`.
Token support widens `normalized.fundamental-facts.v2` in place with units
`tokens`, `<FIAT>/token` (one per currency code, singular `token`), and `bps`,
unit kind `tokens`, and the two categories above. Each new unit has a fixed
pairing: `tokens` requires `unit_kind = "tokens"` and NULL `currency`;
`<FIAT>/token` requires `unit_kind = "currency"` and the matching `currency`;
`bps` requires `unit_kind = "ratio"` and NULL `currency`. Pre-existing units
are not re-validated, so existing artifacts stay valid; older CLIs reject rows
that use the new values. Stablecoins are not currencies: `CURRENCY_CODES` is
unchanged and provider values quoted in a stablecoin carry a separate
controlled `quote_asset` from `QUOTE_ASSETS` (`CURRENCY_CODES` plus `USDC`,
`USDT`, `USDH`); a row whose `quote_asset` is a stablecoin has NULL `currency`.
Canonical publication applies each contract's declared deterministic sort key;
contracts enforce exact schemas,
nullability, unique keys, UTC timestamps, and semantic invariants such as
OHLC ordering, positive FX rates, valid action amounts, and PIT timestamp order.

```text
finresearch --workspace PATH data import-csv CASE_ID FILE \
  --schema instrument-master.v2|daily-prices.v2|fundamental-facts.v2|estimates.v1|corporate-actions.v1|fx-rates.v1|instrument-master.v3|token-supply.v1|instrument-master.v3|model.comps-observations.v1 \
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

### normalized.instrument-master.v3

The v2 fields plus `chain` (lowercase slug, e.g. `hyperliquid-l1`),
`contract_address`, `token_standard` (`native`, `erc20`, `spl`, `hip1`,
`other`), `provider_ids` (sorted `provider:id` pairs joined by `;`, unique
providers, no empty ids, e.g. `coingecko:hyperliquid;yfinance:HYPE32196-USD`),
and a required `asset_class_source` (`provider` or `import`). `chain`,
`contract_address`, and `token_standard` are only allowed when
`asset_class = "crypto"`, and `contract_address` requires `chain`. The token
master is an import (`--schema instrument-master.v3`) because its ids are case
declarations. `reconcile-instrument-master` reads v2 and v3 observations; when
any v3 observation is present it writes
`derived.instrument-master-current.v2` (the v3 fields plus `as_of`; v2
observations contribute NULL token fields and `import`), otherwise it writes
the unchanged v1 output.

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

## Captures

A capture is the exact response or document bytes plus request provenance. It
is the raw layer for token and protocol providers (JSON responses are not
flattened to Parquet) and for agent-fetched material such as web pages. One
capture registers two artifacts, both in manifest v2 cases only:

1. `raw.capture-body`: the exact bytes, non-Parquet, at
   `data/raw/captures/<provider>/<capture-key>/<retrieved-at>.<ext>`. Its
   manifest `sha256` is the content hash.
2. `raw.capture.v1`: a one-row Parquet metadata table at the same stem whose
   only parent is the body artifact. `capture_id` is this artifact's id.

| Field | Meaning |
| --- | --- |
| `capture_id`, `body_artifact_id` | Metadata artifact id and its body parent |
| `provider` | `coingecko`, `hyperliquid`, `defillama`, or `web` |
| `capture_kind` | `api-response`, `web-page`, `document`, `tool-summary` |
| `recorded_by` | `cli` for ingest commands, `agent` for `register-capture` |
| `request_method`, `url`, `request_body` | `GET`/`POST`; absolute `https` URL; canonical JSON text for POST (compact, `type` first, other keys sorted) |
| `http_status` | Required for `api-response`; 2xx whenever present |
| `media_type`, `content_sha256`, `content_bytes` | Body type, SHA-256, size |
| `retrieved_at` | Provider retrieval time (also the manifest `retrieved_at`) |

The unique key is `capture_id`. Validation rejects URLs with userinfo or a
query parameter whose name contains `key`, `token`, or `secret`, so provider
API keys are never persisted. The capture key is a readable host slug plus the
first 12 hex digits of `sha256(method, url, request_body)`; identity is the key
plus retrieval time, so repeated captures append. `tool-summary` labels text an
agent tool returned instead of raw bytes.

```text
finresearch --workspace PATH data register-capture CASE_ID FILE \
  --provider web|coingecko|hyperliquid|defillama \
  --kind api-response|web-page|document|tool-summary \
  --url URL --retrieved-at RFC3339_UTC [--method GET|POST] \
  [--request-body FILE] [--http-status N] [--media-type TYPE] \
  [--expect-sha256 HEX]
```

The CLI computes the SHA-256 itself; `--expect-sha256` fails the command when
the bytes differ from a hash the agent recorded at fetch time. `--media-type`
defaults from the file extension (`.json`, `.html`, `.htm`, `.md`, `.txt`,
`.pdf`) and is required otherwise; `--http-status` is required for
`api-response`. Absolute input paths never enter the manifest. A rerun with the
same bytes and metadata returns the existing receipt; different bytes or
metadata at the same identity are an integrity error.

## Token supply and market data

Token providers use captures (see [Captures](#captures)) instead of flattened
Parquet: the CLI writes the exact response bytes, and normalization parses a
declared capture with a parser selected by `provider` and the URL path or POST
`type`. The same parser accepts a CLI capture and an agent-registered capture
of the same endpoint. An unrecognized endpoint fails with the list of
supported endpoints. All commands require a manifest v2 case and an explicit
`CASE_ID`; no provider id is mapped to an instrument implicitly.

### Free endpoints and fair access

Adapters use only endpoints that answer without an API key, and the CLI has no
key option. Each provider has a host-shared file lock that spaces request
starts across CLI processes: CoinGecko at least 6 seconds, Hyperliquid 0.25
seconds, DefiLlama 0.5 seconds. Redirects are rejected, timeouts are finite,
and the response must be 2xx JSON. A provider error is printed verbatim
(`HTTP <status>: <body>`) and writes nothing.

| Need | Endpoint used |
| --- | --- |
| Token supply, price, market cap, FDV | CoinGecko `/coins/{id}`, `/coins/markets`; Hyperliquid `tokenDetails` |
| Staking | Hyperliquid `validatorSummaries` |
| Other Hyperliquid state | `metaAndAssetCtxs`, `spotMetaAndAssetCtxs`, `vaultDetails`, `spotClearinghouseState`, `fundingHistory` (capture only) |

```text
finresearch --workspace PATH data ingest-coingecko-token CASE_ID COIN_ID \
  [--vs-currency usd]
finresearch --workspace PATH data ingest-coingecko-markets CASE_ID \
  --ids COIN_ID[,COIN_ID...] [--vs-currency usd]
finresearch --workspace PATH data ingest-hyperliquid-info CASE_ID \
  --request token-details|validator-summaries|meta-and-asset-ctxs|spot-meta-and-asset-ctxs|vault-details|spot-clearinghouse-state|funding-history \
  [--token-id HEX] [--address HEX] [--coin NAME --start YYYY-MM-DD]
```

`ingest-coingecko-token` fetches `/api/v3/coins/{COIN_ID}` with localization,
tickers, community, and developer data disabled and a `vs_currency` query
parameter (any `QUOTE_ASSETS` code, lowercase on the wire; the parser reads the
quote back from the URL and defaults to `usd`). `ingest-coingecko-markets`
fetches `/api/v3/coins/markets` for the sorted, distinct id list (at most 250)
in one capture. `ingest-hyperliquid-info` POSTs a canonical body to
`https://api.hyperliquid.xyz/info`: `token-details` requires `--token-id`,
`vault-details` and `spot-clearinghouse-state` require `--address`, and
`funding-history` requires `--coin` and `--start` (UTC midnight) and pages
(500 entries per page) until the retrieval time, one capture per page; every
other combination fails before any request. All pages of one command share its
retrieval time.

### normalized.token-supply.v1

One row per provider observation of one supply metric.

| Field | Meaning |
| --- | --- |
| `provider`, `instrument_id`, `provider_asset_id` | Provider; case-declared token id; the provider's own id |
| `supply_metric` | `circulating`, `total`, `max`, `non_circulating`, `staked`, `locked`, `burned`, `future_emissions` |
| `amount`, `unit` | Finite nonnegative token count; always `tokens` |
| `definition_id` | Provider field path, e.g. `coingecko.market_data.circulating_supply` |
| `holder_address`, `holder_label` | Address-level rows only (`non_circulating`, `staked`, `locked`, `burned`); summary rows have NULL |
| `observed_at`, `observed_at_basis` | Provider as-of time; `provider`, or `retrieval` when the response has no timestamp (then equal to `retrieved_at`) |
| `retrieved_at`, `normalized_at` | Capture retrieval time, or `--retrieved-at` for imports; never the wall clock |
| `capture_artifact_id`, `source_artifact_id` | Originating capture (NULL only for imports without one; when present it must be a declared `raw.capture.v1`); parent artifact |

Unique key `(provider, instrument_id, supply_metric, definition_id,
holder_address, observed_at, source_artifact_id)`; rows are sorted by
`(instrument_id, supply_metric, provider, observed_at, holder_address,
definition_id, source_artifact_id)`. Within one provider observation
`circulating <= total <= max` where present; a violation fails with the values.

### normalized.token-market.v1

Provider market fields for reconciliation and peer multiples: `metric` is
`price`, `market_cap`, `fdv`, or `volume_24h`; `value` is finite and positive
(`volume_24h` may be zero); `quote_asset` is from `QUOTE_ASSETS` and `currency`
is the fiat code or NULL for a stablecoin quote; `fdv_supply_basis` (`total` or
`max`) is required for `fdv` and NULL otherwise. CoinGecko FDV rows carry
`total`, the parser's declared reading of the provider definition. Unique key
`(provider, instrument_id, metric, quote_asset, observed_at,
source_artifact_id)`.

### Normalization and import

```text
finresearch --workspace PATH data normalize-token-supply CASE_ID \
  --capture-artifact-id ID \
  (--instrument-id ID | --instrument-map PROVIDER_ASSET_ID=INSTRUMENT_ID ...)
```

`--instrument-id` is for single-asset captures. A multi-asset capture needs one
`--instrument-map` per asset to normalize: unmapped assets are skipped and
listed, and a map entry absent from the capture fails.

| Capture | Output rows |
| --- | --- |
| CoinGecko `/coins/{id}` | supply `circulating`, `total`, `max` (`observed_at = market_data.last_updated`); market `price`, `market_cap`, `fdv`, `volume_24h` for the URL's quote |
| CoinGecko `/coins/markets` | Per mapped coin: the same rows with `observed_at = last_updated`; null fields and zero market cap or FDV produce no row |
| Hyperliquid `tokenDetails` | supply `circulating`, `total`, `max`, `future_emissions`, plus one `non_circulating` row per `nonCirculatingUserBalances` entry; `observed_at_basis = "retrieval"`; `provider_asset_id` is the request's token id |
| Hyperliquid `validatorSummaries` | one `staked` summary row: the sum of validator `stake` divided by 10^8 (the staking token's wei decimals); `provider_asset_id` is `HYPE` |

CoinGecko `/coins/{id}` and `/coins/markets` rows share the definition ids
`coingecko.market_data.*`, so one reconciliation policy resolves either.
Whether a non-circulating balance is burned or held is an agent judgment that
enters through the model input, not the supply table. Supply rows from web
pages (for example CoinMarketCap) enter through
`data import-csv --schema token-supply.v1` with `capture_artifact_id` set to
the registered `tool-summary` or `web-page` capture; the CSV columns are the
contract fields except `schema_version`, `provider`, `retrieved_at`,
`source_artifact_id`, and `normalized_at`, which the import generates.

### Reconciliation

```text
finresearch --workspace PATH data reconcile-token-supply CASE_ID \
  --instrument-id ID --as-of YYYY-MM-DD \
  --policy analysis/token-supply-policy.toml [--source-artifact-id ID ...]
```

The policy is a strict case-relative TOML whose bytes are hashed into the
output identity and recorded as the input file `token-supply-policy`:

```toml
version = 1
provider_priority = ["hyperliquid", "coingecko", "coinmarketcap"]

[tolerance]           # relative spread (max - min) / min per metric
circulating = 0.02
total = 0.01
max = 0.0

[resolution.circulating]
provider = "coingecko"
definition_id = "coingecko.market_data.circulating_supply"
source_id = "a-circ-definition"
```

For each `supply_metric` the command takes each provider's latest observation
with `observed_at` on or before the cutoff date (address-level rows excluded),
computes `spread = (max - min) / min`, and assigns a status: `single-source`
(one provider; that row), `agree` (`spread <= tolerance`; the first provider in
`provider_priority`), `resolved` (`spread > tolerance` and a
`[resolution.<metric>]` exists; the named provider and definition), or
`conflict` (no resolution; no row is selected). Failures: a metric observed by
two or more providers without a tolerance entry; a resolution that is used but
names an absent observation or a `source_id` that does not resolve to an
evidence or assumption dated on or before the cutoff; equal-time conflicting
observations from one provider; a zero minimum with a positive maximum.

`derived.token-supply-reconciliation.v1` has one row per provider observation
(`instrument_id`, `as_of`, `supply_metric`, `provider`, `definition_id`,
`observed_at`, `amount`, `min_amount`, `max_amount`, `spread`, `tolerance`,
`status`, `selected`, `resolution_source_id`, `source_artifact_id`); at most one
`selected` row per metric and none for a `conflict`. Its parents are the
supply artifacts it observed; its timestamp is `as_of` at UTC midnight.

`case audit` reports, for the latest reconciliation per instrument with
`as_of` on or before the audit cutoff, `token_supply_conflict` for every metric
with status `conflict` (a conflict that no model uses is still reported). It
also reports `token_supply_after_as_of` and `token_market_after_as_of` for
observations dated after the cutoff, and `capture_metadata_mismatch` when a
capture's `content_sha256` differs from its body artifact checksum, or its
`capture_id` or `body_artifact_id` does not match the artifact id and parent.

## Protocol metrics

Protocol metrics describe the economics of a protocol (fees, revenue, volume,
open interest, funding) with the provider's definition attached. They use
captures like token data and are keyed by `entity_id` (the existing entity key
of `fundamental-facts.v2`, for example `hyperliquid`); the value-receiving token
is linked by an optional `instrument_id`.

```text
finresearch --workspace PATH data ingest-defillama CASE_ID SLUG \
  --dataset fees|revenue|holders-revenue|dex-volume|open-interest \
  [--dataset ...]
finresearch --workspace PATH data normalize-protocol-metrics CASE_ID \
  --capture-artifact-id ID [--capture-artifact-id ID ...] \
  --entity-id ID [--instrument-id ID]
```

Each dataset is one capture of
`https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyFees|dailyRevenue|dailyHoldersRevenue`,
`/summary/dexs/{SLUG}?dataType=dailyVolume`, or
`/summary/open-interest/{SLUG}` (keyless endpoints; per-protocol summaries only,
never the large aggregate endpoints). Datasets are fetched in the given order;
a failure stops the command, keeps the captures already completed (each
independently valid), and prints the provider error verbatim.

### normalized.protocol-metrics.v1

One row per provider value of one metric for one period: `provider`,
`entity_id`, `instrument_id`, `provider_protocol_id` (for example the DefiLlama
slug), `sub_protocol` (child protocol or perp market; NULL for the provider
aggregate), `chain` (NULL for aggregate rows and providers without chains), `metric`, `period_type` (`duration` or `instant`), `period_start`
(required for `duration`, NULL for `instant`), `period_end` (inclusive UTC
date), `value`, `unit`, `currency`, `is_complete`, `provider_definition_id`,
`observed_at`, `capture_artifact_id`, `source_artifact_id`, `normalized_at`.

| Metric | Unit | Period |
| --- | --- | --- |
| `volume_perp`, `volume_spot` | fiat | duration |
| `fees`, `revenue`, `holders_revenue`, `supply_side_revenue` | fiat | duration |
| `buybacks`, `burns`, `distributions`, `incentives` | fiat or `tokens` | duration |
| `open_interest` | fiat (notional) | instant |
| `funding_rate_1h` | `bps` | instant |

Volume, fees, and open interest are nonnegative; revenue metrics and funding
may be negative. `currency` equals a fiat `unit` and is NULL for `tokens` and
`bps`. `is_complete` is false exactly when `period_end` is on or after the UTC
date of `normalized_at` (the capture's retrieval time). Unique key
`(provider, provider_protocol_id, sub_protocol, chain, metric, period_start,
period_end, observed_at, source_artifact_id)`. Rows are never summed across chains or children. Parent and child rows are never
summed: consumers select one level explicitly (`sub_protocol IS NULL` is the
provider aggregate).

### normalized.provider-metric-definitions.v1

Written with every protocol-metrics artifact so the provider's definition at
retrieval time travels with the numbers: `provider_definition_id`
(`<provider>.<protocol>.<field>.<text-hash-12>`), `metric`, `provider_field`,
`methodology_text`, `methodology_url`, `definition_sha256` (hash of the
canonical field, text, and URL), `observed_at`, `source_artifact_id`. Because the
id embeds the hash, a changed methodology produces a new id. `data validate`
reports `protocol_metric_definition_missing` when a protocol-metrics artifact
cites a `provider_definition_id` that no declared definitions artifact holds.

### Normalization

| Capture | Output rows |
| --- | --- |
| DefiLlama `totalDataChart` | Aggregate daily rows (`sub_protocol` NULL); a day is the UTC date of the bucket timestamp with `period_start = period_end` and `observed_at` the bucket timestamp. Metric: `fees`, `revenue`, `holders_revenue` by `dataType`; `volume_spot` for `dexs` |
| DefiLlama `totalDataChartBreakdown` | DefiLlama nests chain, then sub-protocol, then value (`{"Hyperliquid L1": {"Hyperliquid Perps": v}}`): one row per (`chain`, `sub_protocol`). A flat `{sub_protocol: value}` breakdown yields rows with NULL `chain`. Null values produce no row |
| DefiLlama `open-interest` | `open_interest` `instant` rows observed at the bucket timestamp |
| Hyperliquid `metaAndAssetCtxs` | Per non-delisted perp market (`sub_protocol` = market name): `open_interest` as `openInterest * markPx` notional USD, `funding_rate_1h` as `funding * 10,000` bps, and a `volume_perp` row from `dayNtlVlm` for the 24 hours ending at retrieval (UTC dates of the window start and end); all observed at retrieval |

Several captures for one entity produce one artifact pair (metrics plus
definitions) with the captures as ordered parents. The Hyperliquid
derivations are recorded as parser-declared definitions (fields
`openInterest_x_markPx`, `funding`, `dayNtlVlm`). `case audit` reports
`protocol_metric_after_as_of` for a `period_end` or `observed_at` after the
cutoff. Perp volume has no free daily series: history accumulates from
repeated `metaAndAssetCtxs` snapshots.

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
| `assumptions.csv` | one analyst assumption per row | unique `id`; optional `source_evidence` is one evidence id or a `;`-separated list of existing ids (no spaces, no duplicates, ascending order); a missing id is named in the error |
| `scenarios.csv` | one scenario parameter value per row | `scenario` is `bear`/`base`/`bull`/`all`; a parameter has either one `all` row (fixed across scenarios) or all three bear/base/bull rows with pairwise-distinct values, never both (`scenario_fixed_conflict`) |
| `catalysts.csv` | one event per row | unique `id`; `impact` is `positive`/`negative`/`neutral`; optional `expected_date` |
| `open_questions.csv` | one open item per row | unique `id`; `importance` `high`/`medium`/`low`; `status` `open`/`answered` |

`evidence.csv` `source_ref` may contain `capture:<capture_id>` tokens beside
free text. `data registers status` and `case audit` report
`register_capture_reference_invalid` when a referenced capture is not a
declared `raw.capture.v1`, and `register_capture_after_observed` when the
capture's `retrieved_at` date is after the evidence `observed_at`.

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
