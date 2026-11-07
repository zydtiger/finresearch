# Token Support Design

Status: partly implemented. Sections marked "Implemented" are normative in
the docs they point to; the rest remains a proposal. Once a part ships, its normative text moves into `case-contract.md`,
`data-ingestion.md`, `modeling.md`, or `reporting.md`, and this document is
reduced to the remaining roadmap.

## Scope and principles

finresearch is built around listed equities: DCF share units, fact units,
comps metrics, and price normalization assume shares, fiat, and exchange
sessions. This design adds tokens without weakening the existing contracts.

- The CLI stays deterministic and offline after retrieval. It ingests
  structured provider APIs, stores agent-fetched unstructured data only
  through strict imports with provenance, reconciles conflicting sources with
  declared tolerances, and owns token valuation arithmetic.
- Agents do web research and judgment. The CLI never runs an agent, scrapes a
  web page, or chooses an authoritative source on its own.
- Every new table is a registered `DatasetContract`; `data validate`,
  `data inspect`, and `case audit` cover it through the shared registry.
- Every new command takes `--workspace PATH` and an explicit `CASE_ID`. No
  provider ids, instrument ids, supply definitions, or peers are inferred.
- New behavior is additive. Existing manifests, contracts, and cases stay
  valid; see [Migration and compatibility](#migration-and-compatibility).

### Worked example

The trial case `hype-2026-11-07` valued Hyperliquid's HYPE with the current
CLI. Its `analysis/pipeline-log.md` records each step. The CLI covered prices,
an analyst instrument-master import, USD fee facts, registers, and audit. All
supply, unlock, protocol-metric, peer, and validator data was fetched and
reconciled by hand into `analysis/raw/`, and the valuation ran in the
analyst-owned `analysis/hype_model.py`. HYPE examples below use that case's
captures:

| Fact | Value | Source in the case |
| --- | --- | --- |
| Valuation close | 78.4549 USD on 2026-11-06 | yfinance `HYPE32196-USD`, 24/7 bar stamped 00:00 UTC |
| Circulating supply | 239,406,814 / 253,077,565 / 301,819,911 | CoinGecko / CoinMarketCap page / Hyperliquid `tokenDetails` |
| Total supply | 951,538,273 / 950,036,532 / 998,778,474 | same order |
| Assistance Fund balance | 48,742,089 HYPE at `0xfefe...` | Hyperliquid `tokenDetails.nonCirculatingUserBalances` |
| Staked | 439,122,361 HYPE | Hyperliquid `validatorSummaries` |
| Fees / holders revenue, 365 days | 897.9M / 682.8M USD | DefiLlama `/summary/fees/hyperliquid` |
| Base DCF per token | 3.43 (FD) / 13.63 (circulating) | `hype_model.py` |

### Gap map

| Pipeline-log step | Gap | Section |
| --- | --- | --- |
| 1a; FX in equity cases | One null bar or OHLC violation blocks normalization | [Null provider bars](#null-provider-bars-in-normalize-daily-prices), [FX](#fx-normalization-and-conversion) |
| 1c, 10 | Calendar-day staleness on 24/7 bars | [Price-age policy](#per-asset-class-price-age-policy) |
| 2 | No chain, contract, or provider ids; yfinance master says `unknown` | [instrument-master.v3](#normalizedinstrument-masterv3) |
| 3 | Supply from three sources, reconciled by hand | [Token supply](#token-supply), [Reconciliation](#cross-source-reconciliation) |
| 4 | Unlock schedule only in prose | [Token unlocks](#token-unlocksv1-import) |
| 5, 6 | Fees and holders revenue forced into SEC-shaped facts; `tokens` rejected | [Protocol metrics](#protocol-metrics), [Units](#units-and-stablecoin-quotes) |
| 7 | Single-id `source_evidence`; captures without lineage | [Captures](#web-and-api-captures), [Registers](#multi-id-source_evidence) |
| 8, 11 | Token model and report outside the CLI | [model token](#model-token) |
| Dilutive equity cases | One share-count leaf for all scenarios | [DCF dilution](#scenario-specific-share-counts-and-issuance-in-the-dcf) |

## Shared foundations

### Token identity

A token is an instrument with a case-declared `instrument_id`, for example
`hype.hyperliquid.l1` as imported in the trial case. Token tables key on that
id plus the provider's own id (`provider_asset_id`): `hyperliquid` for
CoinGecko, `0x0d01dc56dcaaca66ad901c959b4011ec` for the Hyperliquid token id,
`hyperliquid` as a DefiLlama protocol slug. Ingest and normalize commands take
`--instrument-id` explicitly; the CLI never maps a provider id to an
instrument on its own. `instrument-master.v3` records the mapping so audit can
check it.

Protocols that earn revenue use `entity_id` (for example `hyperliquid`), the
existing entity key of `fundamental-facts.v2`. The token that receives value
is linked by `instrument_id`.

### Units and stablecoin quotes

> **Implemented (phase 1)**: see `data-ingestion.md`, "Canonical contracts and
> explicit local import". Model token scales, register unit documentation, and
> peg leaves belong to the later `model token` phase.

Add to the controlled sets in `data_contracts.py`:

| Set | Additions |
| --- | --- |
| `FACT_UNITS` | `tokens`; `<FIAT>/token` for each of the 11 `CURRENCY_CODES` (`USD/token`, `EUR/token`, ...); `bps` |
| `UNIT_KINDS` | `tokens` |
| `FACT_CATEGORIES` | `protocol-metric`, `token-supply` |
| Model token scales | `tokens`, `tokens_k`, `tokens_m`, `tokens_b`, mirroring the DCF share scales |
| Register units | `tokens`, `tokens_k`, `tokens_m`, `tokens_b`, `<FIAT>/token`, `bps` documented as accepted values for `assumptions.csv` and `scenarios.csv` |

Per-unit amounts use the singular `<FIAT>/token`, matching the model output
convention `USD/share`. The plural `USD/shares` stays because it is SEC's
reported unit string. New units carry a fixed `unit_kind`: `tokens` requires
`unit_kind = "tokens"` and NULL `currency`; `<FIAT>/token` requires
`unit_kind = "currency"` and the matching `currency`; `bps` requires
`unit_kind = "ratio"` and NULL `currency`. Existing unit and kind pairs are
not re-validated, so existing artifacts stay valid.

`fundamental-facts.v2` already allows NULL `cik`, `taxonomy`, `form`, and
`accession_number`; the trial import only failed on the unit. Token supply and
protocol metrics nevertheless get their own contracts below, because they need
definition, holder, and period fields that facts do not have. The widened
fact units remain for scalar facts such as a whitepaper max supply.

Stablecoins are not currencies. `CURRENCY_CODES` is unchanged, and every
`currency` column keeps accepting fiat only. Provider values quoted in a
stablecoin carry a separate controlled `quote_asset`:

```text
QUOTE_ASSETS = CURRENCY_CODES ∪ {USDC, USDT, USDH}
```

A row whose `quote_asset` is a stablecoin has NULL `currency`. Models never
treat a stablecoin as its fiat peg implicitly. A token model input that uses a
stablecoin-quoted value must declare a sourced peg leaf, for example
`usdt_usd = { value = 1.0, unit = "USD/USDT", source_id = "a-usdt-par" }`.
Peg units are `<FIAT>/<STABLECOIN>` for the stablecoins in `QUOTE_ASSETS`
and are accepted only on peg leaves. When the case holds a normalized price
series for that stablecoin, audit reports `stablecoin_peg_deviation` if its
close on the model's price session differs from the declared peg by more than
a fixed relative 0.005.

### Web and API captures

> **Implemented (phase 1)**: see `data-ingestion.md`, "Captures", and
> `case-contract.md`, "Capture artifacts". Provider ingest commands and
> normalization from captures are phase 2 and 3; multi-id `source_evidence`
> and `capture:` references are in `data-ingestion.md`, "Research registers".

The trial case saved every API response under `analysis/raw/` with a sidecar
such as:

```json
{"url": "https://api.coingecko.com/api/v3/coins/hyperliquid?...", "method": "GET",
 "retrieved_at_utc": "2026-11-07T20:29:28Z", "http_status": 200, "bytes": 29873,
 "sha256": "e46c4676..."}
```

None of it was visible to the manifest. A capture becomes the common raw
layer for every new provider and for agent-fetched material.

#### Capture artifacts

One capture registers two artifacts:

1. `raw.capture-body`: the exact response or document bytes, non-Parquet,
   below `data/raw/captures/<provider>/<capture-key>/<retrieved-at>.<ext>`.
   Its manifest `sha256` is the content hash.
2. `raw.capture.v1`: a one-row Parquet metadata table whose only parent is the
   body artifact.

Contract `raw.capture.v1`:

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `capture_id` | `String` | no | Artifact id of this metadata row |
| `body_artifact_id` | `String` | no | The `raw.capture-body` parent |
| `provider` | `String` | no | `coingecko`, `hyperliquid`, `defillama`, or `web` |
| `capture_kind` | `String` | no | `api-response`, `web-page`, `document`, `tool-summary` |
| `recorded_by` | `String` | no | `cli` for ingest commands, `agent` for `register-capture` |
| `request_method` | `String` | no | `GET` or `POST` |
| `url` | `String` | no | Absolute `https` URL without credentials |
| `request_body` | `String` | yes | Canonical JSON text for POST bodies, e.g. `{"type":"tokenDetails","tokenId":"0x0d01..."}` |
| `http_status` | `Int16` | yes | Required for `api-response`; must be 2xx |
| `media_type` | `String` | no | e.g. `application/json`, `text/html`, `text/markdown` |
| `content_sha256` | `String` | no | Equals the body artifact checksum |
| `content_bytes` | `Int64` | no | Body size |
| `retrieved_at` | `Datetime(us, UTC)` | no | Provider retrieval time |

Unique key: `(capture_id)`. Validation rejects URLs with userinfo or a query
parameter whose name contains `key`, `token`, or `secret`, so provider API keys
are never persisted. The capture key is a readable host slug plus the first 12
hex digits of `sha256(method, url, request_body)`; identity is the key plus
retrieval time, so repeated captures append.

`tool-summary` is for text an agent tool returned instead of raw bytes (the
trial's CoinMarketCap page). It is stored and hashed like any capture but is
labeled so audit and reports can show that the original page was not kept.

#### `data register-capture`

```text
finresearch --workspace PATH data register-capture CASE_ID FILE \
  --provider web|coingecko|hyperliquid|defillama \
  --kind api-response|web-page|document|tool-summary \
  --url URL --retrieved-at RFC3339_UTC [--method GET|POST] \
  [--request-body FILE] [--http-status N] [--media-type TYPE] \
  [--expect-sha256 HEX]
```

The CLI computes the SHA-256 itself. `--expect-sha256` fails the command when
the bytes differ from a hash the agent recorded at fetch time, which lets the
trial's `*.meta.json` sidecars be registered without trusting them. The
command prints the capture id and content hash. Absolute input paths never
enter the manifest. A rerun with the same bytes and metadata returns the
existing receipt; different bytes at the same identity are an integrity error.

#### Normalizing from captures

Provider ingest commands write a capture with `recorded_by = "cli"`, then
stop. Normalization parses a capture with a versioned parser selected by
`provider` and the URL path or POST `type`. The same parser therefore accepts
a CLI capture and an agent-registered capture of the same endpoint, so the
trial's existing `analysis/raw/*.json` files can be normalized offline after
registration. An unrecognized endpoint fails with the list of supported
endpoints.

This differs from yfinance and SEC, whose raw layer is flattened Parquet.
Those adapters stay as they are. New JSON providers use captures because their
nested responses would otherwise need one raw schema per endpoint, and because
the original bytes are what an auditor needs when a provider later changes a
definition.

#### Multi-id `source_evidence`

`assumptions.csv` `source_evidence` accepts a semicolon-separated list of
evidence ids with no spaces, no duplicates, and ascending order, for example
`e-cg;e-cmc;e-hlapi;e-tokenomist`. Each id must resolve, and the register
error for a missing id names the specific id. A single id stays valid.

`evidence.csv` `source_ref` may contain `capture:<capture_id>` tokens
alongside free text. `data registers status` and `case audit` report
`register_capture_reference_invalid` when a referenced capture is not
declared, and `register_capture_after_observed` when the capture's
`retrieved_at` date is after the evidence `observed_at`.

The trial case's `scenario_not_distinct` failure for `token_denominator` is
addressed by the model input instead: bases that are equal across scenarios
belong in `token-inputs.toml`, not in `scenarios.csv`.

## Must-have

### Token supply

#### `normalized.token-supply.v1`

One row per provider observation of one supply metric.

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `provider` | `String` | no | `coingecko`, `hyperliquid`, or an import provider |
| `instrument_id` | `String` | no | Case-declared token id |
| `provider_asset_id` | `String` | no | Provider's id for the token |
| `supply_metric` | `String` | no | Controlled, below |
| `amount` | `Float64` | no | Token count, finite and nonnegative |
| `unit` | `String` | no | Always `tokens` |
| `definition_id` | `String` | no | Provider field path, e.g. `coingecko.market_data.circulating_supply` |
| `holder_address` | `String` | yes | Address for address-level rows |
| `holder_label` | `String` | yes | Provider label when supplied |
| `observed_at` | `Datetime(us, UTC)` | no | Provider as-of time |
| `observed_at_basis` | `String` | no | `provider` or `retrieval` when the response has no timestamp |
| `retrieved_at` | `Datetime(us, UTC)` | no | Capture retrieval time |
| `capture_artifact_id` | `String` | yes | Originating capture; NULL only for CSV imports without one |
| `source_artifact_id` | `String` | no | Parent artifact |
| `normalized_at` | `Datetime(us, UTC)` | no | Deterministic: capture `retrieved_at`, or `--retrieved-at` for imports; never the wall clock |

Controlled `supply_metric`: `circulating`, `total`, `max`,
`non_circulating`, `staked`, `locked`, `burned`, `future_emissions`.

Unique key: `(provider, instrument_id, supply_metric, definition_id,
holder_address, observed_at, source_artifact_id)`. Sort key: `(instrument_id,
supply_metric, provider, observed_at, holder_address)`.

Semantic rules:

- `observed_at <= retrieved_at`; `observed_at_basis = "retrieval"` requires
  equality.
- Within one provider observation, `circulating <= total <= max` where the
  metrics are present; violations fail normalization with the three values.
- `holder_address` is set only for `non_circulating`, `staked`, `locked`, and
  `burned` rows; a summary row has NULL address.
- `capture_artifact_id`, when present, must be a declared `raw.capture.v1`.

#### `normalized.token-market.v1`

Provider market fields from the same responses. They are stored for
reconciliation and peer multiples; `model token` computes the target's own
market cap and FDV from price and reconciled supply.

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `provider` | `String` | no | Provider |
| `instrument_id` | `String` | no | Token id |
| `provider_asset_id` | `String` | no | Provider id |
| `metric` | `String` | no | `price`, `market_cap`, `fdv`, `volume_24h` |
| `value` | `Float64` | no | Finite, positive (`volume_24h` may be zero) |
| `quote_asset` | `String` | no | From `QUOTE_ASSETS` |
| `currency` | `String` | yes | Fiat code when `quote_asset` is fiat |
| `fdv_supply_basis` | `String` | yes | Required for `fdv`: `total` or `max` |
| `observed_at` | `Datetime(us, UTC)` | no | Provider time |
| `retrieved_at` | `Datetime(us, UTC)` | no | Capture time |
| `capture_artifact_id` | `String` | yes | Capture |
| `source_artifact_id` | `String` | no | Parent |
| `normalized_at` | `Datetime(us, UTC)` | no | Capture `retrieved_at` or import `--retrieved-at` |

Unique key: `(provider, instrument_id, metric, quote_asset, observed_at,
source_artifact_id)`. CoinGecko's FDV for HYPE is price times total supply,
not max, so its rows carry `fdv_supply_basis = "total"`; that is the parser's
declared reading of the provider definition, not an inference from numbers.

#### Free endpoints only

Provider adapters use only endpoints that answer without an API key, and the
CLI has no key option. Each provider gets a host-shared request lock like the
SEC adapter; CoinGecko's keyless tier is throttled, so its requests start at
least 6 seconds apart. Endpoint status, probed on 2026-11-07:

| Need | Free endpoint (used) | Paid (not used) |
| --- | --- | --- |
| Token supply, price, market cap, FDV | CoinGecko `/coins/{id}`, `/coins/markets`; Hyperliquid `tokenDetails` | |
| Price history | yfinance (existing adapter); CoinGecko `/coins/{id}/market_chart` and Hyperliquid `candleSnapshot` are free but not needed | |
| Fees, revenue, holders revenue | DefiLlama `/summary/fees/{slug}?dataType=...` | |
| Spot and outcomes volume | DefiLlama `/summary/dexs/{slug}` (children `Spot Orderbook`, `Outcomes`) | |
| Perp volume | Hyperliquid `metaAndAssetCtxs` `dayNtlVlm` (rolling 24h notional, snapshot only) | DefiLlama `/summary/derivatives/{slug}`, `/overview/derivatives` (HTTP 402) |
| Open interest history | DefiLlama `/summary/open-interest/{slug}` (daily, from 2024-12) | |
| Open interest and funding now | Hyperliquid `metaAndAssetCtxs`; funding history from `fundingHistory` | |
| Staking, vaults, treasury balances | Hyperliquid `validatorSummaries`, `vaultDetails`, `spotClearinghouseState` | |
| Unlock schedules | none | DefiLlama `/emissions`, `/emission/{slug}` (HTTP 402) |

Perp volume history therefore accumulates from repeated `metaAndAssetCtxs`
snapshots; there is no free daily series. Unlock schedules stay agent-owned
and enter through `token-unlocks.v1` imports.

#### CoinGecko ingest

```text
finresearch --workspace PATH data ingest-coingecko-token CASE_ID COIN_ID \
  [--vs-currency usd]
```

Fetches `/api/v3/coins/{COIN_ID}` with localization, tickers, community, and
developer data disabled, and writes one capture. HTTP errors surface verbatim
and write nothing.

```text
finresearch --workspace PATH data ingest-coingecko-markets CASE_ID \
  --ids COIN_ID[,COIN_ID...] [--vs-currency usd]
```

Fetches `/api/v3/coins/markets` for an explicit id list in one capture. This
is the peer-market source: the trial's `cg_markets_hype.json` has the same
shape.

#### Hyperliquid info ingest

```text
finresearch --workspace PATH data ingest-hyperliquid-info CASE_ID \
  --request token-details|validator-summaries|meta-and-asset-ctxs|spot-meta-and-asset-ctxs|vault-details|spot-clearinghouse-state|funding-history \
  [--token-id HEX] [--address HEX] [--coin NAME --start YYYY-MM-DD]
```

POSTs `{"type": ...}` to `https://api.hyperliquid.xyz/info` and writes one
capture. `token-details` requires `--token-id`; `vault-details` and
`spot-clearinghouse-state` require `--address`; `funding-history` requires
`--coin` and an explicit `--start` (UTC midnight) and pages until the
retrieval time, one capture per page. Other combinations are usage errors.

#### Normalization

```text
finresearch --workspace PATH data normalize-token-supply CASE_ID \
  --capture-artifact-id ID \
  (--instrument-id ID | --instrument-map PROVIDER_ASSET_ID=INSTRUMENT_ID ...)
```

`--instrument-id` is for single-asset captures. A multi-asset capture
requires one `--instrument-map` per asset to normalize; unmapped assets are
skipped and listed, and a map entry absent from the capture fails.

| Capture | Output rows |
| --- | --- |
| CoinGecko `/coins/{id}` | supply: `circulating`, `total`, `max`, with `observed_at = market_data.last_updated`; market: `price`, `market_cap`, `fdv`, `volume_24h` for the requested quote |
| CoinGecko `/coins/markets` | Per mapped coin: the same supply and market rows, with `observed_at = last_updated` |
| Hyperliquid `tokenDetails` | supply: `circulating`, `total`, `max`, `future_emissions`, plus one `non_circulating` row per `nonCirculatingUserBalances` entry; `observed_at_basis = "retrieval"` |
| Hyperliquid `validatorSummaries` | supply: one `staked` summary row equal to the sum of validator stake; per-validator rows stay in the capture |

For HYPE, the `tokenDetails` capture yields `non_circulating` rows of
238,104,720 at `0x43e9...` and 48,742,089 at `0xfefe...`. Whether the latter
is burned or held is an agent judgment that enters through the model input,
not through the supply table.

Supply rows from web pages, such as CoinMarketCap, enter through
`data import-csv --schema token-supply.v1` with `capture_artifact_id` set to
the registered `tool-summary` or `web-page` capture.

### Protocol metrics

#### `normalized.protocol-metrics.v1`

One row per provider value of one metric for one period.

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `provider` | `String` | no | `defillama`, `hyperliquid`, or import provider |
| `entity_id` | `String` | no | Protocol entity, e.g. `hyperliquid` |
| `instrument_id` | `String` | yes | Value-receiving token when declared |
| `provider_protocol_id` | `String` | no | e.g. DefiLlama slug `hyperliquid` |
| `sub_protocol` | `String` | yes | Child or market, e.g. `Hyperliquid Perps`, or perp market `HYPE`; NULL for the provider aggregate |
| `metric` | `String` | no | Controlled, below |
| `period_type` | `String` | no | `duration` or `instant` |
| `period_start` | `Date` | yes | UTC date; required for `duration` |
| `period_end` | `Date` | no | UTC date, inclusive |
| `value` | `Float64` | no | Finite |
| `unit` | `String` | no | Fiat code, `tokens`, or `bps` |
| `currency` | `String` | yes | Fiat code for currency units |
| `is_complete` | `Boolean` | no | False for a period that ends on or after the capture's UTC retrieval date |
| `provider_definition_id` | `String` | no | Key into the definitions table |
| `observed_at` | `Datetime(us, UTC)` | no | Provider time, or retrieval for `instant` snapshots |
| `capture_artifact_id` | `String` | yes | Capture |
| `source_artifact_id` | `String` | no | Parent |
| `normalized_at` | `Datetime(us, UTC)` | no | Capture `retrieved_at` or import `--retrieved-at` |

Controlled `metric` and allowed units:

| Metric | Unit | Period |
| --- | --- | --- |
| `volume_perp`, `volume_spot` | fiat | duration |
| `fees`, `revenue`, `holders_revenue`, `supply_side_revenue` | fiat | duration |
| `buybacks`, `burns`, `distributions`, `incentives` | fiat or `tokens` | duration |
| `open_interest` | fiat (notional) | instant |
| `funding_rate_1h` | `bps` | instant |

Unique key: `(provider, provider_protocol_id, sub_protocol, metric,
period_start, period_end, observed_at, source_artifact_id)`.

Semantic rules: volume, fees, and open interest are nonnegative; revenue
metrics may be negative; `period_start <= period_end`; `instant` rows have
NULL `period_start`; units follow the table above. Parent and child rows are
never summed: `sub_protocol IS NULL` is the provider aggregate, and consumers
select one level explicitly. Hyperliquid open interest is stored as notional,
`openInterest * markPx` from the same response, and its
`provider_definition_id` records that derivation.

#### `normalized.provider-metric-definitions.v1`

Written with every protocol-metrics artifact so the provider's definition at
retrieval time travels with the numbers.

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `provider` | `String` | no | Provider |
| `provider_definition_id` | `String` | no | `<provider>.<protocol>.<field>.<text-hash-12>` |
| `metric` | `String` | no | Canonical metric |
| `provider_field` | `String` | no | e.g. `dailyHoldersRevenue` |
| `methodology_text` | `String` | yes | Provider methodology text for that field |
| `methodology_url` | `String` | yes | Provider `methodologyURL` |
| `definition_sha256` | `String` | no | Hash of canonical field, text, and URL |
| `observed_at` | `Datetime(us, UTC)` | no | Capture retrieval time |
| `source_artifact_id` | `String` | no | Parent capture |

Unique key: `(provider_definition_id)`. Every `provider_definition_id` in a
protocol-metrics artifact must resolve to a row in a declared definitions
artifact. Because the id embeds the text hash, a changed methodology produces
a new id, and audit can show when two periods were measured under different
definitions.

#### DefiLlama ingest

```text
finresearch --workspace PATH data ingest-defillama CASE_ID SLUG \
  --dataset fees|revenue|holders-revenue|dex-volume|open-interest \
  [--dataset ...]
```

Each dataset is one capture of
`/summary/fees/{SLUG}?dataType=dailyFees|dailyRevenue|dailyHoldersRevenue`,
`/summary/dexs/{SLUG}?dataType=dailyVolume`, or
`/summary/open-interest/{SLUG}`. Datasets are fetched in the given order; a
failure stops the command and keeps captures already completed, each of which
is independently valid. Provider errors are printed verbatim, for example the
trial's HTTP 400 `Fees for aster not found...`.

```text
finresearch --workspace PATH data normalize-protocol-metrics CASE_ID \
  --capture-artifact-id ID [--capture-artifact-id ID ...] \
  --entity-id ID [--instrument-id ID]
```

Parses `totalDataChart` into aggregate daily rows and
`totalDataChartBreakdown` into `sub_protocol` rows. A DefiLlama day is the UTC
date of the bucket timestamp, with `period_start = period_end`; DefiLlama
open-interest points become `instant` rows observed at the bucket timestamp.
Hyperliquid `metaAndAssetCtxs` captures produce per perp market
`open_interest`, `funding_rate_1h` (`funding` times 10,000), and a
`volume_perp` duration row for the 24 hours ending at retrieval from
`dayNtlVlm`, with UTC dates of the window start and end. Several captures for one entity produce
one artifact pair (metrics plus definitions) with the captures as ordered
parents.

For HYPE, the fees, revenue, and holders-revenue captures produce daily
aggregate rows plus children `Hyperliquid Perps`, `Hyperliquid Spot
Orderbook`, `Hyperliquid HLP`, and `Hyperliquid Outcomes`. The 365 complete
days to 2026-11-06 sum to 897.9M fees and 682.8M holders revenue, matching the
trial model.

### Cross-source reconciliation

```text
finresearch --workspace PATH data reconcile-token-supply CASE_ID \
  --instrument-id ID --as-of YYYY-MM-DD \
  --policy analysis/token-supply-policy.toml \
  [--source-artifact-id ID ...]
```

The policy is a strict, case-relative TOML whose bytes are hashed into the
output identity:

```toml
version = 1
provider_priority = ["hyperliquid", "coingecko", "coinmarketcap"]

[tolerance]           # relative spread (max - min) / min per metric
circulating = 0.02
total = 0.01
max = 0.0
staked = 0.02

[resolution.circulating]
provider = "coingecko"
definition_id = "coingecko.market_data.circulating_supply"
source_id = "a-circ-definition"

[resolution.total]
provider = "coingecko"
definition_id = "coingecko.market_data.total_supply"
source_id = "a-total-definition"

[resolution.max]
provider = "coingecko"
definition_id = "coingecko.market_data.max_supply"
source_id = "a-max-definition"
```

For each `supply_metric`, the command takes each provider's latest
observation with `observed_at` on or before the cutoff (address-level rows
excluded), computes `spread = (max - min) / min`, and assigns a status:

| Status | Rule | Selected row |
| --- | --- | --- |
| `single-source` | One provider | That row |
| `agree` | `spread <= tolerance` | First provider in `provider_priority` |
| `resolved` | `spread > tolerance` and a `[resolution.<metric>]` exists | The named provider and definition |
| `conflict` | `spread > tolerance` and no resolution | None |

A metric observed by two or more providers without a tolerance entry, a
resolution naming an absent
observation, or a `source_id` that does not resolve to evidence or assumption
dated on or before the cutoff fails the command. Equal-time conflicting
observations from one provider fail as in `reconcile-instrument-master`.

#### `derived.token-supply-reconciliation.v1`

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `instrument_id` | `String` | no | Token |
| `as_of` | `Date` | no | Cutoff |
| `supply_metric` | `String` | no | Metric |
| `provider` | `String` | no | Observation provider |
| `definition_id` | `String` | no | Provider definition |
| `observed_at` | `Datetime(us, UTC)` | no | Observation time |
| `amount` | `Float64` | no | Tokens |
| `min_amount`, `max_amount` | `Float64` | no | Across providers for the metric |
| `spread` | `Float64` | no | `(max - min) / min` |
| `tolerance` | `Float64` | no | From policy |
| `status` | `String` | no | `single-source`, `agree`, `resolved`, `conflict` |
| `selected` | `Boolean` | no | At most one true per metric; none when `conflict` |
| `resolution_source_id` | `String` | yes | Required when `resolved` |
| `source_artifact_id` | `String` | no | Observed supply artifact |

Unique key: `(instrument_id, as_of, supply_metric, provider, definition_id)`.

HYPE with the policy above: circulating spread is
`(301.8M - 239.4M) / 239.4M = 26.1%`, so it would be a `conflict` without a
resolution and is `resolved` to CoinGecko with one. Total spread is 5.1%
across the three sources (HL's total includes the Assistance Fund balance),
also resolved. `max` conflicts once the CoinMarketCap import is included
(950.0M, equal to its total, against 1.0B from CoinGecko and Hyperliquid)
and is resolved to CoinGecko. `future_emissions` has one provider and is
`single-source`.

#### Audit coverage

`case audit` adds, for the latest reconciliation per instrument with `as_of`
on or before the audit cutoff:

- `token_supply_conflict` for every metric with status `conflict`;
- `token_supply_after_as_of` and `token_market_after_as_of` for observations
  dated after the cutoff;
- `protocol_metric_after_as_of` for a `period_end` or `observed_at` after the
  cutoff; and
- `capture_metadata_mismatch` when a capture's `content_sha256` differs from
  its body artifact checksum.

A conflict that no model uses is still reported. Audit has no warning level,
so an analyst who accepts a conflict records a resolution rather than
ignoring the issue.

### `model token`

```text
finresearch --workspace PATH model token CASE_ID \
  --input analysis/token-inputs.toml \
  --scenario bear|base|bull|all [--sensitivity RATES;GROWTHS]
```

Like `model dcf`, the command accepts no economic values on the command line.
Its input is strict TOML; every economic leaf is
`{ value, unit, source_id }` resolved from `registers/evidence.csv` or
`registers/assumptions.csv`, dated on or before `as_of`, or a reference to a
declared artifact that the CLI reads.

#### Input `analysis/token-inputs.toml`

```toml
version = 1
as_of = "2026-11-07"
instrument_id = "hype.hyperliquid.l1"
currency = "USD"
value_unit = "USDm"
token_unit = "tokens_m"
discount_convention = "year_end"     # or mid_year
held_buyback_treatment = "exclude"   # or include
buyback_price_basis = "as_of_price"  # only value in v1

[price]
artifact_id = "normalized.daily-prices.yfinance.hype32196-usd.869ed51a..."
session_date = "2026-11-06"

[supply]
reconciliation_artifact_id = "derived.token-supply-reconciliation...."
fully_diluted_metric = "total"       # or max

[anchors]                            # optional; reported, not used in values
protocol_metrics_artifact_id = "normalized.protocol-metrics...."
metric = "holders_revenue"

[scenario.base]
discount_rate = { value = 0.25, unit = "ratio", source_id = "a-r-base" }
capture_mix = { buyback_retire = 0.0, buyback_hold = 1.0, burn = 0.0, distribution = 0.0, source_id = "a-capture-mix" }
terminal = { terminal_growth = { value = 0.03, unit = "ratio", source_id = "a-tg-base" } }
forecast = [
  { period_end = "2027-11-07",
    holders_revenue = { value = 682.8, unit = "USDm", source_id = "a-hr-2027-base" },
    capture_ratio = { value = 0.90, unit = "ratio", source_id = "a-capture-base" },
    unlocks = { value = 119.0, unit = "tokens_m", source_id = "a-unlocks-2027" },  # 12 x 9.92M
    unlock_realization = { value = 0.20, unit = "ratio", source_id = "a-team-sell-base" },
    emissions = { value = 10.0, unit = "tokens_m", source_id = "a-emissions" } },
  # one entry per projection period
]

[scenario.base.multiple]
statistic = "median"                 # min, p25, median, p75, max

[multiples]
metric = "holders_revenue"
window = "annualized_30d"            # or ttm
metric_window_end = "2026-11-06"     # last complete UTC day
market_observed_on = "2026-11-07"    # UTC date of peer market observations
market_artifact_ids = ["normalized.token-market...."]
metric_artifact_ids = ["normalized.protocol-metrics...."]
peers = [
  { instrument_id = "lit.lighter", entity_id = "lighter" },
  { instrument_id = "dydx.dydx-chain", entity_id = "dydx" },
  { instrument_id = "aster.aster", entity_id = "aster" },
]
exclude = [{ instrument_id = "gmx.arbitrum", reason_source_id = "a-gmx-outlier" }]
# Repeat complete, distinct scenario tables for bear and bull.
```

A scenario may override the FD denominator with any non-`conflict`
observation present in the reconciliation artifact, not only the selected
row:
`[scenario.bear] fully_diluted = { metric = "total", provider = "hyperliquid", source_id = "a-bear-gross-supply" }`.
That is how the trial's bear case divides by Hyperliquid's gross 998.8M while
base and bull use the selected CoinGecko 951.5M. `unlocks` may instead come from a
`token-unlocks.v1` artifact once that contract exists:
`[supply] unlocks_artifact_id = "...", schedule_id = "tokenomist-2026-11-07"`;
the CLI then sums scheduled events per projection period, and inline
`unlocks` leaves are rejected.

#### Calculation

For each scenario and period `t` (actual/365 from `as_of`, as in the DCF):

```text
tokenholder_cf_t   = holders_revenue_t * capture_ratio_t
buyback_tokens_t   = tokenholder_cf_t * (buyback_retire + buyback_hold) / price_0
retired_tokens_t   = tokenholder_cf_t * (buyback_retire + burn) / price_0
                     + (held_buyback_treatment == "exclude") * tokenholder_cf_t * buyback_hold / price_0
circulating_t      = circulating_{t-1} + unlocks_t * unlock_realization_t + emissions_t - retired_tokens_t
fully_diluted_t    = fully_diluted_{t-1} - retired_tokens_t
aggregate_value    = sum(tokenholder_cf_t * DF_t) + TV * DF_T
TV                 = tokenholder_cf_T * (1 + g) / (r - g)
per_token_circ     = aggregate_value / circulating_0
per_token_fd       = aggregate_value / fully_diluted_0
implied_cf_scale   = price_0 * fully_diluted_0 / aggregate_value
multiple_value_b   = multiple_b(statistic) * holders_revenue_1   # pre-capture
per_token_mult_b   = multiple_value_b / denominator_b_0
```

Scale conversions follow the DCF: `USDm / tokens_m` is reported as
`USD/token`. `price_0` comes from the declared price artifact and session;
`circulating_0` and `fully_diluted_0` are the `selected` rows of the
reconciliation artifact. Peer multiples are `market_cap / HR` for the
circulating basis and `fdv / HR` for the FD basis, with HR summed over the
last 30 complete UTC days times 365/30, or 365 complete days for `ttm`,
from the aggregate (`sub_protocol IS NULL`) rows.

The denominator is opening supply; the supply path is reported separately as
dilution and absorption. This matches the trial memo and keeps the two effects
visible instead of folding issuance into value.

#### Validation

- Same leaf rules as the DCF: finite values, controlled units, resolvable
  sources dated on or before `as_of`, strictly increasing periods, complete
  and distinct bear/base/bull tables.
- `-1 < g < r`; `capture_ratio` and `unlock_realization` in `[0, 1]`;
  `capture_mix` fractions nonnegative and summing to 1 within `1e-9`;
  `unlocks`, `emissions`, and `holders_revenue` nonnegative.
- The price artifact must be `normalized.daily-prices.v2` for an instrument
  that `instrument-master.v3` maps to `instrument_id`, with the session on or
  before `as_of`. A non-fiat price requires a peg leaf.
- The reconciliation artifact must match `instrument_id`, have `as_of` on or
  before the model `as_of`, and contain a selected row for `circulating` and
  the FD metric; a `conflict` on either aborts the run.
- `circulating_t <= fully_diluted_t` and `circulating_t >= 0` for every
  period; `retired_tokens_t <= circulating_{t-1}`.
- Each declared peer must have exactly one latest market observation whose
  `observed_at` falls on the UTC date `market_observed_on` (on or before
  `as_of`), and a complete HR window ending on `metric_window_end`, which must
  precede `market_observed_on`; nonpositive HR becomes an explicit
  exclusion. At least one valid peer per basis is required before publication.

#### Outputs

A complete run publishes, in this order and with these parent edges:

| Contract | Rows | Key fields |
| --- | --- | --- |
| `model.token-inputs.v1` | One per leaf and resolved artifact value | `run_id, scenario, field, period_end, value, unit, source_id, source_artifact_id` |
| `model.token-supply-path.v1` | One per scenario and period | `opening_circulating, unlocks_realized, emissions, buyback_tokens, retired_tokens, closing_circulating, closing_fully_diluted, absorption_ratio, token_unit` |
| `model.token-cashflows.v1` | One per scenario and period | `holders_revenue, capture_ratio, tokenholder_cash_flow, buyback_cash, burn_cash, distribution_cash, year_fraction, discount_factor, present_value, currency, unit` |
| `model.token-peer-multiples.v1` | One per peer and basis | `peer_instrument_id, basis, numerator, denominator, multiple, status (included/excluded), exclusion_reason` |
| `model.token-results.v1` | One per scenario, method (`dcf`, `multiple`), basis (`circulating`, `fully_diluted`) | `aggregate_value, denominator_tokens, per_token_value, price, upside, terminal_share, multiple_applied, implied_cf_scale, currency, value_unit, token_unit, per_token_unit` |
| `model.token-sensitivity.v1` | Optional grid | `scenario, discount_rate, terminal_growth, per_token_value_fd` |
| `model.token-reconciliation.v1` | Checks | Same fields as `model.dcf-reconciliation.v1` |

Reconciliation checks: supply roll-forward closes each period; PV sum plus
terminal PV equals aggregate value; `per_token_value * denominator` equals
aggregate value; peer summary statistics reproduce from included peers;
on `dcf` / `fully_diluted` rows, `implied_cf_scale * aggregate_value` equals
`price_0 * fully_diluted_0`. `implied_cf_scale` is NULL on every other row. A
failed check prevents publication.

Run identity covers canonical TOML bytes, selected scenarios and grid,
producer version, cutoff, referenced register bytes, and the checksums of
every referenced artifact. `report markdown` and `report html` render token
runs from the same typed context as DCF runs, adding the supply path and peer
table; audit authenticates token runs by full reconstruction, exactly like
DCF and comps runs, with issue codes prefixed `token_run_`.

For HYPE, the base scenario reproduces the trial's 3.43 USD/token on the FD
basis and 13.63 on the circulating basis, and the reverse figure
`implied_cf_scale` is the trial's "scale vs TTM" at 25%.

## Should-have

### `token-unlocks.v1` import

Unlock schedules have no free authoritative API, and the trial's sources
conflicted (Tokenomist 9.92M nominal per month; announced claims 0.14M to
1.75M). Schedules therefore enter through import, one immutable artifact per
schedule version.

```text
finresearch --workspace PATH data import-csv CASE_ID FILE \
  --schema token-unlocks.v1 --provider tokenomist --retrieved-at RFC3339_UTC
```

`normalized.token-unlocks.v1`:

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `provider` | `String` | no | Import provider |
| `instrument_id` | `String` | no | Token |
| `schedule_id` | `String` | no | Agent-declared version, e.g. `tokenomist-2026-11-07` |
| `event_id` | `String` | no | Unique within schedule |
| `category` | `String` | no | `team`, `investors`, `foundation`, `community`, `ecosystem`, `airdrop`, `treasury`, `emissions`, `other` |
| `recipient_label` | `String` | yes | e.g. `Core contributors` |
| `holder_address` | `String` | yes | e.g. `0x43e9...` |
| `unlock_type` | `String` | no | `cliff`, `linear`, `emission` |
| `unlock_date` | `Date` | no | Cliff date or stream start |
| `unlock_end_date` | `Date` | yes | Required for `linear` |
| `amount` | `Float64` | no | Tokens, positive |
| `status` | `String` | no | `scheduled`, `unlocked`, `claimed`, `cancelled` |
| `observed_at` | `Datetime(us, UTC)` | no | When the source stated it |
| `source_ref` | `String` | no | URL or `capture:<id>` |
| `capture_artifact_id` | `String` | yes | Registered capture of the source |
| `source_artifact_id` | `String` | no | Import raw source |
| `normalized_at` | `Datetime(us, UTC)` | no | Import `--retrieved-at` |

Unique key: `(instrument_id, schedule_id, event_id)`. Rules: `unlock_end_date
>= unlock_date`; `linear` requires an end date and `cliff` forbids one;
`source_ref` is non-empty; one artifact holds one `schedule_id`. Linear
streams are prorated by day when summed into model periods. Audit reports
`token_unlocks_exceed_supply` when the scheduled sum of `scheduled` events
exceeds the selected `max` minus `circulating` in the latest reconciliation,
and `token_unlocks_capture_missing` for an unresolved `capture:` reference.

### `normalized.instrument-master.v3`

v2 fields plus:

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `chain` | `String` | yes | Lowercase slug, e.g. `hyperliquid-l1`, `ethereum`, `solana` |
| `contract_address` | `String` | yes | Token id or contract, e.g. `0x0d01dc56dcaaca66ad901c959b4011ec` |
| `token_standard` | `String` | yes | `native`, `erc20`, `spl`, `hip1`, `other` |
| `provider_ids` | `String` | yes | Sorted `provider:id` pairs joined by `;`, e.g. `coingecko:hyperliquid;defillama:hyperliquid;yfinance:HYPE32196-USD` |
| `asset_class_source` | `String` | no | `provider` or `import` |

Rules: `chain`, `contract_address`, and `token_standard` are only allowed
when `asset_class = "crypto"`; `provider_ids` has unique providers and no
empty ids; `contract_address` requires `chain`. The flat string keeps CSV
import exact. `derived.instrument-master-current.v2` carries the same fields,
and `reconcile-instrument-master` reads v2 and v3 observations, treating
missing v3 fields as NULL.

Provider-set asset class: `raw.yfinance.daily-prices.v2` adds a non-null
`quote_type` captured from yfinance at retrieval, like `currency` today.
Normalization maps `CRYPTOCURRENCY` to `crypto`, `EQUITY` to `equity`, `ETF`
to `etf`, `CURRENCY` to `fx`, `INDEX` to `index`, and anything else to
`unknown`, with `asset_class_source = "provider"`. A v1 raw snapshot still
normalizes to `unknown`. CoinGecko and Hyperliquid normalizations do not
write master rows; the token master is an import because its ids are case
declarations.

### Null provider bars in `normalize-daily-prices`

```text
finresearch --workspace PATH data normalize-daily-prices CASE_ID SYMBOL \
  [--raw-artifact-id ID] [--null-bars fail|drop]
```

`fail` is the default and keeps today's behavior. `drop` removes bars whose
open, high, low, or close is NULL and publishes a companion
`normalized.price-gaps.v1`:

| Field | Type | Null | Meaning |
| --- | --- | --- | --- |
| `schema_version` | `UInt16` | no | `1` |
| `instrument_id` | `String` | no | Instrument |
| `session_date` | `Date` | no | Dropped session |
| `reason` | `String` | no | `null_ohlc` |
| `null_fields` | `String` | no | Sorted comma list, e.g. `close,high,low,open` |
| `source_artifact_id` | `String` | no | Raw snapshot |

Unique key `(instrument_id, session_date, source_artifact_id)`; may be empty
(`allow_empty`). The policy is a producer parameter, so `drop` output has a
distinct identity from `fail` output. Bars with present but inconsistent OHLC
still fail; `drop` never hides an ordering violation. For HYPE, `--null-bars
drop` on the 708-row snapshot would publish 707 bars and one gap row for
2026-02-02, the null bar the trial found, replacing its second ingest.

### Per-asset-class price-age policy

```text
finresearch --workspace PATH case audit CASE_ID --as-of YYYY-MM-DD \
  --max-price-age-days N [--max-price-age CLASS=N ...] [--verify-hashes]
```

`--max-price-age CLASS=N` overrides the global limit for one asset class
(`crypto=0`, `equity=3`). Asset class resolves from every master observation
on or before the cutoff linked to the price: the price's own `instrument_id`
(v2 or v3) and any v3 row whose `provider_ids` contains
`yfinance:<provider_symbol>`. `unknown` values are ignored; one remaining
class applies; two different remaining classes are an
`asset_class_conflict` issue; none uses the global limit. For HYPE, the
yfinance master's `unknown` is ignored and the imported v3 row supplies
`crypto`.

24/7 classes (`crypto`, plus `fx` when its provider bars are 24/7, which
yfinance's are not) age in complete UTC daily bars. The bar stamped
`session_date` 00:00 UTC is complete at the end of that UTC day, so at cutoff
date `D` the latest complete bar is `D - 1`:

```text
age_crypto = (D - 1) - latest_session_date   # in days
```

A crypto session equal to `D` is incomplete and is reported as
`price_bar_incomplete` rather than treated as fresh. Complete-bar age is the
default for every price whose class resolves to `crypto`; the flags only
change limits. Other classes keep calendar-day age, so an equity-only case
audits exactly as today. For HYPE at `--as-of 2026-11-07
--max-price-age crypto=0`, the 2026-11-06 bar has age 0 and passes; today the
same cutoff needs `--max-price-age-days 1`.

### FX normalization and conversion

Equity cases priced in one currency and reporting in another could not
normalize `CNYHKD=X` and `EURUSD=X`:
yfinance FX rows have NULL OHLC on holidays and low values above open or
close. FX does not need OHLC.

```text
finresearch --workspace PATH data normalize-fx-rates CASE_ID SYMBOL \
  --base EUR --quote USD [--raw-artifact-id ID]
```

Reads one raw yfinance snapshot, checks that `SYMBOL` is
`<BASE><QUOTE>=X` with the declared codes, and writes the existing
`normalized.fx-rates.v1` from `close` only, with `rate_kind = "close"`. Rows
with NULL close are dropped and recorded in `normalized.price-gaps.v1`; OHLC
ordering is not checked because no OHLC field is published. A nonpositive
close fails.

Conversion is a model step, not a data rewrite. `dcf-inputs.toml` version 2
accepts an optional translation:

```toml
[translation]
currency = "HKD"
fx = { artifact_id = "normalized.fx-rates....", base = "CNY", quote = "HKD", rate_date = "2026-11-06" }
```

The run adds `model.dcf-translation.v1` (`run_id, scenario, per_share_value,
currency, fx_rate, rate_date, translated_per_share_value,
translated_currency`) and a reconciliation check. The rate date must be on or
before `as_of`; audit authenticates the FX artifact like any model input.
`model comps` keeps its rule of one common currency.

### Scenario-specific share counts and issuance in the DCF

`dcf-inputs.toml` version 2, accepted alongside version 1:

```toml
[scenario.base.capitalization]
diluted_shares = { value = 494.2, unit = "shares_m", source_id = "e-shares" }

[[scenario.base.issuance]]
period_end = "2027-12-31"
shares = { value = 40.0, unit = "shares_m", source_id = "a-base-atm-2027" }
proceeds = { value = 1200, unit = "USDm", source_id = "a-base-atm-2027-proceeds" }
```

A scenario `diluted_shares` replaces the global leaf for that scenario.
Issuance proceeds are discounted with the same factors as cash flows and
added to equity value; per-share value divides by opening diluted shares plus
cumulative issued shares. `model.dcf-results.v2` adds
`opening_diluted_shares`, `issued_shares`, `ending_diluted_shares`, and
`issuance_pv`; a new reconciliation check verifies the share roll-forward.
The unit-count roll-forward is the same code path as the token supply path.
This removes the need in heavily dilutive equity cases to fold dilution into
cash flows.

## Nice-to-have

- `case init` creates empty `registers/` and `analysis/` directories. It still
  creates no files. The case contract text changes; existing cases are
  unaffected because both directories are optional.
- An unlock-tracker ingest is dropped: the DefiLlama emissions endpoints
  return HTTP 402 and no free structured source was found. Unlocks stay
  agent-owned imports.

## What stays agent-owned

| Judgment | How it enters the case |
| --- | --- |
| Which supply definition is authoritative; whether Assistance Fund HYPE is burned or held | Evidence and assumption rows; `[resolution.*]` with `source_id` in the reconcile policy; `held_buyback_treatment` and `fully_diluted_metric` in `token-inputs.toml` |
| Which unlock schedule to use; realized versus nominal unlocks | `token-unlocks.v1` import with `schedule_id`, `source_ref`, and a capture; `unlock_realization` leaves |
| Web pages, docs, governance posts, news | `data register-capture` (`web-page`, `document`, or `tool-summary`); evidence rows with `capture:<id>` |
| Peer selection and exclusions | `[multiples] peers` and `exclude` with `reason_source_id` |
| Discount rates, growth, capture ratio, scenario probabilities | Assumption ids on leaves |
| Wash volume, incentives, HLP incidents, validator concentration, regulation | Evidence, open questions, and the memo; numeric inputs only when they become leaves |
| Token identity (chain, contract, provider ids) | `instrument-master.v3` import |

Rules for agents: record the fetch time at retrieval and pass it as
`--retrieved-at`; register the bytes before deriving numbers from them; never
hand-edit CLI-owned files; and keep scripts in `analysis/` only for work the
CLI does not cover, as the trial's `hype_model.py` did.

## Migration and compatibility

| Surface | Change | Compatibility |
| --- | --- | --- |
| Manifest v2 | New artifact kinds only | Compatible; v1 cases still need `case migrate` before token commands |
| New contracts (`raw.capture.v1`, token supply, market, protocol metrics, definitions, reconciliation, unlocks, price gaps, token model tables, `model.dcf-translation.v1`, `model.dcf-results.v2`) | Additive | Compatible |
| `FACT_UNITS`, `UNIT_KINDS`, `FACT_CATEGORIES` | Widened in place | Existing artifacts stay valid; older CLIs reject new rows |
| `normalized.instrument-master` | v3 added | v2 stays readable and importable |
| `raw.yfinance.daily-prices` | v2 adds `quote_type` | v1 snapshots stay valid and normalize to `unknown` |
| `derived.instrument-master-current` | v2 added | v1 stays readable |
| `dcf-inputs.toml` | Version 2 | Version 1 inputs and runs authenticate unchanged |
| `assumptions.csv` `source_evidence` | Accepts `;` lists | Single ids unchanged |
| `normalize-daily-prices` | `--null-bars`, default `fail` | Unchanged default |
| `case audit` | New issue codes; `--max-price-age CLASS=N`; complete-UTC-bar age for `crypto` by default | Equity-only cases get the same result. Crypto ages shrink by one day, and a crypto session dated on the cutoff is a new `price_bar_incomplete` issue; release notes name this behavior change |
| `case init` | Creates two empty directories | Compatible |

The release is a minor bump under the pre-1.0 policy because nothing is
removed. Release notes list the new commands, contracts, and issue codes, and
note that older CLIs cannot validate artifacts that use the widened units.

The HYPE trial case is not modified by this work. A new case, or a successor
`hype-<date>` case, would:

1. register each `analysis/raw/*.json` with `data register-capture
   --expect-sha256` using its sidecar's URL, method, body, status, and
   `retrieved_at_utc`;
2. run `normalize-token-supply` and `normalize-protocol-metrics` on those
   captures and import the CoinMarketCap `tool-summary` values as
   `token-supply.v1`;
3. import an `instrument-master.v3` row with chain `hyperliquid-l1`, the
   contract address, and provider ids;
4. normalize the original 708-row price snapshot with `--null-bars drop`;
5. reconcile supply with a policy and move `hype_model.py` parameters into
   `token-inputs.toml`; and
6. run `model token --scenario all`, report, and audit with
   `--max-price-age crypto=0`.

The equity cases need no migration: their v1 DCF inputs keep working, and
they can adopt version 2 and `normalize-fx-rates` in a new run.

## Agent usage skill

Each phase updates the repository-root `SKILL.md` in the same change: the new
commands, contracts, issue codes, `token-inputs.toml`, the capture-first
evidence rule, supply reconciliation with policy resolutions, the
`held_buyback_treatment` and fully diluted denominator choices, and the
complete-UTC-bar price cutoff for crypto.

## Implementation plan

Each phase is one pull request with tests and doc moves into the normative
docs. Later phases depend on earlier ones only where stated.

| Phase | Content | Depends on |
| --- | --- | --- |
| 1. Foundations | `raw.capture.v1`, `register-capture`, unit and category additions, `QUOTE_ASSETS`, multi-id `source_evidence`, `capture:` references | none |
| 2. Supply | Token supply and market contracts, CoinGecko and Hyperliquid info ingests, `normalize-token-supply`, `reconcile-token-supply`, audit codes | 1 |
| 3. Protocol metrics | Protocol metrics and definitions contracts, DefiLlama ingest, Hyperliquid `metaAndAssetCtxs` parsing, `normalize-protocol-metrics` | 1 |
| 4. Token model | `model token`, output contracts, run authentication, reports, `token_run_*` audit codes; offline e2e test from HYPE fixture captures | 2, 3, 5 |
| 5. Identity and prices | `instrument-master.v3`, yfinance raw v2 `quote_type`, `--null-bars`, price gaps, `--max-price-age` | 1 |
| 6. Unlocks | `token-unlocks.v1` import and model integration | 4 |
| 7. Equity fixes | `normalize-fx-rates`, DCF input version 2 with translation and issuance | 5 for price gaps |
| 8. Nice-to-have | `case init` directories | none |

Phases 2, 3, and 5 can proceed in parallel after phase 1. Tests use recorded
captures under `tests/fixtures/` trimmed from the HYPE case; no test contacts
a provider.

## Decisions

Decided in review on 2026-11-07:

1. **Fact units and categories** are widened in place in
   `fundamental-facts.v2`; no v3.
2. **Free endpoints only.** No API-key option; the endpoint table under
   [Free endpoints only](#free-endpoints-only) lists what is used. Perp volume
   history and unlock schedules have no free structured source.
3. **Captures stay inside the case** under `data/raw/captures/`. A full HYPE
   capture set is about 5 MB, small next to the reproducibility it buys.
   Very large aggregate endpoints (DefiLlama `/overview/fees` is about 30 MB)
   are not ingested; adapters fetch per-protocol summaries.
4. **`provider_ids`** stays one sorted `provider:id;...` string so the
   instrument master remains one flat, strictly importable row per
   observation.
5. **Crypto price age defaults to complete UTC bars** in the same minor
   release as the per-class flags. Only crypto results change, and the
   calendar-day rule is wrong for 24/7 bars: mixed cases would otherwise need
   a limit loose enough for equity weekends, and crypto-only cases could
   never use 0.
6. **Peers are ingest-only in phase 4.** Peer multiples come only from
   `ingest-coingecko-markets` and DefiLlama fee captures, so every multiple
   reproduces from captured bytes. A peer without a free holders-revenue
   series, such as the exchange tokens BNB, OKB, and BGB, is excluded. An
   import schema `model.token-peer-observations.v1`, with rows that must cite
   a capture, is added only when a needed peer lacks API data.

No decisions remain open.
