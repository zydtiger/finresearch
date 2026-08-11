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
- Every snapshot records provider, source symbol, retrieval time, requested
  period, interval, and an independent dataset schema version.
- The manifest records artifact ID, kind, schema version, relative path,
  provider, retrieval time, row count, and SHA-256 checksum.
- A failed Parquet write or manifest update removes only the new partial output.
- Provider adapters may use pandas at their external boundary; internal
  contracts and storage use Polars.

Raw yfinance prices are stored below:

```text
data/raw/yfinance/daily-prices/<symbol-key>/<retrieved-at>.parquet
```

The retrieval timestamp makes the path append-only. Provider symbols that are
not portable path components receive a readable slug plus a short hash.

## Raw yfinance daily-prices v1

Contract identifier: `raw.yfinance.daily-prices.v1`

| Field | Polars dtype | Meaning |
| --- | --- | --- |
| `schema_version` | `UInt16` | Independent dataset contract version |
| `provider` | `String` | Always `yfinance` |
| `provider_symbol` | `String` | Symbol sent to yfinance |
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

The metadata and timestamp fields are non-null. Numeric provider values remain
nullable so raw storage can preserve an incomplete response for later quality
assessment. `(provider_symbol, interval, timestamp)` is unique within one
snapshot.

## Command

```text
finresearch --workspace PATH data ingest-yfinance-prices \
  CASE_ID SYMBOL --start YYYY-MM-DD --end YYYY-MM-DD
```

The command requests unadjusted daily OHLC with actions enabled. The end date is
exclusive, matching yfinance semantics.

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
2. **SEC EDGAR** for filing metadata, primary filing documents, and reported
   XBRL facts. SEC submissions and company facts are available through
   `data.sec.gov`; clients must declare a user agent and respect the current
   fair-access limit.
3. **FRED or U.S. Treasury** for macro series and risk-free-rate inputs. FRED is
   useful when vintage-aware ALFRED observations matter; Treasury is the direct
   source for daily Treasury curve data.

Official references:

- <https://www.sec.gov/search-filings/edgar-application-programming-interfaces>
- <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data>
- <https://fred.stlouisfed.org/docs/api/fred/overview.html>
- <https://home.treasury.gov/treasury-daily-interest-rate-xml-feed>

SEC is required before treating reported fundamentals as research-grade.
FRED/Treasury can wait until the first DCF or macro-sensitive workflow. Free
consensus-estimate data has no equivalent authoritative source; any later
estimate adapter must preserve provider, `estimate_as_of`, retrieval time, and
licensing constraints rather than treating current yfinance estimates as
point-in-time history.
