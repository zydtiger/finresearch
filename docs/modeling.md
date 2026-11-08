# Auditable Modeling Contract v1

`model dcf` is case-backed: it never accepts economic values on the command
line. Its required input is a case-relative `analysis/dcf-inputs.toml` using
this strict shape:

```toml
version = 1
as_of = "2026-12-31"
currency = "USD"
value_unit = "USDm"
share_unit = "shares_m"
discount_convention = "year_end" # or mid_year
terminal_method = "gordon_growth" # or exit_multiple
projection_needs = []

[wacc]
cost_equity = { value = 0.10, unit = "ratio", source_id = "assumption-cost-equity" }
cost_debt = { value = 0.05, unit = "ratio", source_id = "assumption-cost-debt" }
tax_rate = { value = 0.25, unit = "ratio", source_id = "assumption-tax" }
debt_weight = { value = 0.20, unit = "ratio", source_id = "assumption-capital" }

[capitalization]
market_cap = { value = 1000, unit = "USDm", source_id = "evidence-market-cap" }
debt = { value = 200, unit = "USDm", source_id = "evidence-debt" }
cash = { value = 50, unit = "USDm", source_id = "evidence-cash" }
diluted_shares = { value = 100, unit = "shares_m", source_id = "evidence-shares" }

[scenario.bear]
forecast = [{ period_end = "2027-12-31", free_cash_flow = { value = 60, unit = "USDm", source_id = "assumption-bear-fcf" } }]
terminal = { terminal_growth = { value = 0.02, unit = "ratio", source_id = "assumption-bear-growth" } }
# Repeat complete, distinct forecast and terminal tables for base and bull.
```

Every economic leaf has a numeric `value`, controlled dimensional `unit`, and
an id resolved from `registers/evidence.csv` or `registers/assumptions.csv`.
Those source dates must not exceed `as_of`; assumptions' evidence references
remain subject to the register contract. DCF uses actual/365 periods, rejects
non-increasing periods, terminal growth outside `-1 < g < WACC`, and a
nonpositive Gordon terminal value, and publishes
typed inputs, discounted cash flows, results, reconciliation checks, and an
optional long sensitivity grid. Identities cover canonical TOML bytes, selected
scenario/grid, producer version, cutoff, and referenced register bytes.
Numbers must be finite. Currency amount units are exactly `CURRENCY`,
`CURRENCYk`, `CURRENCYm`, or `CURRENCYb`; share units are exactly `shares`,
`shares_k`, `shares_m`, or `shares_b`. Output per-share values are normalized
to `CURRENCY/share`, so `USDm` divided by `shares_m`, for example, is
`USD/share` rather than an ambiguous scaled value.
Gordon terminal value is `final_fcf * (1 + g) / (WACC - g)`. An
`exit_multiple` terminal instead requires separately sourced
`terminal_metric` (an amount in `value_unit`) and `exit_multiple` (unit
`multiple`), and uses `terminal_metric * exit_multiple`; it never assumes the
final FCF is the terminal metric.

```text
finresearch --workspace PATH model dcf CASE_ID --input analysis/dcf-inputs.toml \
  --scenario bear|base|bull|all [--sensitivity WACCS;GROWTHS]
finresearch --workspace PATH model projection-assess CASE_ID --input analysis/dcf-inputs.toml
```

`projection_needs` is an explicit controlled list: `working-capital`,
`capex-depreciation`, `tax`, `cash-debt-interest`, `dilution`,
`liquidity-covenant`, or `balance-sheet-reconciliation`. An empty list records
`not_required`; any declared item records `required`. It is a gate, not a
three-statement model.

Comparable observations use local import schema `model.comps-observations.v1`.
`model comps` accepts only a declared observation artifact and explicit peers;
it does not discover peers, invert FX, or convert fiscal periods. A source
artifact must use one common observation snapshot no later than the explicit
CLI cutoff. The typed model input preserves that source snapshot and records
the actual CLI cutoff as hash-bound `run_as_of`; the latter is the model's
authoritative point-in-time date. New runs publish `model.comps-inputs.v2`,
which also records one canonical requested-metrics list and the one declared
target company. Those fields are part of the run identity and allow the shared
resolver to reproduce the selected PIT rows and every published comps output.
The immutable `model.comps-inputs.v1` remains valid for historic tables but is
not cutoff-authenticatable, so audit and reporting require rerunning it into a
distinct v2 run. All selected observations share one non-null currency. The
source artifact preserves history by knowledge date and source id. For each
company/metric, the run selects the one latest eligible knowledge date; tied
latest observations are conflicts, not tie-broken. Older observations may use
a different period. Every selected company for a metric used by a requested
multiple must share the exact same basis and end date.
There is no fiscal-period conversion.

`market_cap`, `net_debt`, `revenue`, `ebitda`, and `ebit` use only controlled
currency amount scales and are normalized to base currency before division.
`share_price` and `eps` use only `CURRENCY/share`; `diluted_shares` uses a
controlled share scale. Market capitalization, share price, and diluted shares
must be positive; negative revenue or EPS are retained as explicit exclusions.
An EV multiple requires both explicit `market_cap` and
`net_debt`; net debt never defaults to zero. Results and summaries contain only
dimensionless multiples with unit `x`. Missing, zero, or negative denominators
are registered as explicit exclusions, but every requested multiple must still
have at least one valid peer before any model artifact is published. Reconciliation
rows retain an explicit unit (`value_unit`, `CURRENCY/share`, or `x`) and a
computed `passed`, `failed`, or `excluded` status.
Summary `p25`, `median`, and `p75` use sorted linear interpolation at
`(n - 1) * p`.

```text
finresearch --workspace PATH model comps CASE_ID --input ARTIFACT_ID \
  --as-of YYYY-MM-DD --metrics ev_revenue,ev_ebitda,ev_ebit,pe [--target COMPANY_ID]
```

## Token model

`model token` values a token from tokenholder cash flows and a supply path, and
cross-checks with peer revenue multiples. Like `model dcf` it accepts no
economic values on the command line.

```text
finresearch --workspace PATH model token CASE_ID \
  [--input analysis/token-inputs.toml] [--scenario bear|base|bull|all] \
  [--sensitivity RATES;GROWTHS]
```

`token-inputs.toml` is strict `version = 1` TOML. Top-level keys:
`as_of`, `instrument_id`, `currency`, `value_unit` (a currency amount scale such
as `USDm`), `token_unit` (`tokens`, `tokens_k`, `tokens_m`, `tokens_b`),
`discount_convention` (`year_end` or `mid_year`), `held_buyback_treatment`
(`exclude` or `include`), `buyback_price_basis` (only `as_of_price`), the
tables below, and optional `[anchors]` and `[pegs]`.

| Table | Content |
| --- | --- |
| `[price]` | `artifact_id` of a `normalized.daily-prices.v2` table and the `session_date` bar whose `close` is `price_0`. The bar's instrument must equal `instrument_id` or be mapped to it by an `instrument-master.v3` row (`provider_ids` containing `yfinance:<provider_symbol>`) in the artifact named by the optional `master_artifact_id`, which is then required. The bar currency must equal `currency`, and `session_date` must be before `as_of` so the bar is complete |
| `[supply]` | `reconciliation_artifact_id` and `fully_diluted_metric` (`total` or `max`). The reconciliation must match the instrument, have `as_of` on or before the model, and hold a `selected` row for `circulating` and the fully diluted metric; a `conflict` aborts the run |
| `[scenario.bear\|base\|bull]` | `discount_rate`, `capture_mix` (`buyback_retire`, `buyback_hold`, `burn`, `distribution` summing to 1, one `source_id`), `terminal = { terminal_growth = ... }`, a strictly increasing `forecast` array, `[...multiple] statistic` (`min`, `p25`, `median`, `p75`, `max`), and an optional `fully_diluted = { metric, provider, source_id }` that replaces the fully diluted denominator with any non-conflict reconciliation observation |
| `[multiples]` | `metric` (a revenue metric), `window` (`annualized_30d` or `ttm`), `metric_window_end`, `market_observed_on`, `market_artifact_ids`, `metric_artifact_ids`, `peers` (`instrument_id`, `entity_id`), optional `exclude` (`instrument_id`, `reason_source_id`) |
| `[anchors]` | optional `protocol_metrics_artifact_id` and `metric`; validated and recorded as a parent, never used in a value |
| `[pegs]` | optional named leaves with unit `<FIAT>/<USDC\|USDT\|USDH>`, used to convert stablecoin-quoted peer market values |

Every economic leaf is `{ value, unit, source_id }` and resolves to an evidence
or assumption id dated on or before `as_of`. A forecast row has `period_end`,
`holders_revenue` (`value_unit`), `capture_ratio` (`ratio`), `unlocks` and
`emissions` (`token_unit`), and `unlock_realization` (`ratio`). Validation:
`-1 < g < r`, `r > 0`; `capture_ratio` and `unlock_realization` within
`[0, 1]`; `holders_revenue`, `unlocks`, `emissions` nonnegative; complete and
pairwise-distinct bear/base/bull tables; for every period
`0 <= circulating_t <= fully_diluted_t` and `retired_t <= circulating_{t-1}`.
Inline `unlocks` leaves are the only unlock source until a `token-unlocks.v1`
contract exists.

Calculation, per scenario and period `t` (actual/365 from `as_of`, with the
DCF's `mid_year` adjustment):

```text
tokenholder_cf_t = holders_revenue_t * capture_ratio_t
buyback_tokens_t = cf_t * (buyback_retire + buyback_hold) / price_0
retired_tokens_t = cf_t * (buyback_retire + burn) / price_0
                   + [exclude] cf_t * buyback_hold / price_0
circulating_t    = circulating_{t-1} + unlocks_t * unlock_realization_t
                   + emissions_t - retired_t
fully_diluted_t  = fully_diluted_{t-1} - retired_t
aggregate_value  = sum(cf_t * DF_t) + TV * DF_T,  TV = cf_T (1+g) / (r-g)
per_token        = aggregate_value / opening supply  (circulating or fully diluted)
implied_cf_scale = price_0 * fully_diluted_0 / aggregate_value
multiple_value_b = multiple_b(statistic) * holders_revenue_1   # pre-capture
```

Value and token scales convert so that `USDm / tokens_m` reads as
`USD/token`. The denominator is opening supply; the supply path is reported
separately. Peer multiples are `market_cap / HR` (circulating basis) and
`fdv / HR` (fully diluted basis) with HR the sum of the aggregate (`sub_protocol`
and `chain` NULL), complete daily `metric` rows over the 30 (annualized by
365/30) or 365 UTC days ending `metric_window_end`, in the model currency.
Each declared peer needs a market observation on `market_observed_on`
(latest wins; conflicting latest values fail); a missing `market_cap` or `fdv`
(`missing_market_observation`), a window with any missing or incomplete day
(`incomplete_metric_window`), or a nonpositive HR
(`nonpositive_holders_revenue`) is an explicit exclusion on that basis, as is a
peer in `exclude`. Conflicting values for one day still fail. Each basis needs at least one included peer. Statistics use the same
linear interpolation as comps. A market value quoted in a stablecoin needs the
matching peg leaf; there is no FX conversion between fiat currencies.

A complete run publishes, in this order: `model.token-inputs.v1` (one row per
leaf and artifact-resolved value; exactly one of `source_id` and
`source_artifact_id`), `model.token-supply-path.v1`, `model.token-cashflows.v1`,
`model.token-peer-multiples.v1`, `model.token-results.v1` (one row per
scenario, method `dcf` or `multiple`, and basis; `implied_cf_scale` only on
`dcf` / `fully_diluted` rows), the optional `model.token-sensitivity.v1`
(`per_token_value_fd` per rate and growth), and `model.token-reconciliation.v1`.
`absorption_ratio` is `buyback_tokens / (unlocks_realized + emissions)`, NULL
when there is no new supply. Inputs have the referenced artifacts as ordered
parents (price, reconciliation, market, metric, anchor, master); supply path,
cash flows, and peer multiples descend from inputs, results from those three,
and sensitivity and reconciliation from results. Reconciliation checks: supply
roll-forward (both bases), PV sum plus terminal PV equals aggregate value,
`per_token_value * denominator` equals aggregate value for every method and
basis, peer statistics reproduce from included peers, and `implied_cf_scale *
aggregate_value` equals `price_0 * fully_diluted_0`; a failed check prevents
publication. Run identity covers the TOML bytes, selected scenarios and grid,
producer version, the cutoff, register bytes, and the checksum of every
referenced artifact.
