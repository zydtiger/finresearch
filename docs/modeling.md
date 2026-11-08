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

### DCF inputs version 2

`version = 2` is accepted alongside `version = 1`; version 1 inputs, runs, and
outputs are unchanged and keep writing `model.dcf-results.v1`. Version 2 keeps
every version 1 key and adds three optional parts. A version 2 input always
writes `model.dcf-results.v2`.

```toml
[scenario.base.capitalization]
diluted_shares = { value = 110, unit = "shares_m", source_id = "evidence-shares" }

[[scenario.base.issuance]]
period_end = "2027-12-31"
shares = { value = 10, unit = "shares_m", source_id = "assumption-atm" }
proceeds = { value = 50, unit = "USDm", source_id = "assumption-atm-proceeds" }

[translation]
currency = "HKD"
fx = { artifact_id = "normalized.fx-rates....", base = "USD", quote = "HKD", rate_date = "2026-06-26" }
```

- A scenario `capitalization.diluted_shares` replaces the global leaf for that
  scenario (the global leaf stays required). Its `[scenario.<name>.capitalization]`
  table holds no other key.
- `issuance` rows need strictly increasing `period_end` values that each equal a
  forecast `period_end` of that scenario, positive `shares` (share unit), and
  nonnegative `proceeds` (value unit). Proceeds are discounted with that
  period's cash-flow discount factor and added to equity value:
  `equity = EV - net_debt + issuance_pv`. Per-share value divides by opening
  diluted shares plus cumulative issued shares. Terminal value is unchanged.
- `model.dcf-results.v2` adds `opening_diluted_shares`, `issued_shares`,
  `ending_diluted_shares` (share unit), and `issuance_pv` (value unit). The
  reconciliation table gains `share_rollforward` (ending equals opening plus the
  sum of issuance leaves, in the share unit). Issuance leaves appear in
  `model.dcf-inputs` as `issuance_shares:<date>` and `issuance_proceeds:<date>`.
- `[translation]` converts each scenario's per-share value into another currency.
  `fx.base` must be the model currency and `fx.quote` the translation currency;
  the inverse pair is rejected rather than inverted. `rate_date` must be on or
  before `as_of`. The run reads the `close` rate for that exact date from the
  named `normalized.fx-rates.v1` artifact (no earlier-date fallback). The FX
  artifact is the `model.dcf-inputs` parent, its checksum is part of the run
  identity, and audit re-reads it. `model.dcf-translation.v1` has `run_id`,
  `scenario`, `per_share_value`, `currency`, `fx_rate` (as stored),
  `rate_date`, `translated_per_share_value` (`per_share_value * fx_rate`), and
  `translated_currency`, and descends from results. A `translation` check in the
  reconciliation table recomputes the translated value from equity value and
  ending shares. Reports render the table as "Translation" and list the FX
  artifact among source artifact IDs. `model comps` keeps its one-currency rule.

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

`token-inputs.toml` is strict TOML with `version = 1` or `version = 2`;
version 2 adds three optional keys described below. Top-level keys:
`as_of`, `instrument_id`, `currency`, `value_unit` (a currency amount scale such
as `USDm`), `token_unit` (`tokens`, `tokens_k`, `tokens_m`, `tokens_b`),
`discount_convention` (`year_end` or `mid_year`), `held_buyback_treatment`
(`exclude` or `include`), `buyback_price_basis` (only `as_of_price`), the
tables below, and optional `[anchors]` and `[pegs]`.

| Table | Content |
| --- | --- |
| `[price]` | `artifact_id` of a `normalized.daily-prices.v2` table and the `session_date` bar whose `close` is `price_0`. The bar's instrument must equal `instrument_id` or be mapped to it by an `instrument-master.v3` row (`provider_ids` containing `yfinance:<provider_symbol>`) in the artifact named by the optional `master_artifact_id`, which is then required. The bar currency must equal `currency`, and `session_date` must be before `as_of` so the bar is complete |
| `[supply]` | `reconciliation_artifact_id` and `fully_diluted_metric` (`total` or `max`). The reconciliation must match the instrument, have `as_of` on or before the model, and hold a `selected` row for `circulating` and the fully diluted metric; a `conflict` aborts the run. Optional `unlocks_artifact_id` (a `normalized.token-unlocks.v1`) and `schedule_id`, declared together, replace the inline `unlocks` leaves with the imported schedule |
| `[scenario.bear\|base\|bull]` | `discount_rate`, `capture_mix` (`buyback_retire`, `buyback_hold`, `burn`, `distribution` summing to 1, one `source_id`), `terminal = { terminal_growth = ... }`, a strictly increasing `forecast` array, `[...multiple] statistic` (`min`, `p25`, `median`, `p75`, `max`), and an optional `fully_diluted = { metric, provider, source_id, applies_to }` that replaces the fully diluted denominator with any non-conflict reconciliation observation. `applies_to` is a non-empty sorted list drawn from `"dcf"` and `"multiple"` and defaults to both; with `["dcf"]` the multiple results keep the selected denominator, and with `["multiple"]` the DCF and its supply path do. A narrowed override records `fully_diluted_0:dcf` and `fully_diluted_0:multiple` input rows instead of `fully_diluted_0` |
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

Unlocks come from inline `unlocks` leaves, or, when `[supply]` declares
`unlocks_artifact_id` and `schedule_id`, from the imported schedule. The two
are exclusive: a forecast row that still declares `unlocks` next to a schedule,
or omits it without one, is rejected, as is a schedule that does not hold
`schedule_id` for the instrument or has an event observed after `as_of`. Only
`scheduled` events whose date (a stream's end date) is after `as_of` count. A
cliff, or an `emission` without an end date, lands in the period whose
`(previous period_end, period_end]` window holds its `unlock_date`; a `linear`
stream, or an `emission` with an end date, spreads its amount evenly over every
day from start to end inclusive, and the days after `as_of` that fall in a
period are summed into it. `unlock_realization` still applies to the schedule's
per-period amount, and the artifact is a parent of the inputs table and covered
by run authentication. The per-period amounts are recorded as `unlocks:DATE`
input rows with `source_artifact_id`.

Version 2 accepts three optional keys, each rejected in a version 1 file. With
all three at their defaults a version 2 file produces the same numbers as the
equivalent version 1 file; the run id still differs because it covers the TOML
bytes.

| Key | Values (default first) | Effect |
| --- | --- | --- |
| `[supply] emissions_source` | `reserve`, `mint` | `reserve`: forecast `emissions` draw from the fully diluted stock, so they only move tokens from the reserve into circulation. `mint`: forecast `emissions` are new tokens and also grow fully diluted supply |
| `dcf_dilution` (top level) | `none`, `issuance` | `issuance`: each basis's DCF value counts only the share attributable to today's tokens (below) |
| `[multiples] subject_revenue` | `scenario`, `trailing` | `trailing`: the multiple applies to the subject's own trailing revenue over the peers' window instead of the scenario's year-1 `holders_revenue`; requires `subject_entity_id`, which is accepted only with `trailing` |

Scheduled unlocks, including `emission`-kind schedule events, always draw from
the reserve; only forecast `emissions` leaves follow `emissions_source`. An
uncapped or inflationary token (fully diluted equal to circulating, positive
issuance) fails under `reserve` with "circulating exceeds fully diluted
supply"; model it with `emissions_source = "mint"` and no `fully_diluted`
override. A `fully_diluted` override must cite a real provider observation in
the reconciliation, never an analyst projection imported as one.

With `subject_revenue = "trailing"` the subject's entity must have rows for
the multiples `metric` in one of `metric_artifact_ids`; its window is computed
exactly like a peer's (same `window`, `metric_window_end`, and aggregate-row
rules), and a missing entity, an incomplete window, or nonpositive revenue
fails the run. The revenue is recorded as a `subject_revenue` input row
(`value_unit`, `period_end = metric_window_end`, `source_artifact_id` = the
first declared metric artifact holding the subject's rows) in every scenario,
so only the peer statistic differs between scenarios.

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
                   + [mint] emissions_t
aggregate_value  = sum(cf_t * DF_t) + TV * DF_T,  TV = cf_T (1+g) / (r-g)
per_token        = aggregate_value / opening supply  (circulating or fully diluted)
implied_cf_scale = price_0 * fully_diluted_0 / aggregate_value
multiple_value_b = multiple_b(statistic) * holders_revenue_1   # pre-capture
                   [trailing] multiple_b(statistic) * subject trailing revenue
```

With `dcf_dilution = "issuance"` each basis `b` scales its DCF terms by the
share of supply that today's tokens still hold:

```text
factor_b_t       = S0_b / (S0_b + issued_b through t)
  circulating:     S0 = circulating_0,   issued = realized unlocks + emissions
  fully diluted:   S0 = DCF fully_diluted_0, issued = minted emissions
                   (0 under emissions_source = "reserve")
aggregate_value_b = sum(cf_t * DF_t * factor_b_t) + TV * DF_T * factor_b_T
per_token_b      = aggregate_value_b / S0_b
```

Retirements (burns and buybacks) do not enter the factor: their value is
already counted as tokenholder cash flow, so shrinking the share count as
well would count it twice. The terminal value assumes no dilution after the
last period, so set `terminal_growth` net of ongoing issuance. `terminal_share`,
`implied_cf_scale`, and the sensitivity grid's `per_token_value_fd` use the
diluted aggregate of their basis; the `pv_sum` check still verifies the
undiluted sum.

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
roll-forward (both bases; the fully diluted roll-forward adds minted
emissions), PV sum plus terminal PV equals the undiluted aggregate value,
`per_token_value * denominator` equals aggregate value for every method and
basis, peer statistics reproduce from included peers, and `implied_cf_scale *
aggregate_value` equals `price_0 * fully_diluted_0`; a failed check prevents
publication. Run identity covers the TOML bytes, selected scenarios and grid,
producer version, the cutoff, register bytes, and the checksum of every
referenced artifact.
