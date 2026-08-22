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
it does not discover peers, invert FX, or convert fiscal periods. All selected
observations must have row `as_of` exactly equal to the CLI cutoff and share one
non-null currency. The source artifact preserves history by knowledge date and
source id. For each company/metric, the run selects the one latest eligible
knowledge date; tied latest observations are conflicts, not tie-broken. Older
observations may use a different period. Every selected company for a metric
used by a requested multiple must share the exact same basis and end date.
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
