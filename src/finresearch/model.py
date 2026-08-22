"""Pure, unit-explicit valuation calculations with no filesystem dependency."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date

import polars as pl


class ModelError(ValueError):
    """Raised when model inputs violate an economic or dimensional invariant."""


def _finite(*values: float) -> None:
    if any(not math.isfinite(value) for value in values):
        raise ModelError("model values must be finite")


def _finite_result(value: float, operation: str) -> float:
    """Return a finite calculation result or raise a stable domain error."""
    if not math.isfinite(value):
        raise ModelError(f"{operation} result must be finite")
    return value


def _finite_power(base: float, exponent: float, operation: str) -> float:
    """Calculate a finite power while translating float overflow to ModelError."""
    try:
        return _finite_result(base**exponent, operation)
    except OverflowError as exc:
        raise ModelError(f"{operation} result must be finite") from exc


def annualized_return(*, beginning: float, ending: float, years: float) -> float:
    """Return CAGR from positive like-for-like beginning and ending values."""
    _finite(beginning, ending, years)
    if beginning <= 0 or ending <= 0 or years <= 0:
        raise ModelError("annualized return requires positive values and years")
    return _finite_result(
        float((ending / beginning) ** (1 / years) - 1), "annualized return"
    )


def growth(*, current: float, prior: float) -> float:
    """Return growth rate, rejecting a zero denominator."""
    _finite(current, prior)
    if prior == 0:
        raise ModelError("growth prior value must not be zero")
    return _finite_result(current / prior - 1, "growth")


def margin(*, numerator: float, revenue: float) -> float:
    """Return a ratio where numerator and revenue share one value unit."""
    _finite(numerator, revenue)
    if revenue == 0:
        raise ModelError("margin revenue must not be zero")
    return _finite_result(numerator / revenue, "margin")


def diluted_shares(
    *,
    basic: float,
    options_incremental: float = 0.0,
    restricted_stock: float = 0.0,
    convertible_incremental: float = 0.0,
) -> float:
    """Sum explicit diluted-share components without a hidden dilution method."""
    components = (basic, options_incremental, restricted_stock, convertible_incremental)
    _finite(*components)
    if basic <= 0 or any(component < 0 for component in components[1:]):
        raise ModelError(
            "diluted share components must be non-negative and basic positive"
        )
    return _finite_result(sum(components), "diluted shares")


def net_debt(*, debt: float, cash: float) -> float:
    """Return debt less cash in one declared value unit and currency."""
    _finite(debt, cash)
    if debt < 0 or cash < 0:
        raise ModelError("debt and cash must be non-negative")
    return _finite_result(debt - cash, "net debt")


def enterprise_value(*, market_cap: float, net_debt_value: float) -> float:
    """Return equity market capitalization plus net debt."""
    _finite(market_cap, net_debt_value)
    if market_cap <= 0:
        raise ModelError("market_cap must be positive")
    return _finite_result(market_cap + net_debt_value, "enterprise value")


def wacc_from_components(
    *, cost_equity: float, cost_debt: float, tax_rate: float, debt_weight: float
) -> float:
    """Weighted average cost of capital after the debt tax shield."""
    _finite(cost_equity, cost_debt, tax_rate, debt_weight)
    if cost_equity < 0 or cost_debt < 0:
        raise ModelError("capital costs must not be negative")
    if not 0 <= tax_rate <= 1:
        raise ModelError("tax_rate must be between 0 and 1")
    if not 0 <= debt_weight <= 1:
        raise ModelError("debt_weight must be between 0 and 1")
    return _finite_result(
        (1 - debt_weight) * cost_equity + debt_weight * cost_debt * (1 - tax_rate),
        "WACC",
    )


@dataclass(frozen=True)
class DCFInput:
    """Legacy evenly spaced year-end DCF inputs retained for calculator users."""

    forecast_fcfs: tuple[float, ...]
    discount_rate: float
    terminal_growth: float
    net_debt: float = 0.0
    shares_outstanding: float | None = None

    def __post_init__(self) -> None:
        _finite(
            *self.forecast_fcfs, self.discount_rate, self.terminal_growth, self.net_debt
        )
        if self.shares_outstanding is not None:
            _finite(self.shares_outstanding)
        if not self.forecast_fcfs:
            raise ModelError("forecast_fcfs must not be empty")
        if self.discount_rate <= 0:
            raise ModelError("discount_rate must be positive")
        if not -1 < self.terminal_growth < self.discount_rate:
            raise ModelError(
                "terminal_growth must be greater than -1 and below discount_rate"
            )
        if self.forecast_fcfs[-1] <= 0:
            raise ModelError("Gordon terminal requires positive final free cash flow")
        if self.shares_outstanding is not None and self.shares_outstanding <= 0:
            raise ModelError("shares_outstanding must be positive")


@dataclass(frozen=True)
class DCFResult:
    """Valuation outputs with forecast and terminal reconciliation detail."""

    forecast_pvs: tuple[float, ...]
    terminal_value: float
    terminal_pv: float
    enterprise_value: float
    equity_value: float
    per_share_value: float | None


@dataclass(frozen=True)
class DCFPeriod:
    """One explicit dated free-cash-flow forecast in a common currency/unit."""

    period_end: date
    free_cash_flow: float


@dataclass(frozen=True)
class DCFSpecification:
    """Full dated DCF specification for auditable case-backed model runs."""

    as_of: date
    periods: tuple[DCFPeriod, ...]
    wacc: float
    net_debt_value: float
    diluted_shares_value: float
    discount_convention: str = "year_end"
    terminal_method: str = "gordon_growth"
    terminal_growth: float | None = None
    exit_multiple: float | None = None
    terminal_metric: float | None = None

    def __post_init__(self) -> None:
        _finite(self.wacc, self.net_debt_value, self.diluted_shares_value)
        _finite(*(period.free_cash_flow for period in self.periods))
        if self.terminal_growth is not None:
            _finite(self.terminal_growth)
        if self.exit_multiple is not None:
            _finite(self.exit_multiple)
        if self.terminal_metric is not None:
            _finite(self.terminal_metric)
        if not self.periods:
            raise ModelError("DCF periods must not be empty")
        if self.wacc <= 0:
            raise ModelError("WACC must be positive")
        if self.diluted_shares_value <= 0:
            raise ModelError("diluted shares must be positive")
        if self.discount_convention not in {"year_end", "mid_year"}:
            raise ModelError("discount convention must be year_end or mid_year")
        if self.terminal_method not in {"gordon_growth", "exit_multiple"}:
            raise ModelError("terminal method must be gordon_growth or exit_multiple")
        previous = self.as_of
        for period in self.periods:
            if period.period_end <= previous:
                raise ModelError(
                    "period_end rows must be strictly increasing after as_of"
                )
            previous = period.period_end
        if self.terminal_method == "gordon_growth":
            if self.terminal_growth is None or self.exit_multiple is not None:
                raise ModelError("gordon_growth terminal requires terminal_growth only")
            if not -1 < self.terminal_growth < self.wacc:
                raise ModelError(
                    "terminal growth must be greater than -1 and below WACC"
                )
            if self.periods[-1].free_cash_flow <= 0:
                raise ModelError(
                    "Gordon terminal requires positive final free cash flow"
                )
        elif (
            self.exit_multiple is None
            or self.terminal_metric is None
            or self.terminal_growth is not None
        ):
            raise ModelError(
                "exit_multiple terminal requires terminal_metric and exit_multiple"
            )
        elif self.exit_multiple <= 0 or self.terminal_metric <= 0:
            raise ModelError("exit_multiple terminal inputs must be positive")


@dataclass(frozen=True)
class DCFPeriodResult:
    """Discounting detail for one forecast period."""

    period_end: date
    free_cash_flow: float
    year_fraction: float
    discount_factor: float
    present_value: float


@dataclass(frozen=True)
class DetailedDCFResult:
    """Auditable DCF result with terminal and share reconciliations."""

    periods: tuple[DCFPeriodResult, ...]
    terminal_value: float
    terminal_pv: float
    enterprise_value: float
    equity_value: float
    per_share_value: float


def dcf_valuation(input: DCFInput) -> DCFResult:
    """Value legacy annual FCFs with the documented year-end Gordon convention."""
    discount = 1 + input.discount_rate
    forecast_pvs = tuple(
        _finite_result(
            fcf / _finite_power(discount, period, "discount factor"),
            "forecast present value",
        )
        for period, fcf in enumerate(input.forecast_fcfs, start=1)
    )
    terminal_value = _finite_result(
        input.forecast_fcfs[-1]
        * (1 + input.terminal_growth)
        / (input.discount_rate - input.terminal_growth),
        "Gordon terminal value",
    )
    if terminal_value <= 0:
        raise ModelError("Gordon terminal value must be positive")
    terminal_pv = _finite_result(
        terminal_value
        / _finite_power(discount, len(input.forecast_fcfs), "discount factor"),
        "terminal present value",
    )
    enterprise = _finite_result(sum(forecast_pvs) + terminal_pv, "enterprise value")
    equity = _finite_result(enterprise - input.net_debt, "equity value")
    return DCFResult(
        forecast_pvs=forecast_pvs,
        terminal_value=terminal_value,
        terminal_pv=terminal_pv,
        enterprise_value=enterprise,
        equity_value=equity,
        per_share_value=(
            _finite_result(equity / input.shares_outstanding, "per-share value")
            if input.shares_outstanding is not None
            else None
        ),
    )


def dated_dcf_valuation(specification: DCFSpecification) -> DetailedDCFResult:
    """Discount dated FCFs using actual/365 fractions and one terminal method."""
    periods: list[DCFPeriodResult] = []
    for period in specification.periods:
        year_fraction = (period.period_end - specification.as_of).days / 365.0
        if specification.discount_convention == "mid_year":
            year_fraction = max(0.0, year_fraction - 0.5)
        factor = _finite_power(1 + specification.wacc, year_fraction, "discount factor")
        periods.append(
            DCFPeriodResult(
                period_end=period.period_end,
                free_cash_flow=period.free_cash_flow,
                year_fraction=year_fraction,
                discount_factor=factor,
                present_value=_finite_result(
                    period.free_cash_flow / factor, "forecast present value"
                ),
            )
        )
    final_fcf = specification.periods[-1].free_cash_flow
    if specification.terminal_method == "gordon_growth":
        assert specification.terminal_growth is not None
        terminal_value = _finite_result(
            final_fcf
            * (1 + specification.terminal_growth)
            / (specification.wacc - specification.terminal_growth),
            "Gordon terminal value",
        )
        if terminal_value <= 0:
            raise ModelError("Gordon terminal value must be positive")
    else:
        assert specification.exit_multiple is not None
        assert specification.terminal_metric is not None
        terminal_value = _finite_result(
            specification.terminal_metric * specification.exit_multiple,
            "exit terminal value",
        )
    terminal_pv = _finite_result(
        terminal_value / periods[-1].discount_factor, "terminal present value"
    )
    enterprise = _finite_result(
        sum(period.present_value for period in periods) + terminal_pv,
        "enterprise value",
    )
    equity = _finite_result(enterprise - specification.net_debt_value, "equity value")
    return DetailedDCFResult(
        periods=tuple(periods),
        terminal_value=terminal_value,
        terminal_pv=terminal_pv,
        enterprise_value=enterprise,
        equity_value=equity,
        per_share_value=_finite_result(
            equity / specification.diluted_shares_value, "per-share value"
        ),
    )


def dcf_sensitivity(
    input: DCFInput,
    *,
    discount_rates: tuple[float, ...],
    terminal_growths: tuple[float, ...],
) -> pl.DataFrame:
    """Return a compatibility grid of per-share WACC/growth sensitivity values."""
    if input.shares_outstanding is None:
        raise ModelError("sensitivity requires shares_outstanding")
    if not discount_rates or not terminal_growths:
        raise ModelError("sensitivity grids must not be empty")
    _finite(*discount_rates, *terminal_growths)
    if any(rate <= 0 for rate in discount_rates):
        raise ModelError("sensitivity WACC values must be positive")
    if any(growth <= -1 for growth in terminal_growths) or any(
        growth >= rate for growth in terminal_growths for rate in discount_rates
    ):
        raise ModelError(
            "sensitivity terminal growth must be greater than -1 and below every WACC"
        )
    columns: dict[str, list[float]] = {"terminal_growth": []}
    for terminal_growth in terminal_growths:
        columns["terminal_growth"].append(terminal_growth)
        for wacc in discount_rates:
            value = float("nan")
            if terminal_growth < wacc:
                result = dcf_valuation(
                    DCFInput(
                        forecast_fcfs=input.forecast_fcfs,
                        discount_rate=wacc,
                        terminal_growth=terminal_growth,
                        net_debt=input.net_debt,
                        shares_outstanding=input.shares_outstanding,
                    )
                )
                assert result.per_share_value is not None
                value = result.per_share_value
            columns.setdefault(f"wacc_{wacc:.4f}", []).append(value)
    return pl.DataFrame(columns)
