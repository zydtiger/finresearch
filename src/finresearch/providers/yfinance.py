"""Boundary adapter for provider-faithful yfinance price snapshots."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, date, datetime

import polars as pl
import yfinance as yf  # type: ignore[import-untyped]

from finresearch.data_contracts import RAW_YFINANCE_DAILY_PRICES_V1
from finresearch.providers import ProviderError


class YFinanceProviderError(ProviderError):
    """Raised when yfinance cannot produce a valid provider snapshot."""


class YFinancePriceProvider:
    """Fetch daily prices from yfinance and convert only the storage boundary."""

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        """Fetch an unadjusted daily history with actions included."""
        try:
            history = yf.Ticker(symbol).history(
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=False,
                actions=True,
                repair=False,
                keepna=True,
                raise_errors=True,
            )
        except Exception as exc:
            raise YFinanceProviderError(
                f"yfinance failed to fetch daily prices for {symbol}: {exc}"
            ) from exc

        if history.empty:
            raise YFinanceProviderError(
                f"yfinance returned no daily prices for {symbol} "
                f"between {start} and {end}"
            )

        rows = history.reset_index().to_dict(orient="records")
        return yfinance_history_to_frame(
            rows,
            symbol=symbol,
            start=start,
            end=end,
            retrieved_at=retrieved_at,
        )


def yfinance_history_to_frame(
    rows: list[dict[object, object]],
    *,
    symbol: str,
    start: date,
    end: date,
    retrieved_at: datetime,
) -> pl.DataFrame:
    """Convert yfinance boundary records to the exact raw storage schema."""
    if retrieved_at.tzinfo is None:
        raise YFinanceProviderError("retrieved_at must be timezone-aware")
    retrieved_utc = retrieved_at.astimezone(UTC)

    converted: list[dict[str, object]] = []
    for row in rows:
        timestamp_value = _timestamp_from_row(row)
        timestamp, provider_timezone = _normalize_timestamp(timestamp_value)
        converted.append(
            {
                "schema_version": RAW_YFINANCE_DAILY_PRICES_V1.version,
                "provider": "yfinance",
                "provider_symbol": symbol,
                "retrieved_at": retrieved_utc,
                "requested_start": start,
                "requested_end": end,
                "interval": "1d",
                "provider_timezone": provider_timezone,
                "session_date": timestamp_value.date(),
                "timestamp": timestamp,
                "open": _optional_float(row.get("Open")),
                "high": _optional_float(row.get("High")),
                "low": _optional_float(row.get("Low")),
                "close": _optional_float(row.get("Close")),
                "adj_close": _optional_float(row.get("Adj Close")),
                "volume": _optional_integer(row.get("Volume")),
                "dividends": _optional_float(row.get("Dividends")),
                "stock_splits": _optional_float(row.get("Stock Splits")),
                "capital_gains": _optional_float(row.get("Capital Gains")),
            }
        )

    frame = pl.DataFrame(converted, schema=RAW_YFINANCE_DAILY_PRICES_V1.schema)
    RAW_YFINANCE_DAILY_PRICES_V1.validate(frame)
    return frame


def _timestamp_from_row(row: Mapping[object, object]) -> datetime:
    value = row.get("Date", row.get("Datetime"))
    if value is None:
        raise YFinanceProviderError("yfinance history is missing its date index")
    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        value = to_pydatetime()
    if not isinstance(value, datetime):
        raise YFinanceProviderError("yfinance history contains a non-datetime index")
    return value


def _normalize_timestamp(value: datetime) -> tuple[datetime, str]:
    if value.tzinfo is None or value.utcoffset() is None:
        raise YFinanceProviderError(
            "yfinance history returned a timezone-naive timestamp"
        )
    provider_timezone = str(value.tzinfo)
    return value.astimezone(UTC), provider_timezone


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise YFinanceProviderError(f"invalid numeric value from yfinance: {value!r}")
    try:
        converted = float(str(value))
    except (TypeError, ValueError) as exc:
        raise YFinanceProviderError(
            f"invalid numeric value from yfinance: {value!r}"
        ) from exc
    return converted if math.isfinite(converted) else None


def _optional_integer(value: object) -> int | None:
    converted = _optional_float(value)
    return None if converted is None else int(converted)
