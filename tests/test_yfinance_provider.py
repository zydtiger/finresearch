from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from finresearch.providers.yfinance import (
    YFinanceProviderError,
    yfinance_history_to_frame,
)


def test_yfinance_boundary_preserves_session_and_converts_timestamp() -> None:
    retrieved_at = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)
    rows: list[dict[object, object]] = [
        {
            "Date": datetime(2026, 1, 2, tzinfo=ZoneInfo("America/New_York")),
            "Open": 100.0,
            "High": 102.0,
            "Low": 99.0,
            "Close": 101.0,
            "Adj Close": 100.5,
            "Volume": 1_000,
            "Dividends": 0.0,
            "Stock Splits": 0.0,
        }
    ]

    frame = yfinance_history_to_frame(
        rows,
        symbol="AAPL",
        start=date(2026, 1, 2),
        end=date(2026, 1, 3),
        retrieved_at=retrieved_at,
        currency="USD",
    )

    assert frame.row(0, named=True) == {
        "schema_version": 1,
        "provider": "yfinance",
        "provider_symbol": "AAPL",
        "currency": "USD",
        "retrieved_at": retrieved_at,
        "requested_start": date(2026, 1, 2),
        "requested_end": date(2026, 1, 3),
        "interval": "1d",
        "provider_timezone": "America/New_York",
        "session_date": date(2026, 1, 2),
        "timestamp": datetime(2026, 1, 2, 5, tzinfo=UTC),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "adj_close": 100.5,
        "volume": 1_000,
        "dividends": 0.0,
        "stock_splits": 0.0,
        "capital_gains": None,
    }


def test_yfinance_boundary_rejects_naive_timestamp() -> None:
    rows: list[dict[object, object]] = [{"Date": datetime(2026, 1, 2)}]

    with pytest.raises(YFinanceProviderError, match="timezone-naive"):
        yfinance_history_to_frame(
            rows,
            symbol="AAPL",
            start=date(2026, 1, 2),
            end=date(2026, 1, 3),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
            currency="USD",
        )
