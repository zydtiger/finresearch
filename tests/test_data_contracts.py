from datetime import UTC, date, datetime

import polars as pl
import pytest

from finresearch.data_contracts import (
    RAW_YFINANCE_DAILY_PRICES_V1,
    DataContractError,
    get_contract,
)


def test_registry_returns_shared_contract() -> None:
    contract = get_contract("raw.yfinance.daily-prices.v1")

    assert contract is RAW_YFINANCE_DAILY_PRICES_V1


def test_contract_rejects_schema_drift() -> None:
    frame = valid_price_frame().with_columns(pl.lit("unexpected").alias("extra"))

    with pytest.raises(DataContractError, match="schema mismatch"):
        RAW_YFINANCE_DAILY_PRICES_V1.validate(frame)


def test_contract_rejects_required_nulls() -> None:
    frame = valid_price_frame().with_columns(
        pl.lit(None, dtype=pl.String).alias("provider_symbol")
    )

    with pytest.raises(DataContractError, match="provider_symbol"):
        RAW_YFINANCE_DAILY_PRICES_V1.validate(frame)


def test_contract_rejects_duplicate_keys() -> None:
    frame = valid_price_frame()

    with pytest.raises(DataContractError, match="duplicate rows"):
        RAW_YFINANCE_DAILY_PRICES_V1.validate(pl.concat([frame, frame]))


def valid_price_frame(
    *,
    symbol: str = "AAPL",
    retrieved_at: datetime | None = None,
    start: date = date(2026, 1, 2),
    end: date = date(2026, 1, 3),
) -> pl.DataFrame:
    retrieval_time = retrieved_at or datetime(2026, 8, 11, 4, 5, tzinfo=UTC)
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "yfinance",
                "provider_symbol": symbol,
                "retrieved_at": retrieval_time,
                "requested_start": start,
                "requested_end": end,
                "interval": "1d",
                "provider_timezone": "America/New_York",
                "session_date": start,
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
        ],
        schema=RAW_YFINANCE_DAILY_PRICES_V1.schema,
    )
