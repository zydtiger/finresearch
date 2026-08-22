from datetime import UTC, date, datetime

import polars as pl
import pytest

from finresearch.data_contracts import (
    MODEL_COMPS_INPUTS_V1,
    MODEL_COMPS_INPUTS_V2,
    MODEL_COMPS_OBSERVATIONS_V1,
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


def test_comps_inputs_v1_is_immutable_and_v2_binds_one_common_cutoff() -> None:
    v1 = valid_comps_inputs_v1_frame()
    assert MODEL_COMPS_INPUTS_V1.schema == MODEL_COMPS_OBSERVATIONS_V1.schema
    MODEL_COMPS_INPUTS_V1.validate(v1)

    v2 = v1.with_columns(
        pl.lit(2, dtype=pl.UInt16).alias("schema_version"),
        pl.lit("run", dtype=pl.String).alias("run_id"),
        pl.lit(date(2026, 6, 30), dtype=pl.Date).alias("run_as_of"),
        pl.lit("ev_revenue", dtype=pl.String).alias("requested_metrics"),
        pl.lit("target", dtype=pl.String).alias("target_company_id"),
    ).cast(MODEL_COMPS_INPUTS_V2.schema)
    MODEL_COMPS_INPUTS_V2.validate(v2)
    with pytest.raises(DataContractError, match="one common valid date"):
        MODEL_COMPS_INPUTS_V2.validate(
            pl.concat(
                [
                    v2,
                    v2.with_columns(
                        pl.lit(date(2026, 7, 1), dtype=pl.Date).alias("as_of"),
                        pl.lit("e2", dtype=pl.String).alias("source_id"),
                    ),
                ]
            )
        )
    with pytest.raises(DataContractError, match="must not be after run_as_of"):
        MODEL_COMPS_INPUTS_V2.validate(
            v2.with_columns(pl.lit(date(2026, 6, 29), dtype=pl.Date).alias("run_as_of"))
        )


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
                "currency": "USD",
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


def valid_comps_inputs_v1_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "manual",
                "company_id": "target",
                "company_name": "Target",
                "role": "target",
                "metric": "revenue",
                "period_basis": "LTM",
                "period_end": date(2026, 6, 30),
                "knowledge_date": date(2026, 6, 30),
                "as_of": date(2026, 6, 30),
                "value": 1.0,
                "unit": "USDm",
                "currency": "USD",
                "source_id": "e1",
                "source_artifact_id": "source",
                "normalized_at": datetime(2026, 6, 30, tzinfo=UTC),
            }
        ],
        schema=MODEL_COMPS_INPUTS_V1.schema,
    )
