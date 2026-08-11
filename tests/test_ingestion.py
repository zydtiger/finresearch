from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

import finresearch.ingestion as ingestion_module
from finresearch.cases import initialize_case, read_manifest
from finresearch.data_contracts import RAW_YFINANCE_DAILY_PRICES_V1
from finresearch.ingestion import IngestionError, ingest_yfinance_daily_prices


def valid_price_frame(
    *,
    symbol: str,
    retrieved_at: datetime,
    start: date,
    end: date,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "yfinance",
                "provider_symbol": symbol,
                "retrieved_at": retrieved_at,
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


class FakePriceProvider:
    """Return contract-shaped data without making a network request."""

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        return valid_price_frame(
            symbol=symbol,
            retrieved_at=retrieved_at,
            start=start,
            end=end,
        )


def test_ingestion_writes_parquet_and_registers_provenance(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    retrieved_at = datetime(2026, 8, 11, 4, 5, 6, 123456, tzinfo=UTC)

    receipt = ingest_yfinance_daily_prices(
        tmp_path,
        "aapl",
        "AAPL",
        date(2026, 1, 2),
        date(2026, 1, 3),
        provider=FakePriceProvider(),
        retrieved_at=retrieved_at,
    )

    assert receipt.path == (
        case_dir / "data/raw/yfinance/daily-prices/aapl/20260811T040506123456Z.parquet"
    )
    stored = pl.read_parquet(receipt.path)
    assert stored.height == 1
    assert stored.get_column("provider_symbol").to_list() == ["AAPL"]

    artifact = read_manifest(case_dir).artifacts[0]
    assert artifact.artifact_id == receipt.artifact_id
    assert artifact.kind == "raw.yfinance.daily-prices"
    assert artifact.schema_version == 1
    assert artifact.path == receipt.path.relative_to(case_dir).as_posix()
    assert artifact.source == "yfinance"
    assert artifact.sha256 == receipt.sha256
    assert artifact.retrieved_at == "2026-08-11T04:05:06.123456Z"
    assert artifact.row_count == 1


def test_ingestion_never_overwrites_same_snapshot(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    retrieved_at = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)
    arguments = (
        tmp_path,
        "aapl",
        "AAPL",
        date(2026, 1, 2),
        date(2026, 1, 3),
    )
    first = ingest_yfinance_daily_prices(
        *arguments,
        provider=FakePriceProvider(),
        retrieved_at=retrieved_at,
    )
    original = first.path.read_bytes()

    with pytest.raises(IngestionError, match="already exists"):
        ingest_yfinance_daily_prices(
            *arguments,
            provider=FakePriceProvider(),
            retrieved_at=retrieved_at,
        )

    assert first.path.read_bytes() == original
    assert len(read_manifest(tmp_path / "cases/aapl").artifacts) == 1


def test_ingestion_rejects_inconsistent_provider_metadata(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    class WrongSymbolProvider(FakePriceProvider):
        def fetch_daily_prices(
            self,
            symbol: str,
            start: date,
            end: date,
            retrieved_at: datetime,
        ) -> pl.DataFrame:
            return valid_price_frame(
                symbol="MSFT",
                retrieved_at=retrieved_at,
                start=start,
                end=end,
            )

    with pytest.raises(IngestionError, match="provider_symbol"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=WrongSymbolProvider(),
        )


def test_ingestion_rejects_invalid_period_without_fetch(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    with pytest.raises(IngestionError, match="earlier"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 3),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
        )


def test_manifest_failure_removes_new_raw_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def fail_registration(*args: object, **kwargs: object) -> None:
        raise OSError("manifest unavailable")

    monkeypatch.setattr(ingestion_module, "append_artifact", fail_registration)

    with pytest.raises(OSError, match="manifest unavailable"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()
