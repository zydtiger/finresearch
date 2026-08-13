"""Independent process used by ingestion lock tests."""

from __future__ import annotations

import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from finresearch.data_contracts import RAW_YFINANCE_DAILY_PRICES_V1
from finresearch.ingestion import IngestionError, ingest_yfinance_daily_prices


class CoordinatedPriceProvider:
    """Wait until the parent releases all competing processes."""

    def __init__(self, coordination_directory: Path, worker_id: str) -> None:
        self._coordination_directory = coordination_directory
        self._worker_id = worker_id

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        (self._coordination_directory / f"ready-{self._worker_id}").touch()
        release_path = self._coordination_directory / "go"
        deadline = time.monotonic() + 10
        while not release_path.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("process-ingestion barrier timed out")
            time.sleep(0.01)
        return pl.DataFrame(
            [
                {
                    "schema_version": 1,
                    "provider": "yfinance",
                    "provider_symbol": symbol,
                    "currency": "USD",
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


def main() -> int:
    """Run one coordinated ingestion and expose collision through exit status."""
    workspace = Path(sys.argv[1])
    retrieved_at = datetime.fromisoformat(sys.argv[2])
    coordination_directory = Path(sys.argv[3])
    worker_id = sys.argv[4]
    provider = CoordinatedPriceProvider(coordination_directory, worker_id)
    try:
        ingest_yfinance_daily_prices(
            workspace,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=provider,
            retrieved_at=retrieved_at,
        )
    except IngestionError as exc:
        if "already exists" in str(exc):
            return 3
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
