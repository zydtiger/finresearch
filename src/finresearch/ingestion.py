"""Immutable raw-data ingestion and case artifact registration."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import polars as pl

from finresearch.cases import (
    Artifact,
    CaseContractError,
    append_artifact,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import RAW_YFINANCE_DAILY_PRICES_V1


class IngestionError(RuntimeError):
    """Raised when an ingestion cannot complete without partial state."""


class DailyPriceProvider(Protocol):
    """Boundary required by the daily-price ingestion workflow."""

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame: ...


@dataclass(frozen=True)
class IngestionReceipt:
    """Stable result metadata for a persisted raw snapshot."""

    artifact_id: str
    path: Path
    row_count: int
    sha256: str
    retrieved_at: datetime


def ingest_yfinance_daily_prices(
    workspace: Path,
    case_id: str,
    symbol: str,
    start: date,
    end: date,
    *,
    provider: DailyPriceProvider | None = None,
    retrieved_at: datetime | None = None,
) -> IngestionReceipt:
    """Fetch and append one immutable yfinance daily-price snapshot."""
    normalized_symbol = symbol.strip()
    if not normalized_symbol:
        raise IngestionError("symbol must not be empty")
    if start >= end:
        raise IngestionError("start date must be earlier than exclusive end date")

    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    raw_root = resolve_relative_path(case_dir, manifest.paths["raw"], "paths.raw")
    if not raw_root.is_dir():
        raise CaseContractError(f"required raw directory missing: {raw_root}")

    retrieval_time = _retrieval_time(retrieved_at)
    if provider is None:
        from finresearch.providers.yfinance import YFinancePriceProvider

        price_provider: DailyPriceProvider = YFinancePriceProvider()
    else:
        price_provider = provider
    frame = price_provider.fetch_daily_prices(
        normalized_symbol,
        start,
        end,
        retrieval_time,
    )
    RAW_YFINANCE_DAILY_PRICES_V1.validate(frame)
    _validate_snapshot_metadata(
        frame,
        symbol=normalized_symbol,
        start=start,
        end=end,
        retrieved_at=retrieval_time,
    )

    symbol_key = _symbol_key(normalized_symbol)
    timestamp_key = retrieval_time.strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = Path(
        manifest.paths["raw"],
        "yfinance",
        "daily-prices",
        symbol_key,
        f"{timestamp_key}.parquet",
    )
    output_path = resolve_relative_path(
        case_dir,
        relative_path.as_posix(),
        "raw output",
    )
    if output_path.exists():
        raise IngestionError(f"raw snapshot already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = output_path.with_suffix(".parquet.tmp")
    if temporary_path.exists():
        raise IngestionError(f"temporary output already exists: {temporary_path}")

    try:
        frame.write_parquet(
            temporary_path,
            compression="zstd",
            statistics=True,
        )
        sha256 = _sha256(temporary_path)
        temporary_path.replace(output_path)
        artifact_id = f"raw.yfinance.daily-prices.{symbol_key}.{timestamp_key.lower()}"
        append_artifact(
            case_dir,
            Artifact(
                artifact_id=artifact_id,
                kind=RAW_YFINANCE_DAILY_PRICES_V1.name,
                schema_version=RAW_YFINANCE_DAILY_PRICES_V1.version,
                path=relative_path.as_posix(),
                source="yfinance",
                sha256=sha256,
                retrieved_at=_format_utc(retrieval_time),
                row_count=frame.height,
            ),
        )
    except Exception:
        temporary_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        _remove_empty_parents(output_path.parent, raw_root)
        raise

    return IngestionReceipt(
        artifact_id=artifact_id,
        path=output_path,
        row_count=frame.height,
        sha256=sha256,
        retrieved_at=retrieval_time,
    )


def _validate_snapshot_metadata(
    frame: pl.DataFrame,
    *,
    symbol: str,
    start: date,
    end: date,
    retrieved_at: datetime,
) -> None:
    expected = {
        "schema_version": RAW_YFINANCE_DAILY_PRICES_V1.version,
        "provider": "yfinance",
        "provider_symbol": symbol,
        "retrieved_at": retrieved_at,
        "requested_start": start,
        "requested_end": end,
        "interval": "1d",
    }
    for field, value in expected.items():
        unique_values = frame.get_column(field).unique().to_list()
        if unique_values != [value]:
            raise IngestionError(
                f"provider snapshot has inconsistent {field}: {unique_values!r}"
            )


def _retrieval_time(value: datetime | None) -> datetime:
    retrieval_time = value or datetime.now(UTC)
    if retrieval_time.tzinfo is None or retrieval_time.utcoffset() is None:
        raise IngestionError("retrieved_at must be timezone-aware")
    return retrieval_time.astimezone(UTC)


def _symbol_key(symbol: str) -> str:
    lowered = symbol.lower()
    readable = re.sub(r"[^a-z0-9._-]+", "-", lowered).strip("-._")
    readable = readable[:48] or "symbol"
    if readable == lowered and re.fullmatch(r"[a-z0-9][a-z0-9._-]*", readable):
        return readable
    digest = hashlib.sha256(symbol.encode()).hexdigest()[:8]
    return f"{readable}-{digest}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _remove_empty_parents(path: Path, boundary: Path) -> None:
    current = path
    while current != boundary:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
