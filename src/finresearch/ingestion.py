"""Immutable raw-data ingestion and case artifact registration."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import polars as pl
from filelock import FileLock

from finresearch.cases import (
    Artifact,
    CaseContractError,
    CaseManifest,
    append_artifact,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    RAW_SEC_COMPANYFACTS_V1,
    RAW_SEC_SUBMISSIONS_V1,
    RAW_YFINANCE_DAILY_PRICES_V1,
    DatasetContract,
)


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


class SECSubmissionsProvider(Protocol):
    """Boundary required by the SEC submissions ingestion workflow."""

    def fetch_submissions(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame: ...


class SECCompanyFactsProvider(Protocol):
    """Boundary required by the SEC companyfacts ingestion workflow."""

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
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

    case_dir, manifest, raw_root = _case_raw_context(workspace, case_id)

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

    return _persist_raw_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        raw_root=raw_root,
        frame=frame,
        contract=RAW_YFINANCE_DAILY_PRICES_V1,
        provider="yfinance",
        path_parts=("yfinance", "daily-prices", _symbol_key(normalized_symbol)),
        entity_key=_symbol_key(normalized_symbol),
        retrieved_at=retrieval_time,
    )


def ingest_sec_submissions(
    workspace: Path,
    case_id: str,
    cik: str,
    user_agent: str,
    *,
    provider: SECSubmissionsProvider | None = None,
    retrieved_at: datetime | None = None,
) -> IngestionReceipt:
    """Fetch and append one SEC recent-submissions snapshot."""
    from finresearch.providers.sec import (
        SECProvider,
        normalize_cik,
        submissions_url,
        validate_user_agent,
    )

    normalized_cik = normalize_cik(cik)
    declared_user_agent = validate_user_agent(user_agent)
    case_dir, manifest, raw_root = _case_raw_context(workspace, case_id)
    retrieval_time = _retrieval_time(retrieved_at)
    submissions_provider: SECSubmissionsProvider = provider or SECProvider()
    frame = submissions_provider.fetch_submissions(
        normalized_cik,
        declared_user_agent,
        retrieval_time,
    )
    RAW_SEC_SUBMISSIONS_V1.validate(frame)
    _validate_constant_metadata(
        frame,
        {
            "schema_version": RAW_SEC_SUBMISSIONS_V1.version,
            "provider": "sec",
            "cik": normalized_cik,
            "retrieved_at": retrieval_time,
            "source_url": submissions_url(normalized_cik),
        },
    )
    return _persist_raw_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        raw_root=raw_root,
        frame=frame,
        contract=RAW_SEC_SUBMISSIONS_V1,
        provider="sec",
        path_parts=("sec", "submissions", normalized_cik),
        entity_key=normalized_cik,
        retrieved_at=retrieval_time,
    )


def ingest_sec_companyfacts(
    workspace: Path,
    case_id: str,
    cik: str,
    user_agent: str,
    *,
    provider: SECCompanyFactsProvider | None = None,
    retrieved_at: datetime | None = None,
) -> IngestionReceipt:
    """Fetch and append one SEC companyfacts XBRL snapshot."""
    from finresearch.providers.sec import (
        SECProvider,
        companyfacts_url,
        normalize_cik,
        validate_user_agent,
    )

    normalized_cik = normalize_cik(cik)
    declared_user_agent = validate_user_agent(user_agent)
    case_dir, manifest, raw_root = _case_raw_context(workspace, case_id)
    retrieval_time = _retrieval_time(retrieved_at)
    facts_provider: SECCompanyFactsProvider = provider or SECProvider()
    frame = facts_provider.fetch_companyfacts(
        normalized_cik,
        declared_user_agent,
        retrieval_time,
    )
    RAW_SEC_COMPANYFACTS_V1.validate(frame)
    _validate_constant_metadata(
        frame,
        {
            "schema_version": RAW_SEC_COMPANYFACTS_V1.version,
            "provider": "sec",
            "cik": normalized_cik,
            "retrieved_at": retrieval_time,
            "source_url": companyfacts_url(normalized_cik),
        },
    )
    return _persist_raw_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        raw_root=raw_root,
        frame=frame,
        contract=RAW_SEC_COMPANYFACTS_V1,
        provider="sec",
        path_parts=("sec", "companyfacts", normalized_cik),
        entity_key=normalized_cik,
        retrieved_at=retrieval_time,
    )


def _case_raw_context(workspace: Path, case_id: str) -> tuple[Path, CaseManifest, Path]:
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    raw_root = resolve_relative_path(case_dir, manifest.paths["raw"], "paths.raw")
    if not raw_root.is_dir():
        raise CaseContractError(f"required raw directory missing: {raw_root}")
    return case_dir, manifest, raw_root


def _persist_raw_snapshot(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    raw_root: Path,
    frame: pl.DataFrame,
    contract: DatasetContract,
    provider: str,
    path_parts: tuple[str, ...],
    entity_key: str,
    retrieved_at: datetime,
) -> IngestionReceipt:
    contract.validate(frame)
    timestamp_key = retrieved_at.strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = Path(
        manifest.paths["raw"],
        *path_parts,
        f"{timestamp_key}.parquet",
    )
    artifact_id = f"{contract.name}.{entity_key}.{timestamp_key.lower()}"
    with _case_write_lock(case_dir):
        current_manifest = read_manifest(case_dir)
        if any(
            artifact.artifact_id == artifact_id
            or artifact.path == relative_path.as_posix()
            for artifact in current_manifest.artifacts
        ):
            raise IngestionError(
                "raw snapshot already exists or is declared in the manifest: "
                f"{relative_path.as_posix()}"
            )
        output_path = resolve_relative_path(
            case_dir,
            relative_path.as_posix(),
            "raw output",
        )
        if output_path.exists():
            raise IngestionError(f"raw snapshot already exists: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path: Path | None = None
        published = False
        try:
            file_descriptor, temporary_name = tempfile.mkstemp(
                dir=output_path.parent,
                prefix=f".{output_path.name}.",
                suffix=".tmp",
            )
            temporary_path = Path(temporary_name)
            os.close(file_descriptor)
            frame.write_parquet(
                temporary_path,
                compression="zstd",
                statistics=True,
            )
            sha256 = _sha256(temporary_path)
            try:
                os.link(temporary_path, output_path)
            except FileExistsError as exc:
                raise IngestionError(
                    f"raw snapshot already exists: {output_path}"
                ) from exc
            published = True
            temporary_path.unlink()
            append_artifact(
                case_dir,
                Artifact(
                    artifact_id=artifact_id,
                    kind=contract.name,
                    schema_version=contract.version,
                    path=relative_path.as_posix(),
                    source=provider,
                    sha256=sha256,
                    retrieved_at=_format_utc(retrieved_at),
                    row_count=frame.height,
                ),
            )
        except BaseException:
            owns_published_output = published or _paths_share_inode(
                temporary_path,
                output_path,
            )
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            if owns_published_output and not _artifact_is_registered(
                case_dir,
                artifact_id,
                relative_path.as_posix(),
            ):
                output_path.unlink(missing_ok=True)
                _remove_empty_parents(output_path.parent, raw_root)
            raise

    return IngestionReceipt(
        artifact_id=artifact_id,
        path=output_path,
        row_count=frame.height,
        sha256=sha256,
        retrieved_at=retrieved_at,
    )


@contextmanager
def _case_write_lock(case_dir: Path) -> Iterator[None]:
    """Serialize artifact publication and manifest updates per case."""
    lock_path = resolve_relative_path(case_dir, ".finresearch.lock", "case lock")
    with FileLock(lock_path):
        yield


def _artifact_is_registered(
    case_dir: Path,
    artifact_id: str,
    relative_path: str,
) -> bool:
    """Conservatively detect whether manifest publication already succeeded."""
    try:
        manifest = read_manifest(case_dir)
    except Exception:
        # Never delete a published immutable file when registration state is
        # unreadable or ambiguous; later validation can surface an orphan.
        return True
    return any(
        artifact.artifact_id == artifact_id and artifact.path == relative_path
        for artifact in manifest.artifacts
    )


def _paths_share_inode(left: Path | None, right: Path) -> bool:
    """Detect publication if cancellation lands immediately after hard-linking."""
    if left is None:
        return False
    try:
        left_stat = left.stat()
        right_stat = right.lstat()
    except OSError:
        return False
    return (left_stat.st_dev, left_stat.st_ino) == (
        right_stat.st_dev,
        right_stat.st_ino,
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
    _validate_constant_metadata(frame, expected)


def _validate_constant_metadata(
    frame: pl.DataFrame,
    expected: dict[str, object],
) -> None:
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
