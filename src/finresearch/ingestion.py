"""Immutable raw-data ingestion and case artifact registration."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import polars as pl

from finresearch.cases import (
    MANIFEST_V1,
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    InputFileHash,
    append_artifact,
    canonical_parameters_sha256,
    case_directory,
    case_write_lock,
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


class ArtifactIntegrityError(IngestionError):
    """Raised when a deterministic artifact identity has conflicting state."""


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


@dataclass(frozen=True)
class ArtifactPublicationReceipt:
    """Stable result metadata for a deterministic artifact of any file type."""

    artifact_id: str
    path: Path
    sha256: str
    produced_at: datetime
    row_count: int | None = None


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

    case_dir, manifest, _ = _case_raw_context(workspace, case_id)

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
        frame=frame,
        contract=RAW_YFINANCE_DAILY_PRICES_V1,
        provider="yfinance",
        path_parts=("yfinance", "daily-prices", symbol_key(normalized_symbol)),
        entity_key=symbol_key(normalized_symbol),
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
    case_dir, manifest, _ = _case_raw_context(workspace, case_id)
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
    case_dir, manifest, _ = _case_raw_context(workspace, case_id)
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
    frame: pl.DataFrame,
    contract: DatasetContract,
    provider: str,
    path_parts: tuple[str, ...],
    entity_key: str,
    retrieved_at: datetime,
) -> IngestionReceipt:
    """Persist one immutable snapshot below the manifest raw path role."""
    return persist_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="raw",
        frame=frame,
        contract=contract,
        provider=provider,
        path_parts=path_parts,
        entity_key=entity_key,
        retrieved_at=retrieved_at,
    )


def persist_snapshot(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    path_role: str,
    frame: pl.DataFrame,
    contract: DatasetContract,
    provider: str,
    path_parts: tuple[str, ...],
    entity_key: str,
    retrieved_at: datetime,
    source: str | None = None,
    producer: str = "finresearch.data.ingest",
    producer_version: str = "1",
    parameters: dict[str, object] | None = None,
) -> IngestionReceipt:
    """Atomically publish and register one immutable Parquet snapshot.

    Raw snapshots record the provider as their manifest source; normalized
    snapshots pass the raw artifact id as lineage through ``source``.
    """
    if manifest.manifest_version == MANIFEST_V2 and source is not None:
        raise CaseContractError("manifest v2 artifacts must not set legacy source")
    contract.validate(frame)
    timestamp_key = retrieved_at.strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = Path(
        manifest.paths[path_role],
        *path_parts,
        f"{timestamp_key}.parquet",
    )
    artifact_id = f"{contract.name}.{entity_key}.{timestamp_key.lower()}"
    root = resolve_relative_path(
        case_dir,
        manifest.paths[path_role],
        f"paths.{path_role}",
    )
    with case_write_lock(case_dir):
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
                _artifact_for_manifest(
                    current_manifest,
                    artifact_id=artifact_id,
                    kind=contract.name,
                    schema_version=contract.version,
                    path=relative_path.as_posix(),
                    sha256=sha256,
                    retrieved_at=_format_utc(retrieved_at),
                    row_count=frame.height,
                    source=source or provider,
                    producer=producer,
                    producer_version=producer_version,
                    parameters_sha256=canonical_parameters_sha256(
                        parameters
                        or {
                            "contract": contract.name,
                            "contract_version": contract.version,
                            "entity_key": entity_key,
                            "path_parts": list(path_parts),
                            "retrieved_at": _format_utc(retrieved_at),
                        }
                    ),
                    input_artifact_ids=(),
                    input_file_hashes=(),
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
                _remove_empty_parents(output_path.parent, root)
            raise

    return IngestionReceipt(
        artifact_id=artifact_id,
        path=output_path,
        row_count=frame.height,
        sha256=sha256,
        retrieved_at=retrieved_at,
    )


def publish_snapshot(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    path_role: str,
    frame: pl.DataFrame,
    contract: DatasetContract,
    path_parts: tuple[str, ...],
    entity_key: str,
    identity: str,
    producer: str,
    producer_version: str,
    parameters_sha256: str,
    input_artifact_ids: tuple[str, ...],
    produced_at: datetime,
    source: str | None = None,
    extra_input_file_hashes: tuple[InputFileHash, ...] = (),
) -> IngestionReceipt:
    """Publish a deterministic Parquet artifact and return its row receipt."""
    if manifest.manifest_version == MANIFEST_V2 and source is not None:
        raise CaseContractError("manifest v2 artifacts must not set legacy source")
    contract.validate(frame)
    relative_path = Path(
        manifest.paths[path_role],
        *path_parts,
        f"{identity}.parquet",
    )
    receipt = _publish_deterministic_artifact(
        case_dir=case_dir,
        manifest=manifest,
        artifact_id=f"{contract.name}.{entity_key}.{identity}",
        kind=contract.name,
        schema_version=contract.version,
        path_role=path_role,
        relative_path=relative_path,
        producer=producer,
        producer_version=producer_version,
        parameters_sha256=parameters_sha256,
        input_artifact_ids=input_artifact_ids,
        produced_at=produced_at,
        row_count=frame.height,
        write_temporary=lambda output: frame.write_parquet(
            output,
            compression="zstd",
            statistics=True,
        ),
        source=source,
        extra_input_file_hashes=extra_input_file_hashes,
    )
    return IngestionReceipt(
        artifact_id=receipt.artifact_id,
        path=receipt.path,
        row_count=frame.height,
        sha256=receipt.sha256,
        retrieved_at=produced_at,
    )


def publish_artifact_bytes(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    path_role: str,
    kind: str,
    schema_version: int,
    path_parts: tuple[str, ...],
    filename: str,
    entity_key: str,
    identity: str,
    content: bytes,
    producer: str,
    producer_version: str,
    parameters_sha256: str,
    input_artifact_ids: tuple[str, ...],
    produced_at: datetime,
    extra_input_file_hashes: tuple[InputFileHash, ...] = (),
) -> ArtifactPublicationReceipt:
    """Publish deterministic normalized, derived, or report bytes safely."""
    if not filename or "/" in filename or "\\" in filename:
        raise IngestionError("artifact filename must be one portable path component")
    if not isinstance(content, bytes):
        raise IngestionError("artifact content must be bytes")

    def write_content(output: Path) -> None:
        output.write_bytes(content)

    relative_path = Path(manifest.paths[path_role], *path_parts, filename)
    return _publish_deterministic_artifact(
        case_dir=case_dir,
        manifest=manifest,
        artifact_id=f"{kind}.{entity_key}.{identity}",
        kind=kind,
        schema_version=schema_version,
        path_role=path_role,
        relative_path=relative_path,
        producer=producer,
        producer_version=producer_version,
        parameters_sha256=parameters_sha256,
        input_artifact_ids=input_artifact_ids,
        produced_at=produced_at,
        row_count=None,
        write_temporary=write_content,
        extra_input_file_hashes=extra_input_file_hashes,
    )


def _publish_deterministic_artifact(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    artifact_id: str,
    kind: str,
    schema_version: int,
    path_role: str,
    relative_path: Path,
    producer: str,
    producer_version: str,
    parameters_sha256: str,
    input_artifact_ids: tuple[str, ...],
    produced_at: datetime,
    row_count: int | None,
    write_temporary: Callable[[Path], None],
    source: str | None = None,
    extra_input_file_hashes: tuple[InputFileHash, ...] = (),
) -> ArtifactPublicationReceipt:
    """Publish a deterministic artifact without overwriting prior state.

    The caller supplies a stable identity derived only from explicit inputs and
    producer parameters. An identical rerun returns the existing receipt;
    any byte or declaration mismatch at that identity is an integrity failure.
    """
    _require_utc_datetime(produced_at, "produced_at")
    root = resolve_relative_path(
        case_dir,
        manifest.paths[path_role],
        f"paths.{path_role}",
    )
    output_path = resolve_relative_path(
        case_dir,
        relative_path.as_posix(),
        "deterministic output",
    )
    with case_write_lock(case_dir):
        current_manifest = read_manifest(case_dir)
        if current_manifest.manifest_version != manifest.manifest_version:
            raise ArtifactIntegrityError(
                "manifest version changed while publishing deterministic artifact"
            )
        input_file_hashes = _input_file_hashes(
            case_dir,
            current_manifest,
            input_artifact_ids,
        )
        input_file_hashes = _merge_input_file_hashes(
            case_dir,
            input_file_hashes,
            extra_input_file_hashes,
        )
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
            write_temporary(temporary_path)
            sha256 = _sha256(temporary_path)
            expected = _artifact_for_manifest(
                current_manifest,
                artifact_id=artifact_id,
                kind=kind,
                schema_version=schema_version,
                path=relative_path.as_posix(),
                sha256=sha256,
                retrieved_at=_format_utc(produced_at),
                row_count=row_count,
                source=source,
                producer=producer,
                producer_version=producer_version,
                parameters_sha256=parameters_sha256,
                input_artifact_ids=input_artifact_ids,
                input_file_hashes=input_file_hashes,
            )
            same_id = next(
                (
                    artifact
                    for artifact in current_manifest.artifacts
                    if artifact.artifact_id == artifact_id
                ),
                None,
            )
            same_path = next(
                (
                    artifact
                    for artifact in current_manifest.artifacts
                    if artifact.path == relative_path.as_posix()
                ),
                None,
            )
            if same_id is not None or same_path is not None:
                if same_id != same_path or same_id is None:
                    raise ArtifactIntegrityError(
                        "deterministic artifact identity conflicts with an existing "
                        "artifact id or path"
                    )
                if same_id != expected:
                    raise ArtifactIntegrityError(
                        f"deterministic artifact metadata conflict: {artifact_id}"
                    )
                if not output_path.is_file():
                    raise ArtifactIntegrityError(
                        f"declared deterministic artifact is missing: {relative_path}"
                    )
                if _sha256(output_path) != sha256:
                    raise ArtifactIntegrityError(
                        f"deterministic artifact byte conflict: {artifact_id}"
                    )
                temporary_path.unlink()
                return ArtifactPublicationReceipt(
                    artifact_id=artifact_id,
                    path=output_path,
                    sha256=sha256,
                    produced_at=produced_at,
                    row_count=row_count,
                )
            if output_path.exists():
                raise ArtifactIntegrityError(
                    "deterministic artifact output already exists without a matching "
                    f"manifest declaration: {output_path}"
                )
            os.link(temporary_path, output_path)
            published = True
            temporary_path.unlink()
            append_artifact(case_dir, expected)
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
                _remove_empty_parents(output_path.parent, root)
            raise

    return ArtifactPublicationReceipt(
        artifact_id=artifact_id,
        path=output_path,
        sha256=sha256,
        produced_at=produced_at,
        row_count=row_count,
    )


def _artifact_for_manifest(
    manifest: CaseManifest,
    *,
    artifact_id: str,
    kind: str,
    schema_version: int,
    path: str,
    sha256: str,
    retrieved_at: str,
    row_count: int | None,
    source: str | None,
    producer: str,
    producer_version: str,
    parameters_sha256: str,
    input_artifact_ids: tuple[str, ...],
    input_file_hashes: tuple[InputFileHash, ...],
) -> Artifact:
    """Build the strict declaration shape required by the active manifest."""
    if manifest.manifest_version == MANIFEST_V1:
        return Artifact(
            artifact_id=artifact_id,
            kind=kind,
            schema_version=schema_version,
            path=path,
            source=source,
            sha256=sha256,
            retrieved_at=retrieved_at,
            row_count=row_count,
        )
    if manifest.manifest_version == MANIFEST_V2:
        return Artifact(
            artifact_id=artifact_id,
            kind=kind,
            schema_version=schema_version,
            path=path,
            sha256=sha256,
            retrieved_at=retrieved_at,
            row_count=row_count,
            input_artifact_ids=input_artifact_ids,
            producer=producer,
            producer_version=producer_version,
            parameters_sha256=parameters_sha256,
            input_file_hashes=input_file_hashes,
        )
    raise CaseContractError(
        f"unsupported manifest_version {manifest.manifest_version}; expected 1 or 2"
    )


def _input_file_hashes(
    case_dir: Path,
    manifest: CaseManifest,
    input_artifact_ids: tuple[str, ...],
) -> tuple[InputFileHash, ...]:
    """Record ordered parent bytes for v2 output declarations."""
    if manifest.manifest_version == MANIFEST_V1:
        return ()
    by_id = {artifact.artifact_id: artifact for artifact in manifest.artifacts}
    hashes: list[InputFileHash] = []
    for parent_id in input_artifact_ids:
        parent = by_id.get(parent_id)
        if parent is None:
            raise ArtifactIntegrityError(
                f"deterministic artifact input is not declared: {parent_id}"
            )
        parent_path = resolve_relative_path(
            case_dir,
            parent.path,
            f"input artifact {parent_id}",
        )
        if not parent_path.is_file():
            raise ArtifactIntegrityError(
                f"deterministic artifact input is missing: {parent.path}"
            )
        digest = _sha256(parent_path)
        if parent.sha256 is not None and parent.sha256 != digest:
            raise ArtifactIntegrityError(
                f"deterministic artifact input checksum mismatch: {parent_id}"
            )
        hashes.append(
            InputFileHash(
                name=f"artifact.{parent_id}",
                path=parent.path,
                sha256=digest,
            )
        )
    return tuple(hashes)


def _merge_input_file_hashes(
    case_dir: Path,
    parent_hashes: tuple[InputFileHash, ...],
    extra_hashes: tuple[InputFileHash, ...],
) -> tuple[InputFileHash, ...]:
    """Append validated non-artifact inputs without changing parent bindings."""
    names = {record.name for record in parent_hashes}
    paths = {record.path for record in parent_hashes}
    merged = list(parent_hashes)
    for record in extra_hashes:
        if record.name.startswith("artifact."):
            raise ArtifactIntegrityError(
                "extra input file hashes must not replace artifact parent bindings"
            )
        if record.name in names or record.path in paths:
            raise ArtifactIntegrityError("duplicate deterministic input file hash")
        path = resolve_relative_path(
            case_dir,
            record.path,
            f"extra input file {record.name}",
        )
        if not path.is_file() or _sha256(path) != record.sha256:
            raise ArtifactIntegrityError(
                f"extra deterministic input checksum mismatch: {record.name}"
            )
        names.add(record.name)
        paths.add(record.path)
        merged.append(record)
    return tuple(merged)


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
    currency_values = frame.get_column("currency").unique().to_list()
    if len(currency_values) != 1 or not isinstance(currency_values[0], str):
        raise IngestionError(
            f"provider snapshot has inconsistent currency: {currency_values!r}"
        )


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
    _require_utc_datetime(retrieval_time, "retrieved_at")
    return retrieval_time.astimezone(UTC)


def _require_utc_datetime(value: datetime, field: str) -> None:
    """Require an aware timestamp before normalizing it to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise IngestionError(f"{field} must be timezone-aware")


def symbol_key(symbol: str) -> str:
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
