"""Strict, explicit local-file imports into canonical normalized contracts."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, Literal

import polars as pl

from finresearch.cases import (
    MANIFEST_V1,
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    InputFileHash,
    canonical_parameters_sha256,
    case_directory,
    case_write_lock,
    read_manifest,
    resolve_relative_path,
    write_manifest,
)
from finresearch.data_contracts import (
    MODEL_COMPS_OBSERVATIONS_V1,
    NORMALIZED_CORPORATE_ACTIONS_V1,
    NORMALIZED_DAILY_PRICES_V2,
    NORMALIZED_ESTIMATES_V1,
    NORMALIZED_FUNDAMENTAL_FACTS_V2,
    NORMALIZED_FX_RATES_V1,
    NORMALIZED_INSTRUMENT_MASTER_V2,
    DataContractError,
    DatasetContract,
)
from finresearch.ingestion import (
    ArtifactIntegrityError,
    ArtifactPublicationReceipt,
    IngestionError,
)

ImportFormat = Literal["csv", "parquet"]
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_INTEGER = re.compile(r"-?(?:0|[1-9]\d*)")
_DECIMAL = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?")
_PROVIDER = re.compile(r"[a-z0-9](?:[a-z0-9_.-]*[a-z0-9])?")
IMPORT_PRODUCER: Final = "finresearch.data.import"
IMPORT_PRODUCER_VERSION: Final = "1"


@dataclass(frozen=True)
class LocalImportSchema:
    """One strict source projection and its one canonical output contract."""

    name: str
    contract: DatasetContract
    input_schema: pl.Schema
    generated_fields: frozenset[str]


@dataclass(frozen=True)
class LocalImportReceipt:
    """The preserved raw source and derived canonical artifact."""

    raw: ArtifactPublicationReceipt
    normalized: ArtifactPublicationReceipt


def _projection_schema(
    contract: DatasetContract,
    generated_fields: frozenset[str],
) -> pl.Schema:
    return pl.Schema(
        {
            name: dtype
            for name, dtype in contract.schema.items()
            if name not in generated_fields
        }
    )


def _schema(
    name: str,
    contract: DatasetContract,
    generated_fields: frozenset[str],
) -> LocalImportSchema:
    return LocalImportSchema(
        name=name,
        contract=contract,
        input_schema=_projection_schema(contract, generated_fields),
        generated_fields=generated_fields,
    )


IMPORT_SCHEMAS: Final[dict[str, LocalImportSchema]] = {
    "instrument-master.v2": _schema(
        "instrument-master.v2",
        NORMALIZED_INSTRUMENT_MASTER_V2,
        frozenset({"schema_version", "provider", "source_artifact_id"}),
    ),
    "daily-prices.v2": _schema(
        "daily-prices.v2",
        NORMALIZED_DAILY_PRICES_V2,
        frozenset(
            {"schema_version", "provider", "source_artifact_id", "normalized_at"}
        ),
    ),
    "fundamental-facts.v2": _schema(
        "fundamental-facts.v2",
        NORMALIZED_FUNDAMENTAL_FACTS_V2,
        frozenset(
            {"schema_version", "provider", "source_artifact_id", "normalized_at"}
        ),
    ),
    "estimates.v1": _schema(
        "estimates.v1",
        NORMALIZED_ESTIMATES_V1,
        frozenset({"schema_version", "provider", "source_artifact_id", "retrieved_at"}),
    ),
    "corporate-actions.v1": _schema(
        "corporate-actions.v1",
        NORMALIZED_CORPORATE_ACTIONS_V1,
        frozenset(
            {"schema_version", "provider", "source_artifact_id", "normalized_at"}
        ),
    ),
    "fx-rates.v1": _schema(
        "fx-rates.v1",
        NORMALIZED_FX_RATES_V1,
        frozenset(
            {"schema_version", "provider", "source_artifact_id", "normalized_at"}
        ),
    ),
    "model.comps-observations.v1": _schema(
        "model.comps-observations.v1",
        MODEL_COMPS_OBSERVATIONS_V1,
        frozenset(
            {"schema_version", "provider", "source_artifact_id", "normalized_at"}
        ),
    ),
}


def get_import_schema(name: str) -> LocalImportSchema:
    """Return a user-facing local-import schema by its stable name."""
    try:
        return IMPORT_SCHEMAS[name]
    except KeyError as exc:
        choices = ", ".join(sorted(IMPORT_SCHEMAS))
        raise IngestionError(
            f"unknown import schema {name!r}; expected one of: {choices}"
        ) from exc


def parse_import_timestamp(value: str) -> datetime:
    """Parse one explicit RFC3339 UTC timestamp without local-time fallback."""
    if _UTC_TIMESTAMP.fullmatch(value) is None:
        raise IngestionError(
            "retrieved_at must be an RFC3339 UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IngestionError(
            "retrieved_at must be an RFC3339 UTC timestamp ending in Z"
        ) from exc
    return parsed.astimezone(UTC)


def import_csv(
    workspace: Path,
    case_id: str,
    source_file: Path,
    *,
    schema_name: str,
    provider: str,
    retrieved_at: datetime,
) -> LocalImportReceipt:
    """Strictly import one UTF-8 CSV projection and preserve its source bytes."""
    return _import_local_file(
        workspace,
        case_id,
        source_file,
        schema_name=schema_name,
        provider=provider,
        retrieved_at=retrieved_at,
        source_format="csv",
    )


def import_parquet(
    workspace: Path,
    case_id: str,
    source_file: Path,
    *,
    schema_name: str,
    provider: str,
    retrieved_at: datetime,
) -> LocalImportReceipt:
    """Strictly import one exact-schema Parquet projection and preserve bytes."""
    return _import_local_file(
        workspace,
        case_id,
        source_file,
        schema_name=schema_name,
        provider=provider,
        retrieved_at=retrieved_at,
        source_format="parquet",
    )


def _import_local_file(
    workspace: Path,
    case_id: str,
    source_file: Path,
    *,
    schema_name: str,
    provider: str,
    retrieved_at: datetime,
    source_format: ImportFormat,
) -> LocalImportReceipt:
    spec = get_import_schema(schema_name)
    _validate_provider(provider)
    _require_utc(retrieved_at, "retrieved_at")
    if not source_file.is_file():
        raise IngestionError(f"import file must be a readable file: {source_file}")
    try:
        source_bytes = source_file.read_bytes()
    except OSError as exc:
        raise IngestionError(f"cannot read import file: {source_file}") from exc
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    source_frame = (
        _read_csv_projection(source_bytes, spec)
        if source_format == "csv"
        else _read_parquet_projection(source_bytes, spec)
    )

    raw_parameters = {
        "format": source_format,
        "provider": provider,
        "producer": IMPORT_PRODUCER,
        "producer_version": IMPORT_PRODUCER_VERSION,
        "retrieved_at": _format_utc(retrieved_at),
        "schema": spec.name,
        "source_sha256": source_sha256,
    }
    raw_identity = canonical_parameters_sha256(raw_parameters)
    raw_artifact_id = f"raw.import.{spec.name}.{raw_identity}"
    normalized_parameters = {
        "contract": spec.contract.identifier,
        "input_artifact_ids": [raw_artifact_id],
        "producer": IMPORT_PRODUCER,
        "producer_version": IMPORT_PRODUCER_VERSION,
        "raw_identity": raw_identity,
        "retrieved_at": _format_utc(retrieved_at),
        "schema": spec.name,
    }
    normalized_identity = canonical_parameters_sha256(normalized_parameters)
    canonical = _canonical_frame(
        source_frame,
        spec,
        provider=provider,
        retrieved_at=retrieved_at,
        raw_artifact_id=raw_artifact_id,
    )
    try:
        spec.contract.validate(canonical)
    except DataContractError as exc:
        raise IngestionError(
            f"import data does not satisfy {spec.contract.identifier}: {exc}"
        ) from exc
    canonical = spec.contract.canonical_sort(canonical)

    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    with case_write_lock(case_dir):
        manifest = read_manifest(case_dir)
        raw_path = Path(
            manifest.paths["raw"], "imports", spec.name, f"{raw_identity}.source"
        )
        normalized_path = Path(
            manifest.paths["normalized"],
            spec.contract.identifier,
            "imports",
            f"{normalized_identity}.parquet",
        )
        raw_output = resolve_relative_path(
            case_dir, raw_path.as_posix(), "raw import output"
        )
        normalized_output = resolve_relative_path(
            case_dir,
            normalized_path.as_posix(),
            "normalized import output",
        )
        raw_output.parent.mkdir(parents=True, exist_ok=True)
        normalized_output.parent.mkdir(parents=True, exist_ok=True)
        normalized_temp: Path | None = None
        raw_temp: Path | None = None
        created_raw = False
        created_normalized = False
        try:
            normalized_temp = _temporary_path(normalized_output)
            canonical.write_parquet(
                normalized_temp, compression="zstd", statistics=True
            )
            normalized_sha256 = _sha256(normalized_temp)
            raw_artifact = _raw_artifact(
                manifest,
                artifact_id=raw_artifact_id,
                path=raw_path.as_posix(),
                sha256=source_sha256,
                provider=provider,
                retrieved_at=retrieved_at,
                parameters_sha256=raw_identity,
            )
            normalized_artifact = _normalized_artifact(
                manifest,
                artifact_id=f"{spec.contract.name}.import.{normalized_identity}",
                contract=spec.contract,
                path=normalized_path.as_posix(),
                sha256=normalized_sha256,
                raw_artifact=raw_artifact,
                retrieved_at=retrieved_at,
                parameters_sha256=normalized_identity,
                row_count=canonical.height,
            )
            existing = _existing_import_receipts(
                manifest,
                raw_artifact,
                normalized_artifact,
                raw_output,
                normalized_output,
                retrieved_at,
            )
            if existing is not None:
                normalized_temp.unlink(missing_ok=True)
                return existing
            reuse_raw, reuse_normalized = _recover_import_orphans(
                manifest,
                raw_artifact,
                normalized_artifact,
                case_dir,
                raw_output,
                normalized_output,
            )

            if not reuse_raw:
                raw_temp = _temporary_path(raw_output)
                raw_temp.write_bytes(source_bytes)
                os.link(raw_temp, raw_output)
                created_raw = True
                raw_temp.unlink()
            if not reuse_normalized:
                os.link(normalized_temp, normalized_output)
                created_normalized = True
                normalized_temp.unlink()
            write_manifest(
                case_dir,
                CaseManifest(
                    manifest_version=manifest.manifest_version,
                    case_id=manifest.case_id,
                    title=manifest.title,
                    status=manifest.status,
                    paths=manifest.paths,
                    artifacts=(*manifest.artifacts, raw_artifact, normalized_artifact),
                ),
            )
            normalized_temp.unlink(missing_ok=True)
        except BaseException:
            created_raw = created_raw or _paths_share_inode(raw_temp, raw_output)
            created_normalized = created_normalized or _paths_share_inode(
                normalized_temp,
                normalized_output,
            )
            if raw_temp is not None:
                raw_temp.unlink(missing_ok=True)
            if normalized_temp is not None:
                normalized_temp.unlink(missing_ok=True)
            current = _registered_import_ids(
                case_dir,
                raw_artifact_id,
                f"{spec.contract.name}.import.{normalized_identity}",
            )
            if created_normalized and not current[1]:
                normalized_output.unlink(missing_ok=True)
            if created_raw and not current[0]:
                raw_output.unlink(missing_ok=True)
            raise

    return LocalImportReceipt(
        raw=ArtifactPublicationReceipt(
            artifact_id=raw_artifact_id,
            path=raw_output,
            sha256=source_sha256,
            produced_at=retrieved_at,
        ),
        normalized=ArtifactPublicationReceipt(
            artifact_id=normalized_artifact.artifact_id,
            path=normalized_output,
            sha256=normalized_sha256,
            produced_at=retrieved_at,
            row_count=canonical.height,
        ),
    )


def _read_csv_projection(source_bytes: bytes, spec: LocalImportSchema) -> pl.DataFrame:
    names = list(spec.input_schema)
    nullable = set(spec.input_schema) - set(spec.contract.non_nullable)
    rows: list[dict[str, object | None]] = []
    try:
        with io.StringIO(source_bytes.decode("utf-8"), newline="") as handle:
            reader = csv.reader(handle, strict=True)
            header = next(reader, None)
            if header != names:
                raise IngestionError(
                    f"CSV header for {spec.name} must exactly be: {', '.join(names)}"
                )
            for row_number, values in enumerate(reader, start=2):
                if len(values) != len(names):
                    raise IngestionError(
                        f"CSV row {row_number} has {len(values)} columns; "
                        f"expected {len(names)}"
                    )
                rows.append(
                    {
                        name: _parse_csv_value(
                            values[index],
                            spec.input_schema[name],
                            nullable=name in nullable,
                            field=name,
                            row_number=row_number,
                        )
                        for index, name in enumerate(names)
                    }
                )
    except UnicodeDecodeError as exc:
        raise IngestionError("CSV import must be valid UTF-8") from exc
    except csv.Error as exc:
        raise IngestionError(f"CSV import has invalid syntax: {exc}") from exc
    return pl.DataFrame(rows, schema=spec.input_schema)


def _parse_csv_value(
    value: str,
    dtype: pl.DataType,
    *,
    nullable: bool,
    field: str,
    row_number: int,
) -> object | None:
    if value == "":
        if nullable:
            return None
        raise IngestionError(f"CSV row {row_number} field {field} must not be empty")
    try:
        if dtype == pl.String:
            return value
        if dtype == pl.Date:
            if _DATE.fullmatch(value) is None:
                raise ValueError
            return date.fromisoformat(value)
        if dtype == pl.Datetime("us", "UTC"):
            if _UTC_TIMESTAMP.fullmatch(value) is None:
                raise ValueError
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        if dtype == pl.Int64 or dtype == pl.Int32 or dtype == pl.UInt16:
            if _INTEGER.fullmatch(value) is None:
                raise ValueError
            return int(value)
        if dtype == pl.Float64:
            if _DECIMAL.fullmatch(value) is None:
                raise ValueError
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError
            return parsed
    except ValueError:
        pass
    raise IngestionError(
        f"CSV row {row_number} field {field} has invalid {dtype} syntax: {value!r}"
    )


def _read_parquet_projection(
    source_bytes: bytes, spec: LocalImportSchema
) -> pl.DataFrame:
    try:
        frame = pl.read_parquet(io.BytesIO(source_bytes))
    except Exception as exc:
        raise IngestionError(f"cannot read import Parquet bytes: {exc}") from exc
    if frame.schema != spec.input_schema:
        raise IngestionError(
            f"Parquet schema for {spec.name} must exactly be {spec.input_schema}; "
            f"received {frame.schema}"
        )
    return frame


def _canonical_frame(
    source: pl.DataFrame,
    spec: LocalImportSchema,
    *,
    provider: str,
    retrieved_at: datetime,
    raw_artifact_id: str,
) -> pl.DataFrame:
    columns: list[pl.Expr] = []
    for name, dtype in spec.contract.schema.items():
        if name == "schema_version":
            columns.append(pl.lit(spec.contract.version, dtype=pl.UInt16).alias(name))
        elif name == "provider":
            columns.append(pl.lit(provider, dtype=pl.String).alias(name))
        elif name == "source_artifact_id":
            columns.append(pl.lit(raw_artifact_id, dtype=pl.String).alias(name))
        elif name == "normalized_at" or name == "retrieved_at":
            columns.append(pl.lit(retrieved_at, dtype=dtype).alias(name))
        else:
            columns.append(pl.col(name))
    return source.select(columns)


def _raw_artifact(
    manifest: CaseManifest,
    *,
    artifact_id: str,
    path: str,
    sha256: str,
    provider: str,
    retrieved_at: datetime,
    parameters_sha256: str,
) -> Artifact:
    if manifest.manifest_version == MANIFEST_V1:
        return Artifact(
            artifact_id=artifact_id,
            kind="raw.import",
            schema_version=1,
            path=path,
            source=provider,
            sha256=sha256,
            retrieved_at=_format_utc(retrieved_at),
        )
    if manifest.manifest_version == MANIFEST_V2:
        return Artifact(
            artifact_id=artifact_id,
            kind="raw.import",
            schema_version=1,
            path=path,
            sha256=sha256,
            retrieved_at=_format_utc(retrieved_at),
            producer=IMPORT_PRODUCER,
            producer_version=IMPORT_PRODUCER_VERSION,
            parameters_sha256=parameters_sha256,
        )
    raise CaseContractError("unsupported manifest version for import")


def _normalized_artifact(
    manifest: CaseManifest,
    *,
    artifact_id: str,
    contract: DatasetContract,
    path: str,
    sha256: str,
    raw_artifact: Artifact,
    retrieved_at: datetime,
    parameters_sha256: str,
    row_count: int,
) -> Artifact:
    if manifest.manifest_version == MANIFEST_V1:
        return Artifact(
            artifact_id=artifact_id,
            kind=contract.name,
            schema_version=contract.version,
            path=path,
            source=raw_artifact.artifact_id,
            sha256=sha256,
            retrieved_at=_format_utc(retrieved_at),
            row_count=row_count,
        )
    if manifest.manifest_version == MANIFEST_V2:
        return Artifact(
            artifact_id=artifact_id,
            kind=contract.name,
            schema_version=contract.version,
            path=path,
            sha256=sha256,
            retrieved_at=_format_utc(retrieved_at),
            row_count=row_count,
            input_artifact_ids=(raw_artifact.artifact_id,),
            producer=IMPORT_PRODUCER,
            producer_version=IMPORT_PRODUCER_VERSION,
            parameters_sha256=parameters_sha256,
            input_file_hashes=(
                InputFileHash(
                    name=f"artifact.{raw_artifact.artifact_id}",
                    path=raw_artifact.path,
                    sha256=raw_artifact.sha256 or "",
                ),
            ),
        )
    raise CaseContractError("unsupported manifest version for import")


def _existing_import_receipts(
    manifest: CaseManifest,
    raw: Artifact,
    normalized: Artifact,
    raw_path: Path,
    normalized_path: Path,
    retrieved_at: datetime,
) -> LocalImportReceipt | None:
    by_id = {artifact.artifact_id: artifact for artifact in manifest.artifacts}
    existing_raw = by_id.get(raw.artifact_id)
    existing_normalized = by_id.get(normalized.artifact_id)
    if existing_raw is None and existing_normalized is None:
        return None
    if existing_raw != raw or existing_normalized != normalized:
        raise ArtifactIntegrityError(
            "local import identity conflicts with existing metadata"
        )
    if not raw_path.is_file() or not normalized_path.is_file():
        raise ArtifactIntegrityError("declared local import artifact is missing")
    if _sha256(raw_path) != raw.sha256 or _sha256(normalized_path) != normalized.sha256:
        raise ArtifactIntegrityError(
            "declared local import artifact has conflicting bytes"
        )
    return LocalImportReceipt(
        raw=ArtifactPublicationReceipt(
            artifact_id=raw.artifact_id,
            path=raw_path,
            sha256=raw.sha256 or "",
            produced_at=retrieved_at,
        ),
        normalized=ArtifactPublicationReceipt(
            artifact_id=normalized.artifact_id,
            path=normalized_path,
            sha256=normalized.sha256 or "",
            produced_at=retrieved_at,
            row_count=normalized.row_count,
        ),
    )


def _recover_import_orphans(
    manifest: CaseManifest,
    raw: Artifact,
    normalized: Artifact,
    case_dir: Path,
    raw_path: Path,
    normalized_path: Path,
) -> tuple[bool, bool]:
    """Return matching undeclared output files safe to declare atomically.

    An interrupted publication can leave one or both expected files in place.
    Reuse is permitted only when neither identity nor path is declared and each
    existing regular file has the exact bytes this invocation just produced.
    """
    ids = {artifact.artifact_id for artifact in manifest.artifacts}
    paths = {artifact.path for artifact in manifest.artifacts}
    if (
        raw.artifact_id in ids
        or normalized.artifact_id in ids
        or raw.path in paths
        or normalized.path in paths
    ):
        raise ArtifactIntegrityError(
            "local import identity collides with existing artifact state"
        )
    reuse_raw = _matching_orphan(raw_path, case_dir, raw.sha256 or "", "raw")
    reuse_normalized = _matching_orphan(
        normalized_path,
        case_dir,
        normalized.sha256 or "",
        "normalized",
    )
    return reuse_raw, reuse_normalized


def _matching_orphan(
    path: Path,
    case_dir: Path,
    expected_sha256: str,
    label: str,
) -> bool:
    """Verify a pre-existing output is a safe, byte-identical orphan."""
    if not path.exists() and not path.is_symlink():
        return False
    try:
        resolved = path.resolve(strict=True)
        relative = resolved.is_relative_to(case_dir.resolve())
        mode = path.lstat().st_mode
    except OSError as exc:
        raise ArtifactIntegrityError(
            f"local import {label} orphan cannot be inspected"
        ) from exc
    if path.is_symlink() or not relative or not stat.S_ISREG(mode):
        raise ArtifactIntegrityError(
            f"local import {label} orphan is not a regular in-case file"
        )
    if _sha256(path) != expected_sha256:
        raise ArtifactIntegrityError(
            f"local import {label} orphan has conflicting bytes"
        )
    return True


def _temporary_path(output: Path) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=output.parent,
        prefix=f".{output.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    return Path(temporary_name)


def _registered_import_ids(
    case_dir: Path, raw_id: str, normalized_id: str
) -> tuple[bool, bool]:
    try:
        manifest = read_manifest(case_dir)
    except Exception:
        return True, True
    ids = {artifact.artifact_id for artifact in manifest.artifacts}
    return raw_id in ids, normalized_id in ids


def _paths_share_inode(left: Path | None, right: Path) -> bool:
    """Detect a hard-link publication when an interrupt lands inside os.link."""
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


def _validate_provider(provider: str) -> None:
    if _PROVIDER.fullmatch(provider) is None:
        raise IngestionError("provider must be a lowercase portable identifier")


def _require_utc(value: datetime, field: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise IngestionError(f"{field} must be timezone-aware UTC")


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
