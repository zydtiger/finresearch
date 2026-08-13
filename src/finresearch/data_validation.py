"""Deep artifact validation and inspection for row-oriented Parquet snapshots."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from finresearch.cases import (
    Artifact,
    CaseContractError,
    CaseManifest,
    ValidationIssue,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import DataContractError, DatasetContract, get_contract

MAX_PREVIEW_ROWS = 100


class DataValidationError(RuntimeError):
    """Raised when an artifact cannot be validated or inspected at all."""


@dataclass(frozen=True)
class ColumnSummary:
    """One column name and its compact Polars dtype label."""

    name: str
    dtype: str


@dataclass(frozen=True)
class DateRange:
    """Observed minimum and maximum for one date or timestamp column."""

    name: str
    minimum: str | None
    maximum: str | None


@dataclass(frozen=True)
class NullSummary:
    """Null count for one column that contains missing values."""

    name: str
    count: int


@dataclass(frozen=True)
class ArtifactInspection:
    """File, schema, provenance, and preview facts for one artifact."""

    artifact_id: str
    contract_identifier: str
    path: str
    size: int
    sha256: str
    row_count: int
    columns: tuple[ColumnSummary, ...]
    date_ranges: tuple[DateRange, ...]
    nulls: tuple[NullSummary, ...]
    duplicate_key_rows: int | None
    provider: str | None
    provider_symbol: str | None
    cik: str | None
    source_url: str | None
    preview_json: str | None


def validate_artifact(
    workspace: Path,
    case_id: str,
    artifact_id: str | None = None,
) -> tuple[ValidationIssue, ...]:
    """Deep-validate declared artifacts; return issues instead of raising."""
    case_dir = _case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    issues: list[ValidationIssue] = []
    for artifact in _select_artifacts(manifest, artifact_id):
        issues.extend(_validate_artifact(case_dir, artifact))
    return tuple(issues)


def inspect_artifact(
    workspace: Path,
    case_id: str,
    artifact_id: str,
    limit: int,
) -> ArtifactInspection:
    """Deep-validate and inspect one declared Parquet artifact."""
    _validate_limit(limit)
    issues = validate_artifact(workspace, case_id, artifact_id)
    if issues:
        details = "; ".join(f"[{issue.code}] {issue.message}" for issue in issues)
        raise DataValidationError(f"artifact failed validation: {details}")
    case_dir = _case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    selected = _select_artifacts(manifest, artifact_id)
    if len(selected) != 1:
        raise DataValidationError("inspect requires exactly one artifact id")
    return _inspect_artifact(case_dir, selected[0], limit)


def _case_directory(workspace: Path, case_id: str) -> Path:
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    return case_dir


def _select_artifacts(
    manifest: CaseManifest,
    artifact_id: str | None,
) -> tuple[Artifact, ...]:
    if artifact_id is None:
        return tuple(
            artifact
            for artifact in manifest.artifacts
            if Path(artifact.path).suffix == ".parquet"
        )
    selected = [
        artifact
        for artifact in manifest.artifacts
        if artifact.artifact_id == artifact_id
    ]
    if not selected:
        raise DataValidationError(f"artifact not declared: {artifact_id}")
    if Path(selected[0].path).suffix != ".parquet":
        raise DataValidationError(
            f"unsupported non-Parquet artifact: {artifact_id} ({selected[0].path})"
        )
    return tuple(selected)


def _validate_artifact(case_dir: Path, artifact: Artifact) -> list[ValidationIssue]:
    path = resolve_relative_path(
        case_dir,
        artifact.path,
        f"artifact {artifact.artifact_id}",
    )
    if not path.is_file():
        return [
            ValidationIssue(
                "artifact_missing",
                f"artifact missing: {artifact.artifact_id} ({artifact.path})",
            )
        ]

    issues: list[ValidationIssue] = []
    if artifact.sha256 is not None:
        actual = _sha256(path)
        if actual != artifact.sha256:
            issues.append(
                ValidationIssue(
                    "checksum_mismatch",
                    f"artifact {artifact.artifact_id} sha256 mismatch: "
                    f"manifest {artifact.sha256}, file {actual}",
                )
            )

    try:
        frame = pl.read_parquet(path)
    except Exception as exc:
        issues.append(
            ValidationIssue(
                "unreadable",
                f"artifact {artifact.artifact_id} is not a readable parquet "
                f"snapshot: {exc}",
            )
        )
        return issues

    if artifact.row_count is not None and frame.height != artifact.row_count:
        issues.append(
            ValidationIssue(
                "row_count_mismatch",
                f"artifact {artifact.artifact_id} row count mismatch: "
                f"manifest {artifact.row_count}, parquet {frame.height}",
            )
        )

    contract = _registered_contract(artifact)
    if contract is None:
        issues.append(
            ValidationIssue(
                "unknown_contract",
                f"artifact {artifact.artifact_id} kind {artifact.kind!r} version "
                f"{artifact.schema_version} is not a registered dataset contract",
            )
        )
        return issues

    contract_valid = True
    try:
        contract.validate(frame)
    except DataContractError as exc:
        contract_valid = False
        issues.append(
            ValidationIssue(
                "contract_violation",
                f"artifact {artifact.artifact_id}: {exc}",
            )
        )

    if (
        contract_valid
        and artifact.retrieved_at is not None
        and "retrieved_at" in frame.columns
    ):
        expected = _parse_utc(artifact.retrieved_at)
        values = frame.get_column("retrieved_at").unique().to_list()
        if values != [expected]:
            issues.append(
                ValidationIssue(
                    "provenance_mismatch",
                    f"artifact {artifact.artifact_id} manifest retrieved_at "
                    f"differs from the snapshot provenance column",
                )
            )
    return issues


def _inspect_artifact(
    case_dir: Path,
    artifact: Artifact,
    limit: int,
) -> ArtifactInspection:
    path = resolve_relative_path(
        case_dir,
        artifact.path,
        f"artifact {artifact.artifact_id}",
    )
    if not path.is_file():
        raise DataValidationError(
            f"artifact file missing: {artifact.artifact_id} ({artifact.path})"
        )
    try:
        frame = pl.read_parquet(path)
    except Exception as exc:
        raise DataValidationError(
            f"cannot read parquet artifact {artifact.artifact_id}: {exc}"
        ) from exc

    contract_identifier = f"{artifact.kind}.v{artifact.schema_version}"
    contract = _registered_contract(artifact)
    if contract is None:
        raise DataValidationError(f"unknown dataset contract: {contract_identifier}")
    return ArtifactInspection(
        artifact_id=artifact.artifact_id,
        contract_identifier=contract_identifier,
        path=artifact.path,
        size=path.stat().st_size,
        sha256=_sha256(path),
        row_count=frame.height,
        columns=tuple(
            ColumnSummary(name, _dtype_label(dtype))
            for name, dtype in frame.schema.items()
        ),
        date_ranges=_date_ranges(frame),
        nulls=_null_summary(frame),
        duplicate_key_rows=_duplicate_key_rows(frame, contract),
        provider=_constant_text(frame, "provider"),
        provider_symbol=_constant_text(frame, "provider_symbol"),
        cik=_constant_text(frame, "cik"),
        source_url=_constant_text(frame, "source_url"),
        preview_json=frame.head(limit).write_json() if limit else None,
    )


def _validate_limit(limit: int) -> None:
    if limit < 0:
        raise DataValidationError("inspect limit must not be negative")
    if limit > MAX_PREVIEW_ROWS:
        raise DataValidationError(
            f"inspect limit must not exceed {MAX_PREVIEW_ROWS}: {limit}"
        )


def _registered_contract(artifact: Artifact) -> DatasetContract | None:
    identifier = f"{artifact.kind}.v{artifact.schema_version}"
    try:
        return get_contract(identifier)
    except DataContractError:
        return None


def _date_ranges(frame: pl.DataFrame) -> tuple[DateRange, ...]:
    ranges: list[DateRange] = []
    for name, dtype in frame.schema.items():
        if not isinstance(dtype, (pl.Date, pl.Datetime)):
            continue
        column = frame.get_column(name)
        ranges.append(
            DateRange(
                name=name,
                minimum=_format_scalar(column.min()),
                maximum=_format_scalar(column.max()),
            )
        )
    return tuple(ranges)


def _null_summary(frame: pl.DataFrame) -> tuple[NullSummary, ...]:
    counts = frame.null_count().row(0)
    return tuple(
        NullSummary(name=name, count=count)
        for name, count in zip(frame.columns, counts, strict=True)
        if count
    )


def _duplicate_key_rows(
    frame: pl.DataFrame,
    contract: DatasetContract | None,
) -> int | None:
    if contract is None or not contract.unique_key:
        return None
    return int(frame.select(contract.unique_key).is_duplicated().sum())


def _constant_text(frame: pl.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    values = frame.get_column(column).unique().to_list()
    if len(values) != 1 or not isinstance(values[0], str):
        return None
    return values[0]


def _parse_utc(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise DataValidationError(f"invalid retrieved_at timestamp: {value!r}") from exc


def _format_scalar(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def _dtype_label(dtype: object) -> str:
    if isinstance(dtype, pl.Datetime):
        zone = f", {dtype.time_zone}" if dtype.time_zone else ""
        return f"Datetime({dtype.time_unit}{zone})"
    if isinstance(dtype, pl.List):
        return f"List({_dtype_label(dtype.inner)})"
    return str(dtype)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
