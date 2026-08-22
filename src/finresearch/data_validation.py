"""Deep artifact validation and inspection for row-oriented Parquet snapshots."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    ValidationIssue,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    NORMALIZED_INSTRUMENT_MASTER_V2,
    DataContractError,
    DatasetContract,
    get_contract,
)

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
    declared_ids = {artifact.artifact_id for artifact in manifest.artifacts}
    issues: list[ValidationIssue] = []
    for artifact in _select_artifacts(manifest, artifact_id):
        issues.extend(
            _validate_artifact(
                case_dir,
                artifact,
                manifest.manifest_version,
                declared_ids,
                manifest,
            )
        )
    return tuple(issues)


def inspect_artifact(
    workspace: Path,
    case_id: str,
    artifact_id: str,
    limit: int,
) -> ArtifactInspection:
    """Deep-validate and inspect one declared Parquet artifact."""
    _validate_limit(limit)
    case_dir = _case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    selected = _select_artifacts(manifest, artifact_id)
    if len(selected) != 1:
        raise DataValidationError("inspect requires exactly one artifact id")
    if Path(selected[0].path).suffix != ".parquet":
        raise DataValidationError(
            f"data inspect supports Parquet artifacts only: {artifact_id} "
            f"({selected[0].path})"
        )
    issues = validate_artifact(workspace, case_id, artifact_id)
    if issues:
        details = "; ".join(f"[{issue.code}] {issue.message}" for issue in issues)
        raise DataValidationError(f"artifact failed validation: {details}")
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
        return manifest.artifacts
    selected = [
        artifact
        for artifact in manifest.artifacts
        if artifact.artifact_id == artifact_id
    ]
    if not selected:
        raise DataValidationError(f"artifact not declared: {artifact_id}")
    return tuple(selected)


def _validate_artifact(
    case_dir: Path,
    artifact: Artifact,
    manifest_version: int,
    declared_ids: set[str],
    manifest: CaseManifest,
) -> list[ValidationIssue]:
    """Apply common byte checks, then Parquet-specific dataset validation."""
    path = resolve_relative_path(
        case_dir,
        artifact.path,
        f"artifact {artifact.artifact_id}",
    )
    issues = _validate_input_file_hashes(
        case_dir,
        artifact,
        manifest_version,
    )
    if not path.is_file():
        issues.insert(
            0,
            ValidationIssue(
                "artifact_missing",
                f"artifact missing: {artifact.artifact_id} ({artifact.path})",
            ),
        )
        return issues
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

    if Path(artifact.path).suffix != ".parquet":
        return issues

    return [
        *issues,
        *_validate_parquet_artifact(
            case_dir,
            path,
            artifact,
            manifest_version,
            declared_ids,
            manifest,
        ),
    ]


def _validate_parquet_artifact(
    case_dir: Path,
    path: Path,
    artifact: Artifact,
    manifest_version: int,
    declared_ids: set[str],
    manifest: CaseManifest,
) -> list[ValidationIssue]:
    """Apply Parquet dataset and provenance checks after common integrity checks."""
    issues: list[ValidationIssue] = []
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

    if (
        contract_valid
        and not frame.is_empty()
        and "source_artifact_id" in frame.columns
    ):
        sources = frame.get_column("source_artifact_id").unique().to_list()
        if any(not isinstance(source, str) for source in sources):
            issues.append(
                ValidationIssue(
                    "lineage_invalid",
                    f"artifact {artifact.artifact_id} has null or non-string "
                    f"source_artifact_id: {sources!r}",
                )
            )
        else:
            observed_sources = {source for source in sources if isinstance(source, str)}
            allowed_sources = (
                set(artifact.input_artifact_ids)
                if manifest_version == MANIFEST_V2
                else declared_ids
            )
            undeclared_sources = sorted(observed_sources - allowed_sources)
            if undeclared_sources:
                scope = (
                    "manifest input_artifact_ids"
                    if manifest_version == MANIFEST_V2
                    else "declared artifact ids"
                )
                issues.append(
                    ValidationIssue(
                        "lineage_invalid",
                        f"artifact {artifact.artifact_id} source_artifact_id values "
                        f"{undeclared_sources!r} are not in {scope}",
                    )
                )
    if contract_valid:
        issues.extend(
            _validate_master_currency(
                case_dir=case_dir,
                artifact=artifact,
                frame=frame,
                manifest=manifest,
            )
        )
    return issues


def _validate_input_file_hashes(
    case_dir: Path,
    artifact: Artifact,
    manifest_version: int,
) -> list[ValidationIssue]:
    """Verify that every v2 declared input file still has its recorded bytes."""
    if manifest_version != MANIFEST_V2:
        return []
    issues: list[ValidationIssue] = []
    for input_file in artifact.input_file_hashes:
        path = resolve_relative_path(
            case_dir,
            input_file.path,
            f"artifact {artifact.artifact_id} input {input_file.name}",
        )
        if not path.is_file():
            issues.append(
                ValidationIssue(
                    "input_file_missing",
                    f"artifact {artifact.artifact_id} input file missing: "
                    f"{input_file.name} ({input_file.path})",
                )
            )
            continue
        actual = _sha256(path)
        if actual != input_file.sha256:
            issues.append(
                ValidationIssue(
                    "input_file_checksum_mismatch",
                    f"artifact {artifact.artifact_id} input file sha256 mismatch: "
                    f"{input_file.name} ({input_file.path}); manifest "
                    f"{input_file.sha256}, file {actual}",
                )
            )
    return issues


def _validate_master_currency(
    *,
    case_dir: Path,
    artifact: Artifact,
    frame: pl.DataFrame,
    manifest: CaseManifest,
) -> list[ValidationIssue]:
    """Compare every price/action key with all valid v2 master observations."""
    if artifact.kind not in {
        "normalized.daily-prices",
        "normalized.corporate-actions",
    } or artifact.schema_version not in {1, 2}:
        return []
    required = {"provider", "instrument_id", "currency"}
    if not required.issubset(frame.columns):
        return []
    expected_by_key: dict[tuple[str, str], set[str]] = {}
    for master in manifest.artifacts:
        if master.kind != "normalized.instrument-master" or master.schema_version != 2:
            continue
        master_path = resolve_relative_path(
            case_dir,
            master.path,
            f"artifact {master.artifact_id}",
        )
        if not master_path.is_file():
            continue
        try:
            master_frame = pl.read_parquet(master_path)
        except Exception:
            continue
        try:
            NORMALIZED_INSTRUMENT_MASTER_V2.validate(master_frame)
        except DataContractError:
            continue
        for row in master_frame.iter_rows(named=True):
            provider = row["provider"]
            instrument_id = row["instrument_id"]
            currency = row["trading_currency"]
            if all(
                isinstance(value, str) for value in (provider, instrument_id, currency)
            ):
                expected_by_key.setdefault((provider, instrument_id), set()).add(
                    currency
                )
    observed_by_key: dict[tuple[str, str], set[str]] = {}
    for row in frame.iter_rows(named=True):
        provider = row["provider"]
        instrument_id = row["instrument_id"]
        currency = row["currency"]
        if not isinstance(provider, str) or not isinstance(instrument_id, str):
            continue
        # Action rows can legitimately omit a currency and make no comparison
        # claim in that case.
        if isinstance(currency, str):
            observed_by_key.setdefault((provider, instrument_id), set()).add(currency)
    issues: list[ValidationIssue] = []
    for key in sorted(observed_by_key):
        expected = expected_by_key.get(key, set())
        observed = observed_by_key[key]
        if len(expected) == 1 and observed != expected:
            expected_currency = next(iter(expected))
            issues.append(
                ValidationIssue(
                    "currency_mismatch",
                    f"artifact {artifact.artifact_id} currency mismatch for "
                    f"{key[0]}:{key[1]}; expected {expected_currency!r}, "
                    f"observed {sorted(observed)!r}",
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
