"""Case directory and manifest contracts."""

from __future__ import annotations

import re
import shutil
import tempfile
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Final

import tomli_w

MANIFEST_FILENAME: Final = "manifest.toml"
MANIFEST_VERSION: Final = 1
CASE_ID_PATTERN: Final = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
ARTIFACT_NAME_PATTERN: Final = re.compile(r"[a-z0-9](?:[a-z0-9_.-]*[a-z0-9])?")
SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}")
CASE_STATUSES: Final = frozenset({"active", "paused", "completed", "archived"})

DEFAULT_PATHS: Final[dict[str, str]] = {
    "registers": "registers",
    "raw": "data/raw",
    "normalized": "data/normalized",
    "derived": "data/derived",
    "analysis": "analysis",
    "reports": "reports",
}
REQUIRED_PATH_ROLES: Final = ("raw", "normalized", "derived", "reports")


class CaseContractError(ValueError):
    """Raised when a case cannot satisfy the v1 contract."""


@dataclass(frozen=True)
class Artifact:
    """An artifact declared by a case manifest."""

    artifact_id: str
    kind: str
    schema_version: int
    path: str
    source: str | None = None
    sha256: str | None = None
    retrieved_at: str | None = None
    row_count: int | None = None


@dataclass(frozen=True)
class CaseManifest:
    """The supported v1 case manifest."""

    manifest_version: int
    case_id: str
    title: str
    status: str
    paths: Mapping[str, str]
    artifacts: tuple[Artifact, ...]


@dataclass(frozen=True)
class ValidationIssue:
    """A validation problem suitable for CLI display."""

    code: str
    message: str


@dataclass(frozen=True)
class CaseStatus:
    """Observed state of a case and its declared artifacts."""

    case_id: str
    case_dir: Path
    manifest_status: str | None
    required_directories_present: int
    required_directories_total: int
    artifacts_declared: int
    artifacts_present: int
    artifacts_missing: int
    issues: tuple[ValidationIssue, ...]

    @property
    def valid(self) -> bool:
        """Return whether the case passes the complete v1 validation."""
        return not self.issues


def validate_case_id(case_id: str) -> str:
    """Validate and return a stable, path-safe case identifier."""
    if CASE_ID_PATTERN.fullmatch(case_id) is None:
        raise CaseContractError(
            "case ID must be 1-64 lowercase letters, digits, or hyphens, "
            "and must start and end with a letter or digit"
        )
    return case_id


def case_directory(workspace: Path, case_id: str) -> Path:
    """Return the case directory below an explicit workspace."""
    case_dir = workspace / "cases" / validate_case_id(case_id)
    if not case_dir.resolve(strict=False).is_relative_to(workspace.resolve()):
        raise CaseContractError("case directory escapes the workspace")
    return case_dir


def initialize_case(workspace: Path, case_id: str, title: str | None = None) -> Path:
    """Create a v1 case without overwriting an existing path."""
    validated_id = validate_case_id(case_id)
    case_title = title.strip() if title is not None else validated_id
    if not case_title:
        raise CaseContractError("case title must not be empty")
    if len(case_title) > 200:
        raise CaseContractError("case title must be 200 characters or fewer")

    cases_dir = workspace / "cases"
    if cases_dir.exists() and not cases_dir.is_dir():
        raise CaseContractError(f"cases path is not a directory: {cases_dir}")
    cases_dir.mkdir(parents=True, exist_ok=True)

    case_dir = case_directory(workspace, validated_id)
    try:
        case_dir.mkdir()
    except FileExistsError as exc:
        raise CaseContractError(f"case already exists: {validated_id}") from exc

    try:
        for role in REQUIRED_PATH_ROLES:
            (case_dir / DEFAULT_PATHS[role]).mkdir(parents=True)
        write_manifest(case_dir, new_manifest(validated_id, case_title))
    except Exception:
        shutil.rmtree(case_dir)
        raise
    return case_dir


def new_manifest(case_id: str, title: str) -> CaseManifest:
    """Build an empty v1 manifest for a newly initialized case."""
    return CaseManifest(
        manifest_version=MANIFEST_VERSION,
        case_id=case_id,
        title=title,
        status="active",
        paths=dict(DEFAULT_PATHS),
        artifacts=(),
    )


def write_manifest(case_dir: Path, manifest: CaseManifest) -> None:
    """Atomically write a manifest in deterministic field order."""
    payload: dict[str, object] = {
        "manifest_version": manifest.manifest_version,
        "case_id": manifest.case_id,
        "title": manifest.title,
        "status": manifest.status,
        "artifacts": [artifact_to_dict(item) for item in manifest.artifacts],
        "paths": dict(manifest.paths),
    }
    manifest_path = resolve_relative_path(case_dir, MANIFEST_FILENAME, "manifest")
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=case_dir,
        prefix=".manifest.",
        suffix=".tmp",
        delete=False,
    ) as file_handle:
        temporary_path = Path(file_handle.name)
        try:
            file_handle.write(tomli_w.dumps(payload))
            file_handle.flush()
            temporary_path.replace(manifest_path)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise


def artifact_to_dict(artifact: Artifact) -> dict[str, object]:
    """Convert an artifact to its TOML representation."""
    data: dict[str, object] = {
        "id": artifact.artifact_id,
        "kind": artifact.kind,
        "schema_version": artifact.schema_version,
        "path": artifact.path,
    }
    if artifact.source is not None:
        data["source"] = artifact.source
    if artifact.sha256 is not None:
        data["sha256"] = artifact.sha256
    if artifact.retrieved_at is not None:
        data["retrieved_at"] = artifact.retrieved_at
    if artifact.row_count is not None:
        data["row_count"] = artifact.row_count
    return data


def append_artifact(case_dir: Path, artifact: Artifact) -> CaseManifest:
    """Validate and append one artifact without replacing existing entries."""
    manifest = read_manifest(case_dir)
    payload: dict[str, object] = {
        "manifest_version": manifest.manifest_version,
        "case_id": manifest.case_id,
        "title": manifest.title,
        "status": manifest.status,
        "artifacts": [
            artifact_to_dict(item) for item in (*manifest.artifacts, artifact)
        ],
        "paths": dict(manifest.paths),
    }
    updated = parse_manifest(payload, case_dir)
    write_manifest(case_dir, updated)
    return updated


def read_manifest(case_dir: Path) -> CaseManifest:
    """Read and parse a case manifest."""
    manifest_path = resolve_relative_path(case_dir, MANIFEST_FILENAME, "manifest")
    if not manifest_path.is_file():
        raise CaseContractError(f"manifest not found: {manifest_path}")
    try:
        with manifest_path.open("rb") as file_handle:
            data = tomllib.load(file_handle)
    except tomllib.TOMLDecodeError as exc:
        raise CaseContractError(f"invalid TOML in {manifest_path}: {exc}") from exc
    return parse_manifest(data, case_dir)


def parse_manifest(data: Mapping[str, object], case_dir: Path) -> CaseManifest:
    """Parse and validate the structural v1 manifest contract."""
    validate_table_keys(
        data,
        required={
            "manifest_version",
            "case_id",
            "title",
            "status",
            "artifacts",
            "paths",
        },
        field="manifest",
    )
    version = require_integer(data, "manifest_version")
    if version != MANIFEST_VERSION:
        raise CaseContractError(
            f"unsupported manifest_version {version}; expected {MANIFEST_VERSION}"
        )

    case_id = validate_case_id(require_string(data, "case_id"))
    if case_id != case_dir.name:
        raise CaseContractError(
            f"manifest case_id {case_id!r} does not match directory {case_dir.name!r}"
        )

    title = require_string(data, "title").strip()
    if not title:
        raise CaseContractError("manifest title must not be empty")
    if len(title) > 200:
        raise CaseContractError("manifest title must be 200 characters or fewer")

    status = require_string(data, "status")
    if status not in CASE_STATUSES:
        allowed = ", ".join(sorted(CASE_STATUSES))
        raise CaseContractError(
            f"unsupported case status {status!r}; expected {allowed}"
        )

    paths_value = data.get("paths")
    if not isinstance(paths_value, dict):
        raise CaseContractError("manifest paths must be a TOML table")
    paths = parse_paths(paths_value, case_dir)

    artifacts_value = data.get("artifacts")
    if not isinstance(artifacts_value, list):
        raise CaseContractError("manifest artifacts must be an array")
    artifacts = parse_artifacts(artifacts_value, case_dir, paths)

    return CaseManifest(
        manifest_version=version,
        case_id=case_id,
        title=title,
        status=status,
        paths=paths,
        artifacts=artifacts,
    )


def parse_paths(data: Mapping[str, object], case_dir: Path) -> dict[str, str]:
    """Validate the complete path-role table for manifest v1."""
    expected = set(DEFAULT_PATHS)
    actual = set(data)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected: {', '.join(unexpected)}")
        raise CaseContractError(f"invalid manifest paths ({'; '.join(details)})")

    paths: dict[str, str] = {}
    resolved: set[Path] = set()
    for role in DEFAULT_PATHS:
        value = data.get(role)
        if not isinstance(value, str):
            raise CaseContractError(f"manifest path {role!r} must be a string")
        resolved_path = resolve_relative_path(case_dir, value, f"paths.{role}")
        if resolved_path in resolved:
            raise CaseContractError(f"manifest path {value!r} is used more than once")
        paths[role] = value
        resolved.add(resolved_path)
    return paths


def parse_artifacts(
    items: list[object],
    case_dir: Path,
    paths: Mapping[str, str],
) -> tuple[Artifact, ...]:
    """Validate artifacts and their independent schema versions."""
    artifacts: list[Artifact] = []
    artifact_ids: set[str] = set()
    artifact_paths: set[Path] = set()
    allowed_roots = {
        resolve_relative_path(case_dir, value, f"paths.{role}")
        for role, value in paths.items()
    }

    for index, item in enumerate(items):
        field = f"artifacts[{index}]"
        if not isinstance(item, dict):
            raise CaseContractError(f"{field} must be a TOML table")
        validate_table_keys(
            item,
            required={"id", "kind", "schema_version", "path"},
            optional={"source", "sha256", "retrieved_at", "row_count"},
            field=field,
        )
        artifact_id = require_named_string(item, "id", field)
        if ARTIFACT_NAME_PATTERN.fullmatch(artifact_id) is None:
            raise CaseContractError(f"{field}.id has an invalid format")
        if artifact_id in artifact_ids:
            raise CaseContractError(f"duplicate artifact id: {artifact_id}")

        kind = require_named_string(item, "kind", field)
        if ARTIFACT_NAME_PATTERN.fullmatch(kind) is None:
            raise CaseContractError(f"{field}.kind has an invalid format")

        schema_version = require_named_integer(item, "schema_version", field)
        if schema_version < 1:
            raise CaseContractError(f"{field}.schema_version must be positive")

        path = require_named_string(item, "path", field)
        resolved_path = resolve_relative_path(case_dir, path, f"{field}.path")
        if resolved_path in artifact_paths:
            raise CaseContractError(f"duplicate artifact path: {path}")
        if not any(root in resolved_path.parents for root in allowed_roots):
            raise CaseContractError(
                f"{field}.path must be inside a directory declared by manifest paths"
            )

        source = optional_named_string(item, "source", field)
        sha256 = optional_named_string(item, "sha256", field)
        if sha256 is not None and SHA256_PATTERN.fullmatch(sha256) is None:
            raise CaseContractError(f"{field}.sha256 must be 64 lowercase hex digits")

        retrieved_at = optional_named_string(item, "retrieved_at", field)
        if retrieved_at is not None:
            validate_utc_timestamp(retrieved_at, f"{field}.retrieved_at")

        row_count = optional_named_integer(item, "row_count", field)
        if row_count is not None and row_count < 0:
            raise CaseContractError(f"{field}.row_count must not be negative")

        artifacts.append(
            Artifact(
                artifact_id=artifact_id,
                kind=kind,
                schema_version=schema_version,
                path=path,
                source=source,
                sha256=sha256,
                retrieved_at=retrieved_at,
                row_count=row_count,
            )
        )
        artifact_ids.add(artifact_id)
        artifact_paths.add(resolved_path)
    return tuple(artifacts)


def inspect_case(workspace: Path, case_id: str) -> CaseStatus:
    """Inspect manifest, directory, and declared artifact state."""
    case_dir = case_directory(workspace, case_id)
    issues: list[ValidationIssue] = []
    if not case_dir.is_dir():
        issues.append(ValidationIssue("case_missing", f"case not found: {case_id}"))
        return CaseStatus(
            case_id=case_id,
            case_dir=case_dir,
            manifest_status=None,
            required_directories_present=0,
            required_directories_total=len(REQUIRED_PATH_ROLES),
            artifacts_declared=0,
            artifacts_present=0,
            artifacts_missing=0,
            issues=tuple(issues),
        )

    try:
        manifest = read_manifest(case_dir)
    except CaseContractError as exc:
        issues.append(ValidationIssue("manifest_invalid", str(exc)))
        return CaseStatus(
            case_id=case_id,
            case_dir=case_dir,
            manifest_status=None,
            required_directories_present=0,
            required_directories_total=len(REQUIRED_PATH_ROLES),
            artifacts_declared=0,
            artifacts_present=0,
            artifacts_missing=0,
            issues=tuple(issues),
        )

    required_present = 0
    for role in REQUIRED_PATH_ROLES:
        path = resolve_relative_path(case_dir, manifest.paths[role], f"paths.{role}")
        if path.is_dir():
            required_present += 1
        else:
            issues.append(
                ValidationIssue(
                    "directory_missing",
                    f"required directory missing for {role}: {manifest.paths[role]}",
                )
            )

    artifacts_present = 0
    for artifact in manifest.artifacts:
        path = resolve_relative_path(
            case_dir,
            artifact.path,
            f"artifact {artifact.artifact_id}",
        )
        if path.is_file():
            artifacts_present += 1
        else:
            issues.append(
                ValidationIssue(
                    "artifact_missing",
                    f"artifact missing: {artifact.artifact_id} ({artifact.path})",
                )
            )

    declared = len(manifest.artifacts)
    return CaseStatus(
        case_id=case_id,
        case_dir=case_dir,
        manifest_status=manifest.status,
        required_directories_present=required_present,
        required_directories_total=len(REQUIRED_PATH_ROLES),
        artifacts_declared=declared,
        artifacts_present=artifacts_present,
        artifacts_missing=declared - artifacts_present,
        issues=tuple(issues),
    )


def resolve_relative_path(case_dir: Path, value: str, field: str) -> Path:
    """Resolve a portable case-relative path and prevent path escape."""
    if not value or "\\" in value:
        raise CaseContractError(f"{field} must be a non-empty POSIX relative path")
    pure_path = PurePosixPath(value)
    if pure_path.is_absolute() or pure_path == PurePosixPath("."):
        raise CaseContractError(f"{field} must be a case-relative path")
    if ".." in pure_path.parts:
        raise CaseContractError(f"{field} must not contain '..'")

    resolved_case = case_dir.resolve()
    resolved_path = (case_dir / Path(*pure_path.parts)).resolve(strict=False)
    if not resolved_path.is_relative_to(resolved_case):
        raise CaseContractError(f"{field} escapes the case directory")
    return resolved_path


def require_string(data: Mapping[str, object], key: str) -> str:
    """Read a required string from a manifest table."""
    return require_named_string(data, key, "manifest")


def require_integer(data: Mapping[str, object], key: str) -> int:
    """Read a required integer from a manifest table."""
    return require_named_integer(data, key, "manifest")


def validate_table_keys(
    data: Mapping[str, object],
    *,
    required: set[str],
    field: str,
    optional: set[str] | None = None,
) -> None:
    """Reject missing and unknown TOML keys for a versioned table."""
    allowed = required | (optional or set())
    actual = set(data)
    missing = sorted(required - actual)
    unexpected = sorted(actual - allowed)
    details: list[str] = []
    if missing:
        details.append(f"missing: {', '.join(missing)}")
    if unexpected:
        details.append(f"unexpected: {', '.join(unexpected)}")
    if details:
        raise CaseContractError(f"invalid {field} fields ({'; '.join(details)})")


def require_named_string(data: Mapping[str, object], key: str, field: str) -> str:
    """Read a non-empty string from a named TOML table."""
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise CaseContractError(f"{field}.{key} must be a non-empty string")
    return value


def optional_named_string(
    data: Mapping[str, object], key: str, field: str
) -> str | None:
    """Read an optional non-empty string from a named TOML table."""
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise CaseContractError(f"{field}.{key} must be a non-empty string")
    return value


def optional_named_integer(
    data: Mapping[str, object], key: str, field: str
) -> int | None:
    """Read an optional integer while rejecting booleans."""
    value = data.get(key)
    if value is None:
        return None
    if type(value) is not int:
        raise CaseContractError(f"{field}.{key} must be an integer")
    return value


def validate_utc_timestamp(value: str, field: str) -> None:
    """Require a parseable RFC 3339 timestamp explicitly expressed in UTC."""
    if not value.endswith("Z"):
        raise CaseContractError(f"{field} must be an RFC 3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise CaseContractError(f"{field} must be an RFC 3339 UTC timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise CaseContractError(f"{field} must be an RFC 3339 UTC timestamp")


def require_named_integer(data: Mapping[str, object], key: str, field: str) -> int:
    """Read an integer while rejecting booleans."""
    value = data.get(key)
    if type(value) is not int:
        raise CaseContractError(f"{field}.{key} must be an integer")
    return value
