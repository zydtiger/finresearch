"""Case directory and manifest contracts."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import tomllib
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Final

import tomli_w
from filelock import FileLock

MANIFEST_FILENAME: Final = "manifest.toml"
MANIFEST_V1: Final = 1
MANIFEST_V2: Final = 2
MANIFEST_VERSION: Final = MANIFEST_V2
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
    """Raised when a case cannot satisfy its versioned contract."""


@dataclass(frozen=True)
class InputFileHash:
    """A named, case-relative input file and its immutable byte hash."""

    name: str
    path: str
    sha256: str


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
    input_artifact_ids: tuple[str, ...] = ()
    producer: str | None = None
    producer_version: str | None = None
    parameters_sha256: str | None = None
    input_file_hashes: tuple[InputFileHash, ...] = ()


@dataclass(frozen=True)
class CaseManifest:
    """A supported versioned case manifest."""

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
        """Return whether the case passes complete manifest validation."""
        return not self.issues


@dataclass(frozen=True)
class CaseMigrationReceipt:
    """The result of an explicit manifest-only v1-to-v2 migration."""

    case_dir: Path
    migrated: bool
    manifest: CaseManifest


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


@contextmanager
def case_write_lock(case_dir: Path) -> Iterator[None]:
    """Serialize manifest changes and artifact publication for one case."""
    lock_path = resolve_relative_path(case_dir, ".finresearch.lock", "case lock")
    with FileLock(lock_path):
        yield


def initialize_case(workspace: Path, case_id: str, title: str | None = None) -> Path:
    """Create a current-version case without overwriting an existing path."""
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
    """Build an empty current-version manifest for a newly initialized case."""
    return CaseManifest(
        manifest_version=MANIFEST_V2,
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
        "artifacts": [
            artifact_to_dict(item, manifest.manifest_version)
            for item in manifest.artifacts
        ],
        "paths": dict(manifest.paths),
    }
    # Programmatic writes obey the same strict versioned schema as TOML input.
    parse_manifest(payload, case_dir)
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
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise


def artifact_to_dict(artifact: Artifact, manifest_version: int) -> dict[str, object]:
    """Convert an artifact to its TOML representation."""
    validate_artifact_object_version(artifact, manifest_version)
    data: dict[str, object] = {
        "id": artifact.artifact_id,
        "kind": artifact.kind,
        "schema_version": artifact.schema_version,
        "path": artifact.path,
    }
    if manifest_version == MANIFEST_V1:
        if artifact.source is not None:
            data["source"] = artifact.source
    elif manifest_version == MANIFEST_V2:
        data.update(
            {
                "input_artifact_ids": list(artifact.input_artifact_ids),
                "producer": artifact.producer,
                "producer_version": artifact.producer_version,
                "parameters_sha256": artifact.parameters_sha256,
                "input_file_hashes": [
                    {
                        "name": input_file.name,
                        "path": input_file.path,
                        "sha256": input_file.sha256,
                    }
                    for input_file in artifact.input_file_hashes
                ],
            }
        )
    else:
        raise CaseContractError(
            f"unsupported manifest_version {manifest_version}; expected 1 or 2"
        )
    if artifact.sha256 is not None:
        data["sha256"] = artifact.sha256
    if artifact.retrieved_at is not None:
        data["retrieved_at"] = artifact.retrieved_at
    if artifact.row_count is not None:
        data["row_count"] = artifact.row_count
    return data


def validate_artifact_object_version(
    artifact: Artifact,
    manifest_version: int,
) -> None:
    """Reject cross-version Artifact fields before they can be filtered out."""
    if manifest_version == MANIFEST_V1:
        v2_fields = {
            "input_artifact_ids": artifact.input_artifact_ids,
            "producer": artifact.producer,
            "producer_version": artifact.producer_version,
            "parameters_sha256": artifact.parameters_sha256,
            "input_file_hashes": artifact.input_file_hashes,
        }
        present = [name for name, value in v2_fields.items() if value not in ((), None)]
        if present:
            raise CaseContractError(
                f"manifest v1 artifacts must not set v2 fields: {', '.join(present)}"
            )
        return
    if manifest_version == MANIFEST_V2:
        if artifact.source is not None:
            raise CaseContractError("manifest v2 artifacts must not set legacy source")
        if artifact.sha256 is None:
            raise CaseContractError("manifest v2 artifacts must set sha256")
        return
    raise CaseContractError(
        f"unsupported manifest_version {manifest_version}; expected 1 or 2"
    )


def append_artifact(case_dir: Path, artifact: Artifact) -> CaseManifest:
    """Validate and append one artifact without replacing existing entries."""
    manifest = read_manifest(case_dir)
    payload: dict[str, object] = {
        "manifest_version": manifest.manifest_version,
        "case_id": manifest.case_id,
        "title": manifest.title,
        "status": manifest.status,
        "artifacts": [
            artifact_to_dict(item, manifest.manifest_version)
            for item in (*manifest.artifacts, artifact)
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
    """Parse and validate the structural manifest contract for its version."""
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
    if version not in (MANIFEST_V1, MANIFEST_V2):
        raise CaseContractError(
            f"unsupported manifest_version {version}; expected 1 or 2"
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
    artifacts = parse_artifacts(artifacts_value, case_dir, paths, version)

    return CaseManifest(
        manifest_version=version,
        case_id=case_id,
        title=title,
        status=status,
        paths=paths,
        artifacts=artifacts,
    )


def parse_paths(data: Mapping[str, object], case_dir: Path) -> dict[str, str]:
    """Validate the complete path-role table for a versioned manifest."""
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
    manifest_version: int,
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
        if manifest_version == MANIFEST_V1:
            validate_table_keys(
                item,
                required={"id", "kind", "schema_version", "path"},
                optional={"source", "sha256", "retrieved_at", "row_count"},
                field=field,
            )
        else:
            validate_table_keys(
                item,
                required={
                    "id",
                    "kind",
                    "schema_version",
                    "path",
                    "sha256",
                    "input_artifact_ids",
                    "producer",
                    "producer_version",
                    "parameters_sha256",
                    "input_file_hashes",
                },
                optional={"retrieved_at", "row_count"},
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

        source = (
            optional_named_string(item, "source", field)
            if manifest_version == MANIFEST_V1
            else None
        )
        sha256 = (
            require_named_string(item, "sha256", field)
            if manifest_version == MANIFEST_V2
            else optional_named_string(item, "sha256", field)
        )
        if sha256 is not None and SHA256_PATTERN.fullmatch(sha256) is None:
            raise CaseContractError(f"{field}.sha256 must be 64 lowercase hex digits")

        retrieved_at = optional_named_string(item, "retrieved_at", field)
        if retrieved_at is not None:
            validate_utc_timestamp(retrieved_at, f"{field}.retrieved_at")

        row_count = optional_named_integer(item, "row_count", field)
        if row_count is not None and row_count < 0:
            raise CaseContractError(f"{field}.row_count must not be negative")

        input_artifact_ids = (
            parse_input_artifact_ids(item, artifact_id, field)
            if manifest_version == MANIFEST_V2
            else ()
        )
        producer = (
            require_named_string(item, "producer", field)
            if manifest_version == MANIFEST_V2
            else None
        )
        if producer is not None and ARTIFACT_NAME_PATTERN.fullmatch(producer) is None:
            raise CaseContractError(f"{field}.producer has an invalid format")
        producer_version = (
            require_named_string(item, "producer_version", field)
            if manifest_version == MANIFEST_V2
            else None
        )
        parameters_sha256 = (
            require_named_string(item, "parameters_sha256", field)
            if manifest_version == MANIFEST_V2
            else None
        )
        if (
            parameters_sha256 is not None
            and SHA256_PATTERN.fullmatch(parameters_sha256) is None
        ):
            raise CaseContractError(
                f"{field}.parameters_sha256 must be 64 lowercase hex digits"
            )
        input_file_hashes = (
            parse_input_file_hashes(item, case_dir, field)
            if manifest_version == MANIFEST_V2
            else ()
        )

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
                input_artifact_ids=input_artifact_ids,
                producer=producer,
                producer_version=producer_version,
                parameters_sha256=parameters_sha256,
                input_file_hashes=input_file_hashes,
            )
        )
        artifact_ids.add(artifact_id)
        artifact_paths.add(resolved_path)
    parsed = tuple(artifacts)
    if manifest_version == MANIFEST_V2:
        validate_artifact_dag(parsed)
    return parsed


def parse_input_artifact_ids(
    data: Mapping[str, object], artifact_id: str, field: str
) -> tuple[str, ...]:
    """Validate ordered v2 parent ids before resolving the complete DAG."""
    value = data.get("input_artifact_ids")
    if not isinstance(value, list):
        raise CaseContractError(f"{field}.input_artifact_ids must be an array")
    parent_ids: list[str] = []
    seen: set[str] = set()
    for index, parent_id in enumerate(value):
        parent_field = f"{field}.input_artifact_ids[{index}]"
        if not isinstance(parent_id, str) or not parent_id:
            raise CaseContractError(f"{parent_field} must be a non-empty string")
        if ARTIFACT_NAME_PATTERN.fullmatch(parent_id) is None:
            raise CaseContractError(f"{parent_field} has an invalid format")
        if parent_id == artifact_id:
            raise CaseContractError(f"{field} must not declare itself as an input")
        if parent_id in seen:
            raise CaseContractError(
                f"{field} declares duplicate input artifact id: {parent_id}"
            )
        seen.add(parent_id)
        parent_ids.append(parent_id)
    return tuple(parent_ids)


def parse_input_file_hashes(
    data: Mapping[str, object], case_dir: Path, field: str
) -> tuple[InputFileHash, ...]:
    """Validate named case-relative input byte-hash records for manifest v2."""
    value = data.get("input_file_hashes")
    if not isinstance(value, list):
        raise CaseContractError(f"{field}.input_file_hashes must be an array")
    records: list[InputFileHash] = []
    names: set[str] = set()
    paths: set[Path] = set()
    for index, record in enumerate(value):
        record_field = f"{field}.input_file_hashes[{index}]"
        if not isinstance(record, dict):
            raise CaseContractError(f"{record_field} must be a TOML table")
        validate_table_keys(
            record,
            required={"name", "path", "sha256"},
            field=record_field,
        )
        name = require_named_string(record, "name", record_field)
        if ARTIFACT_NAME_PATTERN.fullmatch(name) is None:
            raise CaseContractError(f"{record_field}.name has an invalid format")
        path = require_named_string(record, "path", record_field)
        resolved_path = resolve_relative_path(case_dir, path, f"{record_field}.path")
        sha256 = require_named_string(record, "sha256", record_field)
        if SHA256_PATTERN.fullmatch(sha256) is None:
            raise CaseContractError(
                f"{record_field}.sha256 must be 64 lowercase hex digits"
            )
        if name in names:
            raise CaseContractError(f"duplicate input file hash name: {name}")
        if resolved_path in paths:
            raise CaseContractError(f"duplicate input file hash path: {path}")
        names.add(name)
        paths.add(resolved_path)
        records.append(InputFileHash(name=name, path=path, sha256=sha256))
    return tuple(records)


def validate_artifact_dag(artifacts: tuple[Artifact, ...]) -> None:
    """Require that every v2 lineage edge is declared and acyclic."""
    by_id = {artifact.artifact_id: artifact for artifact in artifacts}
    for artifact in artifacts:
        for parent_id in artifact.input_artifact_ids:
            if parent_id not in by_id:
                raise CaseContractError(
                    f"artifact {artifact.artifact_id} declares missing input artifact: "
                    f"{parent_id}"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(artifact_id: str) -> None:
        if artifact_id in visiting:
            raise CaseContractError(
                f"artifact lineage contains a cycle at: {artifact_id}"
            )
        if artifact_id in visited:
            return
        visiting.add(artifact_id)
        for parent_id in by_id[artifact_id].input_artifact_ids:
            visit(parent_id)
        visiting.remove(artifact_id)
        visited.add(artifact_id)

    for artifact in artifacts:
        visit(artifact.artifact_id)

    for artifact in artifacts:
        _validate_parent_input_hashes(artifact, by_id)


def _validate_parent_input_hashes(
    artifact: Artifact,
    by_id: Mapping[str, Artifact],
) -> None:
    """Bind each v2 parent edge to its canonical immutable input record."""
    parent_ids = set(artifact.input_artifact_ids)
    records_by_name = {
        input_file.name: input_file for input_file in artifact.input_file_hashes
    }
    for parent_id in artifact.input_artifact_ids:
        input_name = f"artifact.{parent_id}"
        input_file = records_by_name.get(input_name)
        if input_file is None:
            raise CaseContractError(
                f"artifact {artifact.artifact_id} missing input file hash for "
                f"parent: {parent_id}"
            )
        parent = by_id[parent_id]
        if input_file.path != parent.path:
            raise CaseContractError(
                f"artifact {artifact.artifact_id} input file hash for parent "
                f"{parent_id} must use path {parent.path!r}"
            )
        if parent.sha256 is not None and input_file.sha256 != parent.sha256:
            raise CaseContractError(
                f"artifact {artifact.artifact_id} input file hash for parent "
                f"{parent_id} must match its declared sha256"
            )

    for input_file in artifact.input_file_hashes:
        if input_file.name.startswith("artifact.") and (
            input_file.name.removeprefix("artifact.") not in parent_ids
        ):
            raise CaseContractError(
                f"artifact {artifact.artifact_id} input file hash "
                f"{input_file.name!r} does not name a declared parent"
            )


def canonical_parameters_sha256(parameters: Mapping[str, object]) -> str:
    """Hash explicitly supplied JSON-compatible producer parameters stably."""
    try:
        encoded = json.dumps(
            parameters,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CaseContractError(
            "producer parameters must be JSON-compatible deterministic values"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


def migrate_case(workspace: Path, case_id: str) -> CaseMigrationReceipt:
    """Explicitly upgrade one v1 manifest to v2 without touching artifacts."""
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    with case_write_lock(case_dir):
        manifest = read_manifest(case_dir)
        if manifest.manifest_version == MANIFEST_V2:
            return CaseMigrationReceipt(
                case_dir=case_dir,
                migrated=False,
                manifest=manifest,
            )

        by_id = {artifact.artifact_id: artifact for artifact in manifest.artifacts}
        actual_sha256_by_id = {
            artifact.artifact_id: _migration_artifact_sha256(case_dir, artifact)
            for artifact in manifest.artifacts
        }
        migrated_artifacts: list[Artifact] = []
        for artifact in manifest.artifacts:
            parent_ids: tuple[str, ...] = ()
            role = _artifact_path_role(case_dir, artifact.path, manifest.paths)
            if (
                role in {"normalized", "derived", "reports"}
                and artifact.source is not None
                and artifact.source in by_id
            ):
                parent_ids = (artifact.source,)
            input_hashes = tuple(
                _migration_input_file_hash(
                    parent=by_id[parent_id],
                    sha256=actual_sha256_by_id[parent_id],
                )
                for parent_id in parent_ids
            )
            migrated_artifacts.append(
                Artifact(
                    artifact_id=artifact.artifact_id,
                    kind=artifact.kind,
                    schema_version=artifact.schema_version,
                    path=artifact.path,
                    sha256=actual_sha256_by_id[artifact.artifact_id],
                    retrieved_at=artifact.retrieved_at,
                    row_count=artifact.row_count,
                    input_artifact_ids=parent_ids,
                    producer="finresearch.case.migrate-v1",
                    producer_version="1",
                    parameters_sha256=canonical_parameters_sha256(
                        {
                            "legacy_artifact": artifact_to_dict(artifact, MANIFEST_V1),
                            "migration_version": 1,
                        }
                    ),
                    input_file_hashes=input_hashes,
                )
            )
        migrated = CaseManifest(
            manifest_version=MANIFEST_V2,
            case_id=manifest.case_id,
            title=manifest.title,
            status=manifest.status,
            paths=manifest.paths,
            artifacts=tuple(migrated_artifacts),
        )
        # Validate before changing the manifest; data files are intentionally untouched.
        parse_manifest(
            {
                "manifest_version": migrated.manifest_version,
                "case_id": migrated.case_id,
                "title": migrated.title,
                "status": migrated.status,
                "artifacts": [
                    artifact_to_dict(artifact, migrated.manifest_version)
                    for artifact in migrated.artifacts
                ],
                "paths": dict(migrated.paths),
            },
            case_dir,
        )
        write_manifest(case_dir, migrated)
        return CaseMigrationReceipt(
            case_dir=case_dir,
            migrated=True,
            manifest=migrated,
        )


def _artifact_path_role(
    case_dir: Path,
    artifact_path: str,
    paths: Mapping[str, str],
) -> str | None:
    """Return the declared role containing an already-validated artifact path."""
    resolved_artifact = resolve_relative_path(case_dir, artifact_path, "artifact path")
    candidates: list[tuple[str, Path]] = []
    for role, value in paths.items():
        root = resolve_relative_path(case_dir, value, f"paths.{role}")
        if root in resolved_artifact.parents:
            candidates.append((role, root))
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: len(candidate[1].parts))[0]


def _migration_artifact_sha256(case_dir: Path, artifact: Artifact) -> str:
    """Verify and return actual legacy artifact bytes for a v2 declaration."""
    path = resolve_relative_path(
        case_dir,
        artifact.path,
        f"migration artifact {artifact.artifact_id}",
    )
    if not path.is_file():
        raise CaseContractError(
            f"migration artifact missing: {artifact.artifact_id} ({artifact.path})"
        )
    digest = _sha256(path)
    if artifact.sha256 is not None and artifact.sha256 != digest:
        raise CaseContractError(
            f"migration artifact checksum mismatch: {artifact.artifact_id}; "
            f"manifest {artifact.sha256}, file {digest}"
        )
    return digest


def _migration_input_file_hash(
    *,
    parent: Artifact,
    sha256: str,
) -> InputFileHash:
    """Build the canonical v2 input record from a verified parent artifact."""
    return InputFileHash(
        name=f"artifact.{parent.artifact_id}",
        path=parent.path,
        sha256=sha256,
    )


def _sha256(path: Path) -> str:
    """Hash a file without loading it into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
