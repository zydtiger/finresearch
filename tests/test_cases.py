from __future__ import annotations

import hashlib
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from finresearch.cases import (
    DEFAULT_PATHS,
    MANIFEST_V2,
    REQUIRED_PATH_ROLES,
    Artifact,
    CaseContractError,
    CaseManifest,
    InputFileHash,
    append_artifact,
    initialize_case,
    inspect_case,
    migrate_case,
    parse_manifest,
    read_manifest,
    write_manifest,
)

FIXTURE_MANIFEST = Path(__file__).parent / "fixtures" / "case-v1" / "manifest.toml"


def test_case_v1_fixture_matches_contract(tmp_path: Path) -> None:
    case_dir = tmp_path / "case-v1"
    case_dir.mkdir()
    shutil.copyfile(FIXTURE_MANIFEST, case_dir / "manifest.toml")

    manifest = read_manifest(case_dir)

    assert manifest.manifest_version == 1
    assert manifest.case_id == "case-v1"
    assert manifest.paths == DEFAULT_PATHS
    assert manifest.artifacts == ()


def test_v1_case_remains_valid_without_implicit_migration(tmp_path: Path) -> None:
    case_dir = tmp_path / "cases" / "case-v1"
    case_dir.mkdir(parents=True)
    shutil.copyfile(FIXTURE_MANIFEST, case_dir / "manifest.toml")
    for role in REQUIRED_PATH_ROLES:
        (case_dir / DEFAULT_PATHS[role]).mkdir(parents=True)

    status = inspect_case(tmp_path, "case-v1")

    assert status.valid
    assert read_manifest(case_dir).manifest_version == 1


def test_initialize_case_creates_only_core_directories(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl-2026-08-11", "Apple valuation")

    manifest = read_manifest(case_dir)
    assert manifest.manifest_version == MANIFEST_V2
    assert manifest.title == "Apple valuation"
    for role in REQUIRED_PATH_ROLES:
        assert (case_dir / manifest.paths[role]).is_dir()
    assert not (case_dir / manifest.paths["registers"]).exists()
    assert not (case_dir / manifest.paths["analysis"]).exists()


@pytest.mark.parametrize(
    "case_id",
    ["AAPL", "-aapl", "aapl-", "aapl/update", "..", "a" * 65],
)
def test_initialize_case_rejects_invalid_ids(tmp_path: Path, case_id: str) -> None:
    with pytest.raises(CaseContractError, match="case ID"):
        initialize_case(tmp_path, case_id)

    assert not (tmp_path / "cases").exists()


def test_initialize_case_rejects_empty_title_without_partial_case(
    tmp_path: Path,
) -> None:
    with pytest.raises(CaseContractError, match="title"):
        initialize_case(tmp_path, "aapl", "   ")

    assert not (tmp_path / "cases").exists()


def test_parse_manifest_rejects_unsupported_version(tmp_path: Path) -> None:
    data = valid_manifest_data()
    data["manifest_version"] = 3

    with pytest.raises(CaseContractError, match="unsupported manifest_version"):
        parse_manifest(data, tmp_path / "case-v1")


def test_parse_manifest_rejects_unknown_top_level_field(tmp_path: Path) -> None:
    data = valid_manifest_data()
    data["manifest_verison"] = 1

    with pytest.raises(CaseContractError, match="unexpected: manifest_verison"):
        parse_manifest(data, tmp_path / "case-v1")


def test_parse_manifest_rejects_path_escape(tmp_path: Path) -> None:
    data = valid_manifest_data()
    paths = data["paths"]
    assert isinstance(paths, dict)
    paths["raw"] = "../outside"

    with pytest.raises(CaseContractError, match="must not contain"):
        parse_manifest(data, tmp_path / "case-v1")


def test_parse_manifest_rejects_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    case_dir = tmp_path / "case-v1"
    case_dir.mkdir()
    (case_dir / "raw-link").symlink_to(outside, target_is_directory=True)
    data = valid_manifest_data()
    paths = data["paths"]
    assert isinstance(paths, dict)
    paths["raw"] = "raw-link"

    with pytest.raises(CaseContractError, match="escapes the case directory"):
        parse_manifest(data, case_dir)


def test_inspect_case_rejects_case_directory_symlink_escape(tmp_path: Path) -> None:
    outside_case = tmp_path.parent / f"{tmp_path.name}-outside-case"
    outside_case.mkdir()
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()
    (cases_dir / "aapl").symlink_to(outside_case, target_is_directory=True)

    with pytest.raises(CaseContractError, match="case directory escapes"):
        inspect_case(tmp_path, "aapl")


def test_read_manifest_rejects_manifest_symlink_escape(tmp_path: Path) -> None:
    case_dir = tmp_path / "case-v1"
    case_dir.mkdir()
    outside_manifest = tmp_path.parent / f"{tmp_path.name}-manifest.toml"
    outside_manifest.write_text(FIXTURE_MANIFEST.read_text(encoding="utf-8"))
    (case_dir / "manifest.toml").symlink_to(outside_manifest)

    with pytest.raises(CaseContractError, match="manifest escapes"):
        read_manifest(case_dir)


def test_parse_manifest_rejects_duplicate_artifact_ids(tmp_path: Path) -> None:
    artifact = valid_artifact_data("market.prices", "data/normalized/prices.parquet")
    data = valid_manifest_data()
    data["artifacts"] = [artifact, dict(artifact, path="data/derived/prices.parquet")]

    with pytest.raises(CaseContractError, match="duplicate artifact id"):
        parse_manifest(data, tmp_path / "case-v1")


def test_parse_manifest_rejects_unknown_artifact_field(tmp_path: Path) -> None:
    artifact = valid_artifact_data("market.prices", "data/normalized/prices.parquet")
    artifact["schema_verison"] = 1
    data = valid_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match="unexpected: schema_verison"):
        parse_manifest(data, tmp_path / "case-v1")


def test_v1_rejects_v2_artifact_fields(tmp_path: Path) -> None:
    artifact = valid_artifact_data("market.prices", "data/raw/prices.parquet")
    artifact["producer"] = "finresearch.test"
    data = valid_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match="unexpected: producer"):
        parse_manifest(data, tmp_path / "case-v1")


def test_v2_rejects_legacy_source_and_requires_producer_metadata(
    tmp_path: Path,
) -> None:
    artifact = valid_v2_artifact_data("market.prices", "data/raw/prices.parquet")
    artifact["source"] = "yfinance"
    data = valid_v2_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match="unexpected: source"):
        parse_manifest(data, tmp_path / "case-v1")

    del artifact["source"]
    del artifact["producer"]
    with pytest.raises(CaseContractError, match="missing: producer"):
        parse_manifest(data, tmp_path / "case-v1")


def test_v2_requires_own_sha256_in_toml_and_programmatic_writes(
    tmp_path: Path,
) -> None:
    artifact = valid_v2_artifact_data("market.prices", "data/raw/prices.parquet")
    del artifact["sha256"]
    data = valid_v2_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match="missing: sha256"):
        parse_manifest(data, tmp_path / "case-v1")

    case_dir = initialize_case(tmp_path, "aapl")
    manifest_before = (case_dir / "manifest.toml").read_bytes()
    with pytest.raises(CaseContractError, match="must set sha256"):
        append_artifact(
            case_dir,
            Artifact(
                artifact_id="market.prices",
                kind="prices",
                schema_version=1,
                path="data/raw/prices.parquet",
                producer="finresearch.test",
                producer_version="1",
                parameters_sha256="a" * 64,
            ),
        )

    assert (case_dir / "manifest.toml").read_bytes() == manifest_before


@pytest.mark.parametrize(
    "v1_incompatible",
    [
        Artifact(
            artifact_id="raw.snapshot",
            kind="raw.data",
            schema_version=1,
            path="data/raw/snapshot.parquet",
            input_artifact_ids=("raw.parent",),
        ),
        Artifact(
            artifact_id="raw.snapshot",
            kind="raw.data",
            schema_version=1,
            path="data/raw/snapshot.parquet",
            producer="finresearch.test",
        ),
        Artifact(
            artifact_id="raw.snapshot",
            kind="raw.data",
            schema_version=1,
            path="data/raw/snapshot.parquet",
            producer_version="1",
        ),
        Artifact(
            artifact_id="raw.snapshot",
            kind="raw.data",
            schema_version=1,
            path="data/raw/snapshot.parquet",
            parameters_sha256="a" * 64,
        ),
        Artifact(
            artifact_id="raw.snapshot",
            kind="raw.data",
            schema_version=1,
            path="data/raw/snapshot.parquet",
            input_file_hashes=(
                InputFileHash(
                    name="input",
                    path="data/raw/input.parquet",
                    sha256="a" * 64,
                ),
            ),
        ),
    ],
)
def test_write_manifest_rejects_programmatic_cross_version_artifact_fields(
    tmp_path: Path,
    v1_incompatible: Artifact,
) -> None:
    v1_case_dir = tmp_path / "cases" / "case-v1"
    v1_case_dir.mkdir(parents=True)
    v1_manifest = CaseManifest(
        manifest_version=1,
        case_id="case-v1",
        title="Legacy case",
        status="active",
        paths=dict(DEFAULT_PATHS),
        artifacts=(),
    )
    write_manifest(v1_case_dir, v1_manifest)
    v1_original = (v1_case_dir / "manifest.toml").read_bytes()
    with pytest.raises(CaseContractError, match="must not set v2 fields"):
        write_manifest(
            v1_case_dir,
            replace(v1_manifest, artifacts=(v1_incompatible,)),
        )

    assert (v1_case_dir / "manifest.toml").read_bytes() == v1_original

    v2_case_dir = initialize_case(tmp_path, "aapl")
    v2_manifest = read_manifest(v2_case_dir)
    v2_original = (v2_case_dir / "manifest.toml").read_bytes()
    v2_incompatible = Artifact(
        artifact_id="raw.snapshot",
        kind="raw.data",
        schema_version=1,
        path="data/raw/snapshot.parquet",
        source="legacy-provider",
        producer="finresearch.test",
        producer_version="1",
        parameters_sha256="a" * 64,
    )

    with pytest.raises(CaseContractError, match="must not set legacy source"):
        write_manifest(
            v2_case_dir,
            replace(v2_manifest, artifacts=(v2_incompatible,)),
        )

    assert (v2_case_dir / "manifest.toml").read_bytes() == v2_original


def test_append_artifact_rejects_legacy_source_for_v2_without_changes(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    original = (case_dir / "manifest.toml").read_bytes()
    incompatible = Artifact(
        artifact_id="raw.snapshot",
        kind="raw.data",
        schema_version=1,
        path="data/raw/snapshot.parquet",
        source="legacy-provider",
        producer="finresearch.test",
        producer_version="1",
        parameters_sha256="a" * 64,
    )

    with pytest.raises(CaseContractError, match="must not set legacy source"):
        append_artifact(case_dir, incompatible)

    assert (case_dir / "manifest.toml").read_bytes() == original


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (
            {"name": "bad/name", "path": "data/raw/input.csv", "sha256": "a" * 64},
            "name has an invalid format",
        ),
        (
            {"name": "input", "path": "../input.csv", "sha256": "a" * 64},
            "must not contain",
        ),
        (
            {"name": "input", "path": "data/raw/input.csv", "sha256": "A" * 64},
            "sha256 must be 64 lowercase",
        ),
    ],
)
def test_v2_validates_input_file_hash_records(
    tmp_path: Path,
    record: dict[str, str],
    message: str,
) -> None:
    artifact = valid_v2_artifact_data("market.prices", "data/raw/prices.parquet")
    artifact["input_file_hashes"] = [record]
    data = valid_v2_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match=message):
        parse_manifest(data, tmp_path / "case-v1")


def test_v2_rejects_missing_duplicate_self_and_cyclic_parents(tmp_path: Path) -> None:
    missing = valid_v2_manifest_data()
    missing_artifact = valid_v2_artifact_data("derived.one", "data/derived/one.parquet")
    missing_artifact["input_artifact_ids"] = ["raw.missing"]
    missing["artifacts"] = [missing_artifact]
    with pytest.raises(CaseContractError, match="missing input artifact"):
        parse_manifest(missing, tmp_path / "case-v1")

    duplicate = valid_v2_manifest_data()
    parent = valid_v2_artifact_data("raw.parent", "data/raw/parent.parquet")
    child = valid_v2_artifact_data("derived.child", "data/derived/child.parquet")
    child["input_artifact_ids"] = ["raw.parent", "raw.parent"]
    duplicate["artifacts"] = [parent, child]
    with pytest.raises(CaseContractError, match="duplicate input artifact id"):
        parse_manifest(duplicate, tmp_path / "case-v1")

    self_parent = valid_v2_manifest_data()
    self_artifact = valid_v2_artifact_data("derived.self", "data/derived/self.parquet")
    self_artifact["input_artifact_ids"] = ["derived.self"]
    self_parent["artifacts"] = [self_artifact]
    with pytest.raises(CaseContractError, match="must not declare itself"):
        parse_manifest(self_parent, tmp_path / "case-v1")

    cycle = valid_v2_manifest_data()
    first = valid_v2_artifact_data("derived.first", "data/derived/first.parquet")
    second = valid_v2_artifact_data("derived.second", "data/derived/second.parquet")
    first["input_artifact_ids"] = ["derived.second"]
    second["input_artifact_ids"] = ["derived.first"]
    cycle["artifacts"] = [first, second]
    with pytest.raises(CaseContractError, match="lineage contains a cycle"):
        parse_manifest(cycle, tmp_path / "case-v1")


@pytest.mark.parametrize(
    ("input_file_hashes", "message"),
    [
        ([], "missing input file hash for parent"),
        (
            [
                {
                    "name": "artifact.raw.parent",
                    "path": "data/raw/wrong.parquet",
                    "sha256": "a" * 64,
                }
            ],
            "must use path 'data/raw/parent.parquet'",
        ),
        (
            [
                {
                    "name": "artifact.raw.parent",
                    "path": "data/raw/parent.parquet",
                    "sha256": "0" * 64,
                }
            ],
            "must match its declared sha256",
        ),
    ],
)
def test_v2_binds_parent_edges_to_canonical_input_file_hashes(
    tmp_path: Path,
    input_file_hashes: list[dict[str, str]],
    message: str,
) -> None:
    parent = valid_v2_artifact_data("raw.parent", "data/raw/parent.parquet")
    parent["sha256"] = "a" * 64
    child = valid_v2_artifact_data("derived.child", "data/derived/child.parquet")
    child["input_artifact_ids"] = ["raw.parent"]
    child["input_file_hashes"] = input_file_hashes
    data = valid_v2_manifest_data()
    data["artifacts"] = [parent, child]

    with pytest.raises(CaseContractError, match=message):
        parse_manifest(data, tmp_path / "case-v1")


def test_v2_allows_non_artifact_input_file_hashes(tmp_path: Path) -> None:
    parent = valid_v2_artifact_data("raw.parent", "data/raw/parent.parquet")
    parent["sha256"] = "a" * 64
    child = valid_v2_artifact_data("derived.child", "data/derived/child.parquet")
    child["input_artifact_ids"] = ["raw.parent"]
    child["input_file_hashes"] = [
        {
            "name": "artifact.raw.parent",
            "path": "data/raw/parent.parquet",
            "sha256": "a" * 64,
        },
        {
            "name": "config.parameters",
            "path": "analysis/parameters.json",
            "sha256": "b" * 64,
        },
    ]
    data = valid_v2_manifest_data()
    data["artifacts"] = [parent, child]

    parsed = parse_manifest(data, tmp_path / "case-v1")

    assert parsed.artifacts[1].input_file_hashes[1].name == "config.parameters"


def test_migrate_v1_to_v2_is_idempotent_and_does_not_rewrite_artifacts(
    tmp_path: Path,
) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    raw_bytes = b"raw bytes"
    normalized_bytes = b"normalized bytes"
    raw = Artifact(
        artifact_id="raw.snapshot",
        kind="raw.data",
        schema_version=1,
        path="data/raw/snapshot.bin",
        source="provider",
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )
    normalized = Artifact(
        artifact_id="normalized.snapshot",
        kind="normalized.data",
        schema_version=1,
        path="data/normalized/snapshot.bin",
        source=raw.artifact_id,
        sha256=hashlib.sha256(normalized_bytes).hexdigest(),
    )
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=1,
            case_id="legacy",
            title="Legacy case",
            status="active",
            paths=dict(DEFAULT_PATHS),
            artifacts=(raw, normalized),
        ),
    )
    raw_path = case_dir / raw.path
    normalized_path = case_dir / normalized.path
    raw_path.parent.mkdir(parents=True)
    normalized_path.parent.mkdir(parents=True)
    raw_path.write_bytes(raw_bytes)
    normalized_path.write_bytes(normalized_bytes)
    original_bytes = (raw_path.read_bytes(), normalized_path.read_bytes())

    migrated = migrate_case(tmp_path, "legacy")

    assert migrated.migrated
    assert migrated.manifest.manifest_version == 2
    migrated_normalized = migrated.manifest.artifacts[1]
    assert migrated_normalized.input_artifact_ids == (raw.artifact_id,)
    assert migrated_normalized.input_file_hashes[0].path == raw.path
    assert (raw_path.read_bytes(), normalized_path.read_bytes()) == original_bytes
    manifest_bytes = (case_dir / "manifest.toml").read_bytes()

    repeated = migrate_case(tmp_path, "legacy")

    assert not repeated.migrated
    assert (case_dir / "manifest.toml").read_bytes() == manifest_bytes


def test_migrate_uses_path_role_for_provider_source_collisions_and_hashes_bytes(
    tmp_path: Path,
) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    raw_bytes = b"raw provider snapshot"
    derived_bytes = b"derived output"
    raw = Artifact(
        artifact_id="yfinance",
        kind="raw.data",
        schema_version=1,
        path="data/raw/yfinance.parquet",
        source="yfinance",
    )
    derived = Artifact(
        artifact_id="normalized.snapshot",
        kind="normalized.data",
        schema_version=1,
        path="data/normalized/snapshot.parquet",
        source="yfinance",
    )
    _write_legacy_manifest(case_dir, (raw, derived))
    raw_path = case_dir / raw.path
    derived_path = case_dir / derived.path
    raw_path.parent.mkdir(parents=True)
    derived_path.parent.mkdir(parents=True)
    raw_path.write_bytes(raw_bytes)
    derived_path.write_bytes(derived_bytes)

    migrated = migrate_case(tmp_path, "legacy")

    migrated_raw, migrated_derived = migrated.manifest.artifacts
    assert migrated_raw.input_artifact_ids == ()
    assert migrated_derived.input_artifact_ids == ("yfinance",)
    assert migrated_raw.sha256 == hashlib.sha256(raw_bytes).hexdigest()
    assert migrated_derived.sha256 == hashlib.sha256(derived_bytes).hexdigest()
    assert migrated_derived.input_file_hashes == (
        InputFileHash(
            name="artifact.yfinance",
            path=raw.path,
            sha256=hashlib.sha256(raw_bytes).hexdigest(),
        ),
    )


def test_migrate_uses_deepest_nested_path_role_for_lineage(tmp_path: Path) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    raw_bytes = b"raw input"
    normalized_bytes = b"normalized output"
    raw = Artifact(
        artifact_id="raw.snapshot",
        kind="raw.data",
        schema_version=1,
        path="data/raw/snapshot.bin",
        source="provider",
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )
    normalized = Artifact(
        artifact_id="normalized.snapshot",
        kind="normalized.data",
        schema_version=1,
        path="data/normalized/snapshot.bin",
        source=raw.artifact_id,
        sha256=hashlib.sha256(normalized_bytes).hexdigest(),
    )
    paths = {**DEFAULT_PATHS, "raw": "data", "normalized": "data/normalized"}
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=1,
            case_id="legacy",
            title="Legacy case",
            status="active",
            paths=paths,
            artifacts=(raw, normalized),
        ),
    )
    (case_dir / raw.path).parent.mkdir(parents=True)
    (case_dir / raw.path).write_bytes(raw_bytes)
    (case_dir / normalized.path).parent.mkdir(parents=True)
    (case_dir / normalized.path).write_bytes(normalized_bytes)

    migrated = migrate_case(tmp_path, "legacy")

    assert migrated.manifest.artifacts[1].input_artifact_ids == (raw.artifact_id,)


@pytest.mark.parametrize(
    ("parent_sha256", "create_parent", "message"),
    [
        ("0" * 64, True, "migration artifact checksum mismatch"),
        (None, False, "migration artifact missing"),
    ],
)
def test_migrate_rejects_invalid_artifact_bytes_without_changes(
    tmp_path: Path,
    parent_sha256: str | None,
    create_parent: bool,
    message: str,
) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    raw = Artifact(
        artifact_id="raw.snapshot",
        kind="raw.data",
        schema_version=1,
        path="data/raw/snapshot.bin",
        source="provider",
        sha256=parent_sha256,
    )
    normalized = Artifact(
        artifact_id="normalized.snapshot",
        kind="normalized.data",
        schema_version=1,
        path="data/normalized/snapshot.bin",
        source=raw.artifact_id,
    )
    _write_legacy_manifest(case_dir, (raw, normalized))
    raw_path = case_dir / raw.path
    normalized_path = case_dir / normalized.path
    normalized_path.parent.mkdir(parents=True)
    normalized_path.write_bytes(b"normalized bytes")
    if create_parent:
        raw_path.parent.mkdir(parents=True)
        raw_path.write_bytes(b"raw bytes")
    manifest_before = (case_dir / "manifest.toml").read_bytes()
    bytes_before = normalized_path.read_bytes()
    raw_before = raw_path.read_bytes() if raw_path.exists() else None

    with pytest.raises(CaseContractError, match=message):
        migrate_case(tmp_path, "legacy")

    assert (case_dir / "manifest.toml").read_bytes() == manifest_before
    assert normalized_path.read_bytes() == bytes_before
    assert (raw_path.read_bytes() if raw_path.exists() else None) == raw_before


def test_migrate_v2_is_idempotent_without_rewriting_artifacts(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    report_path = case_dir / "reports/summary.md"
    report_path.write_bytes(b"# Summary\n")
    append_artifact(
        case_dir,
        Artifact(
            artifact_id="report.summary",
            kind="report.markdown",
            schema_version=1,
            path="reports/summary.md",
            sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
            producer="finresearch.test",
            producer_version="1",
            parameters_sha256="a" * 64,
        ),
    )
    manifest_before = (case_dir / "manifest.toml").read_bytes()
    artifact_before = report_path.read_bytes()

    first = migrate_case(tmp_path, "aapl")
    second = migrate_case(tmp_path, "aapl")

    assert not first.migrated
    assert not second.migrated
    assert (case_dir / "manifest.toml").read_bytes() == manifest_before
    assert report_path.read_bytes() == artifact_before
    assert (case_dir / ".finresearch.lock").is_file()


def test_migrate_rejects_nonraw_lineage_cycle_without_changes(tmp_path: Path) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    normalized = Artifact(
        artifact_id="normalized.one",
        kind="normalized.data",
        schema_version=1,
        path="data/normalized/one.bin",
        source="derived.two",
    )
    derived = Artifact(
        artifact_id="derived.two",
        kind="derived.data",
        schema_version=1,
        path="data/derived/two.bin",
        source="normalized.one",
    )
    _write_legacy_manifest(case_dir, (normalized, derived))
    normalized_path = case_dir / normalized.path
    derived_path = case_dir / derived.path
    normalized_path.parent.mkdir(parents=True)
    derived_path.parent.mkdir(parents=True)
    normalized_path.write_bytes(b"normalized bytes")
    derived_path.write_bytes(b"derived bytes")
    manifest_before = (case_dir / "manifest.toml").read_bytes()
    bytes_before = (normalized_path.read_bytes(), derived_path.read_bytes())

    with pytest.raises(CaseContractError, match="lineage contains a cycle"):
        migrate_case(tmp_path, "legacy")

    assert (case_dir / "manifest.toml").read_bytes() == manifest_before
    assert (normalized_path.read_bytes(), derived_path.read_bytes()) == bytes_before


def _write_legacy_manifest(case_dir: Path, artifacts: tuple[Artifact, ...]) -> None:
    """Write a valid v1 manifest for migration-focused tests."""
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=1,
            case_id=case_dir.name,
            title="Legacy case",
            status="active",
            paths=dict(DEFAULT_PATHS),
            artifacts=artifacts,
        ),
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("retrieved_at", "2026-08-11T04:05:00+00:00", "RFC 3339 UTC"),
        ("retrieved_at", "not-a-timeZ", "RFC 3339 UTC"),
        ("row_count", -1, "must not be negative"),
        ("row_count", True, "must be an integer"),
    ],
)
def test_parse_manifest_rejects_invalid_artifact_provenance(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    artifact = valid_artifact_data("market.prices", "data/raw/prices.parquet")
    artifact[field] = value
    data = valid_manifest_data()
    data["artifacts"] = [artifact]

    with pytest.raises(CaseContractError, match=message):
        parse_manifest(data, tmp_path / "case-v1")


def test_inspect_case_reports_missing_required_directory(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    shutil.rmtree(case_dir / DEFAULT_PATHS["derived"])

    status = inspect_case(tmp_path, "aapl")

    assert not status.valid
    assert status.required_directories_present == 3
    assert status.issues[0].code == "directory_missing"


def test_inspect_case_tracks_declared_artifact_presence(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    manifest = read_manifest(case_dir)
    artifact = Artifact(
        artifact_id="market.prices.daily",
        kind="prices",
        schema_version=1,
        path="data/normalized/prices.parquet",
        sha256=hashlib.sha256(b"parquet-placeholder").hexdigest(),
        producer="finresearch.test",
        producer_version="1",
        parameters_sha256="a" * 64,
    )
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=manifest.manifest_version,
            case_id=manifest.case_id,
            title=manifest.title,
            status=manifest.status,
            paths=manifest.paths,
            artifacts=(artifact,),
        ),
    )

    missing = inspect_case(tmp_path, "aapl")
    assert not missing.valid
    assert missing.artifacts_missing == 1
    assert missing.issues[-1].code == "artifact_missing"

    artifact_path = case_dir / artifact.path
    artifact_path.write_bytes(b"parquet-placeholder")
    present = inspect_case(tmp_path, "aapl")
    assert present.valid
    assert present.artifacts_present == 1
    assert present.artifacts_missing == 0


def valid_manifest_data() -> dict[str, object]:
    """Return mutable valid manifest data for focused parser tests."""
    return {
        "manifest_version": 1,
        "case_id": "case-v1",
        "title": "Case fixture",
        "status": "active",
        "artifacts": [],
        "paths": dict(DEFAULT_PATHS),
    }


def valid_artifact_data(artifact_id: str, path: str) -> dict[str, object]:
    """Return one valid artifact declaration."""
    return {
        "id": artifact_id,
        "kind": "prices",
        "schema_version": 1,
        "path": path,
    }


def valid_v2_manifest_data() -> dict[str, object]:
    """Return mutable v2 manifest data for strict parser tests."""
    return {
        **valid_manifest_data(),
        "manifest_version": 2,
    }


def valid_v2_artifact_data(artifact_id: str, path: str) -> dict[str, object]:
    """Return one complete v2 artifact declaration."""
    return {
        **valid_artifact_data(artifact_id, path),
        "sha256": "a" * 64,
        "input_artifact_ids": [],
        "producer": "finresearch.test",
        "producer_version": "1",
        "parameters_sha256": "a" * 64,
        "input_file_hashes": [],
    }
