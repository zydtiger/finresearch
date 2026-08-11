from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from finresearch.cases import (
    DEFAULT_PATHS,
    REQUIRED_PATH_ROLES,
    Artifact,
    CaseContractError,
    CaseManifest,
    initialize_case,
    inspect_case,
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


def test_initialize_case_creates_only_core_directories(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl-2026-08-11", "Apple valuation")

    manifest = read_manifest(case_dir)
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
    data["manifest_version"] = 2

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
