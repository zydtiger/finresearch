import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner, Result

from finresearch.cases import initialize_case, read_manifest
from finresearch.cli import app
from finresearch.data_contracts import NORMALIZED_FUNDAMENTAL_FACTS_V1
from finresearch.ingestion import IngestionError, ingest_sec_companyfacts
from finresearch.normalization import normalize_fundamental_facts
from finresearch.providers.sec import companyfacts_to_frame

runner = CliRunner()

RETRIEVED_AT = datetime(2026, 8, 11, 3, 12, 45, tzinfo=UTC)
NORMALIZED_AT = datetime(2026, 8, 11, 4, 0, 0, tzinfo=UTC)
CIK = "0000320193"
FIXTURE = Path(__file__).parent / "fixtures" / "sec" / "companyfacts.json"


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


class FakeFactsProvider:
    """Return fixture companyfacts frames without network access."""

    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        assert user_agent == "Finresearch user@example.com"
        return self.frame.with_columns(pl.lit(retrieved_at).alias("retrieved_at"))


def fixture_frame(*, retrieved_at: datetime = RETRIEVED_AT) -> pl.DataFrame:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return companyfacts_to_frame(
        payload,
        expected_cik=CIK,
        source_url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json",
        retrieved_at=retrieved_at,
    )


def build_facts_case(
    tmp_path: Path,
    *,
    frame: pl.DataFrame | None = None,
    snapshots: int = 1,
) -> Path:
    """Ingest one or more raw companyfacts snapshots through the real path."""
    initialize_case(tmp_path, "aapl")
    for index in range(snapshots):
        ingest_sec_companyfacts(
            tmp_path,
            "aapl",
            CIK,
            "Finresearch user@example.com",
            provider=FakeFactsProvider(fixture_frame() if frame is None else frame),
            retrieved_at=RETRIEVED_AT.replace(minute=index),
        )
    return tmp_path


def normalized_frame(workspace: Path) -> pl.DataFrame:
    """Read the single normalized fundamental-facts artifact."""
    manifest = read_manifest(workspace / "cases" / "aapl")
    artifact = next(
        item
        for item in manifest.artifacts
        if item.kind == "normalized.fundamental-facts"
    )
    return pl.read_parquet(workspace / "cases" / "aapl" / artifact.path)


def test_normalize_facts_writes_parsed_table(tmp_path: Path) -> None:
    workspace = build_facts_case(tmp_path)

    receipt = normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        normalized_at=NORMALIZED_AT,
    )

    assert receipt.artifact_id.startswith("normalized.fundamental-facts.0000320193.")
    assert receipt.row_count == 2
    manifest = read_manifest(workspace / "cases" / "aapl")
    artifact = next(
        item
        for item in manifest.artifacts
        if item.kind == "normalized.fundamental-facts"
    )
    assert artifact.path.startswith(
        "data/normalized/normalized.fundamental-facts/0000320193/"
    )
    raw_ids = [
        item.artifact_id
        for item in manifest.artifacts
        if item.kind == "raw.sec.companyfacts"
    ]
    assert artifact.source == raw_ids[0]


def test_normalize_facts_value_parsing(tmp_path: Path) -> None:
    workspace = build_facts_case(tmp_path)
    normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        normalized_at=NORMALIZED_AT,
    )

    frame = normalized_frame(workspace)
    NORMALIZED_FUNDAMENTAL_FACTS_V1.validate(frame)
    revenues = frame.filter(pl.col("concept") == "Revenues").row(0, named=True)
    assert revenues["value_type"] == "integer"
    assert revenues["value_text"] == "300000000000"
    assert revenues["value"] == 300_000_000_000.0
    assert revenues["period_type"] == "duration"
    assert revenues["start_date"] == date(2025, 9, 28)
    assert revenues["end_date"] == date(2026, 6, 27)
    assert revenues["unit"] == "USD"
    assert revenues["form"] == "10-Q"
    assert revenues["filed_date"] == date(2026, 8, 1)
    assert revenues["source_artifact_id"].startswith("raw.sec.companyfacts.")

    public_float = frame.filter(pl.col("concept") == "EntityPublicFloat").row(
        0,
        named=True,
    )
    assert public_float["value_type"] == "number"
    assert public_float["value"] == 2_500_000_000_000.5
    assert public_float["period_type"] == "instant"
    assert public_float["start_date"] is None
    assert public_float["label"] == "Entity Public Float"


def test_normalize_facts_removes_exact_duplicates(tmp_path: Path) -> None:
    duplicated = pl.concat([fixture_frame(), fixture_frame()])
    workspace = build_facts_case(tmp_path, frame=duplicated)
    normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        normalized_at=NORMALIZED_AT,
    )

    assert normalized_frame(workspace).height == 2


def test_normalize_facts_string_values_stay_unparsed(tmp_path: Path) -> None:
    frame = fixture_frame().with_columns(
        pl.lit("string").alias("value_type"),
        pl.lit('"hired"').alias("value_text"),
    )
    workspace = build_facts_case(tmp_path, frame=frame)
    normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        normalized_at=NORMALIZED_AT,
    )

    frame = normalized_frame(workspace)
    assert frame.schema["value"] == pl.Float64
    assert frame["value"].to_list() == [None, None]
    assert frame["value_text"].to_list() == ['"hired"', '"hired"']


def test_normalize_facts_rejects_unparseable_numeric(tmp_path: Path) -> None:
    frame = fixture_frame().with_columns(
        pl.lit("integer").alias("value_type"),
        pl.lit("not-a-number").alias("value_text"),
    )
    workspace = build_facts_case(tmp_path, frame=frame)

    with pytest.raises(IngestionError, match="unparseable numeric fact"):
        normalize_fundamental_facts(workspace, "aapl", CIK)


def test_normalize_facts_no_raw_snapshot_fails(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    with pytest.raises(IngestionError, match="run data ingest-sec-companyfacts"):
        normalize_fundamental_facts(tmp_path, "aapl", CIK)


def test_normalize_facts_multiple_snapshots_requires_selection(
    tmp_path: Path,
) -> None:
    workspace = build_facts_case(tmp_path, snapshots=2)

    with pytest.raises(IngestionError, match="--raw-artifact-id"):
        normalize_fundamental_facts(workspace, "aapl", CIK)

    raw_ids = [
        item.artifact_id
        for item in read_manifest(workspace / "cases" / "aapl").artifacts
        if item.kind == "raw.sec.companyfacts"
    ]
    receipt = normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        raw_artifact_id=raw_ids[0],
        normalized_at=NORMALIZED_AT,
    )
    assert receipt.row_count == 2


def test_normalize_facts_passes_data_validate(tmp_path: Path) -> None:
    workspace = build_facts_case(tmp_path)
    normalize_fundamental_facts(
        workspace,
        "aapl",
        CIK,
        normalized_at=NORMALIZED_AT,
    )

    result = invoke(workspace, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_normalize_facts_cli_reports_receipt(tmp_path: Path) -> None:
    workspace = build_facts_case(tmp_path)

    result = invoke(workspace, "data", "normalize-fundamental-facts", "aapl", CIK)

    assert result.exit_code == 0
    assert "normalized fundamental-facts:" in result.output
    assert "artifact: normalized.fundamental-facts.0000320193." in result.output
    assert "rows: 2" in result.output
