import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner, Result

from finresearch.cases import Artifact, InputFileHash, append_artifact, initialize_case
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_INSTRUMENT_MASTER_V1,
    RAW_SEC_SUBMISSIONS_V1,
    RAW_YFINANCE_DAILY_PRICES_V1,
)
from finresearch.data_validation import DataValidationError, inspect_artifact

runner = CliRunner()

RETRIEVED_AT = datetime(2026, 8, 11, 3, 12, 45, 123456, tzinfo=UTC)
RETRIEVED_AT_TEXT = RETRIEVED_AT.isoformat().replace("+00:00", "Z")
PRICE_ARTIFACT_ID = "raw.yfinance.daily-prices.aapl.snapshot"
PRICE_ARTIFACT_PATH = "data/raw/yfinance/daily-prices/aapl/snapshot.parquet"


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


def price_frame(*, retrieved_at: datetime = RETRIEVED_AT) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "yfinance",
                "provider_symbol": "aapl",
                "currency": "USD",
                "retrieved_at": retrieved_at,
                "requested_start": date(2026, 1, 1),
                "requested_end": date(2026, 1, 8),
                "interval": "1d",
                "provider_timezone": "America/New_York",
                "session_date": date(2026, 1, 2),
                "timestamp": datetime(2026, 1, 2, 5, tzinfo=UTC),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "adj_close": 100.5,
                "volume": 1_000,
                "dividends": 0.0,
                "stock_splits": 0.0,
                "capital_gains": None,
            }
        ],
        schema=RAW_YFINANCE_DAILY_PRICES_V1.schema,
    )


def submissions_frame(*, retrieved_at: datetime = RETRIEVED_AT) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "sec",
                "cik": "0000320193",
                "retrieved_at": retrieved_at,
                "source_url": "https://data.sec.gov/submissions/CIK0000320193.json",
                "entity_name": "APPLE INC",
                "tickers": ["AAPL"],
                "exchanges": ["Nasdaq"],
                "sic": "3571",
                "sic_description": None,
                "accession_number": "0000320193-26-000001",
                "filing_date": date(2026, 1, 2),
                "report_date": None,
                "acceptance_datetime": "2026-01-02T09:00:00-05:00",
                "act": "34",
                "form": "8-K",
                "file_number": None,
                "film_number": None,
                "items": None,
                "size": 123,
                "is_xbrl": True,
                "is_inline_xbrl": False,
                "primary_document": "a8k.htm",
                "primary_doc_description": None,
            }
        ],
        schema=RAW_SEC_SUBMISSIONS_V1.schema,
    )


def write_artifact(
    case_dir: Path,
    frame: pl.DataFrame,
    *,
    artifact_id: str,
    kind: str,
    path: str,
    schema_version: int = 1,
    sha256: str | None = None,
    row_count: int | None = None,
    retrieved_at: str = RETRIEVED_AT_TEXT,
    input_artifact_ids: tuple[str, ...] = (),
    input_file_hashes: tuple[InputFileHash, ...] = (),
) -> Path:
    """Persist a parquet snapshot and declare it in the case manifest."""
    output = case_dir / path
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(output, compression="zstd")
    digest = sha256 or hashlib.sha256(output.read_bytes()).hexdigest()
    append_artifact(
        case_dir,
        Artifact(
            artifact_id=artifact_id,
            kind=kind,
            schema_version=schema_version,
            path=path,
            sha256=digest,
            retrieved_at=retrieved_at,
            row_count=frame.height if row_count is None else row_count,
            producer="finresearch.test",
            producer_version="1",
            parameters_sha256=hashlib.sha256(artifact_id.encode()).hexdigest(),
            input_artifact_ids=input_artifact_ids,
            input_file_hashes=input_file_hashes,
        ),
    )
    return output


@pytest.fixture
def price_case(tmp_path: Path) -> Path:
    """A workspace containing one valid yfinance price artifact."""
    initialize_case(tmp_path, "aapl")
    write_artifact(
        tmp_path / "cases" / "aapl",
        price_frame(),
        artifact_id=PRICE_ARTIFACT_ID,
        kind="raw.yfinance.daily-prices",
        path=PRICE_ARTIFACT_PATH,
    )
    return tmp_path


def test_validate_passes_for_registered_artifact(price_case: Path) -> None:
    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_validate_targets_one_artifact(price_case: Path) -> None:
    result = invoke(price_case, "data", "validate", "aapl", PRICE_ARTIFACT_ID)

    assert result.exit_code == 0
    assert f"valid: {PRICE_ARTIFACT_ID}" in result.output


def test_validate_no_artifacts_is_valid(tmp_path: Path) -> None:
    initialize_case(tmp_path, "empty")

    result = invoke(tmp_path, "data", "validate", "empty")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of empty" in result.output


def test_validate_all_checks_declared_non_parquet_artifact(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    report_path = case_dir / "reports/summary.md"
    report_path.write_text("# Summary\n", encoding="utf-8")
    append_artifact(
        case_dir,
        Artifact(
            artifact_id="report.summary",
            kind="report.markdown",
            schema_version=1,
            path="reports/summary.md",
            producer="finresearch.test",
            producer_version="1",
            parameters_sha256="a" * 64,
            sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_validate_exact_non_parquet_artifact_checks_integrity(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    report_path = case_dir / "reports/summary.md"
    report_path.write_text("# Summary\n", encoding="utf-8")
    append_artifact(
        case_dir,
        Artifact(
            artifact_id="report.summary",
            kind="report.markdown",
            schema_version=1,
            path="reports/summary.md",
            producer="finresearch.test",
            producer_version="1",
            parameters_sha256="a" * 64,
            sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl", "report.summary")

    assert result.exit_code == 0
    assert "valid: report.summary" in result.output


def test_inspect_rejects_non_parquet_artifact(
    price_case: Path,
) -> None:
    case_dir = price_case / "cases" / "aapl"
    report_path = case_dir / "reports/summary.md"
    report_path.write_text("# Summary\n", encoding="utf-8")
    append_artifact(
        case_dir,
        Artifact(
            artifact_id="report.summary",
            kind="report.markdown",
            schema_version=1,
            path="reports/summary.md",
            producer="finresearch.test",
            producer_version="1",
            parameters_sha256="a" * 64,
            sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        ),
    )

    result = invoke(price_case, "data", "inspect", "aapl", "report.summary")

    assert result.exit_code == 1
    assert "data inspect supports Parquet artifacts only" in result.output


def test_validate_checksum_mismatch(price_case: Path) -> None:
    artifact_path = price_case / "cases" / "aapl" / PRICE_ARTIFACT_PATH
    artifact_path.write_bytes(b"tampered snapshot bytes")

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [checksum_mismatch]" in result.output


def test_validate_non_parquet_checksum_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
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
    report_path.write_bytes(b"# Tampered\n")

    result = invoke(price_case, "data", "validate", "aapl", "report.summary")

    assert result.exit_code == 1
    assert "error [checksum_mismatch]" in result.output


def test_validate_row_count_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    write_artifact(
        case_dir,
        price_frame(),
        artifact_id="raw.yfinance.daily-prices.aapl.other",
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/other.parquet",
        row_count=999,
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [row_count_mismatch]" in result.output
    assert "manifest 999, parquet 1" in result.output


def test_validate_unknown_contract(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    write_artifact(
        case_dir,
        price_frame(),
        artifact_id="raw.unknown.foo.snapshot",
        kind="raw.unknown.foo",
        path="data/raw/unknown/foo.parquet",
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [unknown_contract]" in result.output
    assert "raw.unknown.foo" in result.output


def test_validate_contract_violation(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    malformed = price_frame().with_columns(pl.lit("unexpected").alias("extra"))
    write_artifact(
        case_dir,
        malformed,
        artifact_id="raw.yfinance.daily-prices.aapl.bad",
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/bad.parquet",
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [contract_violation]" in result.output
    assert "schema mismatch" in result.output


def test_validate_provenance_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    stale = price_frame(retrieved_at=datetime(2026, 8, 11, 5, 0, tzinfo=UTC))
    write_artifact(
        case_dir,
        stale,
        artifact_id="raw.yfinance.daily-prices.aapl.stale",
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/stale.parquet",
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [provenance_mismatch]" in result.output


def test_validate_reports_only_broken_artifacts(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    write_artifact(
        case_dir,
        price_frame(),
        artifact_id="raw.yfinance.daily-prices.aapl.broken",
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/broken.parquet",
        row_count=999,
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [row_count_mismatch]" in result.output
    assert "raw.yfinance.daily-prices.aapl.broken" in result.output
    assert result.output.count("error [") == 1
    assert PRICE_ARTIFACT_ID not in result.output


def test_validate_unknown_artifact_id(price_case: Path) -> None:
    result = invoke(price_case, "data", "validate", "aapl", "not-declared")

    assert result.exit_code == 1
    assert "artifact not declared: not-declared" in result.output


def test_validate_missing_artifact_file(price_case: Path) -> None:
    artifact_path = price_case / "cases" / "aapl" / PRICE_ARTIFACT_PATH
    artifact_path.unlink()

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [artifact_missing]" in result.output


def test_validate_missing_case(tmp_path: Path) -> None:
    result = invoke(tmp_path, "data", "validate", "missing")

    assert result.exit_code == 1
    assert "case not found: missing" in result.output


def test_inspect_reports_file_and_contract_facts(price_case: Path) -> None:
    result = invoke(price_case, "data", "inspect", "aapl", PRICE_ARTIFACT_ID)

    assert result.exit_code == 0
    assert f"artifact: {PRICE_ARTIFACT_ID}" in result.output
    assert "contract: raw.yfinance.daily-prices.v1" in result.output
    assert "size: " in result.output
    assert "sha256: " in result.output
    assert "rows: 1" in result.output
    assert "provider: yfinance" in result.output
    assert "provider_symbol: aapl" in result.output
    assert "columns (20):" in result.output
    assert "  timestamp: Datetime(us, UTC)" in result.output
    assert "date ranges:" in result.output
    assert "  session_date: 2026-01-02 .. 2026-01-02" in result.output
    assert "capital_gains: 1" in result.output
    assert "duplicate key rows: 0" in result.output
    assert "preview:" in result.output
    preview_lines = result.output.splitlines()
    preview = json.loads(preview_lines[preview_lines.index("preview:") + 1])
    assert preview == [
        {
            "schema_version": 1,
            "provider": "yfinance",
            "provider_symbol": "aapl",
            "currency": "USD",
            "retrieved_at": "2026-08-11T03:12:45.123456+00:00",
            "requested_start": "2026-01-01",
            "requested_end": "2026-01-08",
            "interval": "1d",
            "provider_timezone": "America/New_York",
            "session_date": "2026-01-02",
            "timestamp": "2026-01-02T05:00:00+00:00",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "adj_close": 100.5,
            "volume": 1000,
            "dividends": 0.0,
            "stock_splits": 0.0,
            "capital_gains": None,
        }
    ]


def test_inspect_sec_artifact_shows_cik_and_source(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    write_artifact(
        tmp_path / "cases" / "aapl",
        submissions_frame(),
        artifact_id="raw.sec.submissions.0000320193.snapshot",
        kind="raw.sec.submissions",
        path="data/raw/sec/submissions/0000320193/snapshot.parquet",
    )

    result = invoke(
        tmp_path,
        "data",
        "inspect",
        "aapl",
        "raw.sec.submissions.0000320193.snapshot",
    )

    assert result.exit_code == 0
    assert "provider: sec" in result.output
    assert "cik: 0000320193" in result.output
    assert (
        "source_url: https://data.sec.gov/submissions/CIK0000320193.json"
        in result.output
    )


def test_inspect_rejects_unregistered_contract(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    write_artifact(
        tmp_path / "cases" / "aapl",
        price_frame(),
        artifact_id="raw.unknown.foo.snapshot",
        kind="raw.unknown.foo",
        path="data/raw/unknown/foo.parquet",
    )

    result = invoke(tmp_path, "data", "inspect", "aapl", "raw.unknown.foo.snapshot")

    assert result.exit_code == 1
    assert "[unknown_contract]" in result.output


def test_inspect_limit_zero_omits_preview(price_case: Path) -> None:
    result = invoke(
        price_case,
        "data",
        "inspect",
        "aapl",
        PRICE_ARTIFACT_ID,
        "--limit",
        "0",
    )

    assert result.exit_code == 0
    assert "preview:" not in result.output


def test_inspect_duplicate_key_rows(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    duplicated = pl.concat([price_frame(), price_frame()])
    write_artifact(
        case_dir,
        duplicated,
        artifact_id="raw.yfinance.daily-prices.aapl.duplicated",
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/duplicated.parquet",
    )

    result = invoke(
        price_case,
        "data",
        "inspect",
        "aapl",
        "raw.yfinance.daily-prices.aapl.duplicated",
    )

    assert result.exit_code == 1
    assert "[contract_violation]" in result.output
    assert "duplicate rows" in result.output


def test_inspect_rejects_checksum_mismatch(price_case: Path) -> None:
    artifact_path = price_case / "cases" / "aapl" / PRICE_ARTIFACT_PATH
    price_frame().write_parquet(artifact_path, compression="uncompressed")

    result = invoke(price_case, "data", "inspect", "aapl", PRICE_ARTIFACT_ID)

    assert result.exit_code == 1
    assert "[checksum_mismatch]" in result.output


def test_inspect_rejects_row_count_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    artifact_id = "raw.yfinance.daily-prices.aapl.wrong-rows"
    write_artifact(
        case_dir,
        price_frame(),
        artifact_id=artifact_id,
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/wrong-rows.parquet",
        row_count=2,
    )

    result = invoke(price_case, "data", "inspect", "aapl", artifact_id)

    assert result.exit_code == 1
    assert "[row_count_mismatch]" in result.output


def test_inspect_rejects_provenance_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    artifact_id = "raw.yfinance.daily-prices.aapl.stale-inspection"
    write_artifact(
        case_dir,
        price_frame(retrieved_at=datetime(2026, 8, 11, 5, tzinfo=UTC)),
        artifact_id=artifact_id,
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/stale-inspection.parquet",
    )

    result = invoke(price_case, "data", "inspect", "aapl", artifact_id)

    assert result.exit_code == 1
    assert "[provenance_mismatch]" in result.output


def test_inspect_missing_retrieved_at_reports_contract_error(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    artifact_id = "raw.yfinance.daily-prices.aapl.missing-retrieved-at"
    write_artifact(
        case_dir,
        price_frame().drop("retrieved_at"),
        artifact_id=artifact_id,
        kind="raw.yfinance.daily-prices",
        path="data/raw/yfinance/daily-prices/aapl/missing-retrieved-at.parquet",
    )

    result = invoke(price_case, "data", "inspect", "aapl", artifact_id)

    assert result.exit_code == 1
    assert "[contract_violation]" in result.output
    assert "schema mismatch" in result.output
    assert "ColumnNotFoundError" not in result.output


def test_inspect_rejects_limit_over_cli_cap(price_case: Path) -> None:
    result = invoke(
        price_case,
        "data",
        "inspect",
        "aapl",
        PRICE_ARTIFACT_ID,
        "--limit",
        "101",
    )

    assert result.exit_code == 2
    assert "101 is not in the range 0<=x<=100" in result.output


def test_inspect_service_rejects_limit_over_cap(price_case: Path) -> None:
    with pytest.raises(DataValidationError, match="must not exceed 100"):
        inspect_artifact(price_case, "aapl", PRICE_ARTIFACT_ID, 101)


def test_inspect_unknown_artifact_id(price_case: Path) -> None:
    result = invoke(price_case, "data", "inspect", "aapl", "not-declared")

    assert result.exit_code == 1
    assert "artifact not declared: not-declared" in result.output


def master_frame(*, source_artifact_id: str) -> pl.DataFrame:
    """Build one normalized instrument-master row with explicit lineage."""
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "yfinance",
                "instrument_id": "aapl",
                "provider_symbol": "aapl",
                "currency": "USD",
                "provider_timezone": "America/New_York",
                "first_session_date": date(2026, 1, 2),
                "last_session_date": date(2026, 1, 2),
                "observation_count": 1,
                "source_artifact_id": source_artifact_id,
                "normalized_at": datetime(2026, 8, 11, 4, 0, tzinfo=UTC),
            }
        ],
        schema=NORMALIZED_INSTRUMENT_MASTER_V1.schema,
    )


def test_validate_rejects_dangling_lineage(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    write_artifact(
        case_dir,
        master_frame(source_artifact_id="raw.yfinance.daily-prices.aapl.ghost"),
        artifact_id="normalized.instrument-master.aapl.ghost",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/ghost.parquet",
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [lineage_invalid]" in result.output
    assert "does not match manifest input_artifact_ids" in result.output


def test_validate_accepts_declared_lineage(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    parent_path = case_dir / PRICE_ARTIFACT_PATH
    write_artifact(
        case_dir,
        master_frame(source_artifact_id=PRICE_ARTIFACT_ID),
        artifact_id="normalized.instrument-master.aapl.valid",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/valid.parquet",
        input_artifact_ids=(PRICE_ARTIFACT_ID,),
        input_file_hashes=(
            InputFileHash(
                name=f"artifact.{PRICE_ARTIFACT_ID}",
                path=PRICE_ARTIFACT_PATH,
                sha256=hashlib.sha256(parent_path.read_bytes()).hexdigest(),
            ),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_validate_accepts_source_with_multiple_declared_v2_inputs(
    price_case: Path,
) -> None:
    case_dir = price_case / "cases" / "aapl"
    other_id = "raw.yfinance.daily-prices.aapl.other-input"
    other_path = "data/raw/yfinance/daily-prices/aapl/other-input.parquet"
    other_output = write_artifact(
        case_dir,
        price_frame(),
        artifact_id=other_id,
        kind="raw.yfinance.daily-prices",
        path=other_path,
    )
    parent_path = case_dir / PRICE_ARTIFACT_PATH
    write_artifact(
        case_dir,
        master_frame(source_artifact_id=PRICE_ARTIFACT_ID),
        artifact_id="normalized.instrument-master.aapl.multiple-inputs",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/multiple-inputs.parquet",
        input_artifact_ids=(PRICE_ARTIFACT_ID, other_id),
        input_file_hashes=(
            InputFileHash(
                name=f"artifact.{PRICE_ARTIFACT_ID}",
                path=PRICE_ARTIFACT_PATH,
                sha256=hashlib.sha256(parent_path.read_bytes()).hexdigest(),
            ),
            InputFileHash(
                name=f"artifact.{other_id}",
                path=other_path,
                sha256=hashlib.sha256(other_output.read_bytes()).hexdigest(),
            ),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_validate_rejects_lineage_switched_to_different_declared_parent(
    price_case: Path,
) -> None:
    case_dir = price_case / "cases" / "aapl"
    other_id = "raw.yfinance.daily-prices.aapl.other"
    other_path = "data/raw/yfinance/daily-prices/aapl/other.parquet"
    other_output = write_artifact(
        case_dir,
        price_frame(),
        artifact_id=other_id,
        kind="raw.yfinance.daily-prices",
        path=other_path,
    )
    write_artifact(
        case_dir,
        master_frame(source_artifact_id=PRICE_ARTIFACT_ID),
        artifact_id="normalized.instrument-master.aapl.switched",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/switched.parquet",
        input_artifact_ids=(other_id,),
        input_file_hashes=(
            InputFileHash(
                name=f"artifact.{other_id}",
                path=other_path,
                sha256=hashlib.sha256(other_output.read_bytes()).hexdigest(),
            ),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [lineage_invalid]" in result.output
    assert "does not match manifest input_artifact_ids" in result.output


def test_validate_rejects_v2_input_file_hash_mismatch(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    parent_path = case_dir / PRICE_ARTIFACT_PATH
    write_artifact(
        case_dir,
        master_frame(source_artifact_id=PRICE_ARTIFACT_ID),
        artifact_id="normalized.instrument-master.aapl.bad-hash",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/bad-hash.parquet",
        input_artifact_ids=(PRICE_ARTIFACT_ID,),
        input_file_hashes=(
            InputFileHash(
                name=f"artifact.{PRICE_ARTIFACT_ID}",
                path=PRICE_ARTIFACT_PATH,
                sha256=hashlib.sha256(parent_path.read_bytes()).hexdigest(),
            ),
        ),
    )
    parent_path.write_bytes(b"tampered raw input")

    result = invoke(
        price_case,
        "data",
        "validate",
        "aapl",
        "normalized.instrument-master.aapl.bad-hash",
    )

    assert result.exit_code == 1
    assert "error [input_file_checksum_mismatch]" in result.output
    assert "input file sha256 mismatch" in result.output


def test_validate_rejects_v2_missing_input_file(price_case: Path) -> None:
    case_dir = price_case / "cases" / "aapl"
    parent_path = case_dir / PRICE_ARTIFACT_PATH
    write_artifact(
        case_dir,
        master_frame(source_artifact_id=PRICE_ARTIFACT_ID),
        artifact_id="normalized.instrument-master.aapl.missing-input",
        kind="normalized.instrument-master",
        path="data/normalized/normalized.instrument-master/aapl/missing-input.parquet",
        input_artifact_ids=(PRICE_ARTIFACT_ID,),
        input_file_hashes=(
            InputFileHash(
                name=f"artifact.{PRICE_ARTIFACT_ID}",
                path=PRICE_ARTIFACT_PATH,
                sha256=hashlib.sha256(parent_path.read_bytes()).hexdigest(),
            ),
            InputFileHash(
                name="analysis-input",
                path="analysis/missing-input.csv",
                sha256="0" * 64,
            ),
        ),
    )

    result = invoke(price_case, "data", "validate", "aapl")

    assert result.exit_code == 1
    assert "error [input_file_missing]" in result.output
    assert "input file missing" in result.output


def test_validate_rejects_non_parquet_input_file_hash_mismatch(
    price_case: Path,
) -> None:
    case_dir = price_case / "cases" / "aapl"
    report_path = case_dir / "reports/summary.md"
    report_path.write_text("# Summary\n", encoding="utf-8")
    parent_path = case_dir / PRICE_ARTIFACT_PATH
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
            input_artifact_ids=(PRICE_ARTIFACT_ID,),
            input_file_hashes=(
                InputFileHash(
                    name=f"artifact.{PRICE_ARTIFACT_ID}",
                    path=PRICE_ARTIFACT_PATH,
                    sha256=hashlib.sha256(parent_path.read_bytes()).hexdigest(),
                ),
            ),
        ),
    )
    parent_path.write_bytes(b"tampered raw input")

    result = invoke(price_case, "data", "validate", "aapl", "report.summary")

    assert result.exit_code == 1
    assert "error [input_file_checksum_mismatch]" in result.output
    assert "input file sha256 mismatch" in result.output
