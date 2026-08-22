import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from typer.testing import CliRunner, Result

import finresearch.normalization as normalization_module
from finresearch.cases import (
    DEFAULT_PATHS,
    Artifact,
    CaseManifest,
    initialize_case,
    read_manifest,
    write_manifest,
)
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_DAILY_PRICES_V1,
    NORMALIZED_INSTRUMENT_MASTER_V1,
    RAW_YFINANCE_DAILY_PRICES_V1,
)
from finresearch.ingestion import (
    ArtifactIntegrityError,
    IngestionError,
    ingest_yfinance_daily_prices,
    publish_snapshot,
)
from finresearch.normalization import normalize_daily_prices

runner = CliRunner()

RETRIEVED_AT = datetime(2026, 8, 11, 3, 12, 45, 123456, tzinfo=UTC)
NORMALIZED_AT = datetime(2026, 8, 11, 4, 0, 0, tzinfo=UTC)
SYMBOL = "AAPL"
INSTRUMENT_ID = "aapl"


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


def price_frame(
    *,
    symbol: str = SYMBOL,
    rows: int = 1,
    duplicate_session: bool = False,
    null_close: bool = False,
    currency: str = "USD",
) -> pl.DataFrame:
    """Build a raw-contract frame with the requested shape."""
    sessions: list[dict[str, object]] = []
    for index in range(rows):
        session_date = date(2026, 1, 2 + index)
        sessions.append(
            {
                "schema_version": 1,
                "provider": "yfinance",
                "provider_symbol": symbol,
                "currency": currency,
                "retrieved_at": RETRIEVED_AT,
                "requested_start": date(2026, 1, 1),
                "requested_end": date(2026, 1, 8),
                "interval": "1d",
                "provider_timezone": "America/New_York",
                "session_date": session_date,
                "timestamp": datetime(2026, 1, 2, 5, index, tzinfo=UTC),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": None if null_close else 101.0,
                "adj_close": 100.5,
                "volume": 1_000,
                "dividends": 0.0,
                "stock_splits": 0.0,
                "capital_gains": None,
            }
        )
    if duplicate_session:
        sessions.append(
            {
                **sessions[0],
                "timestamp": datetime(2026, 1, 2, 5, 30, tzinfo=UTC),
            }
        )
    return pl.DataFrame(sessions, schema=RAW_YFINANCE_DAILY_PRICES_V1.schema)


class FakePriceProvider:
    """Return contract-shaped data without making a network request."""

    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        return self.frame.with_columns(pl.lit(retrieved_at).alias("retrieved_at"))


def build_raw_case(
    tmp_path: Path,
    *,
    symbol: str = SYMBOL,
    rows: int = 1,
    duplicate_session: bool = False,
    null_close: bool = False,
    snapshots: int = 1,
    currency: str = "USD",
) -> Path:
    """Ingest one or more raw snapshots through the real ingestion path."""
    initialize_case(tmp_path, "aapl")
    for index in range(snapshots):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            symbol,
            date(2026, 1, 1),
            date(2026, 1, 8),
            provider=FakePriceProvider(
                price_frame(
                    symbol=symbol,
                    rows=rows,
                    duplicate_session=duplicate_session,
                    null_close=null_close,
                    currency=currency,
                )
            ),
            retrieved_at=RETRIEVED_AT.replace(minute=index),
        )
    return tmp_path


def build_v1_raw_case_without_manifest_retrieved_at(tmp_path: Path) -> Path:
    """Create a legacy raw declaration whose timestamp lives only in Parquet."""
    case_dir = initialize_case(tmp_path, "aapl")
    path = "data/raw/yfinance/daily-prices/aapl/legacy.parquet"
    output = case_dir / path
    output.parent.mkdir(parents=True)
    price_frame().write_parquet(output, compression="zstd")
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=1,
            case_id="aapl",
            title="Legacy case",
            status="active",
            paths=dict(DEFAULT_PATHS),
            artifacts=(
                Artifact(
                    artifact_id="raw.yfinance.daily-prices.aapl.legacy",
                    kind="raw.yfinance.daily-prices",
                    schema_version=1,
                    path=path,
                    source="yfinance",
                    sha256=hashlib.sha256(output.read_bytes()).hexdigest(),
                    row_count=1,
                ),
            ),
        ),
    )
    return tmp_path


def normalized_artifacts(workspace: Path) -> tuple[Artifact, Artifact]:
    """Return the normalized artifact declarations from the case manifest."""
    manifest = read_manifest(workspace / "cases" / "aapl")
    master = next(
        artifact
        for artifact in manifest.artifacts
        if artifact.kind == "normalized.instrument-master"
    )
    prices = next(
        artifact
        for artifact in manifest.artifacts
        if artifact.kind == "normalized.daily-prices"
    )
    return master, prices


def test_normalize_writes_master_and_prices(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)

    receipt = normalize_daily_prices(
        workspace,
        "aapl",
        SYMBOL,
        normalized_at=NORMALIZED_AT,
    )

    assert receipt.instrument_master.artifact_id.startswith(
        "normalized.instrument-master.aapl."
    )
    assert receipt.daily_prices.artifact_id.startswith("normalized.daily-prices.aapl.")
    master, prices = normalized_artifacts(workspace)
    assert master.path.startswith("data/normalized/normalized.instrument-master/")
    assert prices.path.startswith("data/normalized/normalized.daily-prices/")
    assert master.source is None
    assert master.input_artifact_ids
    assert master.input_file_hashes
    assert master.row_count == 1
    assert prices.row_count == 1


def test_normalize_outputs_pass_data_validate(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)
    normalize_daily_prices(workspace, "aapl", SYMBOL, normalized_at=NORMALIZED_AT)

    result = invoke(workspace, "data", "validate", "aapl")

    assert result.exit_code == 0
    assert "valid: all declared artifacts of aapl" in result.output


def test_normalize_master_fields(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path, rows=2)
    normalize_daily_prices(workspace, "aapl", SYMBOL, normalized_at=NORMALIZED_AT)

    master_path = (
        workspace
        / "cases"
        / "aapl"
        / next(
            artifact.path
            for artifact in read_manifest(workspace / "cases" / "aapl").artifacts
            if artifact.kind == "normalized.instrument-master"
        )
    )
    frame = pl.read_parquet(master_path)
    NORMALIZED_INSTRUMENT_MASTER_V1.validate(frame)
    row = frame.row(0, named=True)
    assert row["instrument_id"] == INSTRUMENT_ID
    assert row["provider_symbol"] == SYMBOL
    assert row["currency"] == "USD"
    assert row["provider_timezone"] == "America/New_York"
    assert row["first_session_date"] == date(2026, 1, 2)
    assert row["last_session_date"] == date(2026, 1, 3)
    assert row["observation_count"] == 2
    assert row["normalized_at"] == NORMALIZED_AT


def test_normalize_prices_fields(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)
    normalize_daily_prices(workspace, "aapl", SYMBOL, normalized_at=NORMALIZED_AT)

    prices_path = (
        workspace
        / "cases"
        / "aapl"
        / next(
            artifact.path
            for artifact in read_manifest(workspace / "cases" / "aapl").artifacts
            if artifact.kind == "normalized.daily-prices"
        )
    )
    frame = pl.read_parquet(prices_path)
    NORMALIZED_DAILY_PRICES_V1.validate(frame)
    row = frame.row(0, named=True)
    assert row["instrument_id"] == INSTRUMENT_ID
    assert row["provider_symbol"] == SYMBOL
    assert row["price_basis"] == "unadjusted"
    assert row["currency"] == "USD"
    assert row["open"] == 100.0
    assert row["close"] == 101.0
    assert row["dividends"] == 0.0
    assert row["stock_splits"] == 0.0
    assert row["source_artifact_id"].startswith("raw.yfinance.daily-prices.aapl.")
    assert row["normalized_at"] == NORMALIZED_AT


def test_normalize_carries_currency_from_raw_snapshot(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path, symbol="0700.HK", currency="HKD")
    normalize_daily_prices(workspace, "aapl", "0700.HK", normalized_at=NORMALIZED_AT)

    manifest = read_manifest(workspace / "cases" / "aapl")
    for artifact in manifest.artifacts:
        if artifact.kind not in (
            "normalized.instrument-master",
            "normalized.daily-prices",
        ):
            continue
        frame = pl.read_parquet(workspace / "cases" / "aapl" / artifact.path)
        assert frame["currency"].unique().to_list() == ["HKD"]
        assert frame["instrument_id"].unique().to_list() == ["0700.hk"]


def test_normalize_lineage_points_at_raw_artifact(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)
    normalize_daily_prices(workspace, "aapl", SYMBOL, normalized_at=NORMALIZED_AT)

    master, prices = normalized_artifacts(workspace)
    raw_ids = [
        artifact.artifact_id
        for artifact in read_manifest(workspace / "cases" / "aapl").artifacts
        if artifact.kind == "raw.yfinance.daily-prices"
    ]
    assert len(raw_ids) == 1
    assert master.input_artifact_ids == (raw_ids[0],)
    assert prices.input_artifact_ids == (raw_ids[0],)
    assert master.input_file_hashes[0].path == next(
        artifact.path
        for artifact in read_manifest(workspace / "cases" / "aapl").artifacts
        if artifact.artifact_id == raw_ids[0]
    )


def test_normalize_no_raw_snapshot_fails(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    with pytest.raises(IngestionError, match="run data ingest-yfinance-prices"):
        normalize_daily_prices(tmp_path, "aapl", SYMBOL)


def test_normalize_multiple_snapshots_requires_selection(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path, snapshots=2)

    with pytest.raises(IngestionError, match="--raw-artifact-id"):
        normalize_daily_prices(workspace, "aapl", SYMBOL)

    raw_ids = [
        artifact.artifact_id
        for artifact in read_manifest(workspace / "cases" / "aapl").artifacts
        if artifact.kind == "raw.yfinance.daily-prices"
    ]
    receipt = normalize_daily_prices(
        workspace,
        "aapl",
        SYMBOL,
        raw_artifact_id=raw_ids[0],
        normalized_at=NORMALIZED_AT,
    )
    assert receipt.daily_prices.artifact_id.startswith("normalized.daily-prices.aapl.")


def test_normalize_rejects_duplicate_session_dates(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path, duplicate_session=True)

    with pytest.raises(IngestionError, match="duplicate session dates"):
        normalize_daily_prices(workspace, "aapl", SYMBOL)


def test_normalize_rejects_null_prices(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path, null_close=True)

    with pytest.raises(IngestionError, match="cannot be normalized"):
        normalize_daily_prices(workspace, "aapl", SYMBOL)


def test_normalize_rerun_appends_second_pair(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)
    normalize_daily_prices(
        workspace,
        "aapl",
        SYMBOL,
        normalized_at=NORMALIZED_AT,
    )
    normalize_daily_prices(
        workspace,
        "aapl",
        SYMBOL,
        normalized_at=NORMALIZED_AT.replace(minute=30),
    )

    manifest = read_manifest(workspace / "cases" / "aapl")
    normalized = [
        artifact
        for artifact in manifest.artifacts
        if artifact.kind.startswith("normalized.")
    ]
    assert len(normalized) == 4


def test_normalize_deterministic_rerun_reuses_receipts_and_bytes(
    tmp_path: Path,
) -> None:
    workspace = build_raw_case(tmp_path)

    first = normalize_daily_prices(workspace, "aapl", SYMBOL)
    master_path = first.instrument_master.path
    prices_path = first.daily_prices.path
    original_bytes = (master_path.read_bytes(), prices_path.read_bytes())
    second = normalize_daily_prices(workspace, "aapl", SYMBOL)

    assert second.instrument_master == first.instrument_master
    assert second.daily_prices == first.daily_prices
    assert (master_path.read_bytes(), prices_path.read_bytes()) == original_bytes
    manifest = read_manifest(workspace / "cases" / "aapl")
    normalized = [
        item for item in manifest.artifacts if item.kind.startswith("normalized.")
    ]
    assert len(normalized) == 2


def test_normalize_recovers_after_interrupt_between_pair_publications(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = build_raw_case(tmp_path)
    case_dir = workspace / "cases" / "aapl"
    publish = normalization_module._publish_normalized_snapshot
    calls = 0

    def interrupt_prices(**kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KeyboardInterrupt
        return publish(**kwargs)

    monkeypatch.setattr(
        normalization_module,
        "_publish_normalized_snapshot",
        interrupt_prices,
    )
    with pytest.raises(KeyboardInterrupt):
        normalize_daily_prices(
            workspace,
            "aapl",
            SYMBOL,
            normalized_at=NORMALIZED_AT,
        )

    interrupted_manifest = read_manifest(case_dir)
    interrupted_master = next(
        artifact
        for artifact in interrupted_manifest.artifacts
        if artifact.kind == "normalized.instrument-master"
    )
    master_path = case_dir / interrupted_master.path
    master_bytes = master_path.read_bytes()
    assert not any(
        artifact.kind == "normalized.daily-prices"
        for artifact in interrupted_manifest.artifacts
    )

    monkeypatch.setattr(
        normalization_module,
        "_publish_normalized_snapshot",
        publish,
    )
    recovered = normalize_daily_prices(
        workspace,
        "aapl",
        SYMBOL,
        normalized_at=NORMALIZED_AT,
    )

    recovered_manifest = read_manifest(case_dir)
    normalized = [
        artifact
        for artifact in recovered_manifest.artifacts
        if artifact.kind.startswith("normalized.")
    ]
    assert recovered.instrument_master.path == master_path
    assert master_path.read_bytes() == master_bytes
    master_count = sum(
        artifact.kind == "normalized.instrument-master" for artifact in normalized
    )
    prices_count = sum(
        artifact.kind == "normalized.daily-prices" for artifact in normalized
    )
    assert master_count == 1
    assert prices_count == 1


def test_deterministic_publish_rejects_conflicting_bytes_without_overwrite(
    tmp_path: Path,
) -> None:
    workspace = build_raw_case(tmp_path)
    normalize_daily_prices(workspace, "aapl", SYMBOL, normalized_at=NORMALIZED_AT)
    case_dir = workspace / "cases" / "aapl"
    manifest = read_manifest(case_dir)
    master = next(
        item
        for item in manifest.artifacts
        if item.kind == "normalized.instrument-master"
    )
    master_path = case_dir / master.path
    original_bytes = master_path.read_bytes()
    conflicting_frame = pl.read_parquet(master_path).with_columns(
        pl.lit(2, dtype=pl.Int64).alias("observation_count")
    )

    with pytest.raises(ArtifactIntegrityError, match="metadata conflict"):
        publish_snapshot(
            case_dir=case_dir,
            manifest=manifest,
            path_role="normalized",
            frame=conflicting_frame,
            contract=NORMALIZED_INSTRUMENT_MASTER_V1,
            path_parts=("normalized.instrument-master", INSTRUMENT_ID),
            entity_key=INSTRUMENT_ID,
            identity=master.parameters_sha256 or "",
            producer=master.producer or "",
            producer_version=master.producer_version or "",
            parameters_sha256=master.parameters_sha256 or "",
            input_artifact_ids=master.input_artifact_ids,
            produced_at=NORMALIZED_AT,
        )

    assert master_path.read_bytes() == original_bytes
    assert len(read_manifest(case_dir).artifacts) == len(manifest.artifacts)


def test_normalize_cli_reports_both_receipts(tmp_path: Path) -> None:
    workspace = build_raw_case(tmp_path)

    result = invoke(workspace, "data", "normalize-daily-prices", "aapl", SYMBOL)

    assert result.exit_code == 0
    assert "normalized instrument-master:" in result.output
    assert "normalized daily-prices:" in result.output
    assert "artifact: normalized.instrument-master.aapl." in result.output
    assert "artifact: normalized.daily-prices.aapl." in result.output


def test_normalize_cli_v1_without_manifest_retrieved_at_is_deterministic(
    tmp_path: Path,
) -> None:
    workspace = build_v1_raw_case_without_manifest_retrieved_at(tmp_path)

    first = invoke(workspace, "data", "normalize-daily-prices", "aapl", SYMBOL)
    second = invoke(workspace, "data", "normalize-daily-prices", "aapl", SYMBOL)

    assert first.exit_code == 0
    assert second.exit_code == 0
    manifest = read_manifest(workspace / "cases" / "aapl")
    normalized = [
        artifact
        for artifact in manifest.artifacts
        if artifact.kind.startswith("normalized.")
    ]
    assert manifest.manifest_version == 1
    assert len(normalized) == 2
    for artifact in normalized:
        frame = pl.read_parquet(workspace / "cases" / "aapl" / artifact.path)
        assert frame.get_column("normalized_at").unique().to_list() == [RETRIEVED_AT]


def test_normalize_cli_failure_exits_nonzero(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    result = invoke(tmp_path, "data", "normalize-daily-prices", "aapl", SYMBOL)

    assert result.exit_code == 1
    assert "no raw yfinance daily-prices snapshot" in result.output
