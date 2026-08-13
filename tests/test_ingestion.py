import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Barrier

import polars as pl
import pytest

import finresearch.ingestion as ingestion_module
from finresearch.cases import (
    Artifact,
    append_artifact,
    initialize_case,
    read_manifest,
)
from finresearch.data_contracts import RAW_YFINANCE_DAILY_PRICES_V1
from finresearch.ingestion import (
    IngestionError,
    ingest_sec_companyfacts,
    ingest_sec_submissions,
    ingest_yfinance_daily_prices,
)
from finresearch.providers.sec import (
    SECProviderError,
    companyfacts_to_frame,
    submissions_to_frame,
)

SEC_FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "sec"
PROCESS_WORKER = Path(__file__).parent / "process_ingestion_worker.py"


def valid_price_frame(
    *,
    symbol: str,
    retrieved_at: datetime,
    start: date,
    end: date,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": 1,
                "provider": "yfinance",
                "provider_symbol": symbol,
                "currency": "USD",
                "retrieved_at": retrieved_at,
                "requested_start": start,
                "requested_end": end,
                "interval": "1d",
                "provider_timezone": "America/New_York",
                "session_date": start,
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


class FakePriceProvider:
    """Return contract-shaped data without making a network request."""

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        return valid_price_frame(
            symbol=symbol,
            retrieved_at=retrieved_at,
            start=start,
            end=end,
        )


class FakeSECProvider:
    """Return SEC fixture frames without network access."""

    def fetch_submissions(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        assert user_agent == "Finresearch user@example.com"
        payload = json.loads(
            (SEC_FIXTURE_DIRECTORY / "submissions.json").read_text(encoding="utf-8")
        )
        return submissions_to_frame(
            payload,
            expected_cik=cik,
            source_url=f"https://data.sec.gov/submissions/CIK{cik}.json",
            retrieved_at=retrieved_at,
        )

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        assert user_agent == "Finresearch user@example.com"
        payload = json.loads(
            (SEC_FIXTURE_DIRECTORY / "companyfacts.json").read_text(encoding="utf-8")
        )
        return companyfacts_to_frame(
            payload,
            expected_cik=cik,
            source_url=(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"),
            retrieved_at=retrieved_at,
        )


class BarrierPriceProvider(FakePriceProvider):
    """Make concurrent fetches reach persistence at approximately the same time."""

    def __init__(self, barrier: Barrier) -> None:
        self._barrier = barrier

    def fetch_daily_prices(
        self,
        symbol: str,
        start: date,
        end: date,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        frame = super().fetch_daily_prices(symbol, start, end, retrieved_at)
        self._barrier.wait()
        return frame


class UnexpectedSECProvider:
    """Fail if invalid request metadata reaches an injected provider."""

    def fetch_submissions(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        raise AssertionError("provider must not be called")

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        raise AssertionError("provider must not be called")


class WrongSourceURLSECProvider(FakeSECProvider):
    """Return contract-shaped SEC data with forged source provenance."""

    def fetch_submissions(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        return (
            super()
            .fetch_submissions(cik, user_agent, retrieved_at)
            .with_columns(
                pl.lit("https://example.invalid/submissions.json").alias("source_url")
            )
        )

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        return (
            super()
            .fetch_companyfacts(cik, user_agent, retrieved_at)
            .with_columns(
                pl.lit("https://example.invalid/companyfacts.json").alias("source_url")
            )
        )


def test_ingestion_writes_parquet_and_registers_provenance(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    retrieved_at = datetime(2026, 8, 11, 4, 5, 6, 123456, tzinfo=UTC)

    receipt = ingest_yfinance_daily_prices(
        tmp_path,
        "aapl",
        "AAPL",
        date(2026, 1, 2),
        date(2026, 1, 3),
        provider=FakePriceProvider(),
        retrieved_at=retrieved_at,
    )

    assert receipt.path == (
        case_dir / "data/raw/yfinance/daily-prices/aapl/20260811T040506123456Z.parquet"
    )
    stored = pl.read_parquet(receipt.path)
    assert stored.height == 1
    assert stored.get_column("provider_symbol").to_list() == ["AAPL"]

    artifact = read_manifest(case_dir).artifacts[0]
    assert artifact.artifact_id == receipt.artifact_id
    assert artifact.kind == "raw.yfinance.daily-prices"
    assert artifact.schema_version == 1
    assert artifact.path == receipt.path.relative_to(case_dir).as_posix()
    assert artifact.source == "yfinance"
    assert artifact.sha256 == receipt.sha256
    assert artifact.retrieved_at == "2026-08-11T04:05:06.123456Z"
    assert artifact.row_count == 1


def test_ingestion_never_overwrites_same_snapshot(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    retrieved_at = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)
    arguments = (
        tmp_path,
        "aapl",
        "AAPL",
        date(2026, 1, 2),
        date(2026, 1, 3),
    )
    first = ingest_yfinance_daily_prices(
        *arguments,
        provider=FakePriceProvider(),
        retrieved_at=retrieved_at,
    )
    original = first.path.read_bytes()

    with pytest.raises(IngestionError, match="already exists"):
        ingest_yfinance_daily_prices(
            *arguments,
            provider=FakePriceProvider(),
            retrieved_at=retrieved_at,
        )

    assert first.path.read_bytes() == original
    assert len(read_manifest(tmp_path / "cases/aapl").artifacts) == 1


def test_ingestion_does_not_repair_missing_declared_snapshot_with_stale_metadata(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    relative_path = "data/raw/yfinance/daily-prices/aapl/20260811T000000000000Z.parquet"
    artifact_id = "raw.yfinance.daily-prices.aapl.20260811t000000000000z"
    append_artifact(
        case_dir,
        Artifact(
            artifact_id=artifact_id,
            kind="raw.yfinance.daily-prices",
            schema_version=1,
            path=relative_path,
            source="yfinance",
            sha256="0" * 64,
            retrieved_at="2026-08-11T00:00:00Z",
            row_count=999,
        ),
    )

    with pytest.raises(IngestionError, match="declared in the manifest"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert not (case_dir / relative_path).exists()
    artifact = read_manifest(case_dir).artifacts[0]
    assert artifact.sha256 == "0" * 64
    assert artifact.row_count == 999


def test_ingestion_rejects_inconsistent_provider_metadata(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    class WrongSymbolProvider(FakePriceProvider):
        def fetch_daily_prices(
            self,
            symbol: str,
            start: date,
            end: date,
            retrieved_at: datetime,
        ) -> pl.DataFrame:
            return valid_price_frame(
                symbol="MSFT",
                retrieved_at=retrieved_at,
                start=start,
                end=end,
            )

    with pytest.raises(IngestionError, match="provider_symbol"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=WrongSymbolProvider(),
        )


def test_ingestion_rejects_invalid_period_without_fetch(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    with pytest.raises(IngestionError, match="earlier"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 3),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
        )


def test_manifest_failure_removes_new_raw_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def fail_registration(*args: object, **kwargs: object) -> None:
        raise OSError("manifest unavailable")

    monkeypatch.setattr(ingestion_module, "append_artifact", fail_registration)

    with pytest.raises(OSError, match="manifest unavailable"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_interrupted_manifest_registration_removes_new_raw_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def interrupt_registration(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        ingestion_module,
        "append_artifact",
        interrupt_registration,
    )

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_interrupted_manifest_write_removes_temporary_and_raw_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def interrupt_serialization(*args: object, **kwargs: object) -> str:
        raise KeyboardInterrupt

    monkeypatch.setattr("finresearch.cases.tomli_w.dumps", interrupt_serialization)

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list(case_dir.glob(".manifest.*.tmp")) == []
    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_interrupt_after_manifest_commit_preserves_registered_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def commit_then_interrupt(case_dir_argument: Path, artifact: Artifact) -> None:
        append_artifact(case_dir_argument, artifact)
        raise KeyboardInterrupt

    monkeypatch.setattr(
        ingestion_module,
        "append_artifact",
        commit_then_interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    manifest = read_manifest(case_dir)
    assert len(manifest.artifacts) == 1
    assert (case_dir / manifest.artifacts[0].path).is_file()


def test_interrupted_parquet_write_removes_temporary_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")

    def interrupt_write(
        self: pl.DataFrame,
        *args: object,
        **kwargs: object,
    ) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(pl.DataFrame, "write_parquet", interrupt_write)

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert list((case_dir / "data/raw").rglob("*.tmp")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_interrupted_temporary_file_close_removes_temporary_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    real_close = os.close

    def close_then_interrupt(file_descriptor: int) -> None:
        real_close(file_descriptor)
        raise KeyboardInterrupt

    monkeypatch.setattr("finresearch.ingestion.os.close", close_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.tmp")) == []
    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_atomic_publication_does_not_replace_racing_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    real_link = os.link
    sentinel = b"written by another process"

    def race_before_link(source: Path, destination: Path) -> None:
        Path(destination).write_bytes(sentinel)
        real_link(source, destination)

    monkeypatch.setattr("finresearch.ingestion.os.link", race_before_link)

    with pytest.raises(IngestionError, match="already exists"):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    output_path = (
        case_dir / "data/raw/yfinance/daily-prices/aapl/20260811T000000000000Z.parquet"
    )
    assert output_path.read_bytes() == sentinel
    assert list((case_dir / "data/raw").rglob("*.tmp")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_interrupted_atomic_publication_removes_owned_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    real_link = os.link

    def link_then_interrupt(source: Path, destination: Path) -> None:
        real_link(source, destination)
        raise KeyboardInterrupt

    monkeypatch.setattr("finresearch.ingestion.os.link", link_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        ingest_yfinance_daily_prices(
            tmp_path,
            "aapl",
            "AAPL",
            date(2026, 1, 2),
            date(2026, 1, 3),
            provider=FakePriceProvider(),
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    assert list((case_dir / "data/raw").rglob("*.tmp")) == []
    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_sec_ingestions_write_separate_raw_contracts(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    retrieved_at = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)
    provider = FakeSECProvider()

    submissions = ingest_sec_submissions(
        tmp_path,
        "aapl",
        "320193",
        "Finresearch user@example.com",
        provider=provider,
        retrieved_at=retrieved_at,
    )
    companyfacts = ingest_sec_companyfacts(
        tmp_path,
        "aapl",
        "0000320193",
        "Finresearch user@example.com",
        provider=provider,
        retrieved_at=retrieved_at,
    )

    assert submissions.path == (
        case_dir / "data/raw/sec/submissions/0000320193/20260811T040500000000Z.parquet"
    )
    assert companyfacts.path == (
        case_dir / "data/raw/sec/companyfacts/0000320193/20260811T040500000000Z.parquet"
    )
    assert submissions.row_count == 2
    assert companyfacts.row_count == 2
    manifest = read_manifest(case_dir)
    assert [artifact.kind for artifact in manifest.artifacts] == [
        "raw.sec.submissions",
        "raw.sec.companyfacts",
    ]
    assert all(artifact.source == "sec" for artifact in manifest.artifacts)


def test_sec_ingestions_validate_user_agent_before_provider_override(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    provider = UnexpectedSECProvider()

    with pytest.raises(SECProviderError, match="contact email"):
        ingest_sec_submissions(
            tmp_path,
            "aapl",
            "320193",
            "invalid",
            provider=provider,
        )
    with pytest.raises(SECProviderError, match="contact email"):
        ingest_sec_companyfacts(
            tmp_path,
            "aapl",
            "320193",
            "invalid",
            provider=provider,
        )


def test_sec_ingestions_reject_forged_source_urls(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    provider = WrongSourceURLSECProvider()

    with pytest.raises(IngestionError, match="source_url"):
        ingest_sec_submissions(
            tmp_path,
            "aapl",
            "320193",
            "Finresearch user@example.com",
            provider=provider,
        )
    with pytest.raises(IngestionError, match="source_url"):
        ingest_sec_companyfacts(
            tmp_path,
            "aapl",
            "320193",
            "Finresearch user@example.com",
            provider=provider,
        )

    assert list((case_dir / "data/raw").rglob("*.parquet")) == []
    assert read_manifest(case_dir).artifacts == ()


def test_concurrent_different_snapshots_preserve_both_manifest_entries(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    provider = BarrierPriceProvider(Barrier(2))
    retrieval_times = [
        datetime(2026, 8, 11, 4, 5, tzinfo=UTC),
        datetime(2026, 8, 11, 4, 6, tzinfo=UTC),
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                ingest_yfinance_daily_prices,
                tmp_path,
                "aapl",
                "AAPL",
                date(2026, 1, 2),
                date(2026, 1, 3),
                provider=provider,
                retrieved_at=retrieved_at,
            )
            for retrieved_at in retrieval_times
        ]
        receipts = [future.result() for future in futures]

    assert len(read_manifest(case_dir).artifacts) == 2
    assert all(receipt.path.is_file() for receipt in receipts)
    assert (case_dir / ".finresearch.lock").is_file()


def test_concurrent_same_snapshot_cannot_delete_successful_output(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    provider = BarrierPriceProvider(Barrier(2))
    retrieved_at = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                ingest_yfinance_daily_prices,
                tmp_path,
                "aapl",
                "AAPL",
                date(2026, 1, 2),
                date(2026, 1, 3),
                provider=provider,
                retrieved_at=retrieved_at,
            )
            for _ in range(2)
        ]
        outcomes: list[object] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except IngestionError as exc:
                outcomes.append(exc)

    successes = [item for item in outcomes if not isinstance(item, Exception)]
    failures = [item for item in outcomes if isinstance(item, IngestionError)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert "already exists" in str(failures[0])
    manifest = read_manifest(case_dir)
    assert len(manifest.artifacts) == 1
    assert (case_dir / manifest.artifacts[0].path).is_file()


@pytest.mark.parametrize(
    ("retrieval_times", "expected_codes", "expected_artifacts"),
    [
        (
            ("2026-08-11T04:05:00+00:00", "2026-08-11T04:06:00+00:00"),
            [0, 0],
            2,
        ),
        (
            ("2026-08-11T04:05:00+00:00", "2026-08-11T04:05:00+00:00"),
            [0, 3],
            1,
        ),
    ],
)
def test_cross_process_ingestion_preserves_manifest_consistency(
    tmp_path: Path,
    retrieval_times: tuple[str, str],
    expected_codes: list[int],
    expected_artifacts: int,
) -> None:
    case_dir = initialize_case(tmp_path, "aapl")
    coordination_directory = tmp_path / "coordination"
    coordination_directory.mkdir()
    processes = [
        subprocess.Popen(  # noqa: S603 - fixed local interpreter and test helper.
            [
                sys.executable,
                str(PROCESS_WORKER),
                str(tmp_path),
                retrieved_at,
                str(coordination_directory),
                str(index),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index, retrieved_at in enumerate(retrieval_times)
    ]
    deadline = time.monotonic() + 10
    while len(list(coordination_directory.glob("ready-*"))) < 2:
        if time.monotonic() >= deadline:
            for process in processes:
                process.terminate()
            pytest.fail("process-ingestion workers did not reach the barrier")
        time.sleep(0.01)
    (coordination_directory / "go").touch()

    outputs = [process.communicate(timeout=15) for process in processes]
    codes = sorted(process.returncode for process in processes)

    assert codes == expected_codes, outputs
    manifest = read_manifest(case_dir)
    assert len(manifest.artifacts) == expected_artifacts
    assert all((case_dir / artifact.path).is_file() for artifact in manifest.artifacts)
    assert len(list((case_dir / "data/raw").rglob("*.parquet"))) == expected_artifacts
