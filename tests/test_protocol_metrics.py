"""Protocol metric ingest, normalization, definitions, and audit coverage."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from token_support import RETRIEVED_AT, FakeFetcher, fixture_bytes, new_case, ok
from typer.testing import CliRunner, Result

from finresearch.capture_ingestion import ingest_defillama, ingest_hyperliquid_info
from finresearch.cases import CaseManifest, read_manifest, write_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_PROTOCOL_METRICS_V1,
    DataContractError,
)
from finresearch.ingestion import IngestionError
from finresearch.protocol_normalization import normalize_protocol_metrics
from finresearch.providers import ProviderError, defillama
from finresearch.providers.http import FetchedResponse

runner = CliRunner()
FEES_URL = "https://api.llama.fi/summary/fees/hyperliquid?dataType=dailyFees"
HOLDERS_URL = (
    "https://api.llama.fi/summary/fees/hyperliquid?dataType=dailyHoldersRevenue"
)
OI_URL = "https://api.llama.fi/summary/open-interest/hyperliquid"


def invoke(workspace: Path, *arguments: str) -> Result:
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    new_case(tmp_path)
    return tmp_path


def read_artifact(workspace: Path, artifact_id: str) -> pl.DataFrame:
    manifest = read_manifest(workspace / "cases" / "hype")
    artifact = next(a for a in manifest.artifacts if a.artifact_id == artifact_id)
    return pl.read_parquet(workspace / "cases" / "hype" / artifact.path)


def capture_defillama(workspace: Path, datasets: tuple[str, ...]) -> list[str]:
    names = {
        "fees": "defillama_fees_hyperliquid.json",
        "holders-revenue": "defillama_holders_revenue_hyperliquid.json",
        "open-interest": "defillama_open_interest_hyperliquid.json",
    }
    fetcher = FakeFetcher([ok(fixture_bytes(names[item])) for item in datasets])
    receipts = ingest_defillama(
        workspace,
        "hype",
        "hyperliquid",
        datasets,
        fetcher=fetcher,
        retrieved_at=RETRIEVED_AT,
    )
    return [receipt.capture_id for receipt in receipts]


def test_defillama_ingest_urls_order_and_partial_failure(workspace: Path) -> None:
    fetcher = FakeFetcher(
        [
            ok(fixture_bytes("defillama_fees_hyperliquid.json")),
            FetchedResponse(400, b"Fees for aster not found", "text/plain"),
        ]
    )
    seen: list[str] = []
    with pytest.raises(ProviderError, match="HTTP 400: Fees for aster not found"):
        ingest_defillama(
            workspace,
            "hype",
            "hyperliquid",
            ("fees", "revenue", "open-interest"),
            fetcher=fetcher,
            retrieved_at=RETRIEVED_AT,
            report=lambda receipt: seen.append(receipt.capture_id),
        )
    assert [request[1] for request in fetcher.requests] == [
        FEES_URL,
        "https://api.llama.fi/summary/fees/hyperliquid?dataType=dailyRevenue",
    ]
    assert len(seen) == 1
    assert len(read_manifest(workspace / "cases" / "hype").artifacts) == 2
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    assert defillama.summary_url("hyperliquid", "dex-volume").endswith(
        "/summary/dexs/hyperliquid?dataType=dailyVolume"
    )
    for bad in (("bogus",), ("fees", "fees"), ()):
        with pytest.raises((ProviderError, IngestionError)):
            ingest_defillama(workspace, "hype", "hyperliquid", bad)
    with pytest.raises(ProviderError, match="slug"):
        defillama.summary_url("Bad Slug", "fees")


def test_normalize_fees_and_holders_revenue_with_definitions(workspace: Path) -> None:
    captures = capture_defillama(workspace, ("fees", "holders-revenue"))
    receipt = normalize_protocol_metrics(
        workspace,
        "hype",
        capture_artifact_ids=tuple(captures),
        entity_id="hyperliquid",
        instrument_id="hype.hyperliquid.l1",
    )

    metrics = read_artifact(workspace, receipt.metrics.artifact_id)
    NORMALIZED_PROTOCOL_METRICS_V1.validate(metrics)
    assert set(metrics["metric"]) == {"fees", "holders_revenue"}
    aggregate = metrics.filter(
        (pl.col("metric") == "holders_revenue") & pl.col("sub_protocol").is_null()
    ).sort("period_end")
    assert aggregate["value"].to_list() == [2200000.0, 2100000.0, 2300000.0, 1100000.0]
    assert aggregate["period_start"].to_list() == aggregate["period_end"].to_list()
    assert aggregate["is_complete"].to_list() == [True, True, True, False]
    assert aggregate["unit"].unique().to_list() == ["USD"]
    assert aggregate["instrument_id"].unique().to_list() == ["hype.hyperliquid.l1"]
    children = metrics.filter(pl.col("sub_protocol").is_not_null())
    assert set(children["sub_protocol"]) == {
        "Hyperliquid Perps",
        "Hyperliquid Spot Orderbook",
    }
    assert aggregate["observed_at"][0] == datetime(2026, 11, 4, tzinfo=UTC)

    definitions = read_artifact(workspace, receipt.definitions.artifact_id)
    assert set(definitions["metric"]) == {"fees", "holders_revenue"}
    holders = definitions.filter(pl.col("metric") == "holders_revenue").row(
        0, named=True
    )
    assert holders["provider_definition_id"].startswith(
        "defillama.hyperliquid.dailyHoldersRevenue."
    )
    assert holders["methodology_text"].startswith("Share of fees")
    assert holders["methodology_url"].endswith("fees/hyperliquid")
    assert set(metrics["provider_definition_id"]) == set(
        definitions["provider_definition_id"]
    )
    manifest = read_manifest(workspace / "cases" / "hype")
    declared = next(
        a for a in manifest.artifacts if a.artifact_id == receipt.metrics.artifact_id
    )
    assert declared.input_artifact_ids == tuple(captures)
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    again = normalize_protocol_metrics(
        workspace,
        "hype",
        capture_artifact_ids=tuple(captures),
        entity_id="hyperliquid",
        instrument_id="hype.hyperliquid.l1",
    )
    assert again.metrics.artifact_id == receipt.metrics.artifact_id
    assert len(read_manifest(workspace / "cases" / "hype").artifacts) == len(
        manifest.artifacts
    )


def test_changed_methodology_changes_the_definition_id(
    workspace: Path, tmp_path: Path
) -> None:
    first = capture_defillama(workspace, ("fees",))
    changed = json.loads(fixture_bytes("defillama_fees_hyperliquid.json"))
    changed["methodology"]["Fees"] = "Redefined fees."
    (receipt,) = ingest_defillama(
        workspace,
        "hype",
        "hyperliquid",
        ("fees",),
        fetcher=FakeFetcher([ok(json.dumps(changed).encode())]),
        retrieved_at=datetime(2026, 11, 8, 1, tzinfo=UTC),
    )
    ids = []
    for capture in (first[0], receipt.capture_id):
        result = normalize_protocol_metrics(
            workspace, "hype", capture_artifact_ids=(capture,), entity_id="hyperliquid"
        )
        ids.append(
            read_artifact(workspace, result.definitions.artifact_id)[
                "provider_definition_id"
            ].to_list()
        )
    assert ids[0] != ids[1]


def test_open_interest_rows_are_instant(workspace: Path) -> None:
    (capture,) = capture_defillama(workspace, ("open-interest",))
    receipt = normalize_protocol_metrics(
        workspace, "hype", capture_artifact_ids=(capture,), entity_id="hyperliquid"
    )
    metrics = read_artifact(workspace, receipt.metrics.artifact_id).sort("period_end")
    assert set(metrics["period_type"]) == {"instant"}
    assert metrics["period_start"].null_count() == metrics.height
    assert metrics["value"].to_list() == [1.5e9, 1.6e9, 1.55e9, 1.7e9]
    assert metrics["is_complete"].to_list() == [True, True, True, False]


def test_hyperliquid_asset_contexts(workspace: Path) -> None:
    (capture,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "meta-and-asset-ctxs",
        fetcher=FakeFetcher(
            [ok(fixture_bytes("hyperliquid_meta_and_asset_ctxs.json"))]
        ),
        retrieved_at=RETRIEVED_AT,
    )
    receipt = normalize_protocol_metrics(
        workspace,
        "hype",
        capture_artifact_ids=(capture.capture_id,),
        entity_id="hyperliquid",
    )
    metrics = read_artifact(workspace, receipt.metrics.artifact_id)
    assert set(metrics["sub_protocol"]) == {"BTC", "HYPE"}
    hype = {
        row["metric"]: row
        for row in metrics.filter(pl.col("sub_protocol") == "HYPE").iter_rows(
            named=True
        )
    }
    assert hype["open_interest"]["value"] == pytest.approx(12000000.0 * 78.44)
    assert hype["open_interest"]["period_type"] == "instant"
    assert hype["open_interest"]["unit"] == "USD"
    assert hype["funding_rate_1h"]["value"] == pytest.approx(-0.2)
    assert hype["funding_rate_1h"]["unit"] == "bps"
    assert hype["funding_rate_1h"]["currency"] is None
    assert hype["volume_perp"]["value"] == 350000000.0
    assert hype["volume_perp"]["period_start"] == date(2026, 11, 6)
    assert hype["volume_perp"]["period_end"] == date(2026, 11, 7)
    assert metrics["is_complete"].to_list() == [False] * metrics.height
    assert set(metrics["observed_at"]) == {RETRIEVED_AT}
    definitions = read_artifact(workspace, receipt.definitions.artifact_id)
    assert definitions["metric"].sort().to_list() == [
        "funding_rate_1h",
        "open_interest",
        "volume_perp",
    ]
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0


def test_unsupported_capture_and_usage_errors(workspace: Path) -> None:
    (capture,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "validator-summaries",
        fetcher=FakeFetcher(
            [ok(fixture_bytes("hyperliquid_validator_summaries.json"))]
        ),
        retrieved_at=RETRIEVED_AT,
    )
    with pytest.raises(IngestionError, match="supported endpoints: defillama"):
        normalize_protocol_metrics(
            workspace,
            "hype",
            capture_artifact_ids=(capture.capture_id,),
            entity_id="hyperliquid",
        )
    with pytest.raises(IngestionError, match="at least once"):
        normalize_protocol_metrics(
            workspace, "hype", capture_artifact_ids=(), entity_id="hyperliquid"
        )


def test_missing_definitions_are_reported_by_validate(workspace: Path) -> None:
    captures = capture_defillama(workspace, ("fees",))
    normalize_protocol_metrics(
        workspace, "hype", capture_artifact_ids=tuple(captures), entity_id="hyperliquid"
    )
    case_dir = workspace / "cases" / "hype"
    manifest = read_manifest(case_dir)
    write_manifest(
        case_dir,
        CaseManifest(
            manifest.manifest_version,
            manifest.case_id,
            manifest.title,
            manifest.status,
            manifest.paths,
            tuple(
                a
                for a in manifest.artifacts
                if a.kind != "normalized.provider-metric-definitions"
            ),
        ),
    )
    result = invoke(workspace, "data", "validate", "hype")
    assert result.exit_code == 1
    assert "[protocol_metric_definition_missing]" in result.output


def test_protocol_metrics_contract_rules() -> None:
    row = {
        "schema_version": 1,
        "provider": "defillama",
        "entity_id": "e",
        "instrument_id": None,
        "provider_protocol_id": "p",
        "sub_protocol": None,
        "metric": "fees",
        "period_type": "duration",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 1),
        "value": 1.0,
        "unit": "USD",
        "currency": "USD",
        "is_complete": True,
        "provider_definition_id": "d",
        "observed_at": RETRIEVED_AT,
        "capture_artifact_id": None,
        "source_artifact_id": "s",
        "normalized_at": RETRIEVED_AT,
    }

    def check(**overrides: object) -> None:
        NORMALIZED_PROTOCOL_METRICS_V1.validate(
            pl.DataFrame(
                [{**row, **overrides}], schema=NORMALIZED_PROTOCOL_METRICS_V1.schema
            )
        )

    check()
    check(metric="revenue", value=-5.0)
    check(metric="burns", unit="tokens", currency=None)
    check(
        metric="funding_rate_1h",
        unit="bps",
        currency=None,
        period_type="instant",
        period_start=None,
        value=-3.0,
    )
    for bad in (
        {"value": -1.0},
        {"metric": "bogus"},
        {"unit": "tokens", "currency": None},
        {"metric": "open_interest", "unit": "bps", "currency": None},
        {"currency": None},
        {"period_start": date(2026, 1, 2)},
        {"period_type": "instant"},
        {"is_complete": False},
        {"period_end": date(2026, 11, 7)},
    ):
        with pytest.raises(DataContractError):
            check(**bad)


def test_audit_reports_protocol_metric_after_cutoff(workspace: Path) -> None:
    captures = capture_defillama(workspace, ("fees",))
    normalize_protocol_metrics(
        workspace, "hype", capture_artifact_ids=tuple(captures), entity_id="hyperliquid"
    )
    early = invoke(
        workspace,
        "case",
        "audit",
        "hype",
        "--as-of",
        "2026-11-05",
        "--max-price-age-days",
        "1",
    )
    assert early.exit_code == 1
    assert "[protocol_metric_after_as_of]" in early.output
    late = invoke(
        workspace,
        "case",
        "audit",
        "hype",
        "--as-of",
        "2026-11-08",
        "--max-price-age-days",
        "1",
    )
    assert late.exit_code == 0, late.output


def test_cli_ingest_and_normalize(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetcher = FakeFetcher([ok(fixture_bytes("defillama_fees_hyperliquid.json"))])
    monkeypatch.setattr(defillama, "default_fetcher", lambda: fetcher)
    ingested = invoke(
        workspace,
        "data",
        "ingest-defillama",
        "hype",
        "hyperliquid",
        "--dataset",
        "fees",
    )
    assert ingested.exit_code == 0, ingested.output
    capture_id = next(
        line.split(": ", 1)[1]
        for line in ingested.output.splitlines()
        if line.startswith("capture: ")
    )
    normalized = invoke(
        workspace,
        "data",
        "normalize-protocol-metrics",
        "hype",
        "--capture-artifact-id",
        capture_id,
        "--entity-id",
        "hyperliquid",
    )
    assert normalized.exit_code == 0, normalized.output
    assert "normalized protocol-metrics:" in normalized.output
    assert "normalized provider-metric-definitions:" in normalized.output
