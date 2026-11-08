"""Instrument-master v3, class-aware reconciliation, and per-class price age."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from token_support import import_master_v3, import_prices, new_case
from typer.testing import CliRunner, Result

from finresearch.cases import read_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_INSTRUMENT_MASTER_V3,
    DataContractError,
)

runner = CliRunner()


def invoke(workspace: Path, *arguments: str) -> Result:
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


def audit(workspace: Path, as_of: str, *extra: str) -> Result:
    return invoke(
        workspace, "case", "audit", "hype", "--as-of", as_of,
        "--max-price-age-days", "5", *extra,
    )  # fmt: skip


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    new_case(tmp_path)
    return tmp_path


def v3_row(**overrides: object) -> pl.DataFrame:
    from datetime import UTC, datetime

    row: dict[str, object] = {
        name: None for name in NORMALIZED_INSTRUMENT_MASTER_V3.schema
    }
    row.update(
        schema_version=3,
        provider="manual",
        instrument_id="i",
        asset_class="crypto",
        trading_currency="USD",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        source_artifact_id="s",
        asset_class_source="import",
    )
    row.update(overrides)
    return pl.DataFrame([row], schema=NORMALIZED_INSTRUMENT_MASTER_V3.schema)


def test_master_v3_rules() -> None:
    NORMALIZED_INSTRUMENT_MASTER_V3.validate(
        v3_row(
            chain="hyperliquid-l1",
            contract_address="0xabc",
            token_standard="hip1",
            provider_ids="coingecko:hyperliquid;yfinance:HYPE-USD",
        )
    )
    for bad in (
        {"asset_class": "equity", "chain": "ethereum"},
        {"contract_address": "0xabc"},
        {"chain": "Ethereum"},
        {"token_standard": "bogus", "chain": "x"},
        {"provider_ids": "yfinance:A;coingecko:B"},
        {"provider_ids": "coingecko:A;coingecko:B"},
        {"provider_ids": "coingecko:"},
        {"asset_class_source": "guess"},
    ):
        with pytest.raises(DataContractError):
            NORMALIZED_INSTRUMENT_MASTER_V3.validate(v3_row(**bad))


def test_reconcile_writes_v2_only_when_v3_input_is_present(
    workspace: Path, tmp_path: Path
) -> None:
    import_master_v3(workspace, tmp_path)
    result = invoke(
        workspace,
        "data",
        "reconcile-instrument-master",
        "hype",
        "--as-of",
        "2026-11-07",
    )
    assert result.exit_code == 0, result.output
    manifest = read_manifest(workspace / "cases" / "hype")
    derived = next(a for a in manifest.artifacts if a.kind.startswith("derived."))
    assert (derived.kind, derived.schema_version) == (
        "derived.instrument-master-current",
        2,
    )
    frame = pl.read_parquet(workspace / "cases" / "hype" / derived.path)
    assert frame["provider_ids"].to_list() == [
        "coingecko:hyperliquid;yfinance:HYPE32196-USD"
    ]
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0


def test_crypto_ages_in_complete_utc_bars(workspace: Path, tmp_path: Path) -> None:
    import_master_v3(workspace, tmp_path)
    import_prices(workspace, tmp_path, [("2026-11-05", 70.0), ("2026-11-06", 78.0)])

    assert audit(workspace, "2026-11-07", "--max-price-age", "crypto=0").exit_code == 0
    stale = audit(workspace, "2026-11-08", "--max-price-age", "crypto=0")
    assert stale.exit_code == 1 and "[price_stale]" in stale.output
    assert "(maximum 0)" in stale.output
    # Without the class limit the global limit applies to the complete-bar age.
    assert audit(workspace, "2026-11-08").exit_code == 0
    unknown = audit(workspace, "2026-11-08", "--max-price-age", "bogus=1")
    assert unknown.exit_code == 1 and "unknown asset class" in unknown.output
    bad = audit(workspace, "2026-11-08", "--max-price-age", "crypto")
    assert bad.exit_code == 1 and "CLASS=N" in bad.output


def test_crypto_bar_dated_on_the_cutoff_is_incomplete(
    workspace: Path, tmp_path: Path
) -> None:
    import_master_v3(workspace, tmp_path)
    import_prices(workspace, tmp_path, [("2026-11-06", 78.0), ("2026-11-07", 79.0)])
    result = audit(workspace, "2026-11-07", "--max-price-age", "crypto=0")
    assert result.exit_code == 1
    assert "[price_bar_incomplete]" in result.output
    assert "[price_stale]" not in result.output


def test_non_crypto_and_unclassified_prices_keep_calendar_age(
    workspace: Path, tmp_path: Path
) -> None:
    import_prices(workspace, tmp_path, [("2026-11-06", 78.0)])
    # No master: the session on the cutoff is fresh and age is calendar days.
    assert audit(workspace, "2026-11-06").exit_code == 0
    assert audit(workspace, "2026-11-11").exit_code == 0
    assert audit(workspace, "2026-11-12").exit_code == 1


def test_equity_class_uses_calendar_days_and_conflicts_are_reported(
    workspace: Path, tmp_path: Path
) -> None:
    import_master_v3(
        workspace, tmp_path, asset_class="equity", provider_ids="yfinance:HYPE32196-USD"
    )
    import_prices(workspace, tmp_path, [("2026-11-06", 78.0)])
    assert audit(workspace, "2026-11-06", "--max-price-age", "equity=0").exit_code == 0
    assert audit(workspace, "2026-11-07", "--max-price-age", "equity=0").exit_code == 1

    import_master_v3(
        workspace,
        tmp_path,
        instrument_id="other.crypto",
        provider_ids="yfinance:HYPE32196-USD",
        name="master2.csv",
    )
    conflict = audit(workspace, "2026-11-07")
    assert conflict.exit_code == 1 and "[asset_class_conflict]" in conflict.output


def test_master_observed_after_cutoff_is_not_an_issue_and_not_resolved(
    workspace: Path, tmp_path: Path
) -> None:
    import_master_v3(workspace, tmp_path)
    import_prices(workspace, tmp_path, [("2026-11-05", 70.0), ("2026-11-06", 78.0)])
    import_master_v3(
        workspace,
        tmp_path,
        instrument_id="late.equity",
        asset_class="equity",
        name="late.csv",
        observed_at="2026-11-09T08:00:00Z",
    )

    before = audit(workspace, "2026-11-07")
    assert before.exit_code == 0, before.output
    assert "master_observed_after_as_of" not in before.output
    # The later equity observation is ignored, so the class is not in conflict.
    assert "asset_class_conflict" not in before.output
    after = audit(workspace, "2026-11-10")
    assert after.exit_code == 1 and "[asset_class_conflict]" in after.output
