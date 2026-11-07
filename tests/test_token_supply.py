"""Token supply ingest, normalization, import, reconciliation, and audit."""

from __future__ import annotations

import csv
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from token_support import RETRIEVED_AT, FakeFetcher, fixture_bytes, new_case, ok
from typer.testing import CliRunner, Result

from finresearch.auditing import _capture_issues
from finresearch.capture_ingestion import (
    ingest_coingecko_markets,
    ingest_coingecko_token,
    ingest_hyperliquid_info,
)
from finresearch.captures import register_capture
from finresearch.cases import Artifact, read_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_TOKEN_MARKET_V1,
    NORMALIZED_TOKEN_SUPPLY_V1,
    DataContractError,
)
from finresearch.ingestion import IngestionError
from finresearch.providers import ProviderError, coingecko, hyperliquid
from finresearch.providers.http import FetchedResponse, HostRequestRateLimiter
from finresearch.token_normalization import normalize_token_supply
from finresearch.token_reconciliation import parse_supply_policy

runner = CliRunner()
TOKEN_ID = "0x0d01dc56dcaaca66ad901c959b4011ec"


def invoke(workspace: Path, *arguments: str) -> Result:
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    new_case(tmp_path)
    return tmp_path


def capture_coin(workspace: Path) -> str:
    receipt = ingest_coingecko_token(
        workspace,
        "hype",
        "hyperliquid",
        fetcher=FakeFetcher([ok(fixture_bytes("coingecko_coin_hyperliquid.json"))]),
        retrieved_at=RETRIEVED_AT,
    )
    return receipt.capture_id


def capture_token_details(workspace: Path) -> str:
    (receipt,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "token-details",
        token_id=TOKEN_ID,
        fetcher=FakeFetcher([ok(fixture_bytes("hyperliquid_token_details.json"))]),
        retrieved_at=RETRIEVED_AT,
    )
    return receipt.capture_id


def read_artifact(workspace: Path, artifact_id: str) -> pl.DataFrame:
    manifest = read_manifest(workspace / "cases" / "hype")
    artifact = next(a for a in manifest.artifacts if a.artifact_id == artifact_id)
    return pl.read_parquet(workspace / "cases" / "hype" / artifact.path)


def test_coingecko_ingest_records_a_cli_capture(workspace: Path) -> None:
    fetcher = FakeFetcher([ok(fixture_bytes("coingecko_coin_hyperliquid.json"))])
    receipt = ingest_coingecko_token(
        workspace, "hype", "hyperliquid", fetcher=fetcher, retrieved_at=RETRIEVED_AT
    )

    [(method, url, body)] = fetcher.requests
    assert (method, body) == ("GET", None)
    assert url.startswith("https://api.coingecko.com/api/v3/coins/hyperliquid?")
    assert "localization=false" in url and "tickers=false" in url
    frame = read_artifact(workspace, receipt.capture_id)
    assert frame["recorded_by"].to_list() == ["cli"]
    assert frame["capture_kind"].to_list() == ["api-response"]
    assert receipt.content_sha256 == _sha(
        fixture_bytes("coingecko_coin_hyperliquid.json")
    )
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0


def test_http_errors_surface_verbatim_and_write_nothing(workspace: Path) -> None:
    fetcher = FakeFetcher(
        [FetchedResponse(429, b'{"status":"rate limited"}', "application/json")]
    )
    with pytest.raises(ProviderError, match=r"HTTP 429: .*rate limited"):
        ingest_coingecko_token(
            workspace, "hype", "hyperliquid", fetcher=fetcher, retrieved_at=RETRIEVED_AT
        )
    assert read_manifest(workspace / "cases" / "hype").artifacts == ()
    with pytest.raises(ProviderError, match="non-JSON"):
        ingest_coingecko_token(
            workspace,
            "hype",
            "hyperliquid",
            fetcher=FakeFetcher([FetchedResponse(200, b"<html>", "text/html")]),
        )


def test_coingecko_markets_ids_are_sorted_and_validated(workspace: Path) -> None:
    fetcher = FakeFetcher([ok(fixture_bytes("coingecko_markets.json"))])
    ingest_coingecko_markets(
        workspace,
        "hype",
        ("hyperliquid", "dydx-chain"),
        vs_currency="USD",
        fetcher=fetcher,
        retrieved_at=RETRIEVED_AT,
    )
    assert "ids=dydx-chain,hyperliquid" in fetcher.requests[0][1]
    assert "vs_currency=usd" in fetcher.requests[0][1]
    with pytest.raises(ProviderError, match="invalid CoinGecko coin id"):
        coingecko.markets_url(("Bad Id",), "usd")
    with pytest.raises(IngestionError, match="vs-currency"):
        ingest_coingecko_token(workspace, "hype", "hyperliquid", vs_currency="btc")


def test_hyperliquid_requests_and_usage_errors(workspace: Path) -> None:
    fetcher = FakeFetcher([ok(fixture_bytes("hyperliquid_token_details.json"))])
    (receipt,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "token-details",
        token_id=TOKEN_ID.upper().replace("0X", "0x"),
        fetcher=fetcher,
        retrieved_at=RETRIEVED_AT,
    )
    assert fetcher.requests == [
        (
            "POST",
            "https://api.hyperliquid.xyz/info",
            f'{{"type":"tokenDetails","tokenId":"{TOKEN_ID}"}}',
        )
    ]
    assert receipt.capture_id.startswith("raw.capture.api-hyperliquid-xyz-")
    for request, options in (
        ("token-details", {}),
        ("token-details", {"token_id": TOKEN_ID, "coin": "HYPE"}),
        ("vault-details", {}),
        ("funding-history", {"coin": "HYPE"}),
        ("validator-summaries", {"address": "0x" + "1" * 40}),
        ("nonsense", {}),
    ):
        with pytest.raises(ProviderError):
            ingest_hyperliquid_info(workspace, "hype", request, **options)  # type: ignore[arg-type]


def test_funding_history_pages_until_retrieval_time(workspace: Path) -> None:
    start_ms = hyperliquid.utc_midnight_ms(date(2026, 11, 7))
    full = [{"coin": "HYPE", "time": start_ms + index * 1000} for index in range(500)]
    tail = [{"coin": "HYPE", "time": start_ms + 500_000 + index} for index in range(3)]
    fetcher = FakeFetcher(
        [ok(json.dumps(full).encode()), ok(json.dumps(tail).encode())]
    )
    receipts = ingest_hyperliquid_info(
        workspace,
        "hype",
        "funding-history",
        coin="HYPE",
        start=date(2026, 11, 7),
        fetcher=fetcher,
        retrieved_at=RETRIEVED_AT,
    )
    assert len(receipts) == 2
    first_body = json.loads(fetcher.requests[0][2] or "")
    second_body = json.loads(fetcher.requests[1][2] or "")
    assert first_body["startTime"] == start_ms
    assert second_body["startTime"] == start_ms + 499_000 + 1
    assert first_body["type"] == "fundingHistory"


def test_host_rate_limiter_spaces_requests(tmp_path: Path) -> None:
    limiter = HostRequestRateLimiter("test", 0.2, lock_path=tmp_path / "t.lock")
    limiter.wait()
    started = time.monotonic()
    limiter.wait()
    assert time.monotonic() - started >= 0.15
    assert coingecko.COINGECKO_MINIMUM_REQUEST_INTERVAL_SECONDS >= 6.0


def test_normalize_coingecko_coin(workspace: Path) -> None:
    capture_id = capture_coin(workspace)
    receipt = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=capture_id,
        instrument_id="hype.hyperliquid.l1",
    )

    supply = read_artifact(workspace, receipt.supply.artifact_id)
    NORMALIZED_TOKEN_SUPPLY_V1.validate(supply)
    rows = {row["supply_metric"]: row for row in supply.iter_rows(named=True)}
    assert rows["circulating"]["amount"] == 239406814.37
    assert rows["total"]["amount"] == 951538272.91
    assert rows["max"]["amount"] == 1e9
    assert rows["circulating"]["definition_id"] == (
        "coingecko.market_data.circulating_supply"
    )
    assert rows["total"]["observed_at"] == datetime(
        2026, 11, 7, 20, 28, 57, 123000, tzinfo=UTC
    )
    assert rows["total"]["observed_at_basis"] == "provider"
    assert rows["total"]["capture_artifact_id"] == capture_id
    assert rows["total"]["source_artifact_id"] == capture_id
    assert rows["total"]["normalized_at"] == RETRIEVED_AT

    assert receipt.market is not None
    market = read_artifact(workspace, receipt.market.artifact_id)
    NORMALIZED_TOKEN_MARKET_V1.validate(market)
    by_metric = {row["metric"]: row for row in market.iter_rows(named=True)}
    assert by_metric["price"]["value"] == 78.4549
    assert by_metric["fdv"]["fdv_supply_basis"] == "total"
    assert by_metric["price"]["currency"] == "USD"
    assert by_metric["volume_24h"]["quote_asset"] == "USD"
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    again = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=capture_id,
        instrument_id="hype.hyperliquid.l1",
    )
    assert again.supply.artifact_id == receipt.supply.artifact_id
    assert len(read_manifest(workspace / "cases" / "hype").artifacts) == 4


def test_normalize_markets_requires_maps_and_lists_skipped(workspace: Path) -> None:
    receipt = ingest_coingecko_markets(
        workspace,
        "hype",
        ("hyperliquid", "dydx-chain"),
        fetcher=FakeFetcher([ok(fixture_bytes("coingecko_markets.json"))]),
        retrieved_at=RETRIEVED_AT,
    )
    with pytest.raises(IngestionError, match="--instrument-map"):
        normalize_token_supply(
            workspace, "hype", capture_artifact_id=receipt.capture_id, instrument_id="x"
        )
    with pytest.raises(IngestionError, match="not in the capture"):
        normalize_token_supply(
            workspace,
            "hype",
            capture_artifact_id=receipt.capture_id,
            instrument_map={"absent": "x"},
        )
    with pytest.raises(IngestionError, match="exactly one"):
        normalize_token_supply(
            workspace, "hype", capture_artifact_id=receipt.capture_id
        )

    result = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=receipt.capture_id,
        instrument_map={"hyperliquid": "hype.hyperliquid.l1"},
    )
    assert result.skipped_assets == ("dydx-chain",)
    supply = read_artifact(workspace, result.supply.artifact_id)
    assert set(supply["instrument_id"]) == {"hype.hyperliquid.l1"}
    assert set(supply["provider_asset_id"]) == {"hyperliquid"}

    both = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=receipt.capture_id,
        instrument_map={
            "hyperliquid": "hype.hyperliquid.l1",
            "dydx-chain": "dydx.chain",
        },
    )
    market = read_artifact(workspace, both.market.artifact_id if both.market else "")
    dydx = market.filter(pl.col("instrument_id") == "dydx.chain")
    assert set(dydx["metric"]) == {"price", "market_cap", "volume_24h"}


def test_normalize_hyperliquid_token_details_and_validators(workspace: Path) -> None:
    details_id = capture_token_details(workspace)
    receipt = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=details_id,
        instrument_id="hype.hyperliquid.l1",
    )
    assert receipt.market is None
    supply = read_artifact(workspace, receipt.supply.artifact_id)
    assert set(supply["observed_at_basis"]) == {"retrieval"}
    assert set(supply["provider_asset_id"]) == {TOKEN_ID}
    summary = {
        row["supply_metric"]: row["amount"]
        for row in supply.filter(pl.col("holder_address").is_null()).iter_rows(
            named=True
        )
    }
    assert summary == {
        "circulating": 301819911.0,
        "total": 998778474.0,
        "max": 1e9,
        "future_emissions": 0.0,
    }
    holders = supply.filter(pl.col("holder_address").is_not_null())
    assert sorted(holders["amount"].to_list()) == [48742089.0, 238104720.0]
    assert set(holders["supply_metric"]) == {"non_circulating"}

    (validators,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "validator-summaries",
        fetcher=FakeFetcher(
            [ok(fixture_bytes("hyperliquid_validator_summaries.json"))]
        ),
        retrieved_at=RETRIEVED_AT,
    )
    staked = normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=validators.capture_id,
        instrument_id="hype.hyperliquid.l1",
    )
    frame = read_artifact(workspace, staked.supply.artifact_id)
    assert frame["supply_metric"].to_list() == ["staked"]
    assert frame["amount"].to_list() == [439122361.0]


def test_normalize_rejects_unsupported_endpoint_and_bad_ordering(
    workspace: Path, tmp_path: Path
) -> None:
    page = tmp_path / "page.html"
    page.write_text("<html>supply</html>")
    capture = register_capture(
        workspace,
        "hype",
        page,
        provider="web",
        kind="web-page",
        url="https://example.com/hype",
        retrieved_at=RETRIEVED_AT,
    )
    with pytest.raises(IngestionError, match="supported endpoints: coingecko"):
        normalize_token_supply(
            workspace, "hype", capture_artifact_id=capture.capture_id, instrument_id="x"
        )

    broken = json.loads(fixture_bytes("hyperliquid_token_details.json"))
    broken["circulatingSupply"] = "2000000000.0"
    (receipt,) = ingest_hyperliquid_info(
        workspace,
        "hype",
        "token-details",
        token_id="0xabc",
        fetcher=FakeFetcher([ok(json.dumps(broken).encode())]),
        retrieved_at=RETRIEVED_AT,
    )
    with pytest.raises(IngestionError, match="circulating <= total <= max"):
        normalize_token_supply(
            workspace, "hype", capture_artifact_id=receipt.capture_id, instrument_id="x"
        )


def test_token_supply_contract_rules() -> None:
    row = {
        "schema_version": 1,
        "provider": "p",
        "instrument_id": "i",
        "provider_asset_id": "a",
        "supply_metric": "circulating",
        "amount": 1.0,
        "unit": "tokens",
        "definition_id": "d",
        "holder_address": None,
        "holder_label": None,
        "observed_at": RETRIEVED_AT,
        "observed_at_basis": "provider",
        "retrieved_at": RETRIEVED_AT,
        "capture_artifact_id": None,
        "source_artifact_id": "s",
        "normalized_at": RETRIEVED_AT,
    }

    def check(**overrides: object) -> None:
        NORMALIZED_TOKEN_SUPPLY_V1.validate(
            pl.DataFrame(
                [{**row, **overrides}], schema=NORMALIZED_TOKEN_SUPPLY_V1.schema
            )
        )

    check()
    check(supply_metric="staked", holder_address="0x1")
    for bad in (
        {"holder_address": "0x1"},
        {"amount": -1.0},
        {"unit": "shares"},
        {"supply_metric": "bogus"},
        {"observed_at": datetime(2026, 11, 8, tzinfo=UTC)},
        {
            "observed_at_basis": "retrieval",
            "observed_at": datetime(2026, 11, 7, tzinfo=UTC),
        },
    ):
        with pytest.raises(DataContractError):
            check(**bad)


SUPPLY_CSV_HEADER = [
    "instrument_id",
    "provider_asset_id",
    "supply_metric",
    "amount",
    "unit",
    "definition_id",
    "holder_address",
    "holder_label",
    "observed_at",
    "observed_at_basis",
    "capture_artifact_id",
]


def write_cmc_csv(path: Path, capture_id: str) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(SUPPLY_CSV_HEADER)
        for metric, amount in (
            ("circulating", "253077565"),
            ("total", "950036532"),
            ("max", "950036532"),
        ):
            writer.writerow(
                [
                    "hype.hyperliquid.l1",
                    "hyperliquid",
                    metric,
                    amount,
                    "tokens",
                    f"coinmarketcap.page.{metric}",
                    "",
                    "",
                    "2026-11-07T20:30:00Z",
                    "retrieval",
                    capture_id,
                ]
            )


def import_cmc(workspace: Path, tmp_path: Path) -> str:
    page = tmp_path / "cmc.md"
    page.write_text("CoinMarketCap HYPE: circulating 253,077,565")
    capture = register_capture(
        workspace,
        "hype",
        page,
        provider="web",
        kind="tool-summary",
        url="https://coinmarketcap.com/currencies/hyperliquid/",
        retrieved_at=datetime(2026, 11, 7, 20, 30, tzinfo=UTC),
    )
    source = tmp_path / "cmc.csv"
    write_cmc_csv(source, capture.capture_id)
    result = invoke(
        workspace,
        "data",
        "import-csv",
        "hype",
        str(source),
        "--schema",
        "token-supply.v1",
        "--provider",
        "coinmarketcap",
        "--retrieved-at",
        "2026-11-07T20:30:00Z",
    )
    assert result.exit_code == 0, result.output
    return next(
        line.split(": ", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("artifact: normalized.token-supply")
    )


def test_import_csv_token_supply_checks_capture_reference(
    workspace: Path, tmp_path: Path
) -> None:
    import_cmc(workspace, tmp_path)
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    source = tmp_path / "bad.csv"
    write_cmc_csv(source, "raw.capture.missing.2026")
    missing = invoke(
        workspace,
        "data",
        "import-csv",
        "hype",
        str(source),
        "--schema",
        "token-supply.v1",
        "--provider",
        "coinmarketcap",
        "--retrieved-at",
        "2026-11-07T20:30:00Z",
    )
    assert missing.exit_code == 1
    assert "raw.capture.missing.2026" in missing.output


POLICY = """\
version = 1
provider_priority = ["hyperliquid", "coingecko", "coinmarketcap"]

[tolerance]
circulating = 0.02
total = 0.01
max = 0.0
staked = 0.02

[resolution.circulating]
provider = "coingecko"
definition_id = "coingecko.market_data.circulating_supply"
source_id = "a-circ-definition"

[resolution.total]
provider = "coingecko"
definition_id = "coingecko.market_data.total_supply"
source_id = "a-total-definition"

[resolution.max]
provider = "coingecko"
definition_id = "coingecko.market_data.max_supply"
source_id = "a-max-definition"
"""


def write_registers(workspace: Path, assumption_ids: tuple[str, ...]) -> None:
    registers = workspace / "cases" / "hype" / "registers"
    registers.mkdir(exist_ok=True)
    (registers / "evidence.csv").write_text(
        "id,claim,source_type,source_ref,observed_at,notes\n"
        "e-1,definition,external,provider docs,2026-11-07,\n"
    )
    lines = ["id,parameter,value,unit,rationale,source_evidence,updated_at"]
    lines += [f"{name},p,1,ratio,r,e-1,2026-11-07" for name in assumption_ids]
    (registers / "assumptions.csv").write_text("\n".join(lines) + "\n")


def build_supply(workspace: Path, tmp_path: Path) -> None:
    normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=capture_coin(workspace),
        instrument_id="hype.hyperliquid.l1",
    )
    normalize_token_supply(
        workspace,
        "hype",
        capture_artifact_id=capture_token_details(workspace),
        instrument_id="hype.hyperliquid.l1",
    )
    import_cmc(workspace, tmp_path)
    policy = workspace / "cases" / "hype" / "analysis"
    policy.mkdir(exist_ok=True)
    (policy / "token-supply-policy.toml").write_text(POLICY)


def reconcile(workspace: Path, as_of: str = "2026-11-07") -> Result:
    return invoke(
        workspace,
        "data",
        "reconcile-token-supply",
        "hype",
        "--instrument-id",
        "hype.hyperliquid.l1",
        "--as-of",
        as_of,
        "--policy",
        "analysis/token-supply-policy.toml",
    )


def test_reconcile_statuses_resolution_and_idempotence(
    workspace: Path, tmp_path: Path
) -> None:
    build_supply(workspace, tmp_path)
    write_registers(
        workspace, ("a-circ-definition", "a-total-definition", "a-max-definition")
    )
    result = reconcile(workspace)
    assert result.exit_code == 0, result.output
    artifact_id = next(
        line.split(": ", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("artifact: ")
    )
    frame = read_artifact(workspace, artifact_id)
    selected = {
        row["supply_metric"]: row
        for row in frame.filter(pl.col("selected")).iter_rows(named=True)
    }
    assert {name: row["status"] for name, row in selected.items()} == {
        "circulating": "resolved",
        "total": "resolved",
        "max": "resolved",
        "future_emissions": "single-source",
    }
    assert selected["circulating"]["provider"] == "coingecko"
    assert selected["circulating"]["amount"] == 239406814.37
    assert selected["circulating"]["resolution_source_id"] == "a-circ-definition"
    assert round(selected["circulating"]["spread"], 3) == 0.261
    assert round(selected["total"]["spread"], 3) == 0.051
    assert frame.filter(pl.col("supply_metric") == "circulating").height == 3
    assert "staked" not in frame["supply_metric"].to_list()
    manifest = read_manifest(workspace / "cases" / "hype")
    artifact = next(a for a in manifest.artifacts if a.artifact_id == artifact_id)
    assert any(
        item.name == "token-supply-policy" for item in artifact.input_file_hashes
    )
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    again = reconcile(workspace)
    assert again.output == result.output
    assert len(read_manifest(workspace / "cases" / "hype").artifacts) == len(
        manifest.artifacts
    )


def test_reconcile_agreement_conflict_and_failures(
    workspace: Path, tmp_path: Path
) -> None:
    build_supply(workspace, tmp_path)
    write_registers(workspace, ("a-circ-definition",))
    policy_file = workspace / "cases/hype/analysis/token-supply-policy.toml"

    # Without a resolution a wide spread is recorded as a conflict.
    policy_file.write_text(
        POLICY.split("[resolution.circulating]")[0].replace(
            "max = 0.0\n", "max = 0.5\n"
        )
    )
    result = reconcile(workspace)
    assert result.exit_code == 0, result.output
    artifact_id = next(
        line.split(": ", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("artifact: ")
    )
    frame = read_artifact(workspace, artifact_id)
    assert frame.filter(pl.col("selected")).height == 2  # max and future_emissions
    status = dict(frame.select("supply_metric", "status").unique().iter_rows())
    assert status["circulating"] == "conflict"
    assert status["max"] == "agree"
    max_rows = frame.filter(pl.col("supply_metric") == "max")
    assert max_rows.filter(pl.col("selected"))["provider"].to_list() == ["hyperliquid"]

    # Missing tolerance, absent resolution observation, bad source id.
    policy_file.write_text(POLICY.replace("max = 0.0\n", ""))
    missing = reconcile(workspace)
    assert missing.exit_code == 1 and "no tolerance entry" in missing.output

    policy_file.write_text(
        POLICY.replace(
            'definition_id = "coingecko.market_data.circulating_supply"',
            'definition_id = "coingecko.nope"',
        )
    )
    absent = reconcile(workspace)
    assert absent.exit_code == 1 and "absent observation" in absent.output

    policy_file.write_text(POLICY)
    unresolved = reconcile(workspace)  # resolution sources are not registered
    assert (
        unresolved.exit_code == 1
        and "is not an evidence or assumption id" in unresolved.output
    )

    policy_file.write_text(POLICY + "extra = 1\n")
    assert "invalid supply policy" in reconcile(workspace).output
    assert reconcile(workspace, as_of="2026-11-06").exit_code == 1  # nothing yet


def test_policy_parser_is_strict() -> None:
    assert parse_supply_policy(POLICY.encode()).tolerance["total"] == 0.01
    for bad in (
        POLICY.replace("version = 1", "version = 2"),
        POLICY.replace("circulating = 0.02", "circulating = -1"),
        POLICY.replace("[tolerance]", "[tolerance]\nbogus = 1"),
        POLICY.replace('source_id = "a-max-definition"', ""),
        POLICY.replace(
            'provider_priority = ["hyperliquid", "coingecko", "coinmarketcap"]',
            "provider_priority = []",
        ),
    ):
        with pytest.raises(IngestionError, match="invalid supply policy"):
            parse_supply_policy(bad.encode())


def test_audit_reports_conflict_cutoff_and_capture_mismatch(
    workspace: Path, tmp_path: Path
) -> None:
    build_supply(workspace, tmp_path)
    write_registers(workspace, ("a-circ-definition",))
    (workspace / "cases/hype/analysis/token-supply-policy.toml").write_text(
        POLICY.split("[resolution.circulating]")[0]
    )
    assert reconcile(workspace).exit_code == 0

    audited = invoke(
        workspace,
        "case",
        "audit",
        "hype",
        "--as-of",
        "2026-11-07",
        "--max-price-age-days",
        "1",
    )
    assert audited.exit_code == 1
    assert "[token_supply_conflict]" in audited.output
    assert "supply metric circulating is in conflict" in audited.output
    assert "token_supply_after_as_of" not in audited.output

    early = invoke(
        workspace,
        "case",
        "audit",
        "hype",
        "--as-of",
        "2026-11-06",
        "--max-price-age-days",
        "1",
    )
    assert "[token_supply_after_as_of]" in early.output
    assert "[token_market_after_as_of]" in early.output

    capture = Artifact(
        artifact_id="raw.capture.x.1",
        kind="raw.capture",
        schema_version=1,
        path="p",
        input_artifact_ids=("raw.capture-body.x.1",),
    )
    body = Artifact("raw.capture-body.x.1", "raw.capture-body", 1, "b", sha256="a" * 64)
    frame = pl.DataFrame(
        {
            "capture_id": ["raw.capture.x.1"],
            "body_artifact_id": ["raw.capture-body.x.1"],
            "content_sha256": ["b" * 64],
        }
    )
    [issue] = _capture_issues(capture, frame, {body.artifact_id: body})
    assert issue.code == "capture_metadata_mismatch"
    assert (
        _capture_issues(
            capture,
            frame.with_columns(pl.lit("a" * 64).alias("content_sha256")),
            {body.artifact_id: body},
        )
        == []
    )


def test_cli_ingest_and_normalize(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetcher = FakeFetcher([ok(fixture_bytes("coingecko_coin_hyperliquid.json"))])
    monkeypatch.setattr(coingecko, "default_fetcher", lambda: fetcher)
    ingested = invoke(
        workspace, "data", "ingest-coingecko-token", "hype", "hyperliquid"
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
        "normalize-token-supply",
        "hype",
        "--capture-artifact-id",
        capture_id,
        "--instrument-map",
        "hyperliquid=hype.hyperliquid.l1",
    )
    assert normalized.exit_code == 0, normalized.output
    assert "normalized token-supply:" in normalized.output
    assert "normalized token-market:" in normalized.output
    bad = invoke(
        workspace,
        "data",
        "normalize-token-supply",
        "hype",
        "--capture-artifact-id",
        capture_id,
        "--instrument-map",
        "oops",
    )
    assert bad.exit_code == 1 and "PROVIDER_ASSET_ID=INSTRUMENT_ID" in bad.output
    usage = invoke(
        workspace,
        "data",
        "ingest-hyperliquid-info",
        "hype",
        "--request",
        "token-details",
    )
    assert usage.exit_code == 1 and "--token-id" in usage.output


def _sha(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()
