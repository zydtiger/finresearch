"""Token model: calculation, validation, authentication, reports, and e2e."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest
from token_support import (
    RETRIEVED_AT,
    fixture_bytes,
    import_master_v3,
    import_prices,
    new_case,
)
from typer.testing import CliRunner, Result

from finresearch.cases import CaseContractError, read_manifest
from finresearch.cli import app
from finresearch.reporting import load_report_context
from finresearch.token_model import run_token

runner = CliRunner()
CASE = "hype"
INSTRUMENT = "hype.hyperliquid.l1"
POLICY = """\
version = 1
provider_priority = ["hyperliquid", "coingecko"]

[tolerance]
circulating = 0.02
total = 0.01
max = 0.0

[resolution.circulating]
provider = "coingecko"
definition_id = "coingecko.market_data.circulating_supply"
source_id = "a-circ-def"

[resolution.total]
provider = "coingecko"
definition_id = "coingecko.market_data.total_supply"
source_id = "a-total-def"
"""
ASSUMPTIONS = [
    "a-r-bear",
    "a-r-base",
    "a-r-bull",
    "a-g-bear",
    "a-g-base",
    "a-g-bull",
    "a-hr-bear",
    "a-hr-base",
    "a-hr-bull",
    "a-mix",
    "a-cap",
    "a-unlock",
    "a-real",
    "a-emit",
    "a-circ-def",
    "a-total-def",
    "a-bear-gross",
    "a-gmx-outlier",
    "a-usdt",
]
PEG_USDT = (
    '[pegs]\nusdt_usd = { value = 1.0, unit = "USD/USDT", '
    'source_id = "a-usdt" }\n\n[multiples]'
)
PEG_BAD = PEG_USDT.replace("USD/USDT", "USD/BTC")
HOLDERS_URL = "https://api.llama.fi/summary/fees/{slug}?dataType=dailyHoldersRevenue"


def invoke(workspace: Path, *arguments: str) -> Result:
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


def ok(workspace: Path, *arguments: str) -> Result:
    result = invoke(workspace, *arguments)
    assert result.exit_code == 0, result.output
    return result


def peer_chart(daily: float) -> dict[str, object]:
    end = date(2026, 11, 7)
    return {
        "methodology": None,
        "totalDataChart": [
            [int((end - timedelta(days=i)).strftime("%s")) + 0, daily]
            for i in reversed(range(40))
        ],
    }


def utc_seconds(day: date) -> int:
    from datetime import UTC, datetime

    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


def register(
    workspace: Path,
    tmp_path: Path,
    name: str,
    content: bytes,
    url: str,
    provider: str,
    body: str | None = None,
) -> str:
    source = tmp_path / name
    source.write_bytes(content)
    arguments = [
        "data", "register-capture", CASE, str(source), "--provider", provider,
        "--kind", "api-response", "--url", url,
        "--retrieved-at", RETRIEVED_AT.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--http-status", "200",
    ]  # fmt: skip
    if body is not None:
        body_file = tmp_path / f"{name}.body"
        body_file.write_text(body)
        arguments += ["--method", "POST", "--request-body", str(body_file)]
    result = ok(workspace, *arguments)
    return next(
        line.split(": ", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("capture: ")
    )


def artifact_ids(workspace: Path, kind: str) -> list[str]:
    manifest = read_manifest(workspace / "cases" / CASE)
    return sorted(a.artifact_id for a in manifest.artifacts if a.kind == kind)


def write_registers(workspace: Path) -> None:
    registers = workspace / "cases" / CASE / "registers"
    registers.mkdir(exist_ok=True)
    (registers / "evidence.csv").write_text(
        "id,claim,source_type,source_ref,observed_at,notes\n"
        "e-1,note,external,docs,2026-11-07,\n"
    )
    lines = ["id,parameter,value,unit,rationale,source_evidence,updated_at"]
    lines += [f"{item},p,1,ratio,r,e-1,2026-11-07" for item in ASSUMPTIONS]
    (registers / "assumptions.csv").write_text("\n".join(lines) + "\n")


def scenario_toml(
    name: str, rate: float, growth: float, revenue: float, periods: list[str],
    extra: str = "",
) -> str:  # fmt: skip
    forecast = "".join(
        f'  {{ period_end = "{day}", '
        f'holders_revenue = {{ value = {revenue}, unit = "USDm", '
        f'source_id = "a-hr-{name}" }}, '
        'capture_ratio = { value = 0.9, unit = "ratio", source_id = "a-cap" }, '
        'unlocks = { value = 119.0, unit = "tokens_m", source_id = "a-unlock" }, '
        'unlock_realization = { value = 0.2, unit = "ratio", source_id = "a-real" }, '
        'emissions = { value = 10.0, unit = "tokens_m", source_id = "a-emit" } },\n'
        for day in periods
    )
    return (
        f"[scenario.{name}]\n"
        f'discount_rate = {{ value = {rate}, unit = "ratio", '
        f'source_id = "a-r-{name}" }}\n'
        "capture_mix = { buyback_retire = 0.0, buyback_hold = 1.0, burn = 0.0, "
        'distribution = 0.0, source_id = "a-mix" }\n'
        f'terminal = {{ terminal_growth = {{ value = {growth}, unit = "ratio", '
        f'source_id = "a-g-{name}" }} }}\n'
        f"forecast = [\n{forecast}]\n"
        f"{extra}"
        f'[scenario.{name}.multiple]\nstatistic = "median"\n\n'
    )


def token_toml(workspace: Path, **tweaks: str) -> str:
    price = artifact_ids(workspace, "normalized.daily-prices")[0]
    master = artifact_ids(workspace, "normalized.instrument-master")[0]
    recon = artifact_ids(workspace, "derived.token-supply-reconciliation")[0]
    markets = artifact_ids(workspace, "normalized.token-market")
    metrics = artifact_ids(workspace, "normalized.protocol-metrics")

    def quoted(items: list[str]) -> str:
        return "[" + ", ".join(f'"{i}"' for i in items) + "]"

    text = (
        "version = 1\n"
        'as_of = "2026-11-07"\n'
        f'instrument_id = "{INSTRUMENT}"\n'
        'currency = "USD"\nvalue_unit = "USDm"\ntoken_unit = "tokens_m"\n'
        'discount_convention = "year_end"\nheld_buyback_treatment = "exclude"\n'
        'buyback_price_basis = "as_of_price"\n\n'
        f'[price]\nartifact_id = "{price}"\nsession_date = "2026-11-06"\n'
        f'master_artifact_id = "{master}"\n\n'
        f'[supply]\nreconciliation_artifact_id = "{recon}"\n'
        'fully_diluted_metric = "total"\n\n'
        + scenario_toml(
            "bear",
            0.30,
            0.02,
            500.0,
            ["2027-11-07", "2028-11-07"],
            'fully_diluted = { metric = "total", provider = "hyperliquid", '
            'source_id = "a-bear-gross" }\n',
        )
        + scenario_toml("base", 0.25, 0.03, 600.0, ["2027-11-07"])
        + scenario_toml("bull", 0.20, 0.04, 700.0, ["2027-11-07"])
        + '[multiples]\nmetric = "holders_revenue"\nwindow = "annualized_30d"\n'
        'metric_window_end = "2026-11-06"\nmarket_observed_on = "2026-11-07"\n'
        f"market_artifact_ids = {quoted(markets)}\n"
        f"metric_artifact_ids = {quoted(metrics)}\n"
        "peers = [\n"
        '  { instrument_id = "dydx.dydx-chain", entity_id = "dydx" },\n'
        '  { instrument_id = "aster.aster", entity_id = "aster" },\n'
        '  { instrument_id = "zero.zero-coin", entity_id = "zero" },\n'
        "]\n"
        'exclude = [{ instrument_id = "gmx.arbitrum", '
        'reason_source_id = "a-gmx-outlier" }]\n'
    )
    for old, new in tweaks.items():
        key = old.replace("__", " ")
        assert key in text, key
        text = text.replace(key, new)
    return text


def build_case(workspace: Path, tmp_path: Path, *, resolve_supply: bool = True) -> None:
    new_case(workspace)
    import_master_v3(workspace, tmp_path)
    import_prices(workspace, tmp_path, [("2026-11-05", 70.0), ("2026-11-06", 78.4549)])
    coin = register(
        workspace, tmp_path, "coin.json",
        fixture_bytes("coingecko_coin_hyperliquid.json"),
        "https://api.coingecko.com/api/v3/coins/hyperliquid?vs_currency=usd",
        "coingecko",
    )  # fmt: skip
    details = register(
        workspace, tmp_path, "details.json",
        fixture_bytes("hyperliquid_token_details.json"),
        "https://api.hyperliquid.xyz/info", "hyperliquid",
        body='{"type":"tokenDetails","tokenId":"0x0d01dc56dcaaca66ad901c959b4011ec"}',
    )  # fmt: skip
    markets = register(
        workspace, tmp_path, "markets.json",
        fixture_bytes("coingecko_markets_peers.json"),
        "https://api.coingecko.com/api/v3/coins/markets?ids=aster,dydx-chain,zero-coin&vs_currency=usd",
        "coingecko",
    )  # fmt: skip
    for capture in (coin, details):
        ok(workspace, "data", "normalize-token-supply", CASE,
           "--capture-artifact-id", capture, "--instrument-id", INSTRUMENT)  # fmt: skip
    ok(workspace, "data", "normalize-token-supply", CASE,
       "--capture-artifact-id", markets,
       "--instrument-map", "dydx-chain=dydx.dydx-chain",
       "--instrument-map", "aster=aster.aster",
       "--instrument-map", "zero-coin=zero.zero-coin")  # fmt: skip
    for slug, daily in (("dydx", 100000.0), ("aster", 200000.0), ("zero", 0.0)):
        chart = peer_chart(daily)
        chart["totalDataChart"] = [
            [utc_seconds(end_day), daily]
            for end_day in (
                date(2026, 11, 7) - timedelta(days=i) for i in reversed(range(40))
            )
        ]
        capture = register(
            workspace, tmp_path, f"{slug}.json", json.dumps(chart).encode(),
            HOLDERS_URL.format(slug=slug), "defillama",
        )  # fmt: skip
        ok(workspace, "data", "normalize-protocol-metrics", CASE,
           "--capture-artifact-id", capture, "--entity-id", slug)  # fmt: skip
    write_registers(workspace)
    analysis = workspace / "cases" / CASE / "analysis"
    analysis.mkdir(exist_ok=True)
    policy = POLICY if resolve_supply else POLICY.split("[resolution.circulating]")[0]
    (analysis / "token-supply-policy.toml").write_text(policy)
    ok(workspace, "data", "reconcile-token-supply", CASE,
       "--instrument-id", INSTRUMENT, "--as-of", "2026-11-07",
       "--policy", "analysis/token-supply-policy.toml")  # fmt: skip


def write_inputs(workspace: Path, text: str) -> None:
    analysis = workspace / "cases" / CASE / "analysis"
    analysis.mkdir(exist_ok=True)
    (analysis / "token-inputs.toml").write_text(text)


@pytest.fixture
def built(tmp_path: Path) -> Path:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    build_case(workspace, tmp_path)
    write_inputs(workspace, token_toml(workspace))
    return workspace


def results(workspace: Path, run_id: str) -> pl.DataFrame:
    manifest = read_manifest(workspace / "cases" / CASE)
    artifact = next(
        a
        for a in manifest.artifacts
        if a.artifact_id == f"model.token-results.results.{run_id}"
    )
    return pl.read_parquet(workspace / "cases" / CASE / artifact.path)


def run_id_of(result: Result) -> str:
    return next(
        line.split(": ", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("run_id: ")
    )


def test_base_scenario_matches_hand_computed_values(built: Path) -> None:
    result = ok(built, "model", "token", CASE, "--sensitivity", "0.2,0.25;0.02,0.03")
    run_id = run_id_of(result)
    frame = results(built, run_id)
    assert frame.height == 12  # 3 scenarios x 2 methods x 2 bases

    price = 78.4549
    circulating, total = 239406814.37, 951538272.91
    cash_flow = 600.0 * 0.9
    aggregate = cash_flow / 1.25 + cash_flow * 1.03 / (0.25 - 0.03) / 1.25
    base = {
        (row["method"], row["basis"]): row
        for row in frame.filter(pl.col("scenario") == "base").iter_rows(named=True)
    }
    dcf_fd = base[("dcf", "fully_diluted")]
    assert dcf_fd["aggregate_value"] == pytest.approx(aggregate, rel=1e-12)
    assert dcf_fd["per_token_value"] == pytest.approx(
        aggregate * 1e6 / total, rel=1e-12
    )
    assert base[("dcf", "circulating")]["per_token_value"] == pytest.approx(
        aggregate * 1e6 / circulating, rel=1e-12
    )
    assert dcf_fd["implied_cf_scale"] == pytest.approx(
        price * total / (aggregate * 1e6), rel=1e-12
    )
    assert dcf_fd["upside"] == pytest.approx(dcf_fd["per_token_value"] / price - 1)
    assert dcf_fd["terminal_share"] == pytest.approx(
        (cash_flow * 1.03 / 0.22 / 1.25) / aggregate, rel=1e-12
    )
    assert base[("dcf", "circulating")]["implied_cf_scale"] is None
    assert base[("multiple", "fully_diluted")]["implied_cf_scale"] is None
    assert dcf_fd["denominator_tokens"] == pytest.approx(total / 1e6)

    # Peer multiples: dydx and aster included, zero-revenue peer excluded.
    dydx_fdv = 1e9 / (100000.0 * 30 * 365 / 30)
    aster_fdv = 1.2e9 / (200000.0 * 365)
    median_fdv = (dydx_fdv + aster_fdv) / 2
    multiple_fd = base[("multiple", "fully_diluted")]
    assert multiple_fd["multiple_applied"] == pytest.approx(median_fdv)
    assert multiple_fd["aggregate_value"] == pytest.approx(median_fdv * 600.0)
    assert multiple_fd["per_token_value"] == pytest.approx(
        median_fdv * 600.0 * 1e6 / total
    )

    # Bear overrides the FD denominator with Hyperliquid's gross total supply.
    bear_fd = frame.filter(
        (pl.col("scenario") == "bear")
        & (pl.col("method") == "dcf")
        & (pl.col("basis") == "fully_diluted")
    ).row(0, named=True)
    assert bear_fd["denominator_tokens"] == pytest.approx(998778474.0 / 1e6)

    manifest = read_manifest(built / "cases" / CASE)
    kinds = [a.kind for a in manifest.artifacts if a.kind.startswith("model.token")]
    assert kinds == [
        "model.token-inputs",
        "model.token-supply-path",
        "model.token-cashflows",
        "model.token-peer-multiples",
        "model.token-results",
        "model.token-sensitivity",
        "model.token-reconciliation",
    ]
    by_kind = {
        a.kind: a for a in manifest.artifacts if a.kind.startswith("model.token")
    }
    assert by_kind["model.token-results"].input_artifact_ids == (
        by_kind["model.token-supply-path"].artifact_id,
        by_kind["model.token-cashflows"].artifact_id,
        by_kind["model.token-peer-multiples"].artifact_id,
    )
    assert len(by_kind["model.token-inputs"].input_artifact_ids) >= 7

    path = pl.read_parquet(
        built / "cases" / CASE / by_kind["model.token-supply-path"].path
    ).filter(pl.col("scenario") == "base")
    row = path.row(0, named=True)
    retired = cash_flow / price  # USDm / (USD/token) = tokens_m
    assert row["retired_tokens"] == pytest.approx(retired)
    assert row["closing_circulating"] == pytest.approx(
        circulating / 1e6 + 119.0 * 0.2 + 10.0 - retired
    )
    assert row["closing_fully_diluted"] == pytest.approx(total / 1e6 - retired)
    reconciliation = pl.read_parquet(
        built / "cases" / CASE / by_kind["model.token-reconciliation"].path
    )
    assert reconciliation["passed"].all()

    again = ok(built, "model", "token", CASE, "--sensitivity", "0.2,0.25;0.02,0.03")
    assert run_id_of(again) == run_id
    assert len(read_manifest(built / "cases" / CASE).artifacts) == len(
        manifest.artifacts
    )


def test_peer_exclusions_are_explicit(built: Path) -> None:
    run_id = run_id_of(ok(built, "model", "token", CASE, "--scenario", "base"))
    manifest = read_manifest(built / "cases" / CASE)
    artifact = next(
        a
        for a in manifest.artifacts
        if a.artifact_id == f"model.token-peer-multiples.peer-multiples.{run_id}"
    )
    peers = pl.read_parquet(built / "cases" / CASE / artifact.path)
    reasons = {
        (r["peer_instrument_id"], r["basis"]): (r["status"], r["exclusion_reason"])
        for r in peers.iter_rows(named=True)
    }
    assert reasons[("zero.zero-coin", "circulating")] == (
        "excluded", "nonpositive_holders_revenue"
    )  # fmt: skip
    assert reasons[("zero.zero-coin", "fully_diluted")] == (
        "excluded", "missing_market_observation"
    )  # fmt: skip
    assert reasons[("gmx.arbitrum", "circulating")] == (
        "excluded",
        "excluded:a-gmx-outlier",
    )
    assert reasons[("dydx.dydx-chain", "fully_diluted")] == ("included", None)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "terminal_growth = { value = 0.03",
            "terminal_growth = { value = 0.30",
            "-1 < g",
        ),
        (
            "capture_ratio = { value = 0.9",
            "capture_ratio = { value = 1.5",
            r"within \[0, 1\]",
        ),
        ("buyback_hold = 1.0", "buyback_hold = 0.5", "sum to 1"),
        ('statistic = "median"', 'statistic = "mean"', "statistic"),
        ('"a-hr-base"', '"a-missing"', "dangling source ids"),
        (
            'period_end = "2027-11-07", holders_revenue = { value = 600.0',
            'period_end = "2026-11-07", holders_revenue = { value = 600.0',
            "strictly increase",
        ),
        ('session_date = "2026-11-06"', 'session_date = "2026-11-08"', "session_date"),
        ('currency = "USD"', 'currency = "EUR"', "currency"),
        (
            'fully_diluted_metric = "total"',
            'fully_diluted_metric = "burned"',
            "total or max",
        ),
        ('window = "annualized_30d"', 'window = "ttm"', "no valid peer remains"),
        ("version = 1", "version = 2", "version = 1"),
        (
            'holders_revenue = { value = 600.0, unit = "USDm"',
            'holders_revenue = { value = 6000000000.0, unit = "USDm"',
            "retires more tokens",
        ),
        (
            'buyback_price_basis = "as_of_price"',
            'buyback_price_basis = "spot"',
            "as_of_price",
        ),
        (
            'holders_revenue = { value = 600.0, unit = "USDm"',
            'holders_revenue = { value = 600.0, unit = "USD"',
            "unit",
        ),
    ],
)
def test_validation_failures(built: Path, old: str, new: str, message: str) -> None:
    write_inputs(built, token_toml(built, **{old.replace(" ", "__"): new}))
    with pytest.raises(CaseContractError, match=message):
        run_token(built, CASE, scenario="all")


def test_unresolved_supply_conflict_aborts(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    build_case(workspace, tmp_path, resolve_supply=False)
    write_inputs(workspace, token_toml(workspace))
    with pytest.raises(CaseContractError, match="conflict for circulating"):
        run_token(workspace, CASE)


def test_peg_leaf_is_required_for_stablecoin_quotes(built: Path) -> None:
    text = token_toml(built)
    assert "[pegs]" not in text
    write_inputs(
        built,
        text.replace(
            "[multiples]",
            PEG_USDT,
        ),
    )
    ok(built, "model", "token", CASE, "--scenario", "base")
    write_inputs(
        built,
        text.replace(
            "[multiples]",
            PEG_BAD,
        ),
    )
    with pytest.raises(CaseContractError, match="USDC"):
        run_token(built, CASE)


def audit(workspace: Path, *extra: str) -> Result:
    return invoke(
        workspace, "case", "audit", CASE, "--as-of", "2026-11-07",
        "--max-price-age-days", "5", "--max-price-age", "crypto=0", *extra,
    )  # fmt: skip


def test_offline_end_to_end_report_and_audit(built: Path) -> None:
    run_id = run_id_of(ok(built, "model", "token", CASE))
    assert audit(built, "--verify-hashes").exit_code == 0

    markdown = ok(built, "report", "markdown", CASE, "--model-run-id", run_id)
    html = ok(built, "report", "html", CASE, "--model-run-id", run_id)
    paths = [
        line.split(": ", 1)[1]
        for output in (markdown, html)
        for line in output.output.splitlines()
        if line.startswith("path: ")
    ]
    text = Path(paths[0]).read_text()
    assert "# TOKEN report" in text
    assert "## Supply path" in text and "## Peer multiples" in text
    assert "dydx.dydx-chain" in text and "fully_diluted" in text
    page = Path(paths[1]).read_text()
    assert "<h2>Supply path</h2>" in page and "<h2>Peer multiples</h2>" in page
    assert (
        Path(paths[0]).read_bytes()
        == Path(
            ok(built, "report", "markdown", CASE, "--model-run-id", run_id)
            .output.split("path: ")[1]
            .split("\n")[0]
        ).read_bytes()
    )
    assert load_report_context(built, CASE, run_id).family == "token"
    assert audit(built, "--verify-hashes").exit_code == 0


def test_audit_detects_tampering(built: Path) -> None:
    ok(built, "model", "token", CASE)
    assert audit(built).exit_code == 0

    inputs = built / "cases" / CASE / "analysis" / "token-inputs.toml"
    original = inputs.read_text()
    inputs.write_text(original.replace("value = 600.0", "value = 601.0"))
    changed = audit(built)
    assert changed.exit_code == 1
    assert "[token_run_identity_invalid]" in changed.output
    inputs.write_text(original)
    assert audit(built).exit_code == 0

    manifest = read_manifest(built / "cases" / CASE)
    results_artifact = next(
        a for a in manifest.artifacts if a.kind == "model.token-results"
    )
    target = built / "cases" / CASE / results_artifact.path
    frame = pl.read_parquet(target)
    target.unlink()
    frame.with_columns(pl.col("per_token_value") * 2).write_parquet(target)
    tampered = audit(built, "--verify-hashes")
    assert tampered.exit_code == 1
    assert "token_run_identity_invalid" in tampered.output


def test_audit_reports_token_source_after_cutoff(built: Path) -> None:
    ok(built, "model", "token", CASE)
    early = invoke(
        built, "case", "audit", CASE, "--as-of", "2026-11-05",
        "--max-price-age-days", "99",
    )  # fmt: skip
    assert early.exit_code == 1
    assert "[token_run_after_as_of]" in early.output


def test_price_session_must_precede_as_of(built: Path) -> None:
    write_inputs(
        built,
        token_toml(
            built, **{'session_date = "2026-11-06"': 'session_date = "2026-11-07"'}
        ),
    )
    with pytest.raises(CaseContractError, match="before as_of"):
        run_token(built, CASE)


def test_overflowing_discount_factor_is_rejected(built: Path) -> None:
    text = token_toml(built, **{"value = 0.3,": "value = 1e200,"})
    write_inputs(built, text)
    with pytest.raises(CaseContractError, match="finite"):
        run_token(built, CASE)


def test_metric_rows_observed_after_as_of_are_ignored(built: Path) -> None:
    from datetime import UTC, datetime

    from finresearch.token_model import (
        Peer,
        _holders_revenue_window,
        load_token_input,
    )

    config, _, _ = load_token_input(
        built / "cases" / CASE, "analysis/token-inputs.toml"
    )
    days = [date(2026, 11, 6) - timedelta(days=i) for i in range(30)]

    def frame(observed: datetime) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "entity_id": ["p"] * 30,
                "metric": ["holders_revenue"] * 30,
                "sub_protocol": [None] * 30,
                "chain": [None] * 30,
                "period_type": ["duration"] * 30,
                "unit": ["USD"] * 30,
                "period_start": days,
                "period_end": days,
                "value": [1.0] * 30,
                "is_complete": [True] * 30,
                "observed_at": [observed] * 30,
            },
            schema_overrides={"observed_at": pl.Datetime("us", "UTC")},
        )

    peer = Peer("x", "p")
    assert _holders_revenue_window(
        config, frame(datetime(2026, 11, 7, tzinfo=UTC)), peer
    ) == pytest.approx(365.0)
    assert (
        _holders_revenue_window(config, frame(datetime(2026, 11, 8, tzinfo=UTC)), peer)
        is None
    )
    short = frame(datetime(2026, 11, 7, tzinfo=UTC)).filter(
        pl.col("period_end") < date(2026, 11, 6)
    )
    assert _holders_revenue_window(config, short, peer) is None


UNLOCK_HEADER = (
    "instrument_id,schedule_id,event_id,category,recipient_label,holder_address,"
    "unlock_type,unlock_date,unlock_end_date,amount,status,observed_at,source_ref,"
    "capture_artifact_id"
)
SCHEDULE = "tokenomist-2026-11-07"
UNLOCK_LEAF = 'unlocks = { value = 119.0, unit = "tokens_m", source_id = "a-unlock" }, '
SUPPLY_LINE = 'fully_diluted_metric = "total"\n'
BEAR_OVERRIDE = (
    'fully_diluted = { metric = "total", provider = "hyperliquid", '
    'source_id = "a-bear-gross"'
)


def unlock_row(
    event: str,
    kind: str = "cliff",
    start: str = "2027-03-01",
    end: str = "",
    amount: float = 20e6,
    status: str = "scheduled",
    schedule: str = SCHEDULE,
    ref: str = "https://tokenomist.example/hype",
    capture: str = "",
    observed: str = "2026-11-01T00:00:00Z",
) -> str:
    return (
        f"{INSTRUMENT},{schedule},{event},team,,,{kind},{start},{end},{amount},"
        f"{status},{observed},{ref},{capture}"
    )


def default_unlock_rows() -> list[str]:
    return [
        unlock_row("a-cliff"),
        # 1M tokens a day: 365 days land in period one, 10 in the bear's second.
        unlock_row("b-stream", "linear", "2026-11-08", "2027-11-17", 375e6),
        unlock_row("c-cancelled", status="cancelled", amount=50e6),
        unlock_row("d-done", status="unlocked", amount=60e6),
        unlock_row("e-on-as-of", start="2026-11-07", amount=7e6),
    ]


def import_unlocks(workspace: Path, tmp_path: Path, rows: list[str]) -> None:
    from token_support import import_csv_text

    import_csv_text(
        workspace,
        tmp_path,
        "unlocks.csv",
        UNLOCK_HEADER + "\n" + "\n".join(rows) + "\n",
        "token-unlocks.v1",
        provider="tokenomist",
    )


def scheduled_toml(workspace: Path, schedule: str = SCHEDULE, **tweaks: str) -> str:
    artifact = artifact_ids(workspace, "normalized.token-unlocks")[0]
    supply = (
        f'{SUPPLY_LINE}unlocks_artifact_id = "{artifact}"\nschedule_id = "{schedule}"\n'
    )
    return token_toml(
        workspace,
        **{
            UNLOCK_LEAF.replace(" ", "__"): "",
            SUPPLY_LINE.replace(" ", "__"): supply,
        },
        **tweaks,
    )


def table(workspace: Path, kind: str, run_id: str) -> pl.DataFrame:
    manifest = read_manifest(workspace / "cases" / CASE)
    artifact = next(
        a
        for a in manifest.artifacts
        if a.artifact_id.endswith(run_id) and a.kind == kind
    )
    return pl.read_parquet(workspace / "cases" / CASE / artifact.path)


def test_unlock_schedule_drives_supply_path(built: Path, tmp_path: Path) -> None:
    import_unlocks(built, tmp_path, default_unlock_rows())
    write_inputs(built, scheduled_toml(built))
    run_id = run_id_of(ok(built, "model", "token", CASE))

    path = table(built, "model.token-supply-path", run_id)

    def realized(scenario: str) -> list[float]:
        rows = path.filter(pl.col("scenario") == scenario).sort("period_end")
        return rows["unlocks_realized"].to_list()

    # (20M cliff + 365M of the stream) in tokens_m, times realization 0.2.
    assert realized("base") == pytest.approx([77.0])
    assert realized("bear") == pytest.approx([77.0, 2.0])

    inputs = table(built, "model.token-inputs", run_id)
    row = inputs.filter(
        (pl.col("scenario") == "bear") & (pl.col("field") == "unlocks:2028-11-07")
    ).row(0, named=True)
    assert row["value"] == pytest.approx(10.0)
    assert row["source_id"] is None
    assert (
        row["source_artifact_id"] == artifact_ids(built, "normalized.token-unlocks")[0]
    )
    assert not inputs.filter(pl.col("source_id") == "a-unlock").height

    manifest = read_manifest(built / "cases" / CASE)
    inputs_artifact = next(
        a for a in manifest.artifacts if a.kind == "model.token-inputs"
    )
    assert (
        artifact_ids(built, "normalized.token-unlocks")[0]
        in inputs_artifact.input_artifact_ids
    )
    assert run_id_of(ok(built, "model", "token", CASE)) == run_id
    assert audit(built, "--verify-hashes").exit_code == 0


def test_unlock_schedule_is_part_of_run_identity(built: Path, tmp_path: Path) -> None:
    import_unlocks(built, tmp_path, default_unlock_rows())
    (first_id,) = artifact_ids(built, "normalized.token-unlocks")
    text = scheduled_toml(built)
    write_inputs(built, text)
    first = run_id_of(ok(built, "model", "token", CASE, "--scenario", "base"))
    revised = default_unlock_rows()
    revised[0] = unlock_row("a-cliff", amount=21e6)
    import_unlocks(built, tmp_path, revised)
    (second_id,) = set(artifact_ids(built, "normalized.token-unlocks")) - {first_id}
    write_inputs(built, text.replace(first_id, second_id))
    second = run_id_of(ok(built, "model", "token", CASE, "--scenario", "base"))
    assert first != second


@pytest.mark.parametrize(
    ("tweaks", "message"),
    [
        ({"": ""}, "declares unlocks"),
        ({"__": ""}, "needs unlocks"),
        ({"schedule_id": "tokenomist-other"}, "does not hold schedule_id"),
    ],
)
def test_unlock_schedule_validation(
    built: Path, tmp_path: Path, tweaks: dict[str, str], message: str
) -> None:
    import_unlocks(built, tmp_path, default_unlock_rows())
    text = scheduled_toml(built)
    if message == "declares unlocks":
        text = text.replace(
            'capture_ratio = { value = 0.9, unit = "ratio", source_id = "a-cap" }, ',
            'capture_ratio = { value = 0.9, unit = "ratio", source_id = "a-cap" }, '
            + UNLOCK_LEAF,
        )
    elif message == "needs unlocks":
        text = token_toml(built).replace(UNLOCK_LEAF, "")
    else:
        text = scheduled_toml(built, schedule="tokenomist-other")
    write_inputs(built, text)
    with pytest.raises(CaseContractError, match=message):
        run_token(built, CASE)


def test_unlock_keys_are_declared_together(built: Path, tmp_path: Path) -> None:
    import_unlocks(built, tmp_path, default_unlock_rows())
    text = scheduled_toml(built).replace(f'schedule_id = "{SCHEDULE}"\n', "")
    write_inputs(built, text)
    with pytest.raises(CaseContractError, match="declared together"):
        run_token(built, CASE)


def test_unlocks_observed_after_as_of_are_rejected(built: Path, tmp_path: Path) -> None:
    rows = default_unlock_rows()
    rows[0] = unlock_row("a-cliff", observed="2026-11-09T00:00:00Z")
    import_unlocks(built, tmp_path, rows)
    write_inputs(built, scheduled_toml(built))
    with pytest.raises(CaseContractError, match="observed after as_of"):
        run_token(built, CASE)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([unlock_row("x", "linear")], "linear unlocks require"),
        ([unlock_row("x", "cliff", end="2027-04-01")], "cliff unlocks forbid"),
        (
            [unlock_row("x", "linear", "2027-04-01", "2027-03-01")],
            "must not precede",
        ),
        ([unlock_row("x", amount=0.0)], "amount"),
        ([unlock_row("x"), unlock_row("x")], "duplicate rows"),
        ([unlock_row("x"), unlock_row("y", schedule="other")], "one schedule_id"),
        ([unlock_row("x", ref=" ")], "source_ref"),
        ([unlock_row("x", capture="missing-capture")], "not a declared"),
    ],
)
def test_unlock_import_rejects_invalid_schedules(
    built: Path, tmp_path: Path, rows: list[str], message: str
) -> None:
    source = tmp_path / "bad.csv"
    source.write_text(UNLOCK_HEADER + "\n" + "\n".join(rows) + "\n")
    result = invoke(
        built, "data", "import-csv", CASE, str(source), "--schema",
        "token-unlocks.v1", "--provider", "tokenomist",
        "--retrieved-at", "2026-11-07T00:00:00Z",
    )  # fmt: skip
    assert result.exit_code != 0
    assert message in result.output


def test_audit_reports_unlocks_above_remaining_supply(
    built: Path, tmp_path: Path
) -> None:
    ok(built, "model", "token", CASE)
    assert audit(built).exit_code == 0
    # max 1.0B minus circulating 239.4M leaves about 760.6M.
    import_unlocks(built, tmp_path, [unlock_row("big", amount=800e6)])
    flagged = audit(built)
    assert flagged.exit_code == 1
    assert "[token_unlocks_exceed_supply]" in flagged.output


def test_audit_checks_unlock_capture_references(built: Path, tmp_path: Path) -> None:
    capture = register(
        built, tmp_path, "unlocks.json", b'{"events": []}',
        "https://tokenomist.example/hype", "web",
    )  # fmt: skip
    import_unlocks(
        built,
        tmp_path,
        [unlock_row("cited", ref=f"capture:{capture}", capture=capture)],
    )
    assert audit(built).exit_code == 0
    import_unlocks(
        built,
        tmp_path,
        [unlock_row("uncited", ref="capture:not-registered", schedule="v2")],
    )
    flagged = audit(built)
    assert flagged.exit_code == 1
    assert "[token_unlocks_capture_missing]" in flagged.output
    assert "not-registered" in flagged.output


def bear_fd(workspace: Path, run_id: str) -> dict[tuple[str, str], float]:
    frame = results(workspace, run_id).filter(
        (pl.col("scenario") == "bear") & (pl.col("basis") == "fully_diluted")
    )
    return {
        (r["method"], "den"): r["denominator_tokens"]
        for r in frame.iter_rows(named=True)
    }


def test_fully_diluted_override_scope(built: Path) -> None:
    both = run_id_of(ok(built, "model", "token", CASE))
    assert bear_fd(built, both) == {
        ("dcf", "den"): pytest.approx(998778474.0 / 1e6),
        ("multiple", "den"): pytest.approx(998778474.0 / 1e6),
    }
    explicit = token_toml(
        built,
        **{
            BEAR_OVERRIDE.replace(" ", "__"): BEAR_OVERRIDE
            + ', applies_to = ["dcf", "multiple"]'
        },
    )
    write_inputs(built, explicit)
    same = run_id_of(ok(built, "model", "token", CASE))
    assert bear_fd(built, same) == bear_fd(built, both)

    scoped = token_toml(
        built,
        **{BEAR_OVERRIDE.replace(" ", "__"): BEAR_OVERRIDE + ', applies_to = ["dcf"]'},
    )
    write_inputs(built, scoped)
    run_id = run_id_of(ok(built, "model", "token", CASE))
    assert bear_fd(built, run_id) == {
        ("dcf", "den"): pytest.approx(998778474.0 / 1e6),
        ("multiple", "den"): pytest.approx(951538272.91 / 1e6),
    }
    inputs = table(built, "model.token-inputs", run_id).filter(
        pl.col("scenario") == "bear"
    )
    fields = {
        r["field"]: r
        for r in inputs.iter_rows(named=True)
        if r["field"].startswith("fully_diluted_0")
    }
    assert set(fields) == {"fully_diluted_0:dcf", "fully_diluted_0:multiple"}
    assert fields["fully_diluted_0:dcf"]["source_id"] == "a-bear-gross"
    assert fields["fully_diluted_0:multiple"]["source_artifact_id"] is not None
    # Base and bull keep the selected denominator for both methods.
    base = results(built, run_id).filter(
        (pl.col("scenario") == "base") & (pl.col("basis") == "fully_diluted")
    )
    assert (
        base["denominator_tokens"].to_list() == [pytest.approx(951538272.91 / 1e6)] * 2
    )


def test_scoped_override_run_authenticates(built: Path) -> None:
    scoped = token_toml(
        built,
        **{
            BEAR_OVERRIDE.replace(" ", "__"): BEAR_OVERRIDE
            + ', applies_to = ["multiple"]'
        },
    )
    write_inputs(built, scoped)
    ok(built, "model", "token", CASE, "--sensitivity", "0.2,0.25;0.02,0.03")
    result = audit(built, "--verify-hashes")
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    "applies_to",
    ["[]", '["multiple", "dcf"]', '["dcf", "dcf"]', '["dcf", "fd"]', '"dcf"'],
)
def test_fully_diluted_override_scope_validation(built: Path, applies_to: str) -> None:
    write_inputs(
        built,
        token_toml(
            built,
            **{
                BEAR_OVERRIDE.replace(" ", "__"): BEAR_OVERRIDE
                + f", applies_to = {applies_to}"
            },
        ),
    )
    with pytest.raises(CaseContractError, match="applies_to"):
        run_token(built, CASE)


def test_audit_reports_stablecoin_peg_deviation(built: Path, tmp_path: Path) -> None:
    write_inputs(built, token_toml(built).replace("[multiples]", PEG_USDT))
    import_prices(
        built, tmp_path, [("2026-11-06", 0.9990)], instrument_id="manual.usdt",
        symbol="USDT-USD", name="usdt.csv",
    )  # fmt: skip
    ok(built, "model", "token", CASE, "--scenario", "base")
    assert audit(built).exit_code == 0

    write_inputs(
        built,
        token_toml(built).replace(
            "[multiples]", PEG_USDT.replace("value = 1.0", "value = 1.01")
        ),
    )
    ok(built, "model", "token", CASE, "--scenario", "base")
    flagged = audit(built)
    assert flagged.exit_code == 1
    assert "[stablecoin_peg_deviation]" in flagged.output
    assert "usdt_usd" in flagged.output
