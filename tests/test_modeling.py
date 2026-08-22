"""Focused contracts for pure and case-backed analytical models."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from math import inf, nan
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from typer.testing import CliRunner

import finresearch.modeling as modeling_module
from finresearch.auditing import audit_case
from finresearch.cases import (
    canonical_parameters_sha256,
    initialize_case,
    read_manifest,
    write_manifest,
)
from finresearch.cli import app
from finresearch.data_contracts import (
    MODEL_COMPS_INPUTS_V1,
    MODEL_COMPS_RECONCILIATION_V1,
    MODEL_COMPS_RESULTS_V1,
    MODEL_COMPS_SUMMARY_V1,
    MODEL_DCF_RESULTS_V1,
    DataContractError,
)
from finresearch.data_validation import validate_artifact
from finresearch.ingestion import ArtifactIntegrityError, IngestionError
from finresearch.local_import import IMPORT_SCHEMAS, import_parquet
from finresearch.model import (
    DCFInput,
    DCFPeriod,
    DCFSpecification,
    ModelError,
    annualized_return,
    dated_dcf_valuation,
    dcf_sensitivity,
    diluted_shares,
    enterprise_value,
    growth,
    margin,
    net_debt,
    wacc_from_components,
)
from finresearch.modeling import projection_assessment, run_comps, run_dcf
from finresearch.reporting import ReportError, generate_report, load_report_context

runner = CliRunner()


def _write_sources(case_dir: Path) -> None:
    registers = case_dir / "registers"
    registers.mkdir()
    registers.joinpath("evidence.csv").write_text(
        "id,claim,source_type,source_ref,observed_at,notes\n"
        "e1,x,filing,x,2026-01-01,\n"
        "e2,x,filing,x,2026-01-01,\n"
        "e3,x,filing,x,2026-01-01,\n"
        "e4,x,filing,x,2026-01-01,\n",
        encoding="utf-8",
    )
    registers.joinpath("assumptions.csv").write_text(
        "id,parameter,value,unit,rationale,source_evidence,updated_at\n"
        "a1,x,x,ratio,x,e1,2026-01-01\n"
        "a2,x,x,ratio,x,e1,2026-01-01\n"
        "a3,x,x,ratio,x,e1,2026-01-01\n"
        "a4,x,x,ratio,x,e1,2026-01-01\n"
        "a5,x,x,ratio,x,e1,2026-01-01\n"
        "a6,x,x,ratio,x,e1,2026-01-01\n",
        encoding="utf-8",
    )


def _write_dcf_input(case_dir: Path, *, needs: str = "[]") -> None:
    analysis = case_dir / "analysis"
    analysis.mkdir()
    analysis.joinpath("dcf-inputs.toml").write_text(
        f"""version = 1
as_of = "2026-06-30"
currency = "USD"
value_unit = "USDm"
share_unit = "shares_m"
discount_convention = "year_end"
terminal_method = "gordon_growth"
projection_needs = {needs}
[wacc]
cost_equity = {{ value=0.1, unit="ratio", source_id="a1" }}
cost_debt = {{ value=0.05, unit="ratio", source_id="a2" }}
tax_rate = {{ value=0.25, unit="ratio", source_id="a3" }}
debt_weight = {{ value=0.2, unit="ratio", source_id="a4" }}
[capitalization]
market_cap = {{ value=1000, unit="USDm", source_id="e1" }}
debt = {{ value=200, unit="USDm", source_id="e2" }}
cash = {{ value=50, unit="USDm", source_id="e3" }}
diluted_shares = {{ value=100, unit="shares_m", source_id="e4" }}
[scenario.bear]
{_forecast_line(70)}
terminal = {{terminal_growth={{value=0.02,unit="ratio",source_id="a6"}}}}
[scenario.base]
{_forecast_line(80)}
terminal = {{terminal_growth={{value=0.02,unit="ratio",source_id="a6"}}}}
[scenario.bull]
{_forecast_line(90)}
terminal = {{terminal_growth={{value=0.02,unit="ratio",source_id="a6"}}}}
""",
        encoding="utf-8",
    )


def _forecast_line(value: int) -> str:
    return (
        'forecast = [{ period_end="2026-12-31", '
        f'free_cash_flow={{ value={value}, unit="USDm", source_id="a5" }} }}]'
    )


def _comps_input_row(
    *,
    company_id: str = "peer-a",
    role: str = "peer",
    metric: str = "revenue",
    value: float = 20.0,
    unit: str = "USDm",
    period_basis: str = "LTM",
    period_end: date = date(2026, 6, 30),
    knowledge_date: date = date(2026, 6, 30),
    as_of: date = date(2026, 6, 30),
    source_id: str = "e1",
) -> dict[str, object]:
    return {
        "company_id": company_id,
        "company_name": company_id,
        "role": role,
        "metric": metric,
        "period_basis": period_basis,
        "period_end": period_end,
        "knowledge_date": knowledge_date,
        "as_of": as_of,
        "value": value,
        "unit": unit,
        "currency": "USD",
        "source_id": source_id,
    }


def _import_comps_rows(
    tmp_path: Path, case_id: str, rows: list[dict[str, object]]
) -> str:
    source = tmp_path / f"{case_id}-comps.parquet"
    pl.DataFrame(
        rows,
        schema=IMPORT_SCHEMAS["model.comps-observations.v1"].input_schema,
    ).write_parquet(source)
    receipt = import_parquet(
        tmp_path,
        case_id,
        source,
        schema_name="model.comps-observations.v1",
        provider="manual",
        retrieved_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    return receipt.normalized.artifact_id


def _complete_comps_rows(*, amount_unit: str = "USDm") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for company, role, market_cap, net_debt_value, revenue, price, eps in (
        ("target", "target", 100.0, 10.0, 20.0, 10.0, 2.0),
        ("peer-a", "peer", 120.0, 10.0, 20.0, 12.0, 2.0),
        ("peer-b", "peer", 150.0, 10.0, 30.0, 15.0, 3.0),
    ):
        for metric, value, unit in (
            ("market_cap", market_cap, amount_unit),
            ("net_debt", net_debt_value, amount_unit),
            ("revenue", revenue, amount_unit),
            ("share_price", price, "USD/share"),
            ("eps", eps, "USD/share"),
        ):
            rows.append(
                _comps_input_row(
                    company_id=company,
                    role=role,
                    metric=metric,
                    value=value,
                    unit=unit,
                )
            )
    return rows


def _legacy_comps_run_id(case_dir: Path, source_id: str) -> str:
    source = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.artifact_id == source_id
    )
    return canonical_parameters_sha256(
        {
            "input_artifact_id": source_id,
            "input_sha256": source.sha256,
            "as_of": "2026-06-30",
            "metrics": ["ev_revenue"],
            "target": "target",
            "producer": modeling_module.MODEL_PRODUCER,
            "producer_version": modeling_module.MODEL_PRODUCER_VERSION,
            "register_hashes": [
                (item.path, item.sha256)
                for item in modeling_module._register_hashes(case_dir)
            ],
        }
    )


def test_dated_dcf_uses_actual_365_and_mid_year() -> None:
    specification = DCFSpecification(
        as_of=date(2026, 1, 1),
        periods=(DCFPeriod(date(2027, 1, 1), 100.0),),
        wacc=0.1,
        net_debt_value=0,
        diluted_shares_value=10,
        terminal_growth=0.02,
    )
    result = dated_dcf_valuation(specification)
    assert result.periods[0].year_fraction == pytest.approx(1.0)
    assert result.enterprise_value == pytest.approx(
        result.periods[0].present_value + result.terminal_pv
    )
    assert annualized_return(beginning=100, ending=121, years=2) == pytest.approx(0.1)
    mid_year = dated_dcf_valuation(
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2027, 1, 1), 100.0),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=10,
            discount_convention="mid_year",
            terminal_growth=0.02,
        )
    )
    assert mid_year.periods[0].year_fraction == pytest.approx(0.5)
    exit_multiple = dated_dcf_valuation(
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2027, 1, 1), 100.0),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=10,
            terminal_method="exit_multiple",
            exit_multiple=10,
            terminal_metric=100,
        )
    )
    assert exit_multiple.terminal_value == 1000
    with pytest.raises(ModelError, match="strictly increasing"):
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2026, 1, 1), 1),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=1,
            terminal_growth=0.02,
        )


def test_case_dcf_reruns_and_projection_gate(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)

    first = run_dcf(tmp_path, "demo")
    second = run_dcf(tmp_path, "demo")
    assessment = projection_assessment(tmp_path, "demo")

    assert first == second
    assert len(first.receipts) == 4
    assert len(read_manifest(case_dir).artifacts) == 5
    assert assessment.receipts[0].artifact_id.startswith("model.projection-assessment")
    assert validate_artifact(tmp_path, "demo") == ()


def test_case_dcf_cli_requires_case_backed_input(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)

    result = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "model",
            "dcf",
            "demo",
            "--input",
            "analysis/dcf-inputs.toml",
            "--scenario",
            "base",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "run_id:" in result.output
    assert "model.dcf-results" in result.output


def test_comps_declared_observations_and_summary(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    source = tmp_path / "comps.parquet"
    rows = []
    for company, role, market_cap, revenue, share_price, eps in (
        ("target", "target", 100.0, 20.0, 10.0, 2.0),
        ("peer-a", "peer", 120.0, 20.0, 12.0, 2.0),
        ("peer-b", "peer", 150.0, 30.0, 15.0, 3.0),
    ):
        for metric, value in (
            ("market_cap", market_cap),
            ("net_debt", 10.0),
            ("revenue", revenue),
            ("share_price", share_price),
            ("eps", eps),
        ):
            rows.append(
                {
                    "company_id": company,
                    "company_name": company,
                    "role": role,
                    "metric": metric,
                    "period_basis": "LTM",
                    "period_end": date(2026, 6, 30),
                    "knowledge_date": date(2026, 6, 30),
                    "as_of": date(2026, 6, 30),
                    "value": value,
                    "unit": (
                        "USD/share" if metric in {"share_price", "eps"} else "USDm"
                    ),
                    "currency": "USD",
                    "source_id": "e1",
                }
            )
    pl.DataFrame(
        rows,
        schema=IMPORT_SCHEMAS["model.comps-observations.v1"].input_schema,
    ).write_parquet(source)
    imported = import_parquet(
        tmp_path,
        "demo",
        source,
        schema_name="model.comps-observations.v1",
        provider="manual",
        retrieved_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=imported.normalized.artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue", "pe"),
    )

    assert len(result.receipts) == 4
    assert result == run_comps(
        tmp_path,
        "demo",
        input_artifact_id=imported.normalized.artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue", "pe"),
    )
    assert validate_artifact(tmp_path, "demo") == ()


def test_pure_model_metrics_and_invalid_domains() -> None:
    assert annualized_return(beginning=100, ending=121, years=2) == pytest.approx(0.1)
    assert growth(current=120, prior=100) == pytest.approx(0.2)
    assert margin(numerator=25, revenue=100) == pytest.approx(0.25)
    assert diluted_shares(
        basic=100, options_incremental=5, restricted_stock=2, convertible_incremental=3
    ) == pytest.approx(110)
    assert net_debt(debt=100, cash=25) == pytest.approx(75)
    assert enterprise_value(market_cap=100, net_debt_value=25) == pytest.approx(125)
    assert wacc_from_components(
        cost_equity=0.1, cost_debt=0.05, tax_rate=0.2, debt_weight=0.4
    ) == pytest.approx(0.076)
    with pytest.raises(ModelError):
        annualized_return(beginning=0, ending=1, years=1)
    with pytest.raises(ModelError):
        growth(current=1, prior=0)
    with pytest.raises(ModelError):
        margin(numerator=1, revenue=0)
    with pytest.raises(ModelError):
        diluted_shares(basic=1, options_incremental=-1)
    with pytest.raises(ModelError):
        enterprise_value(market_cap=0, net_debt_value=0)
    with pytest.raises(ModelError):
        wacc_from_components(cost_equity=nan, cost_debt=0, tax_rate=0, debt_weight=0)


def test_dcf_actual_365_leap_timing_and_terminal_equations() -> None:
    leap = DCFSpecification(
        as_of=date(2024, 2, 29),
        periods=(DCFPeriod(date(2025, 3, 1), 100.0),),
        wacc=0.1,
        net_debt_value=0,
        diluted_shares_value=10,
        terminal_growth=0.02,
    )
    leap_result = dated_dcf_valuation(leap)
    assert leap_result.periods[0].year_fraction == pytest.approx(366 / 365)
    assert leap_result.terminal_value == pytest.approx(100 * 1.02 / (0.1 - 0.02))
    exit_result = dated_dcf_valuation(
        replace(
            leap,
            terminal_method="exit_multiple",
            terminal_growth=None,
            terminal_metric=80,
            exit_multiple=12,
        )
    )
    assert exit_result.terminal_value == pytest.approx(960)
    assert exit_result.terminal_pv == pytest.approx(
        960 / exit_result.periods[-1].discount_factor
    )


def test_dcf_public_finite_and_sensitivity_domains() -> None:
    with pytest.raises(ModelError, match="finite"):
        annualized_return(beginning=1e-308, ending=1e308, years=0.5)
    with pytest.raises(ModelError, match="finite"):
        DCFInput(
            forecast_fcfs=(1,),
            discount_rate=0.1,
            terminal_growth=0.02,
            shares_outstanding=inf,
        )
    with pytest.raises(ModelError, match="positive"):
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2027, 1, 1), 1),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=1,
            terminal_method="exit_multiple",
            terminal_metric=1,
            exit_multiple=0,
        )
    legacy = DCFInput(
        forecast_fcfs=(100,),
        discount_rate=0.1,
        terminal_growth=0.02,
        shares_outstanding=10,
    )
    with pytest.raises(ModelError, match="empty"):
        dcf_sensitivity(legacy, discount_rates=(), terminal_growths=(0.02,))
    with pytest.raises(ModelError, match="finite"):
        dcf_sensitivity(legacy, discount_rates=(nan,), terminal_growths=(0.02,))
    with pytest.raises(ModelError, match="below every WACC"):
        dcf_sensitivity(legacy, discount_rates=(0.03,), terminal_growths=(0.03,))


@pytest.mark.parametrize("terminal_growth", (-1.0, -1.01))
def test_gordon_growth_requires_domain_above_negative_one(
    terminal_growth: float,
) -> None:
    with pytest.raises(ModelError, match="greater than -1"):
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2027, 1, 1), 100),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=10,
            terminal_growth=terminal_growth,
        )
    with pytest.raises(ModelError, match="positive final"):
        DCFSpecification(
            as_of=date(2026, 1, 1),
            periods=(DCFPeriod(date(2027, 1, 1), 0),),
            wacc=0.1,
            net_debt_value=0,
            diluted_shares_value=10,
            terminal_growth=0.02,
        )


def test_case_gordon_domain_and_extreme_wacc_publish_nothing(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    input_path = case_dir / "analysis" / "dcf-inputs.toml"
    original = input_path.read_text(encoding="utf-8")
    input_path.write_text(original.replace("value=0.02", "value=-1"), encoding="utf-8")
    with pytest.raises(Exception, match="greater than -1"):
        run_dcf(tmp_path, "demo")
    assert not read_manifest(case_dir).artifacts
    input_path.write_text(original.replace("value=70", "value=0"), encoding="utf-8")
    with pytest.raises(ModelError, match="positive final"):
        run_dcf(tmp_path, "demo", scenario="bear")
    assert not read_manifest(case_dir).artifacts
    input_path.write_text(
        original.replace("value=0.1", "value=1e308").replace(
            "2026-12-31", "2126-12-31"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ModelError, match="finite"):
        run_dcf(tmp_path, "demo", scenario="bear")
    assert not read_manifest(case_dir).artifacts


def test_model_execution_rejects_v1_manifest(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    write_manifest(case_dir, replace(read_manifest(case_dir), manifest_version=1))
    with pytest.raises(Exception, match="case migrate"):
        run_dcf(tmp_path, "demo")
    with pytest.raises(Exception, match="case migrate"):
        projection_assessment(tmp_path, "demo")
    with pytest.raises(Exception, match="case migrate"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id="not-needed",
            as_of=date(2026, 6, 30),
            metrics=("pe",),
        )


def test_dcf_scenarios_sensitivity_and_hash_identity(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    first = run_dcf(
        tmp_path,
        "demo",
        sensitivity=((0.08, 0.1), (0.01, 0.02)),
    )
    second = run_dcf(
        tmp_path,
        "demo",
        sensitivity=((0.1,), (0.02,)),
    )
    assert first.run_id != second.run_id
    manifest = read_manifest(case_dir)
    results = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-results"
    )
    results_frame = pl.read_parquet(case_dir / results.path)
    assert results_frame["scenario"].sort().to_list() == ["base", "bear", "bull"]
    assert results_frame["per_share_value"].n_unique() == 3
    sensitivity_artifact = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-sensitivity"
    )
    sensitivity_frame = pl.read_parquet(case_dir / sensitivity_artifact.path)
    assert sensitivity_frame.height == 12
    assert sensitivity_frame["per_share_unit"].unique().to_list() == ["USD/share"]


def test_dcf_invalid_input_or_overflow_publishes_nothing(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    input_path = case_dir / "analysis" / "dcf-inputs.toml"
    input_path.write_text(
        input_path.read_text(encoding="utf-8").replace("value=70", "value=1e308"),
        encoding="utf-8",
    )
    with pytest.raises((IngestionError, ModelError), match="finite"):
        run_dcf(tmp_path, "demo", scenario="bear")
    assert not read_manifest(case_dir).artifacts


def test_dcf_invalid_sensitivity_and_input_hash_tamper_are_detectable(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    with pytest.raises(IngestionError, match="grids must not be empty"):
        run_dcf(tmp_path, "demo", sensitivity=((), (0.02,)))
    assert not read_manifest(case_dir).artifacts
    run_dcf(tmp_path, "demo")
    input_path = case_dir / "analysis" / "dcf-inputs.toml"
    input_path.write_text(
        input_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
    )
    evidence_path = case_dir / "registers" / "evidence.csv"
    evidence_path.write_text(
        evidence_path.read_text(encoding="utf-8") + "# tampered\n",
        encoding="utf-8",
    )
    issues = validate_artifact(tmp_path, "demo")
    assert "input_file_checksum_mismatch" in {issue.code for issue in issues}
    messages = {issue.message for issue in issues}
    assert any("file.dcf-inputs" in message for message in messages)
    assert any("register.evidence" in message for message in messages)


def test_dcf_source_reference_and_register_cutoff_fail_before_publication(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    input_path = case_dir / "analysis" / "dcf-inputs.toml"
    original_input = input_path.read_text(encoding="utf-8")
    input_path.write_text(
        original_input.replace('source_id="a5"', 'source_id="missing"'),
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="dangling source"):
        run_dcf(tmp_path, "demo")
    assert not read_manifest(case_dir).artifacts
    input_path.write_text(original_input, encoding="utf-8")
    evidence_path = case_dir / "registers" / "evidence.csv"
    evidence_path.write_text(
        evidence_path.read_text(encoding="utf-8").replace("2026-01-01", "2026-12-31"),
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="dated after"):
        run_dcf(tmp_path, "demo")
    assert not read_manifest(case_dir).artifacts


def test_model_output_contract_rejects_nonfinite_float(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    result = run_dcf(tmp_path, "demo", scenario="base")
    frame = pl.read_parquet(case_dir / result.receipts[2].path).with_columns(
        pl.lit(float("nan"), dtype=pl.Float64).alias("wacc")
    )
    with pytest.raises(DataContractError, match="wacc must be finite"):
        MODEL_DCF_RESULTS_V1.validate(frame)


@pytest.mark.parametrize("failure_at", (1, 2, 3, 4))
def test_dcf_partial_publish_recovers_deterministically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_at: int
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    original = modeling_module._publish
    count = 0

    def interrupted(
        publish_case_dir: Path,
        manifest: Any,
        contract: Any,
        run_id: str,
        label: str,
        frame: pl.DataFrame,
        parents: tuple[str, ...],
        as_of: date,
        extra: tuple[Any, ...],
    ) -> Any:
        nonlocal count
        count += 1
        if count == failure_at:
            raise OSError("injected publication interruption")
        return original(
            publish_case_dir,
            manifest,
            contract,
            run_id,
            label,
            frame,
            parents,
            as_of,
            extra,
        )

    monkeypatch.setattr(modeling_module, "_publish", interrupted)
    with pytest.raises(OSError, match="injected"):
        run_dcf(tmp_path, "demo")
    monkeypatch.setattr(modeling_module, "_publish", original)
    recovered = run_dcf(tmp_path, "demo")
    assert len(recovered.receipts) == 4
    assert len(read_manifest(case_dir).artifacts) == 4
    assert recovered == run_dcf(tmp_path, "demo")
    assert validate_artifact(tmp_path, "demo") == ()


@pytest.mark.parametrize("failure_at", (1, 2, 3, 4, 5))
def test_dcf_sensitivity_partial_publish_recovers_all_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_at: int
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    original = modeling_module._publish
    count = 0

    def interrupted(
        publish_case_dir: Path,
        manifest: Any,
        contract: Any,
        run_id: str,
        label: str,
        frame: pl.DataFrame,
        parents: tuple[str, ...],
        as_of: date,
        extra: tuple[Any, ...],
    ) -> Any:
        nonlocal count
        count += 1
        if count == failure_at:
            raise OSError("injected publication interruption")
        return original(
            publish_case_dir,
            manifest,
            contract,
            run_id,
            label,
            frame,
            parents,
            as_of,
            extra,
        )

    sensitivity = ((0.08,), (0.02,))
    monkeypatch.setattr(modeling_module, "_publish", interrupted)
    with pytest.raises(OSError, match="injected"):
        run_dcf(tmp_path, "demo", sensitivity=sensitivity)
    monkeypatch.setattr(modeling_module, "_publish", original)
    recovered = run_dcf(tmp_path, "demo", sensitivity=sensitivity)
    assert len(recovered.receipts) == 5
    assert len(read_manifest(case_dir).artifacts) == 5
    assert recovered == run_dcf(tmp_path, "demo", sensitivity=sensitivity)
    assert validate_artifact(tmp_path, "demo") == ()


def test_dcf_rerun_rejects_tampered_published_output(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    first = run_dcf(tmp_path, "demo", scenario="base")
    first.receipts[2].path.write_bytes(b"tampered")
    with pytest.raises(ArtifactIntegrityError, match="byte conflict"):
        run_dcf(tmp_path, "demo", scenario="base")
    assert "checksum_mismatch" in {
        issue.code for issue in validate_artifact(tmp_path, "demo")
    }


def test_dcf_reconciliation_uses_published_per_share_unit(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    result = run_dcf(tmp_path, "demo", scenario="base")
    reconciliation = pl.read_parquet(case_dir / result.receipts[3].path)
    per_share = reconciliation.filter(pl.col("check") == "per_share")
    enterprise = reconciliation.filter(pl.col("check") == "enterprise_value")
    assert per_share["unit"].to_list() == ["USD/share"]
    assert enterprise["unit"].to_list() == ["USDm"]
    assert per_share["actual"].to_list() == per_share["expected"].to_list()
    assert reconciliation["status"].unique().to_list() == ["passed"]


def test_comps_amount_scales_normalize_before_multiples(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    for row in rows:
        if row["company_id"] == "peer-a" and row["metric"] == "market_cap":
            row.update(value=1000.0, unit="USDm")
        if row["company_id"] == "peer-a" and row["metric"] == "net_debt":
            row.update(value=0.0, unit="USDm")
        if row["company_id"] == "peer-a" and row["metric"] == "revenue":
            row.update(value=1.0, unit="USDb")
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    results = pl.read_parquet(case_dir / result.receipts[1].path)
    peer_value = results.filter(pl.col("company_id") == "peer-a")["value"][0]
    assert peer_value == pytest.approx(1.0)
    assert results["unit"].unique().to_list() == ["x"]


def test_pe_ignores_unrequested_ev_periods(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    for row in rows:
        if row["company_id"] == "peer-a" and row["metric"] in {
            "market_cap",
            "net_debt",
        }:
            row["period_end"] = date(2026, 3, 31)
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("pe",),
    )
    output = pl.read_parquet(case_dir / result.receipts[1].path)
    assert output["multiple"].unique().to_list() == ["pe"]
    assert validate_artifact(tmp_path, "demo") == ()


def test_pe_ignores_unrequested_extreme_ev_amount(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    next(
        row
        for row in rows
        if row["company_id"] == "peer-a" and row["metric"] == "market_cap"
    ).update(value=1e308, unit="USDb")
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("pe",),
    )
    assert pl.read_parquet(case_dir / result.receipts[1].path)[
        "multiple"
    ].unique().to_list() == ["pe"]
    assert validate_artifact(tmp_path, "demo") == ()


def test_comps_rejects_invalid_metric_units_at_import(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    with pytest.raises(IngestionError, match="CURRENCY/share"):
        _import_comps_rows(
            tmp_path,
            "demo",
            [_comps_input_row(metric="eps", unit="USDm")],
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )


def test_comps_requires_net_debt_and_each_requested_peer_metric(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = [
        row
        for row in _complete_comps_rows()
        if not (row["role"] == "peer" and row["metric"] in {"net_debt", "eps"})
    ]
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="no valid peer observations"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue", "pe"),
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )


def test_comps_exclusions_reconcile_and_keep_valid_peer_run(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    rows = [
        row
        for row in rows
        if not (row["company_id"] == "peer-b" and row["metric"] == "net_debt")
    ]
    peer_c_rows = []
    for row in rows:
        if row["company_id"] == "peer-a":
            copied = dict(row)
            copied.update(company_id="peer-c", company_name="peer-c")
            if copied["metric"] == "revenue":
                copied["value"] = -5.0
            peer_c_rows.append(copied)
    artifact_id = _import_comps_rows(tmp_path, "demo", rows + peer_c_rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    checks = pl.read_parquet(case_dir / result.receipts[3].path).filter(
        pl.col("status") == "excluded"
    )
    assert (
        checks.select((pl.col("actual") - pl.col("expected")) == pl.col("difference"))
        .to_series()
        .all()
    )
    assert checks["passed"].unique().to_list() == [False]
    assert checks["unit"].unique().to_list() == ["x"]
    assert set(checks["scenario"].to_list()) == {"peer-b", "peer-c"}
    assert validate_artifact(tmp_path, "demo") == ()


def test_comps_summary_uses_exact_linear_quantiles(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    artifact_id = _import_comps_rows(tmp_path, "demo", _complete_comps_rows())
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    summary = pl.read_parquet(case_dir / result.receipts[2].path)
    values = {row["statistic"]: row["value"] for row in summary.iter_rows(named=True)}
    assert values["p25"] == pytest.approx(5.625)
    assert values["median"] == pytest.approx(5.916666666666667)
    assert values["p75"] == pytest.approx(6.208333333333333)


@pytest.mark.parametrize(
    ("metric", "unit"),
    (
        ("market_cap", "USDm"),
        ("share_price", "USD/share"),
        ("diluted_shares", "shares_m"),
    ),
)
def test_comps_contract_rejects_nonpositive_market_price_and_shares(
    tmp_path: Path, metric: str, unit: str
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    with pytest.raises(IngestionError, match="must be positive"):
        _import_comps_rows(
            tmp_path,
            "demo",
            [_comps_input_row(metric=metric, unit=unit, value=0.0)],
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )


def test_comps_allows_negative_eps_and_revenue_as_exclusions(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    for row in rows:
        if row["company_id"] == "peer-b" and row["metric"] in {"eps", "revenue"}:
            row["value"] = -1.0
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue", "pe"),
    )
    checks = pl.read_parquet(case_dir / result.receipts[3].path)
    assert checks.filter(pl.col("status") == "excluded").height == 2
    assert validate_artifact(tmp_path, "demo") == ()


def test_comps_overflow_and_wrong_explicit_target_publish_nothing(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    next(
        row
        for row in rows
        if row["company_id"] == "peer-a" and row["metric"] == "revenue"
    ).update(value=1e308, unit="USDb")
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="normalization result must be finite"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )
    rows = _complete_comps_rows()
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="--target must equal"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
            target="peer-a",
        )


def test_comps_rejects_dangling_source_before_model_publication(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    rows[0]["source_id"] = "missing"
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="dangling source"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )


def test_comps_pit_uses_latest_history_and_keeps_import_history(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    rows.append(
        _comps_input_row(
            company_id="peer-a",
            metric="revenue",
            value=10.0,
            unit="USDm",
            period_basis="NTM",
            period_end=date(2026, 3, 31),
            knowledge_date=date(2026, 3, 31),
            source_id="e2",
        )
    )
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    result = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    inputs = pl.read_parquet(case_dir / result.receipts[0].path)
    selected = inputs.filter(
        (pl.col("company_id") == "peer-a") & (pl.col("metric") == "revenue")
    )
    assert selected.height == 1
    assert selected["value"][0] == pytest.approx(20.0)
    raw_history = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.artifact_id == artifact_id
    )
    assert (
        pl.read_parquet(case_dir / raw_history.path)
        .filter((pl.col("company_id") == "peer-a") & (pl.col("metric") == "revenue"))
        .height
        == 2
    )


def test_comps_pit_rejects_same_latest_date_conflict_before_publication(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    conflict = dict(rows[0])
    conflict.update(value=999.0, source_id="e2")
    rows.append(conflict)
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="conflicting rows at latest"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    assert not any(
        item.kind.startswith("model.") and item.kind != "model.comps-observations"
        for item in read_manifest(case_dir).artifacts
    )


def test_comps_requires_common_snapshot_period_and_target_role(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    rows = _complete_comps_rows()
    next(
        row
        for row in rows
        if row["company_id"] == "peer-a" and row["metric"] == "revenue"
    ).update(as_of=date(2026, 6, 29), knowledge_date=date(2026, 6, 29))
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="common snapshot"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    rows = _complete_comps_rows()
    next(
        row
        for row in rows
        if row["company_id"] == "peer-a" and row["metric"] == "revenue"
    ).update(period_end=date(2026, 3, 31))
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="common period_basis and period_end"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    rows = _complete_comps_rows()
    next(
        row
        for row in rows
        if row["company_id"] == "peer-a" and row["metric"] == "share_price"
    ).update(period_end=date(2026, 3, 31))
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="common period_basis and period_end"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("pe",),
        )
    rows = _complete_comps_rows()
    for row in rows:
        if row["company_id"] == "peer-a":
            row["role"] = "target"
    artifact_id = _import_comps_rows(tmp_path, "demo", rows)
    with pytest.raises(IngestionError, match="exactly one declared target"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )


@pytest.mark.parametrize("failure_at", (1, 2, 3, 4))
def test_comps_partial_publish_recovers_without_duplicate_model_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_at: int
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    artifact_id = _import_comps_rows(tmp_path, "demo", _complete_comps_rows())
    original = modeling_module._publish
    count = 0

    def interrupted(
        publish_case_dir: Path,
        manifest: Any,
        contract: Any,
        run_id: str,
        label: str,
        frame: pl.DataFrame,
        parents: tuple[str, ...],
        as_of: date,
        extra: tuple[Any, ...],
    ) -> Any:
        nonlocal count
        count += 1
        if count == failure_at:
            raise OSError("injected publication interruption")
        return original(
            publish_case_dir,
            manifest,
            contract,
            run_id,
            label,
            frame,
            parents,
            as_of,
            extra,
        )

    monkeypatch.setattr(modeling_module, "_publish", interrupted)
    with pytest.raises(OSError, match="injected"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    monkeypatch.setattr(modeling_module, "_publish", original)
    recovered = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert len(recovered.receipts) == 4
    assert (
        len(
            [
                item
                for item in read_manifest(case_dir).artifacts
                if item.kind.startswith("model.")
                and item.kind != "model.comps-observations"
            ]
        )
        == 4
    )
    assert recovered == run_comps(
        tmp_path,
        "demo",
        input_artifact_id=artifact_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert validate_artifact(tmp_path, "demo") == ()


def test_legacy_comps_v1_partial_is_valid_and_v2_rerun_uses_distinct_identity(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    source_id = _import_comps_rows(tmp_path, "demo", _complete_comps_rows())
    manifest = read_manifest(case_dir)
    source = next(item for item in manifest.artifacts if item.artifact_id == source_id)
    legacy_id = _legacy_comps_run_id(case_dir, source_id)
    legacy = modeling_module._publish(
        case_dir,
        manifest,
        MODEL_COMPS_INPUTS_V1,
        legacy_id,
        "inputs",
        pl.read_parquet(case_dir / source.path),
        (source_id, *source.input_artifact_ids),
        date(2026, 6, 30),
        modeling_module._register_hashes(case_dir),
    )
    assert legacy.artifact_id.endswith(legacy_id)
    assert validate_artifact(tmp_path, "demo") == ()
    assert audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    ).valid

    recovered = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert recovered.run_id != legacy_id
    assert recovered == run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert validate_artifact(tmp_path, "demo") == ()


def test_legacy_comps_v1_full_run_is_rejected_as_unverifiable_but_v2_reruns(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    source_id = _import_comps_rows(tmp_path, "demo", _complete_comps_rows())
    manifest = read_manifest(case_dir)
    source = next(item for item in manifest.artifacts if item.artifact_id == source_id)
    legacy_id = _legacy_comps_run_id(case_dir, source_id)
    extra = modeling_module._register_hashes(case_dir)
    selected = modeling_module._select_comps_pit(
        pl.read_parquet(case_dir / source.path), date(2026, 6, 30)
    )
    required_components = modeling_module._required_comps_components(("ev_revenue",))
    basis = modeling_module._validate_comps_periods(selected, required_components)
    rows, checks = modeling_module._comps_rows(
        selected,
        legacy_id,
        ("ev_revenue",),
        "target",
        "USD",
        basis,
        required_components,
    )
    legacy_input = modeling_module._publish(
        case_dir,
        manifest,
        MODEL_COMPS_INPUTS_V1,
        legacy_id,
        "inputs",
        pl.read_parquet(case_dir / source.path),
        (source_id, *source.input_artifact_ids),
        date(2026, 6, 30),
        extra,
    )
    legacy_results = modeling_module._publish(
        case_dir,
        read_manifest(case_dir),
        MODEL_COMPS_RESULTS_V1,
        legacy_id,
        "results",
        pl.DataFrame(rows, schema=MODEL_COMPS_RESULTS_V1.schema),
        (legacy_input.artifact_id,),
        date(2026, 6, 30),
        extra,
    )
    summary = pl.DataFrame(
        modeling_module._comps_summary(rows, legacy_id, "USD", ("ev_revenue",)),
        schema=MODEL_COMPS_SUMMARY_V1.schema,
    )
    checks_frame = pl.DataFrame(checks, schema=MODEL_COMPS_RECONCILIATION_V1.schema)
    for contract, label, frame in (
        (MODEL_COMPS_SUMMARY_V1, "summary", summary),
        (MODEL_COMPS_RECONCILIATION_V1, "reconciliation", checks_frame),
    ):
        modeling_module._publish(
            case_dir,
            read_manifest(case_dir),
            contract,
            legacy_id,
            label,
            frame,
            (legacy_results.artifact_id,),
            date(2026, 6, 30),
            extra,
        )
    assert validate_artifact(tmp_path, "demo") == ()
    audit = audit_case(tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1)
    assert any("model.comps-inputs.v1" in issue.message for issue in audit.issues)
    with pytest.raises(ReportError, match="model.comps-inputs.v1"):
        load_report_context(tmp_path, "demo", legacy_id)
    current = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert current.run_id != legacy_id
    assert current == run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert generate_report(
        tmp_path, "demo", model_run_id=current.run_id, format="markdown"
    ).path.is_file()
    assert validate_artifact(tmp_path, "demo") == ()


def test_comps_v2_partial_run_audit_is_stable_and_rerun_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    source_id = _import_comps_rows(tmp_path, "demo", _complete_comps_rows())
    original = modeling_module._publish
    calls = 0

    def interrupted(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected publication interruption")
        return original(*args, **kwargs)

    monkeypatch.setattr(modeling_module, "_publish", interrupted)
    with pytest.raises(OSError, match="injected"):
        run_comps(
            tmp_path,
            "demo",
            input_artifact_id=source_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    monkeypatch.setattr(modeling_module, "_publish", original)
    audit = audit_case(tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1)
    assert audit.valid
    recovered = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    assert len(recovered.receipts) == 4
    assert audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    ).valid


def test_model_cli_help_distinguishes_dcf_path_and_comps_artifact() -> None:
    dcf_help = runner.invoke(app, ["--workspace", "/tmp", "model", "dcf", "--help"])
    comps_help = runner.invoke(app, ["--workspace", "/tmp", "model", "comps", "--help"])
    assert dcf_help.exit_code == 0
    assert comps_help.exit_code == 0
    assert "path to strict dcf-inputs.toml" in dcf_help.output
    assert "Artifact id for model.comps-observations.v1" in comps_help.output
