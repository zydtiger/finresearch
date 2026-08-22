"""Auditable case-backed DCF, comparable-company, and projection-gate workflows."""

from __future__ import annotations

import hashlib
import json
import math
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    CaseContractError,
    InputFileHash,
    canonical_parameters_sha256,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    CURRENCY_CODES,
    MODEL_COMPS_INPUTS_V1,
    MODEL_COMPS_OBSERVATIONS_V1,
    MODEL_COMPS_RECONCILIATION_V1,
    MODEL_COMPS_RESULTS_V1,
    MODEL_COMPS_SUMMARY_V1,
    MODEL_DCF_CASHFLOWS_V1,
    MODEL_DCF_INPUTS_V1,
    MODEL_DCF_RECONCILIATION_V1,
    MODEL_DCF_RESULTS_V1,
    MODEL_DCF_SENSITIVITY_V1,
    MODEL_PROJECTION_ASSESSMENT_V1,
    DataContractError,
    DatasetContract,
)
from finresearch.ingestion import IngestionError, IngestionReceipt, publish_snapshot
from finresearch.model import (
    DCFPeriod,
    DCFSpecification,
    ModelError,
    dated_dcf_valuation,
    enterprise_value,
    net_debt,
    wacc_from_components,
)
from finresearch.registers import ModelSource, load_model_sources

MODEL_PRODUCER = "finresearch.model"
MODEL_PRODUCER_VERSION = "1"
_SCENARIOS = ("bear", "base", "bull")
_PROJECTION_NEEDS = {
    "working-capital",
    "capex-depreciation",
    "tax",
    "cash-debt-interest",
    "dilution",
    "liquidity-covenant",
    "balance-sheet-reconciliation",
}


@dataclass(frozen=True)
class ValueInput:
    value: float
    unit: str
    source_id: str


@dataclass(frozen=True)
class DCFScenario:
    name: str
    forecasts: tuple[tuple[date, ValueInput], ...]
    terminal: ValueInput
    terminal_metric: ValueInput | None = None


@dataclass(frozen=True)
class DCFCaseInput:
    as_of: date
    currency: str
    value_unit: str
    share_unit: str
    discount_convention: str
    terminal_method: str
    wacc: dict[str, ValueInput]
    capitalization: dict[str, ValueInput]
    scenarios: dict[str, DCFScenario]
    projection_needs: tuple[str, ...]


@dataclass(frozen=True)
class ModelRun:
    run_id: str
    receipts: tuple[IngestionReceipt, ...]


def load_dcf_input(
    case_dir: Path, relative_input: str
) -> tuple[DCFCaseInput, bytes, Path]:
    """Load and validate one strict, case-relative dcf-inputs.toml v1 file."""
    if Path(relative_input).is_absolute():
        raise CaseContractError("DCF input must be a case-relative path")
    path = resolve_relative_path(case_dir, relative_input, "DCF input")
    if not path.is_file():
        raise CaseContractError(f"DCF input file is missing: {relative_input}")
    raw = path.read_bytes()
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise CaseContractError("DCF input must be valid UTF-8 TOML") from exc
    expected = {
        "version",
        "as_of",
        "currency",
        "value_unit",
        "share_unit",
        "discount_convention",
        "terminal_method",
        "wacc",
        "capitalization",
        "scenario",
        "projection_needs",
    }
    if set(data) != expected or data.get("version") != 1:
        raise CaseContractError(
            "DCF input must use exactly the dcf-inputs.toml v1 keys"
        )
    as_of = _parse_date(data["as_of"], "as_of")
    currency = _controlled_text(data["currency"], "currency")
    if currency not in CURRENCY_CODES:
        raise CaseContractError("currency must be a controlled ISO currency code")
    value_unit = _controlled_text(data["value_unit"], "value_unit")
    share_unit = _controlled_text(data["share_unit"], "share_unit")
    if value_unit not in {currency, f"{currency}k", f"{currency}m", f"{currency}b"}:
        raise CaseContractError("value_unit must be a controlled currency amount scale")
    if share_unit not in {"shares", "shares_k", "shares_m", "shares_b"}:
        raise CaseContractError("share_unit must be a controlled share scale")
    convention = data["discount_convention"]
    method = data["terminal_method"]
    if convention not in {"year_end", "mid_year"}:
        raise CaseContractError("discount_convention must be year_end or mid_year")
    if method not in {"gordon_growth", "exit_multiple"}:
        raise CaseContractError(
            "terminal_method must be gordon_growth or exit_multiple"
        )
    wacc_raw = _mapping(data["wacc"], "wacc")
    if set(wacc_raw) != {"cost_equity", "cost_debt", "tax_rate", "debt_weight"}:
        raise CaseContractError("wacc must contain all and only WACC components")
    wacc = {name: _value(value, name, "ratio") for name, value in wacc_raw.items()}
    capitalization_raw = _mapping(data["capitalization"], "capitalization")
    if set(capitalization_raw) != {"market_cap", "debt", "cash", "diluted_shares"}:
        raise CaseContractError(
            "capitalization must contain market_cap, debt, cash, diluted_shares"
        )
    capitalization = {
        "market_cap": _value(
            capitalization_raw["market_cap"], "market_cap", value_unit
        ),
        "debt": _value(capitalization_raw["debt"], "debt", value_unit),
        "cash": _value(capitalization_raw["cash"], "cash", value_unit),
        "diluted_shares": _value(
            capitalization_raw["diluted_shares"], "diluted_shares", share_unit
        ),
    }
    scenarios_raw = _mapping(data["scenario"], "scenario")
    if set(scenarios_raw) != set(_SCENARIOS):
        raise CaseContractError(
            "scenario must contain distinct bear, base, and bull tables"
        )
    scenarios: dict[str, DCFScenario] = {}
    for scenario in _SCENARIOS:
        scenario_raw = _mapping(scenarios_raw[scenario], f"scenario.{scenario}")
        if set(scenario_raw) != {"forecast", "terminal"}:
            raise CaseContractError(
                f"scenario.{scenario} must contain forecast and terminal"
            )
        forecast_raw = scenario_raw["forecast"]
        if not isinstance(forecast_raw, list) or not forecast_raw:
            raise CaseContractError(
                f"scenario.{scenario}.forecast must be a non-empty array"
            )
        forecasts: list[tuple[date, ValueInput]] = []
        previous = as_of
        for index, row in enumerate(forecast_raw):
            record = _mapping(row, f"scenario.{scenario}.forecast[{index}]")
            if set(record) != {"period_end", "free_cash_flow"}:
                raise CaseContractError(
                    "DCF forecast rows require period_end and free_cash_flow"
                )
            period_end = _parse_date(record["period_end"], "period_end")
            if period_end <= previous:
                raise CaseContractError(
                    "DCF forecast period_end rows must strictly increase after as_of"
                )
            previous = period_end
            forecasts.append(
                (
                    period_end,
                    _value(record["free_cash_flow"], "free_cash_flow", value_unit),
                )
            )
        terminal_raw = _mapping(
            scenario_raw["terminal"], f"scenario.{scenario}.terminal"
        )
        if method == "gordon_growth":
            if set(terminal_raw) != {"terminal_growth"}:
                raise CaseContractError("Gordon terminal must contain terminal_growth")
            terminal = _value(
                terminal_raw["terminal_growth"], "terminal_growth", "ratio"
            )
            if terminal.value <= -1:
                raise CaseContractError(
                    "terminal_growth must be greater than -1 for Gordon growth"
                )
            terminal_metric = None
        else:
            if set(terminal_raw) != {"terminal_metric", "exit_multiple"}:
                raise CaseContractError(
                    "exit terminal must contain terminal_metric and exit_multiple"
                )
            terminal = _value(
                terminal_raw["exit_multiple"], "exit_multiple", "multiple"
            )
            terminal_metric = _value(
                terminal_raw["terminal_metric"], "terminal_metric", value_unit
            )
        scenarios[scenario] = DCFScenario(
            scenario,
            tuple(forecasts),
            terminal,
            terminal_metric,
        )
    fingerprints = {
        json.dumps(_scenario_fingerprint(value), sort_keys=True)
        for value in scenarios.values()
    }
    if len(fingerprints) != len(_SCENARIOS):
        raise CaseContractError("bear, base, and bull DCF scenarios must be distinct")
    needs = data["projection_needs"]
    if not isinstance(needs, list) or any(
        item not in _PROJECTION_NEEDS for item in needs
    ):
        raise CaseContractError(
            "projection_needs contains an unsupported controlled reason"
        )
    return (
        DCFCaseInput(
            as_of,
            currency,
            value_unit,
            share_unit,
            cast(str, convention),
            cast(str, method),
            wacc,
            capitalization,
            scenarios,
            tuple(sorted(set(needs))),
        ),
        raw,
        path,
    )


def run_dcf(
    workspace: Path,
    case_id: str,
    *,
    input_path: str = "analysis/dcf-inputs.toml",
    scenario: Literal["bear", "base", "bull", "all"] = "all",
    sensitivity: tuple[tuple[float, ...], tuple[float, ...]] | None = None,
) -> ModelRun:
    """Execute deterministic DCF artifacts from a strict case input and registers."""
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "auditable models require manifest v2; run case migrate first"
        )
    config, input_bytes, actual_input = load_dcf_input(case_dir, input_path)
    sources = load_model_sources(case_dir, as_of=config.as_of)
    _validate_dcf_sources(config, sources)
    selected = _SCENARIOS if scenario == "all" else (scenario,)
    if scenario not in {*_SCENARIOS, "all"}:
        raise IngestionError("scenario must be bear, base, bull, or all")
    if sensitivity is not None:
        if config.terminal_method != "gordon_growth":
            raise IngestionError("WACC/growth sensitivity requires gordon_growth")
        if not sensitivity[0] or not sensitivity[1]:
            raise IngestionError("DCF sensitivity grids must not be empty")
        if any(
            not isinstance(value, (int, float)) or not math.isfinite(value)
            for value in (*sensitivity[0], *sensitivity[1])
        ):
            raise IngestionError("DCF sensitivity values must be finite")
        if any(rate <= 0 for rate in sensitivity[0]) or any(
            growth_rate <= -1 or growth_rate >= rate
            for rate in sensitivity[0]
            for growth_rate in sensitivity[1]
        ):
            raise IngestionError(
                "each sensitivity terminal growth must be greater than -1 and "
                "below WACC"
            )
    extra_hashes = _model_input_hashes(case_dir, actual_input, input_bytes)
    run_id = _run_id(input_bytes, config.as_of, selected, sensitivity, extra_hashes)
    wacc = wacc_from_components(
        **{name: item.value for name, item in config.wacc.items()}
    )
    nd = net_debt(
        debt=config.capitalization["debt"].value,
        cash=config.capitalization["cash"].value,
    )
    enterprise_value(
        market_cap=config.capitalization["market_cap"].value, net_debt_value=nd
    )
    input_rows: list[dict[str, object]] = []
    cashflow_rows: list[dict[str, object]] = []
    result_rows: list[dict[str, object]] = []
    reconciliation_rows: list[dict[str, object]] = []
    sensitivity_rows: list[dict[str, object]] = []
    for name in selected:
        definition = config.scenarios[name]
        for field, item in {**config.wacc, **config.capitalization}.items():
            source = sources[item.source_id]
            input_rows.append(
                _input_row(run_id, name, field, item, config.currency, source)
            )
        for period_end, fcf in definition.forecasts:
            source = sources[fcf.source_id]
            input_rows.append(
                _input_row(
                    run_id,
                    name,
                    f"free_cash_flow:{period_end}",
                    fcf,
                    config.currency,
                    source,
                    period_end,
                )
            )
        terminal_source = sources[definition.terminal.source_id]
        input_rows.append(
            _input_row(
                run_id,
                name,
                config.terminal_method,
                definition.terminal,
                config.currency,
                terminal_source,
            )
        )
        if definition.terminal_metric is not None:
            metric_source = sources[definition.terminal_metric.source_id]
            input_rows.append(
                _input_row(
                    run_id,
                    name,
                    "terminal_metric",
                    definition.terminal_metric,
                    config.currency,
                    metric_source,
                )
            )
        specification = DCFSpecification(
            as_of=config.as_of,
            periods=tuple(
                DCFPeriod(day, input.value) for day, input in definition.forecasts
            ),
            wacc=wacc,
            net_debt_value=nd,
            diluted_shares_value=config.capitalization["diluted_shares"].value,
            discount_convention=config.discount_convention,
            terminal_method=config.terminal_method,
            terminal_growth=(
                definition.terminal.value
                if config.terminal_method == "gordon_growth"
                else None
            ),
            exit_multiple=(
                definition.terminal.value
                if config.terminal_method == "exit_multiple"
                else None
            ),
            terminal_metric=(
                definition.terminal_metric.value
                if definition.terminal_metric is not None
                else None
            ),
        )
        detail = dated_dcf_valuation(specification)
        if not math.isfinite(detail.enterprise_value) or detail.enterprise_value == 0:
            raise IngestionError("DCF enterprise value must be finite and non-zero")
        for period in detail.periods:
            cashflow_rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": name,
                    "period_end": period.period_end,
                    "year_fraction": period.year_fraction,
                    "free_cash_flow": period.free_cash_flow,
                    "discount_factor": period.discount_factor,
                    "present_value": period.present_value,
                    "currency": config.currency,
                    "unit": config.value_unit,
                }
            )
        published_per_share = (
            detail.per_share_value
            * _unit_scale(config.value_unit, config.currency)
            / _share_scale(config.share_unit)
        )
        result_rows.append(
            {
                "schema_version": 1,
                "run_id": run_id,
                "scenario": name,
                "terminal_value": detail.terminal_value,
                "terminal_pv": detail.terminal_pv,
                "enterprise_value": detail.enterprise_value,
                "equity_value": detail.equity_value,
                "per_share_value": published_per_share,
                "currency": config.currency,
                "value_unit": config.value_unit,
                "share_unit": config.share_unit,
                "per_share_unit": f"{config.currency}/share",
                "wacc": wacc,
                "discount_convention": config.discount_convention,
                "terminal_method": config.terminal_method,
                "terminal_ev_share": detail.terminal_pv / detail.enterprise_value,
            }
        )
        reconciliation_rows.extend(
            _dcf_reconciliations(
                run_id,
                name,
                detail,
                config.capitalization["diluted_shares"].value,
                config.value_unit,
                config.currency,
                config.share_unit,
            )
        )
        if sensitivity is not None:
            for rate in sensitivity[0]:
                for terminal_growth in sensitivity[1]:
                    altered = DCFSpecification(
                        **{
                            **specification.__dict__,
                            "wacc": rate,
                            "terminal_growth": terminal_growth,
                        }
                    )
                    sensitivity_rows.append(
                        {
                            "schema_version": 1,
                            "run_id": run_id,
                            "scenario": name,
                            "wacc": rate,
                            "terminal_growth": terminal_growth,
                            "per_share_value": dated_dcf_valuation(
                                altered
                            ).per_share_value
                            * _unit_scale(config.value_unit, config.currency)
                            / _share_scale(config.share_unit),
                            "currency": config.currency,
                            "share_unit": config.share_unit,
                            "per_share_unit": f"{config.currency}/share",
                        }
                    )
    input_frame = pl.DataFrame(input_rows, schema=MODEL_DCF_INPUTS_V1.schema)
    cash_frame = pl.DataFrame(cashflow_rows, schema=MODEL_DCF_CASHFLOWS_V1.schema)
    results_frame = pl.DataFrame(result_rows, schema=MODEL_DCF_RESULTS_V1.schema)
    reconciliation_frame = pl.DataFrame(
        reconciliation_rows, schema=MODEL_DCF_RECONCILIATION_V1.schema
    )
    sensitivity_frame = (
        pl.DataFrame(sensitivity_rows, schema=MODEL_DCF_SENSITIVITY_V1.schema)
        if sensitivity_rows
        else None
    )
    for contract, frame in (
        (MODEL_DCF_INPUTS_V1, input_frame),
        (MODEL_DCF_CASHFLOWS_V1, cash_frame),
        (MODEL_DCF_RESULTS_V1, results_frame),
        (MODEL_DCF_RECONCILIATION_V1, reconciliation_frame),
    ):
        contract.validate(frame)
    if sensitivity_frame is not None:
        MODEL_DCF_SENSITIVITY_V1.validate(sensitivity_frame)
    input_receipt = _publish(
        case_dir,
        manifest,
        MODEL_DCF_INPUTS_V1,
        run_id,
        "inputs",
        input_frame,
        (),
        config.as_of,
        extra_hashes,
    )
    cash_receipt = _publish(
        case_dir,
        manifest,
        MODEL_DCF_CASHFLOWS_V1,
        run_id,
        "cashflows",
        cash_frame,
        (input_receipt.artifact_id,),
        config.as_of,
        extra_hashes,
    )
    results_receipt = _publish(
        case_dir,
        manifest,
        MODEL_DCF_RESULTS_V1,
        run_id,
        "results",
        results_frame,
        (cash_receipt.artifact_id,),
        config.as_of,
        extra_hashes,
    )
    reconciliation_receipt = _publish(
        case_dir,
        manifest,
        MODEL_DCF_RECONCILIATION_V1,
        run_id,
        "reconciliation",
        reconciliation_frame,
        (results_receipt.artifact_id,),
        config.as_of,
        extra_hashes,
    )
    receipts = [input_receipt, cash_receipt, results_receipt, reconciliation_receipt]
    if sensitivity_frame is not None:
        receipts.append(
            _publish(
                case_dir,
                manifest,
                MODEL_DCF_SENSITIVITY_V1,
                run_id,
                "sensitivity",
                sensitivity_frame,
                (results_receipt.artifact_id,),
                config.as_of,
                extra_hashes,
            )
        )
    return ModelRun(run_id, tuple(receipts))


def projection_assessment(
    workspace: Path, case_id: str, *, input_path: str = "analysis/dcf-inputs.toml"
) -> ModelRun:
    """Persist the explicit projection gate; it never invents linked statements."""
    case_dir = case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "auditable models require manifest v2; run case migrate first"
        )
    config, input_bytes, actual_input = load_dcf_input(case_dir, input_path)
    sources = load_model_sources(case_dir, as_of=config.as_of)
    _validate_dcf_sources(config, sources)
    extra_hashes = _model_input_hashes(case_dir, actual_input, input_bytes)
    run_id = _run_id(
        input_bytes, config.as_of, ("projection-assessment",), None, extra_hashes
    )
    reasons = config.projection_needs or ("direct-free-cash-flow-traceable",)
    status = "required" if config.projection_needs else "not_required"
    frame = pl.DataFrame(
        [
            {
                "schema_version": 1,
                "run_id": run_id,
                "status": status,
                "reason": reason,
                "as_of": config.as_of,
            }
            for reason in reasons
        ],
        schema=MODEL_PROJECTION_ASSESSMENT_V1.schema,
    )
    MODEL_PROJECTION_ASSESSMENT_V1.validate(frame)
    receipt = _publish(
        case_dir,
        manifest,
        MODEL_PROJECTION_ASSESSMENT_V1,
        run_id,
        "projection-assessment",
        frame,
        (),
        config.as_of,
        extra_hashes,
    )
    return ModelRun(run_id, (receipt,))


def run_comps(
    workspace: Path,
    case_id: str,
    *,
    input_artifact_id: str,
    as_of: date,
    metrics: tuple[str, ...],
    target: str | None = None,
) -> ModelRun:
    """Compute declared peer multiples and linear-interpolated summary statistics."""
    allowed = {"ev_revenue", "ev_ebitda", "ev_ebit", "pe"}
    if not metrics or any(metric not in allowed for metric in metrics):
        raise IngestionError(
            "metrics must be selected from ev_revenue, ev_ebitda, ev_ebit, pe"
        )
    metrics = tuple(sorted(set(metrics)))
    case_dir = case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "auditable models require manifest v2; run case migrate first"
        )
    artifact = next(
        (item for item in manifest.artifacts if item.artifact_id == input_artifact_id),
        None,
    )
    if artifact is None or artifact.kind != MODEL_COMPS_OBSERVATIONS_V1.name:
        raise IngestionError("input artifact must be model.comps-observations.v1")
    source_path = resolve_relative_path(case_dir, artifact.path, "comps input artifact")
    frame = pl.read_parquet(source_path)
    try:
        MODEL_COMPS_OBSERVATIONS_V1.validate(frame)
    except DataContractError as exc:
        raise IngestionError(f"comps input contract failure: {exc}") from exc
    if frame.filter(pl.col("as_of") != as_of).height:
        raise IngestionError(
            "comps observations must all use the CLI --as-of common snapshot"
        )
    sources = load_model_sources(case_dir, as_of=as_of)
    missing = sorted(set(frame["source_id"].to_list()) - set(sources))
    if missing:
        raise IngestionError(f"comps observations have dangling source ids: {missing}")
    future_sources = sorted(
        source_id
        for source_id in set(frame["source_id"].to_list())
        if sources[source_id].effective_date > as_of
    )
    if future_sources:
        raise IngestionError(
            f"comps sources are dated after model as_of: {future_sources}"
        )
    company_roles = {
        cast(str, key[0]): set(group["role"].to_list())
        for key, group in frame.group_by("company_id", maintain_order=True)
    }
    if any(len(roles) != 1 for roles in company_roles.values()):
        raise IngestionError("each comps company must have one consistent role")
    targets = [
        company for company, roles in company_roles.items() if roles == {"target"}
    ]
    if len(targets) != 1:
        raise IngestionError("comps input requires exactly one declared target")
    selected_frame = _select_comps_pit(frame, as_of)
    currencies = selected_frame["currency"].unique().to_list()
    if len(set(currencies)) != 1:
        raise IngestionError("comps observations must use one non-null currency")
    currency = cast(str, currencies[0])
    required_components = _required_comps_components(metrics)
    basis_by_metric = _validate_comps_periods(selected_frame, required_components)
    selected_target = target or _single_target(frame)
    if selected_target != targets[0]:
        raise IngestionError("--target must equal the declared role=target company")
    bytes_hash = _sha256(source_path)
    extra = _register_hashes(case_dir)
    run_id = canonical_parameters_sha256(
        {
            "input_artifact_id": input_artifact_id,
            "input_sha256": bytes_hash,
            "as_of": as_of.isoformat(),
            "metrics": sorted(set(metrics)),
            "target": selected_target,
            "producer": MODEL_PRODUCER,
            "producer_version": MODEL_PRODUCER_VERSION,
            "register_hashes": [(item.path, item.sha256) for item in extra],
        }
    )
    input_frame = selected_frame.with_columns(
        pl.lit(1, dtype=pl.UInt16).alias("schema_version")
    )
    rows, checks = _comps_rows(
        selected_frame,
        run_id,
        metrics,
        selected_target,
        currency,
        basis_by_metric,
        required_components,
    )
    peer_metrics = {
        row["multiple"]
        for row in rows
        if row["role"] == "peer" and row["multiple"] in metrics
    }
    missing_peer_metrics = sorted(set(metrics) - peer_metrics)
    if missing_peer_metrics:
        raise IngestionError(
            "requested comps multiples have no valid peer observations: "
            f"{missing_peer_metrics}"
        )
    results = pl.DataFrame(rows, schema=MODEL_COMPS_RESULTS_V1.schema)
    summary = pl.DataFrame(
        _comps_summary(rows, run_id, currency, metrics),
        schema=MODEL_COMPS_SUMMARY_V1.schema,
    )
    checks_frame = pl.DataFrame(checks, schema=MODEL_COMPS_RECONCILIATION_V1.schema)
    for contract, output in (
        (MODEL_COMPS_INPUTS_V1, input_frame),
        (MODEL_COMPS_RESULTS_V1, results),
        (MODEL_COMPS_SUMMARY_V1, summary),
        (MODEL_COMPS_RECONCILIATION_V1, checks_frame),
    ):
        contract.validate(output)
    input_receipt = _publish(
        case_dir,
        manifest,
        MODEL_COMPS_INPUTS_V1,
        run_id,
        "inputs",
        input_frame,
        (input_artifact_id, *artifact.input_artifact_ids),
        as_of,
        extra,
    )
    results_receipt = _publish(
        case_dir,
        manifest,
        MODEL_COMPS_RESULTS_V1,
        run_id,
        "results",
        results,
        (input_receipt.artifact_id,),
        as_of,
        extra,
    )
    summary_receipt = _publish(
        case_dir,
        manifest,
        MODEL_COMPS_SUMMARY_V1,
        run_id,
        "summary",
        summary,
        (results_receipt.artifact_id,),
        as_of,
        extra,
    )
    checks_receipt = _publish(
        case_dir,
        manifest,
        MODEL_COMPS_RECONCILIATION_V1,
        run_id,
        "reconciliation",
        checks_frame,
        (results_receipt.artifact_id,),
        as_of,
        extra,
    )
    return ModelRun(
        run_id, (input_receipt, results_receipt, summary_receipt, checks_receipt)
    )


def _publish(
    case_dir: Path,
    manifest: Any,
    contract: DatasetContract,
    run_id: str,
    label: str,
    frame: pl.DataFrame,
    parents: tuple[str, ...],
    as_of: date,
    extra: tuple[InputFileHash, ...],
) -> IngestionReceipt:
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="derived",
        frame=contract.canonical_sort(frame),
        contract=contract,
        path_parts=("models", contract.name, run_id),
        entity_key=label,
        identity=run_id,
        producer=MODEL_PRODUCER,
        producer_version=MODEL_PRODUCER_VERSION,
        parameters_sha256=run_id,
        input_artifact_ids=parents,
        produced_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
        extra_input_file_hashes=extra,
    )


def _validate_dcf_sources(
    config: DCFCaseInput, sources: dict[str, ModelSource]
) -> None:
    values = list(config.wacc.values()) + list(config.capitalization.values())
    for scenario in config.scenarios.values():
        values.extend(value for _, value in scenario.forecasts)
        values.append(scenario.terminal)
        if scenario.terminal_metric is not None:
            values.append(scenario.terminal_metric)
    missing = sorted({value.source_id for value in values} - set(sources))
    if missing:
        raise CaseContractError(f"DCF inputs have dangling source ids: {missing}")
    future = sorted(
        value.source_id
        for value in values
        if sources[value.source_id].effective_date > config.as_of
    )
    if future:
        raise CaseContractError(f"DCF sources are dated after model as_of: {future}")


def _model_input_hashes(
    case_dir: Path, input_path: Path, input_bytes: bytes
) -> tuple[InputFileHash, ...]:
    relative = input_path.resolve().relative_to(case_dir.resolve()).as_posix()
    return (
        InputFileHash(
            name="file.dcf-inputs",
            path=relative,
            sha256=hashlib.sha256(input_bytes).hexdigest(),
        ),
    ) + _register_hashes(case_dir)


def _register_hashes(case_dir: Path) -> tuple[InputFileHash, ...]:
    manifest = read_manifest(case_dir)
    root = resolve_relative_path(
        case_dir, manifest.paths["registers"], "paths.registers"
    )
    records: list[InputFileHash] = []
    for filename in ("evidence.csv", "assumptions.csv"):
        path = root / filename
        if path.is_file():
            records.append(
                InputFileHash(
                    name=f"register.{filename[:-4]}",
                    path=path.relative_to(case_dir).as_posix(),
                    sha256=_sha256(path),
                )
            )
    return tuple(records)


def _run_id(
    input_bytes: bytes,
    as_of: date,
    selected: tuple[str, ...],
    sensitivity: tuple[tuple[float, ...], tuple[float, ...]] | None,
    hashes: tuple[InputFileHash, ...],
) -> str:
    return canonical_parameters_sha256(
        {
            "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
            "as_of": as_of.isoformat(),
            "scenario": list(selected),
            "sensitivity": sensitivity,
            "producer": MODEL_PRODUCER,
            "producer_version": MODEL_PRODUCER_VERSION,
            "input_hashes": [(item.path, item.sha256) for item in hashes],
        }
    )


def _input_row(
    run_id: str,
    scenario: str,
    field: str,
    value: ValueInput,
    currency: str,
    source: ModelSource,
    period_end: date | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "scenario": scenario,
        "field": field,
        "period_end": period_end,
        "value": value.value,
        "unit": value.unit,
        "currency": currency,
        "source_id": value.source_id,
        "source_kind": source.kind,
    }


def _dcf_reconciliations(
    run_id: str,
    scenario: str,
    detail: Any,
    diluted_shares_value: float,
    value_unit: str,
    currency: str,
    share_unit: str,
) -> list[dict[str, object]]:
    forecast_sum = sum(period.present_value for period in detail.periods)
    enterprise_expected = forecast_sum + detail.terminal_pv
    enterprise_difference = detail.enterprise_value - enterprise_expected
    per_share_expected = (
        detail.equity_value
        / diluted_shares_value
        * _unit_scale(value_unit, currency)
        / _share_scale(share_unit)
    )
    per_share_actual = (
        detail.per_share_value
        * _unit_scale(value_unit, currency)
        / _share_scale(share_unit)
    )
    per_share_difference = per_share_actual - per_share_expected
    enterprise_passed = _within_reconciliation_tolerance(
        enterprise_difference, enterprise_expected
    )
    per_share_passed = _within_reconciliation_tolerance(
        per_share_difference, per_share_expected
    )
    return [
        {
            "schema_version": 1,
            "run_id": run_id,
            "scenario": scenario,
            "check": "enterprise_value",
            "actual": detail.enterprise_value,
            "expected": enterprise_expected,
            "difference": enterprise_difference,
            "passed": enterprise_passed,
            "status": "passed" if enterprise_passed else "failed",
            "unit": value_unit,
        },
        {
            "schema_version": 1,
            "run_id": run_id,
            "scenario": scenario,
            "check": "per_share",
            "actual": per_share_actual,
            "expected": per_share_expected,
            "difference": per_share_difference,
            "passed": per_share_passed,
            "status": "passed" if per_share_passed else "failed",
            "unit": f"{currency}/share",
        },
    ]


def _within_reconciliation_tolerance(difference: float, expected: float) -> bool:
    return abs(difference) <= 1e-9 * max(1.0, abs(expected))


def _select_comps_pit(frame: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """Select one latest, unambiguous observation per company and metric."""
    selected: list[dict[str, object]] = []
    for key, company_metric in frame.group_by(
        ["company_id", "metric"], maintain_order=False
    ):
        company_id, metric = cast(tuple[str, str], key)
        eligible = company_metric.filter(pl.col("knowledge_date") <= as_of)
        if eligible.is_empty():
            continue
        latest = cast(date, eligible["knowledge_date"].max())
        latest_rows = eligible.filter(pl.col("knowledge_date") == latest)
        if latest_rows.height != 1:
            raise IngestionError(
                "comps observations have conflicting rows at latest knowledge_date "
                f"for {company_id}/{metric}"
            )
        selected.append(latest_rows.to_dicts()[0])
    if not selected:
        raise IngestionError("comps observations have no eligible PIT rows")
    return pl.DataFrame(selected, schema=MODEL_COMPS_OBSERVATIONS_V1.schema)


def _validate_comps_periods(
    frame: pl.DataFrame, required_components: set[str]
) -> dict[str, str]:
    """Require no period or fiscal-basis conversion across selected companies."""
    periods: dict[str, str] = {}
    for metric in sorted(required_components):
        observed = frame.filter(pl.col("metric") == metric)
        if observed.is_empty():
            continue
        combinations = {
            (cast(str, row["period_basis"]), cast(date, row["period_end"]))
            for row in observed.iter_rows(named=True)
        }
        if len(combinations) != 1:
            raise IngestionError(
                "comps selected observations must use a common period_basis and "
                f"period_end for metric {metric}"
            )
        periods[metric] = next(iter(combinations))[0]
    return periods


def _required_comps_components(metrics: tuple[str, ...]) -> set[str]:
    """Return only the observations economically needed by requested multiples."""
    required: set[str] = set()
    for multiple in metrics:
        if multiple == "pe":
            required |= {"share_price", "eps"}
        else:
            required |= {"market_cap", "net_debt", multiple.removeprefix("ev_")}
    return required


def _amount_in_base_currency(row: dict[str, object], currency: str) -> float:
    """Normalize one controlled amount metric without inferring foreign exchange."""
    unit = cast(str, row["unit"])
    scale = _unit_scale(unit, currency)
    value = cast(float, row["value"]) * scale
    if not math.isfinite(value):
        raise IngestionError("comps amount normalization result must be finite")
    return value


def _comps_rows(
    frame: pl.DataFrame,
    run_id: str,
    metrics: tuple[str, ...],
    target: str,
    currency: str,
    basis_by_metric: dict[str, str],
    required_components: set[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    checks: list[dict[str, object]] = []
    amount_metrics = {"market_cap", "net_debt", "revenue", "ebitda", "ebit"}
    for company_key, company in frame.group_by("company_id", maintain_order=False):
        company_id = cast(str, company_key[0])
        role = cast(str, company["role"][0])
        records = {
            cast(str, row["metric"]): row for row in company.iter_rows(named=True)
        }
        values = {
            metric: (
                _amount_in_base_currency(record, currency)
                if metric in amount_metrics
                else cast(float, record["value"])
            )
            for metric, record in records.items()
            if metric in required_components
        }
        ev: float | None = None
        if "market_cap" in values and "net_debt" in values:
            try:
                ev = enterprise_value(
                    market_cap=values["market_cap"],
                    net_debt_value=values["net_debt"],
                )
            except ModelError as exc:
                raise IngestionError(
                    f"invalid comps enterprise value for {company_id}"
                ) from exc
        for multiple in metrics:
            if multiple.startswith("ev_"):
                denominator_metric = multiple.removeprefix("ev_")
                numerator, denominator = ev, values.get(denominator_metric)
            else:
                denominator_metric = "eps"
                numerator, denominator = values.get("share_price"), values.get("eps")
            if numerator is None or denominator is None or denominator <= 0:
                reason = (
                    "missing_market_cap_or_net_debt"
                    if multiple.startswith("ev_") and ev is None
                    else "missing_or_nonpositive_denominator"
                )
                actual = float(denominator or 0.0)
                expected = 0.0
                checks.append(
                    {
                        "schema_version": 1,
                        "run_id": run_id,
                        "scenario": company_id,
                        "check": f"excluded:{multiple}:{reason}",
                        "actual": actual,
                        "expected": expected,
                        "difference": actual - expected,
                        "passed": False,
                        "status": "excluded",
                        "unit": "x",
                    }
                )
                continue
            multiple_value = numerator / denominator
            if not math.isfinite(multiple_value):
                raise IngestionError("comps multiple result must be finite")
            rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "company_id": company_id,
                    "role": role,
                    "multiple": multiple,
                    "value": multiple_value,
                    "unit": "x",
                    "currency": currency,
                    "period_basis": basis_by_metric[denominator_metric],
                }
            )
            actual = multiple_value
            expected = multiple_value
            difference = actual - expected
            passed = _within_reconciliation_tolerance(difference, expected)
            checks.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": company_id,
                    "check": f"included:{multiple}",
                    "actual": actual,
                    "expected": expected,
                    "difference": difference,
                    "passed": passed,
                    "status": "passed" if passed else "failed",
                    "unit": "x",
                }
            )
    return rows, checks


def _comps_summary(
    rows: list[dict[str, object]],
    run_id: str,
    currency: str,
    requested_metrics: tuple[str, ...],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for multiple in sorted(set(requested_metrics)):
        values = sorted(
            cast(float, row["value"])
            for row in rows
            if row["multiple"] == multiple and row["role"] == "peer"
        )
        if not values:
            continue
        basis_values = {
            cast(str, row["period_basis"])
            for row in rows
            if row["multiple"] == multiple
        }
        if len(basis_values) != 1:
            raise IngestionError(f"incompatible period basis for {multiple}")
        basis = next(iter(basis_values))
        stats = {
            "min": values[0],
            "p25": _quantile(values, 0.25),
            "median": _quantile(values, 0.5),
            "mean": sum(values) / len(values),
            "p75": _quantile(values, 0.75),
            "max": values[-1],
        }
        output.extend(
            {
                "schema_version": 1,
                "run_id": run_id,
                "multiple": multiple,
                "statistic": name,
                "value": value,
                "count": len(values),
                "unit": "x",
                "currency": currency,
                "period_basis": basis,
            }
            for name, value in stats.items()
        )
    return output


def _quantile(values: list[float], probability: float) -> float:
    """Linear interpolation at index (n-1)*p, documented in modeling.md."""
    index = (len(values) - 1) * probability
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (index - lower)


def _single_target(frame: pl.DataFrame) -> str:
    targets = frame.filter(pl.col("role") == "target")["company_id"].unique().to_list()
    if len(targets) != 1:
        raise IngestionError("comps input requires exactly one target or --target")
    return cast(str, targets[0])


def _value(raw: object, label: str, expected_unit: str) -> ValueInput:
    mapping = _mapping(raw, label)
    if (
        set(mapping) != {"value", "unit", "source_id"}
        or not isinstance(mapping["value"], (int, float))
        or isinstance(mapping["value"], bool)
        or not math.isfinite(float(mapping["value"]))
    ):
        raise CaseContractError(
            f"{label} must contain numeric value, unit, and source_id"
        )
    if (
        mapping["unit"] != expected_unit
        or not isinstance(mapping["source_id"], str)
        or not mapping["source_id"]
    ):
        raise CaseContractError(f"{label} has an incompatible unit or empty source_id")
    return ValueInput(float(mapping["value"]), expected_unit, mapping["source_id"])


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise CaseContractError(f"{label} must be a TOML table")
    return cast(dict[str, object], value)


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise CaseContractError(f"{label} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CaseContractError(f"{label} must be YYYY-MM-DD") from exc


def _controlled_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise CaseContractError(f"{label} must be a non-empty string")
    return value


def _scenario_fingerprint(value: DCFScenario) -> dict[str, object]:
    return {
        "forecasts": [
            (day.isoformat(), item.value, item.unit) for day, item in value.forecasts
        ],
        "terminal": (value.terminal.value, value.terminal.unit),
        "terminal_metric": (
            None
            if value.terminal_metric is None
            else (value.terminal_metric.value, value.terminal_metric.unit)
        ),
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _unit_scale(unit: str, currency: str) -> float:
    return {
        currency: 1.0,
        f"{currency}k": 1e3,
        f"{currency}m": 1e6,
        f"{currency}b": 1e9,
    }[unit]


def _share_scale(unit: str) -> float:
    return {"shares": 1.0, "shares_k": 1e3, "shares_m": 1e6, "shares_b": 1e9}[unit]
