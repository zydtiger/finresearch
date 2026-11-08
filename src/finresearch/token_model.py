"""Auditable token valuation: strict inputs, supply path, multiples, and runs."""

from __future__ import annotations

import hashlib
import json
import math
import tomllib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Literal, cast

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    InputFileHash,
    canonical_parameters_sha256,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    CURRENCY_CODES,
    MODEL_TOKEN_CASHFLOWS_V1,
    MODEL_TOKEN_INPUTS_V1,
    MODEL_TOKEN_PEER_MULTIPLES_V1,
    MODEL_TOKEN_RECONCILIATION_V1,
    MODEL_TOKEN_RESULTS_V1,
    MODEL_TOKEN_SENSITIVITY_V1,
    MODEL_TOKEN_SUPPLY_PATH_V1,
    PEG_UNIT_PATTERN,
    TOKEN_SCALES,
    provider_id_pairs,
)
from finresearch.ingestion import IngestionError
from finresearch.modeling import (
    MODEL_PRODUCER,
    MODEL_PRODUCER_VERSION,
    ModelRun,
    ValueInput,
    _canonicalize_dcf_sensitivity,
    _controlled_text,
    _expected_register_paths,
    _mapping,
    _parse_date,
    _publish,
    _quantile,
    _read_validated_model_frame,
    _register_hashes,
    _require_register_paths,
    _unit_scale,
    _validate_declared_input_files,
    _value,
    _within_reconciliation_tolerance,
)
from finresearch.registers import ModelSource, load_model_sources

_SCENARIOS: Final = ("bear", "base", "bull")
_STATISTICS: Final = ("min", "p25", "median", "p75", "max")
_METHODS: Final = ("dcf", "multiple")
_BASES: Final = ("circulating", "fully_diluted")
_WINDOWS: Final = {"annualized_30d": 30, "ttm": 365}
_MULTIPLE_METRICS: Final = frozenset(
    {"fees", "revenue", "holders_revenue", "supply_side_revenue"}
)
_TOP_KEYS: Final = {
    "version",
    "as_of",
    "instrument_id",
    "currency",
    "value_unit",
    "token_unit",
    "discount_convention",
    "held_buyback_treatment",
    "buyback_price_basis",
    "price",
    "supply",
    "scenario",
    "multiples",
}
_MIX_KEYS: Final = ("buyback_retire", "buyback_hold", "burn", "distribution")
_FORECAST_KEYS: Final = {
    "period_end",
    "holders_revenue",
    "capture_ratio",
    "unlocks",
    "unlock_realization",
    "emissions",
}


@dataclass(frozen=True)
class TokenPeriod:
    period_end: date
    holders_revenue: ValueInput
    capture_ratio: ValueInput
    unlocks: ValueInput
    unlock_realization: ValueInput
    emissions: ValueInput


@dataclass(frozen=True)
class FullyDilutedOverride:
    metric: str
    provider: str
    source_id: str


@dataclass(frozen=True)
class TokenScenario:
    name: str
    discount_rate: ValueInput
    mix: dict[str, float]
    mix_source_id: str
    terminal_growth: ValueInput
    periods: tuple[TokenPeriod, ...]
    statistic: str
    override: FullyDilutedOverride | None


@dataclass(frozen=True)
class Peer:
    instrument_id: str
    entity_id: str


@dataclass(frozen=True)
class Multiples:
    metric: str
    window: str
    metric_window_end: date
    market_observed_on: date
    market_artifact_ids: tuple[str, ...]
    metric_artifact_ids: tuple[str, ...]
    peers: tuple[Peer, ...]
    exclude: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class TokenConfig:
    as_of: date
    instrument_id: str
    currency: str
    value_unit: str
    token_unit: str
    discount_convention: str
    held_buyback_treatment: str
    price_artifact_id: str
    session_date: date
    master_artifact_id: str | None
    reconciliation_artifact_id: str
    fully_diluted_metric: str
    anchors: tuple[str, str] | None
    pegs: dict[str, ValueInput]
    scenarios: dict[str, TokenScenario]
    multiples: Multiples


@dataclass(frozen=True)
class TokenData:
    """Everything a run reads from referenced case artifacts."""

    price: float
    reconciliation: pl.DataFrame
    market: pl.DataFrame
    metrics: pl.DataFrame
    parents: tuple[str, ...]
    parent_bindings: tuple[tuple[str, str], ...]


def _token_scale(unit: str) -> float:
    return {"tokens": 1.0, "tokens_k": 1e3, "tokens_m": 1e6, "tokens_b": 1e9}[unit]


def _strings(raw: object, label: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not all(isinstance(i, str) and i for i in raw):
        raise CaseContractError(f"{label} must be an array of non-empty strings")
    if len(set(raw)) != len(raw):
        raise CaseContractError(f"{label} must not repeat ids")
    return tuple(raw)


def _keys(
    table: dict[str, object],
    required: set[str],
    label: str,
    optional: set[str] | None = None,
) -> None:
    extra = set(table) - required - (optional or set())
    if extra or required - set(table):
        raise CaseContractError(
            f"{label} must contain exactly {sorted(required)}"
            + (f" plus optional {sorted(optional)}" if optional else "")
        )


def load_token_input(
    case_dir: Path, relative_input: str
) -> tuple[TokenConfig, bytes, Path]:
    """Load and strictly validate one case-relative token-inputs.toml v1 file."""
    if Path(relative_input).is_absolute():
        raise CaseContractError("token input must be a case-relative path")
    path = resolve_relative_path(case_dir, relative_input, "token input")
    if not path.is_file():
        raise CaseContractError(f"token input file is missing: {relative_input}")
    raw = path.read_bytes()
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise CaseContractError("token input must be valid UTF-8 TOML") from exc
    if data.get("version") != 1 or isinstance(data.get("version"), bool):
        raise CaseContractError("token input must use version = 1")
    _keys(data, _TOP_KEYS, "token input", {"anchors", "pegs"})
    as_of = _parse_date(data["as_of"], "as_of")
    instrument_id = _controlled_text(data["instrument_id"], "instrument_id")
    currency = _controlled_text(data["currency"], "currency")
    if currency not in CURRENCY_CODES:
        raise CaseContractError("currency must be a controlled ISO currency code")
    value_unit = _controlled_text(data["value_unit"], "value_unit")
    token_unit = _controlled_text(data["token_unit"], "token_unit")
    if value_unit not in {currency, f"{currency}k", f"{currency}m", f"{currency}b"}:
        raise CaseContractError("value_unit must be a controlled currency amount scale")
    if token_unit not in TOKEN_SCALES:
        raise CaseContractError("token_unit must be a controlled token scale")
    if data["discount_convention"] not in {"year_end", "mid_year"}:
        raise CaseContractError("discount_convention must be year_end or mid_year")
    if data["held_buyback_treatment"] not in {"exclude", "include"}:
        raise CaseContractError("held_buyback_treatment must be exclude or include")
    if data["buyback_price_basis"] != "as_of_price":
        raise CaseContractError("buyback_price_basis must be as_of_price")

    price = _mapping(data["price"], "price")
    _keys(price, {"artifact_id", "session_date"}, "price", {"master_artifact_id"})
    master = price.get("master_artifact_id")
    if master is not None:
        master = _controlled_text(master, "price.master_artifact_id")
    supply = _mapping(data["supply"], "supply")
    _keys(supply, {"reconciliation_artifact_id", "fully_diluted_metric"}, "supply")
    if supply["fully_diluted_metric"] not in {"total", "max"}:
        raise CaseContractError("supply.fully_diluted_metric must be total or max")
    anchors: tuple[str, str] | None = None
    if "anchors" in data:
        anchor_table = _mapping(data["anchors"], "anchors")
        _keys(anchor_table, {"protocol_metrics_artifact_id", "metric"}, "anchors")
        anchors = (
            _controlled_text(anchor_table["protocol_metrics_artifact_id"], "anchors"),
            _controlled_text(anchor_table["metric"], "anchors.metric"),
        )
    pegs: dict[str, ValueInput] = {}
    for name, raw_peg in _mapping(data.get("pegs", {}), "pegs").items():
        peg_table = _mapping(raw_peg, f"pegs.{name}")
        unit = peg_table.get("unit")
        match = PEG_UNIT_PATTERN.fullmatch(unit) if isinstance(unit, str) else None
        if match is None or match.group(1) not in CURRENCY_CODES:
            raise CaseContractError(f"pegs.{name} unit must be <FIAT>/<USDC|USDT|USDH>")
        peg = _value(peg_table, f"pegs.{name}", cast(str, unit))
        if peg.value <= 0:
            raise CaseContractError(f"pegs.{name} must be positive")
        pegs[name] = peg

    scenarios_raw = _mapping(data["scenario"], "scenario")
    if set(scenarios_raw) != set(_SCENARIOS):
        raise CaseContractError(
            "scenario must contain distinct bear, base, and bull tables"
        )
    scenarios = {
        name: _load_scenario(
            name,
            _mapping(scenarios_raw[name], f"scenario.{name}"),
            as_of,
            value_unit,
            token_unit,
        )
        for name in _SCENARIOS
    }
    fingerprints = {
        json.dumps(_fingerprint(item), sort_keys=True) for item in scenarios.values()
    }
    if len(fingerprints) != len(_SCENARIOS):
        raise CaseContractError("bear, base, and bull token scenarios must be distinct")

    mult = _mapping(data["multiples"], "multiples")
    _keys(
        mult,
        {
            "metric",
            "window",
            "metric_window_end",
            "market_observed_on",
            "market_artifact_ids",
            "metric_artifact_ids",
            "peers",
        },
        "multiples",
        {"exclude"},
    )
    if mult["metric"] not in _MULTIPLE_METRICS:
        raise CaseContractError(
            f"multiples.metric must be one of {sorted(_MULTIPLE_METRICS)}"
        )
    if mult["window"] not in _WINDOWS:
        raise CaseContractError("multiples.window must be annualized_30d or ttm")
    window_end = _parse_date(mult["metric_window_end"], "metric_window_end")
    observed_on = _parse_date(mult["market_observed_on"], "market_observed_on")
    if not window_end < observed_on <= as_of:
        raise CaseContractError(
            "metric_window_end must precede market_observed_on, "
            "which must not be after as_of"
        )
    peers_raw = mult["peers"]
    if not isinstance(peers_raw, list) or not peers_raw:
        raise CaseContractError("multiples.peers must be a non-empty array")
    peers: list[Peer] = []
    for index, item in enumerate(peers_raw):
        peer_table = _mapping(item, f"multiples.peers[{index}]")
        _keys(peer_table, {"instrument_id", "entity_id"}, f"multiples.peers[{index}]")
        peers.append(
            Peer(
                _controlled_text(peer_table["instrument_id"], "peer instrument_id"),
                _controlled_text(peer_table["entity_id"], "peer entity_id"),
            )
        )
    if len({peer.instrument_id for peer in peers}) != len(peers):
        raise CaseContractError("multiples.peers must not repeat an instrument_id")
    exclusions: list[tuple[str, str]] = []
    exclude_raw = mult.get("exclude", [])
    if not isinstance(exclude_raw, list):
        raise CaseContractError("multiples.exclude must be an array")
    for index, item in enumerate(exclude_raw):
        table = _mapping(item, f"multiples.exclude[{index}]")
        _keys(
            table, {"instrument_id", "reason_source_id"}, f"multiples.exclude[{index}]"
        )
        exclusions.append(
            (
                _controlled_text(table["instrument_id"], "excluded instrument_id"),
                _controlled_text(table["reason_source_id"], "reason_source_id"),
            )
        )
    excluded_ids = [instrument for instrument, _ in exclusions]
    if len(set(excluded_ids)) != len(excluded_ids) or set(excluded_ids) & {
        peer.instrument_id for peer in peers
    }:
        raise CaseContractError(
            "multiples.exclude must be distinct and disjoint from peers"
        )
    config = TokenConfig(
        as_of=as_of,
        instrument_id=instrument_id,
        currency=currency,
        value_unit=value_unit,
        token_unit=token_unit,
        discount_convention=cast(str, data["discount_convention"]),
        held_buyback_treatment=cast(str, data["held_buyback_treatment"]),
        price_artifact_id=_controlled_text(price["artifact_id"], "price.artifact_id"),
        session_date=_parse_date(price["session_date"], "price.session_date"),
        master_artifact_id=master,
        reconciliation_artifact_id=_controlled_text(
            supply["reconciliation_artifact_id"], "supply.reconciliation_artifact_id"
        ),
        fully_diluted_metric=supply["fully_diluted_metric"],
        anchors=anchors,
        pegs=pegs,
        scenarios=scenarios,
        multiples=Multiples(
            metric=mult["metric"],
            window=mult["window"],
            metric_window_end=window_end,
            market_observed_on=observed_on,
            market_artifact_ids=_strings(
                mult["market_artifact_ids"], "multiples.market_artifact_ids"
            ),
            metric_artifact_ids=_strings(
                mult["metric_artifact_ids"], "multiples.metric_artifact_ids"
            ),
            peers=tuple(peers),
            exclude=tuple(exclusions),
        ),
    )
    # Price class resolution is not available while loading the input, so every
    # price must be a complete bar: a session on the cutoff would pass here but
    # fail audit as an incomplete crypto bar.
    if config.session_date >= as_of:
        raise CaseContractError(
            "price.session_date must be before as_of (a complete daily bar)"
        )
    if (
        not config.multiples.market_artifact_ids
        or not config.multiples.metric_artifact_ids
    ):
        raise CaseContractError("multiples need market and metric artifact ids")
    return config, raw, path


def _load_scenario(
    name: str,
    raw: dict[str, object],
    as_of: date,
    value_unit: str,
    token_unit: str,
) -> TokenScenario:
    _keys(
        raw,
        {"discount_rate", "capture_mix", "terminal", "forecast", "multiple"},
        f"scenario.{name}",
        {"fully_diluted"},
    )
    rate = _value(raw["discount_rate"], "discount_rate", "ratio")
    if rate.value <= 0:
        raise CaseContractError("discount_rate must be positive")
    mix_raw = _mapping(raw["capture_mix"], f"scenario.{name}.capture_mix")
    _keys(mix_raw, {*_MIX_KEYS, "source_id"}, f"scenario.{name}.capture_mix")
    mix: dict[str, float] = {}
    for key in _MIX_KEYS:
        fraction = mix_raw[key]
        if (
            isinstance(fraction, bool)
            or not isinstance(fraction, (int, float))
            or not math.isfinite(fraction)
            or fraction < 0
        ):
            raise CaseContractError(f"capture_mix.{key} must be a nonnegative number")
        mix[key] = float(fraction)
    if abs(sum(mix.values()) - 1.0) > 1e-9:
        raise CaseContractError("capture_mix fractions must sum to 1")
    mix_source = _controlled_text(mix_raw["source_id"], "capture_mix.source_id")
    terminal_raw = _mapping(raw["terminal"], f"scenario.{name}.terminal")
    _keys(terminal_raw, {"terminal_growth"}, f"scenario.{name}.terminal")
    growth = _value(terminal_raw["terminal_growth"], "terminal_growth", "ratio")
    if not -1 < growth.value < rate.value:
        raise CaseContractError("terminal_growth must satisfy -1 < g < discount_rate")
    forecast_raw = raw["forecast"]
    if not isinstance(forecast_raw, list) or not forecast_raw:
        raise CaseContractError(f"scenario.{name}.forecast must be a non-empty array")
    periods: list[TokenPeriod] = []
    previous = as_of
    for index, item in enumerate(forecast_raw):
        row = _mapping(item, f"scenario.{name}.forecast[{index}]")
        _keys(row, _FORECAST_KEYS, f"scenario.{name}.forecast[{index}]")
        period_end = _parse_date(row["period_end"], "period_end")
        if period_end <= previous:
            raise CaseContractError(
                "forecast period_end rows must strictly increase after as_of"
            )
        previous = period_end
        period = TokenPeriod(
            period_end,
            _value(row["holders_revenue"], "holders_revenue", value_unit),
            _value(row["capture_ratio"], "capture_ratio", "ratio"),
            _value(row["unlocks"], "unlocks", token_unit),
            _value(row["unlock_realization"], "unlock_realization", "ratio"),
            _value(row["emissions"], "emissions", token_unit),
        )
        for label, leaf in (
            ("capture_ratio", period.capture_ratio),
            ("unlock_realization", period.unlock_realization),
        ):
            if not 0 <= leaf.value <= 1:
                raise CaseContractError(f"{label} must be within [0, 1]")
        for label, leaf in (
            ("holders_revenue", period.holders_revenue),
            ("unlocks", period.unlocks),
            ("emissions", period.emissions),
        ):
            if leaf.value < 0:
                raise CaseContractError(f"{label} must be nonnegative")
        periods.append(period)
    multiple_raw = _mapping(raw["multiple"], f"scenario.{name}.multiple")
    _keys(multiple_raw, {"statistic"}, f"scenario.{name}.multiple")
    if multiple_raw["statistic"] not in _STATISTICS:
        raise CaseContractError(f"multiple.statistic must be one of {_STATISTICS}")
    override: FullyDilutedOverride | None = None
    if "fully_diluted" in raw:
        table = _mapping(raw["fully_diluted"], f"scenario.{name}.fully_diluted")
        _keys(table, {"metric", "provider", "source_id"}, "fully_diluted")
        if table["metric"] not in {"total", "max"}:
            raise CaseContractError("fully_diluted.metric must be total or max")
        override = FullyDilutedOverride(
            table["metric"],
            _controlled_text(table["provider"], "fully_diluted.provider"),
            _controlled_text(table["source_id"], "fully_diluted.source_id"),
        )
    return TokenScenario(
        name,
        rate,
        mix,
        mix_source,
        growth,
        tuple(periods),
        multiple_raw["statistic"],
        override,
    )


def _fingerprint(scenario: TokenScenario) -> dict[str, object]:
    return {
        "rate": scenario.discount_rate.value,
        "mix": scenario.mix,
        "growth": scenario.terminal_growth.value,
        "periods": [
            (
                p.period_end.isoformat(),
                p.holders_revenue.value,
                p.capture_ratio.value,
                p.unlocks.value,
                p.unlock_realization.value,
                p.emissions.value,
            )
            for p in scenario.periods
        ],
        "statistic": scenario.statistic,
        "override": None
        if scenario.override is None
        else (scenario.override.metric, scenario.override.provider),
    }


def token_source_ids(config: TokenConfig) -> set[str]:
    """Every register id the input cites."""
    ids: set[str] = {value.source_id for value in config.pegs.values()}
    ids |= {reason for _, reason in config.multiples.exclude}
    for scenario in config.scenarios.values():
        ids |= {scenario.discount_rate.source_id, scenario.terminal_growth.source_id}
        ids.add(scenario.mix_source_id)
        if scenario.override is not None:
            ids.add(scenario.override.source_id)
        for period in scenario.periods:
            ids |= {
                period.holders_revenue.source_id,
                period.capture_ratio.source_id,
                period.unlocks.source_id,
                period.unlock_realization.source_id,
                period.emissions.source_id,
            }
    return ids


def validate_token_sources(
    config: TokenConfig, sources: dict[str, ModelSource]
) -> None:
    ids = token_source_ids(config)
    missing = sorted(ids - set(sources))
    if missing:
        raise CaseContractError(f"token inputs have dangling source ids: {missing}")
    future = sorted(i for i in ids if sources[i].effective_date > config.as_of)
    if future:
        raise CaseContractError(f"token sources are dated after model as_of: {future}")


def token_parent_ids(config: TokenConfig) -> tuple[str, ...]:
    """Ordered, de-duplicated artifact parents of a run's inputs table."""
    ordered = [
        config.price_artifact_id,
        config.reconciliation_artifact_id,
        *config.multiples.market_artifact_ids,
        *config.multiples.metric_artifact_ids,
    ]
    if config.anchors is not None:
        ordered.append(config.anchors[0])
    if config.master_artifact_id is not None:
        ordered.append(config.master_artifact_id)
    return tuple(dict.fromkeys(ordered))


def _artifact_frame(
    case_dir: Path,
    by_id: dict[str, Artifact],
    artifact_id: str,
    kind: str,
    version: int,
    *,
    verify_hashes: bool,
) -> pl.DataFrame:
    artifact = by_id.get(artifact_id)
    if artifact is None:
        raise CaseContractError(f"token input artifact is not declared: {artifact_id}")
    if artifact.kind != kind or artifact.schema_version != version:
        raise CaseContractError(f"{artifact_id} must be {kind}.v{version}")
    return _read_validated_model_frame(case_dir, artifact, verify_hashes=verify_hashes)


def load_token_data(
    case_dir: Path,
    manifest: CaseManifest,
    config: TokenConfig,
    *,
    verify_hashes: bool,
) -> TokenData:
    """Read and validate every artifact the run depends on."""
    by_id = {artifact.artifact_id: artifact for artifact in manifest.artifacts}

    def frame(artifact_id: str, kind: str, version: int) -> pl.DataFrame:
        return _artifact_frame(
            case_dir, by_id, artifact_id, kind, version, verify_hashes=verify_hashes
        )

    prices = frame(config.price_artifact_id, "normalized.daily-prices", 2)
    bars = prices.filter(pl.col("session_date") == config.session_date)
    if bars.height != 1:
        raise CaseContractError(
            f"price artifact must hold exactly one bar for {config.session_date}"
        )
    bar = bars.row(0, named=True)
    if bar["currency"] != config.currency:
        raise CaseContractError("price currency must equal the model currency")
    if bar["instrument_id"] != config.instrument_id:
        if config.master_artifact_id is None:
            raise CaseContractError(
                "price instrument differs from instrument_id; declare "
                "price.master_artifact_id (an instrument-master.v3 mapping)"
            )
        master = frame(config.master_artifact_id, "normalized.instrument-master", 3)
        link = f"yfinance:{bar['provider_symbol']}"
        linked = False
        for row in master.iter_rows(named=True):
            if (
                row["instrument_id"] == config.instrument_id
                and row["observed_at"].date() <= config.as_of
                and row["provider_ids"] is not None
                and link
                in {f"{a}:{b}" for a, b in provider_id_pairs(row["provider_ids"])}
            ):
                linked = True
        if not linked:
            raise CaseContractError(
                f"instrument master does not map {link} to {config.instrument_id}"
            )
    price = float(bar["close"])

    reconciliation = frame(
        config.reconciliation_artifact_id, "derived.token-supply-reconciliation", 1
    )
    reconciliation = reconciliation.filter(
        pl.col("instrument_id") == config.instrument_id
    )
    as_ofs = reconciliation["as_of"].unique().to_list()
    if len(as_ofs) != 1:
        raise CaseContractError(
            "reconciliation must hold exactly one as_of for the instrument"
        )
    if as_ofs[0] > config.as_of:
        raise CaseContractError("reconciliation as_of is after the model as_of")
    for metric in ("circulating", config.fully_diluted_metric):
        rows = reconciliation.filter(pl.col("supply_metric") == metric)
        if rows.filter(pl.col("status") == "conflict").height:
            raise CaseContractError(
                f"supply reconciliation has a conflict for {metric}"
            )
        if rows.filter(pl.col("selected")).height != 1:
            raise CaseContractError(f"reconciliation has no selected {metric} row")

    market = pl.concat(
        [
            frame(i, "normalized.token-market", 1)
            for i in config.multiples.market_artifact_ids
        ]
    )
    metrics = pl.concat(
        [
            frame(i, "normalized.protocol-metrics", 1)
            for i in config.multiples.metric_artifact_ids
        ]
    )
    if config.anchors is not None:
        anchor = frame(config.anchors[0], "normalized.protocol-metrics", 1)
        if config.anchors[1] not in set(anchor["metric"].to_list()):
            raise CaseContractError("anchors.metric is not in the anchor artifact")
    parents = token_parent_ids(config)
    bindings = tuple((parent, by_id[parent].sha256 or "") for parent in parents)
    return TokenData(price, reconciliation, market, metrics, parents, bindings)


def _selected(data: TokenData, metric: str) -> float:
    rows = data.reconciliation.filter(
        (pl.col("supply_metric") == metric) & pl.col("selected")
    )
    return float(rows["amount"][0])


def _fully_diluted_0(
    data: TokenData, config: TokenConfig, scenario: TokenScenario
) -> float:
    if scenario.override is None:
        return _selected(data, config.fully_diluted_metric)
    override = scenario.override
    rows = data.reconciliation.filter(
        (pl.col("supply_metric") == override.metric)
        & (pl.col("provider") == override.provider)
    )
    if rows.is_empty():
        raise CaseContractError(
            f"fully_diluted override names no observation: "
            f"{override.provider} {override.metric}"
        )
    if rows.filter(pl.col("status") == "conflict").height:
        raise CaseContractError("fully_diluted override must not use a conflict")
    return float(rows["amount"][0])


@dataclass(frozen=True)
class _Path:
    period: TokenPeriod
    year_fraction: float
    discount_factor: float
    cash_flow: float
    opening_circ: float
    unlocks: float
    emissions: float
    buyback: float
    retired: float
    closing_circ: float
    opening_fd: float
    closing_fd: float


@dataclass(frozen=True)
class _Dcf:
    paths: tuple[_Path, ...]
    terminal_pv: float
    aggregate: float


def _discounted(
    config: TokenConfig,
    scenario: TokenScenario,
    price: float,
    circ0: float,
    fd0: float,
    rate: float,
    growth: float,
) -> _Dcf:
    """Run the supply path and discounting; supply units are token_unit."""
    vs = _unit_scale(config.value_unit, config.currency)
    ts = _token_scale(config.token_unit)
    tokens_per_value = vs / (price * ts)
    mix = scenario.mix
    paths: list[_Path] = []
    circ, fd = circ0, fd0
    for period in scenario.periods:
        cash = period.holders_revenue.value * period.capture_ratio.value
        retired = cash * (mix["buyback_retire"] + mix["burn"]) * tokens_per_value
        if config.held_buyback_treatment == "exclude":
            retired += cash * mix["buyback_hold"] * tokens_per_value
        buyback = (
            cash * (mix["buyback_retire"] + mix["buyback_hold"]) * tokens_per_value
        )
        unlocks = period.unlocks.value * period.unlock_realization.value
        closing_circ = circ + unlocks + period.emissions.value - retired
        closing_fd = fd - retired
        if retired > circ * (1 + 1e-12) or closing_circ < -1e-9 * max(1.0, circ0):
            raise CaseContractError(
                f"scenario {scenario.name} retires more tokens than circulate "
                f"in {period.period_end}"
            )
        if closing_circ > closing_fd * (1 + 1e-12):
            raise CaseContractError(
                f"scenario {scenario.name} circulating exceeds fully diluted supply "
                f"in {period.period_end}"
            )
        years = (period.period_end - config.as_of).days / 365.0
        if config.discount_convention == "mid_year":
            years = max(0.0, years - 0.5)
        try:
            factor = (1 + rate) ** years
        except OverflowError:
            factor = math.inf
        if not math.isfinite(factor) or factor <= 0:
            raise CaseContractError(
                f"scenario {scenario.name} discount factor must be finite "
                f"for rate {rate} over {years} years"
            )
        paths.append(
            _Path(
                period,
                years,
                factor,
                cash,
                circ,
                unlocks,
                period.emissions.value,
                buyback,
                retired,
                closing_circ,
                fd,
                closing_fd,
            )
        )
        circ, fd = closing_circ, closing_fd
    last = paths[-1]
    terminal = last.cash_flow * (1 + growth) / (rate - growth)
    terminal_pv = terminal / last.discount_factor
    aggregate = sum(p.cash_flow / p.discount_factor for p in paths) + terminal_pv
    if not math.isfinite(aggregate) or aggregate <= 0:
        raise CaseContractError(
            f"scenario {scenario.name} aggregate value must be finite and positive"
        )
    return _Dcf(tuple(paths), terminal_pv, aggregate)


def _quote_rate(config: TokenConfig, quote_asset: str) -> float:
    if quote_asset == config.currency:
        return 1.0
    if quote_asset in CURRENCY_CODES:
        raise CaseContractError(
            f"peer market value is quoted in {quote_asset}; no FX conversion exists"
        )
    unit = f"{config.currency}/{quote_asset}"
    rates = sorted({peg.value for peg in config.pegs.values() if peg.unit == unit})
    if len(rates) != 1:
        raise CaseContractError(
            f"a market value quoted in {quote_asset} requires exactly one "
            f"{unit} peg leaf"
        )
    return rates[0]


def _holders_revenue_window(
    config: TokenConfig, metrics: pl.DataFrame, peer: Peer
) -> float:
    multiples = config.multiples
    days = _WINDOWS[multiples.window]
    rows = metrics.filter(
        (pl.col("entity_id") == peer.entity_id)
        & (pl.col("metric") == multiples.metric)
        & pl.col("sub_protocol").is_null()
        & pl.col("chain").is_null()
        & (pl.col("period_type") == "duration")
        & (pl.col("unit") == config.currency)
        & (pl.col("observed_at").dt.date() <= config.as_of)
    )
    values: dict[date, set[float]] = {}
    incomplete: set[date] = set()
    for row in rows.iter_rows(named=True):
        if row["period_start"] != row["period_end"]:
            continue
        values.setdefault(row["period_end"], set()).add(row["value"])
        if not row["is_complete"]:
            incomplete.add(row["period_end"])
    total = 0.0
    for offset in range(days):
        day = multiples.metric_window_end - timedelta(days=offset)
        if day not in values or day in incomplete:
            raise CaseContractError(
                f"peer {peer.entity_id} has no complete {multiples.metric} for {day}"
            )
        if len(values[day]) != 1:
            raise CaseContractError(
                f"peer {peer.entity_id} has conflicting {multiples.metric} for {day}"
            )
        total += next(iter(values[day]))
    return total * 365 / 30 if multiples.window == "annualized_30d" else total


def _peer_rows(
    config: TokenConfig, data: TokenData, run_id: str
) -> list[dict[str, object]]:
    multiples = config.multiples
    rows: list[dict[str, object]] = []

    def row(
        peer: str,
        entity: str | None,
        basis: str,
        numerator: float | None,
        denominator: float | None,
        reason: str | None,
    ) -> dict[str, object]:
        included = reason is None
        assert not included or (numerator is not None and denominator is not None)
        return {
            "schema_version": 1,
            "run_id": run_id,
            "peer_instrument_id": peer,
            "peer_entity_id": entity,
            "basis": basis,
            "numerator": numerator,
            "denominator": denominator,
            "multiple": (
                None
                if not included
                else cast(float, numerator) / cast(float, denominator)
            ),
            "status": "included" if included else "excluded",
            "exclusion_reason": reason,
            "currency": config.currency,
        }

    for peer in sorted(multiples.peers, key=lambda item: item.instrument_id):
        observed = data.market.filter(
            (pl.col("instrument_id") == peer.instrument_id)
            & (pl.col("observed_at").dt.date() == multiples.market_observed_on)
            & (pl.col("observed_at").dt.date() <= config.as_of)
        )
        if observed.is_empty():
            raise CaseContractError(
                f"peer {peer.instrument_id} has no market observation on "
                f"{multiples.market_observed_on}"
            )
        revenue = _holders_revenue_window(config, data.metrics, peer)
        for basis, metric in (("circulating", "market_cap"), ("fully_diluted", "fdv")):
            candidates = observed.filter(pl.col("metric") == metric)
            if candidates.is_empty():
                rows.append(
                    row(
                        peer.instrument_id,
                        peer.entity_id,
                        basis,
                        None,
                        None,
                        "missing_market_observation",
                    )
                )
                continue
            latest = candidates["observed_at"].max()
            values = {
                float(item["value"]) * _quote_rate(config, item["quote_asset"])
                for item in candidates.filter(
                    pl.col("observed_at") == latest
                ).iter_rows(named=True)
            }
            if len(values) != 1:
                raise CaseContractError(
                    f"peer {peer.instrument_id} has conflicting latest {metric}"
                )
            numerator = next(iter(values))
            if revenue <= 0:
                rows.append(
                    row(
                        peer.instrument_id,
                        peer.entity_id,
                        basis,
                        numerator,
                        revenue,
                        "nonpositive_holders_revenue",
                    )
                )
            else:
                rows.append(
                    row(
                        peer.instrument_id,
                        peer.entity_id,
                        basis,
                        numerator,
                        revenue,
                        None,
                    )
                )
    for instrument, source_id in sorted(multiples.exclude):
        for basis in _BASES:
            rows.append(
                row(instrument, None, basis, None, None, f"excluded:{source_id}")
            )
    for basis in _BASES:
        if not any(r["basis"] == basis and r["status"] == "included" for r in rows):
            raise CaseContractError(f"no valid peer remains for the {basis} basis")
    return rows


def _statistic(rows: list[dict[str, object]], basis: str, name: str) -> float:
    values = sorted(
        cast(float, r["multiple"])
        for r in rows
        if r["basis"] == basis and r["status"] == "included"
    )
    probability = {"min": 0.0, "p25": 0.25, "median": 0.5, "p75": 0.75, "max": 1.0}
    return _quantile(values, probability[name])


def _check(
    run_id: str, scenario: str, name: str, actual: float, expected: float, unit: str
) -> dict[str, object]:
    difference = actual - expected
    passed = _within_reconciliation_tolerance(difference, expected)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "scenario": scenario,
        "check": name,
        "actual": actual,
        "expected": expected,
        "difference": difference,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "unit": unit,
    }


def build_token_frames(
    config: TokenConfig,
    data: TokenData,
    run_id: str,
    selected: tuple[str, ...],
    sensitivity: tuple[tuple[float, ...], tuple[float, ...]] | None,
) -> dict[str, pl.DataFrame]:
    """Build every deterministic token model table from authenticated inputs."""
    vs = _unit_scale(config.value_unit, config.currency)
    ts = _token_scale(config.token_unit)
    price = data.price
    circ_raw = _selected(data, "circulating")
    circ0 = circ_raw / ts
    peers = _peer_rows(config, data, run_id)
    price_unit = f"{config.currency}/token"
    input_rows: list[dict[str, object]] = []
    supply_rows: list[dict[str, object]] = []
    cash_rows: list[dict[str, object]] = []
    result_rows: list[dict[str, object]] = []
    check_rows: list[dict[str, object]] = []
    sensitivity_rows: list[dict[str, object]] = []

    def leaf(
        scenario: str, field: str, item: ValueInput, period_end: date | None = None
    ) -> None:
        input_rows.append(
            {
                "schema_version": 1,
                "run_id": run_id,
                "scenario": scenario,
                "field": field,
                "period_end": period_end,
                "value": item.value,
                "unit": item.unit,
                "source_id": item.source_id,
                "source_artifact_id": None,
            }
        )

    def resolved(
        scenario: str, field: str, value: float, unit: str, source: str, artifact: bool
    ) -> None:
        input_rows.append(
            {
                "schema_version": 1,
                "run_id": run_id,
                "scenario": scenario,
                "field": field,
                "period_end": None,
                "value": value,
                "unit": unit,
                "source_id": None if artifact else source,
                "source_artifact_id": source if artifact else None,
            }
        )

    for name in selected:
        scenario = config.scenarios[name]
        fd_raw = _fully_diluted_0(data, config, scenario)
        fd0 = fd_raw / ts
        if circ_raw > fd_raw:
            raise CaseContractError(
                f"scenario {name} opening circulating exceeds fully diluted supply"
            )
        rate = scenario.discount_rate.value
        growth = scenario.terminal_growth.value
        run = _discounted(config, scenario, price, circ0, fd0, rate, growth)

        resolved(name, "price", price, price_unit, config.price_artifact_id, True)
        resolved(
            name,
            "circulating_0",
            circ0,
            config.token_unit,
            config.reconciliation_artifact_id,
            True,
        )
        if scenario.override is None:
            resolved(
                name,
                "fully_diluted_0",
                fd0,
                config.token_unit,
                config.reconciliation_artifact_id,
                True,
            )
        else:
            resolved(
                name,
                "fully_diluted_0",
                fd0,
                config.token_unit,
                scenario.override.source_id,
                False,
            )
        leaf(name, "discount_rate", scenario.discount_rate)
        leaf(name, "terminal_growth", scenario.terminal_growth)
        for key in _MIX_KEYS:
            input_rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": name,
                    "field": f"capture_mix.{key}",
                    "period_end": None,
                    "value": scenario.mix[key],
                    "unit": "ratio",
                    "source_id": scenario.mix_source_id,
                    "source_artifact_id": None,
                }
            )
        for peg_name, peg in sorted(config.pegs.items()):
            leaf(name, f"peg.{peg_name}", peg)
        for path in run.paths:
            period = path.period
            day = period.period_end
            for field, item in (
                ("holders_revenue", period.holders_revenue),
                ("capture_ratio", period.capture_ratio),
                ("unlocks", period.unlocks),
                ("unlock_realization", period.unlock_realization),
                ("emissions", period.emissions),
            ):
                leaf(name, f"{field}:{day}", item, day)
            mix = scenario.mix
            cash_rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": name,
                    "period_end": day,
                    "holders_revenue": period.holders_revenue.value,
                    "capture_ratio": period.capture_ratio.value,
                    "tokenholder_cash_flow": path.cash_flow,
                    "buyback_cash": path.cash_flow
                    * (mix["buyback_retire"] + mix["buyback_hold"]),
                    "burn_cash": path.cash_flow * mix["burn"],
                    "distribution_cash": path.cash_flow * mix["distribution"],
                    "year_fraction": path.year_fraction,
                    "discount_factor": path.discount_factor,
                    "present_value": path.cash_flow / path.discount_factor,
                    "currency": config.currency,
                    "unit": config.value_unit,
                }
            )
            new_supply = path.unlocks + path.emissions
            supply_rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": name,
                    "period_end": day,
                    "opening_circulating": path.opening_circ,
                    "unlocks_realized": path.unlocks,
                    "emissions": path.emissions,
                    "buyback_tokens": path.buyback,
                    "retired_tokens": path.retired,
                    "closing_circulating": path.closing_circ,
                    "opening_fully_diluted": path.opening_fd,
                    "closing_fully_diluted": path.closing_fd,
                    "absorption_ratio": path.buyback / new_supply
                    if new_supply > 0
                    else None,
                    "token_unit": config.token_unit,
                }
            )

        first_revenue = scenario.periods[0].holders_revenue.value
        dens = {"circulating": circ0, "fully_diluted": fd0}
        multiples = {
            basis: _statistic(peers, basis, scenario.statistic) for basis in _BASES
        }
        aggregates = {("dcf", basis): run.aggregate for basis in _BASES} | {
            ("multiple", basis): multiples[basis] * first_revenue for basis in _BASES
        }
        for (method, basis), aggregate in aggregates.items():
            per_token = aggregate * vs / (dens[basis] * ts)
            dcf = method == "dcf"
            result_rows.append(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scenario": name,
                    "method": method,
                    "basis": basis,
                    "aggregate_value": aggregate,
                    "denominator_tokens": dens[basis],
                    "per_token_value": per_token,
                    "price": price,
                    "upside": per_token / price - 1,
                    "terminal_share": run.terminal_pv / aggregate if dcf else None,
                    "multiple_applied": None if dcf else multiples[basis],
                    "implied_cf_scale": (
                        price * fd_raw / (aggregate * vs)
                        if dcf and basis == "fully_diluted"
                        else None
                    ),
                    "currency": config.currency,
                    "value_unit": config.value_unit,
                    "token_unit": config.token_unit,
                    "per_token_unit": price_unit,
                }
            )
            check_rows.append(
                _check(
                    run_id,
                    name,
                    f"per_token_value:{method}:{basis}",
                    per_token * dens[basis] * ts / vs,
                    aggregate,
                    config.value_unit,
                )
            )
        check_rows.append(
            _check(
                run_id,
                name,
                "pv_sum",
                run.aggregate,
                sum(p.cash_flow / p.discount_factor for p in run.paths)
                + run.terminal_pv,
                config.value_unit,
            )
        )
        check_rows.append(
            _check(
                run_id,
                name,
                "supply_roll_forward_circulating",
                run.paths[-1].closing_circ,
                circ0 + sum(p.unlocks + p.emissions - p.retired for p in run.paths),
                config.token_unit,
            )
        )
        check_rows.append(
            _check(
                run_id,
                name,
                "supply_roll_forward_fully_diluted",
                run.paths[-1].closing_fd,
                fd0 - sum(p.retired for p in run.paths),
                config.token_unit,
            )
        )
        check_rows.append(
            _check(
                run_id,
                name,
                "implied_cf_scale",
                (price * fd_raw / (run.aggregate * vs)) * run.aggregate * vs,
                price * fd_raw,
                config.currency,
            )
        )
        for basis in _BASES:
            check_rows.append(
                _check(
                    run_id,
                    name,
                    f"peer_statistic:{basis}",
                    multiples[basis],
                    _statistic(peers, basis, scenario.statistic),
                    "x",
                )
            )
        if sensitivity is not None:
            for sens_rate in sensitivity[0]:
                for sens_growth in sensitivity[1]:
                    altered = _discounted(
                        config, scenario, price, circ0, fd0, sens_rate, sens_growth
                    )
                    sensitivity_rows.append(
                        {
                            "schema_version": 1,
                            "run_id": run_id,
                            "scenario": name,
                            "discount_rate": sens_rate,
                            "terminal_growth": sens_growth,
                            "per_token_value_fd": altered.aggregate * vs / fd_raw,
                            "currency": config.currency,
                            "token_unit": config.token_unit,
                            "per_token_unit": price_unit,
                        }
                    )
    frames: dict[str, pl.DataFrame] = {
        "model.token-inputs": pl.DataFrame(
            input_rows, schema=MODEL_TOKEN_INPUTS_V1.schema
        ),
        "model.token-supply-path": pl.DataFrame(
            supply_rows, schema=MODEL_TOKEN_SUPPLY_PATH_V1.schema
        ),
        "model.token-cashflows": pl.DataFrame(
            cash_rows, schema=MODEL_TOKEN_CASHFLOWS_V1.schema
        ),
        "model.token-peer-multiples": pl.DataFrame(
            peers, schema=MODEL_TOKEN_PEER_MULTIPLES_V1.schema
        ),
        "model.token-results": pl.DataFrame(
            result_rows, schema=MODEL_TOKEN_RESULTS_V1.schema
        ),
        "model.token-reconciliation": pl.DataFrame(
            check_rows, schema=MODEL_TOKEN_RECONCILIATION_V1.schema
        ),
    }
    if sensitivity_rows:
        frames["model.token-sensitivity"] = pl.DataFrame(
            sensitivity_rows, schema=MODEL_TOKEN_SENSITIVITY_V1.schema
        )
    for kind, table in frames.items():
        contract = {
            "model.token-inputs": MODEL_TOKEN_INPUTS_V1,
            "model.token-supply-path": MODEL_TOKEN_SUPPLY_PATH_V1,
            "model.token-cashflows": MODEL_TOKEN_CASHFLOWS_V1,
            "model.token-peer-multiples": MODEL_TOKEN_PEER_MULTIPLES_V1,
            "model.token-results": MODEL_TOKEN_RESULTS_V1,
            "model.token-reconciliation": MODEL_TOKEN_RECONCILIATION_V1,
            "model.token-sensitivity": MODEL_TOKEN_SENSITIVITY_V1,
        }[kind]
        contract.validate(table)
    return frames


def token_input_hashes(
    case_dir: Path, input_path: Path, input_bytes: bytes
) -> tuple[InputFileHash, ...]:
    relative = input_path.resolve().relative_to(case_dir.resolve()).as_posix()
    return (
        InputFileHash(
            name="file.token-inputs",
            path=relative,
            sha256=hashlib.sha256(input_bytes).hexdigest(),
        ),
    ) + _register_hashes(case_dir)


def token_run_id(
    input_sha256: str,
    as_of: date,
    selected: tuple[str, ...],
    sensitivity: tuple[tuple[float, ...], tuple[float, ...]] | None,
    hashes: tuple[InputFileHash, ...],
    bindings: tuple[tuple[str, str], ...],
) -> str:
    return canonical_parameters_sha256(
        {
            "family": "token",
            "input_sha256": input_sha256,
            "as_of": as_of.isoformat(),
            "scenario": list(selected),
            "sensitivity": sensitivity,
            "producer": MODEL_PRODUCER,
            "producer_version": MODEL_PRODUCER_VERSION,
            "input_hashes": [(item.path, item.sha256) for item in hashes],
            "artifact_hashes": [list(item) for item in bindings],
        }
    )


def run_token(
    workspace: Path,
    case_id: str,
    *,
    input_path: str = "analysis/token-inputs.toml",
    scenario: Literal["bear", "base", "bull", "all"] = "all",
    sensitivity: tuple[tuple[float, ...], tuple[float, ...]] | None = None,
) -> ModelRun:
    """Execute deterministic token artifacts from strict input and case data."""
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "auditable models require manifest v2; run case migrate first"
        )
    if scenario not in {*_SCENARIOS, "all"}:
        raise IngestionError("scenario must be bear, base, bull, or all")
    config, input_bytes, actual_input = load_token_input(case_dir, input_path)
    sources = load_model_sources(case_dir, as_of=config.as_of)
    validate_token_sources(config, sources)
    selected = _SCENARIOS if scenario == "all" else (scenario,)
    if sensitivity is not None:
        sensitivity = _canonicalize_dcf_sensitivity(sensitivity, "gordon_growth")
    data = load_token_data(case_dir, manifest, config, verify_hashes=True)
    extra = token_input_hashes(case_dir, actual_input, input_bytes)
    run_id = token_run_id(
        hashlib.sha256(input_bytes).hexdigest(),
        config.as_of,
        selected,
        sensitivity,
        extra,
        data.parent_bindings,
    )
    frames = build_token_frames(config, data, run_id, selected, sensitivity)
    failed = frames["model.token-reconciliation"].filter(pl.col("status") == "failed")
    if failed.height:
        raise CaseContractError(
            f"token reconciliation failed: {failed['check'].to_list()}"
        )
    return publish_token_run(
        case_dir, manifest, config, frames, run_id, data.parents, extra
    )


def publish_token_run(
    case_dir: Path,
    manifest: CaseManifest,
    config: TokenConfig,
    frames: dict[str, pl.DataFrame],
    run_id: str,
    parents: tuple[str, ...],
    extra: tuple[InputFileHash, ...],
) -> ModelRun:
    contracts = {
        "model.token-inputs": MODEL_TOKEN_INPUTS_V1,
        "model.token-supply-path": MODEL_TOKEN_SUPPLY_PATH_V1,
        "model.token-cashflows": MODEL_TOKEN_CASHFLOWS_V1,
        "model.token-peer-multiples": MODEL_TOKEN_PEER_MULTIPLES_V1,
        "model.token-results": MODEL_TOKEN_RESULTS_V1,
        "model.token-sensitivity": MODEL_TOKEN_SENSITIVITY_V1,
        "model.token-reconciliation": MODEL_TOKEN_RECONCILIATION_V1,
    }
    receipts = []
    ids: dict[str, str] = {}
    for kind, label, parent_kinds in TOKEN_PUBLICATION_ORDER:
        if kind not in frames:
            continue
        parent_ids = (
            parents
            if kind == "model.token-inputs"
            else tuple(ids[parent] for parent in parent_kinds)
        )
        receipt = _publish(
            case_dir,
            manifest,
            contracts[kind],
            run_id,
            label,
            frames[kind],
            parent_ids,
            config.as_of,
            extra,
        )
        ids[kind] = receipt.artifact_id
        receipts.append(receipt)
    return ModelRun(run_id, tuple(receipts))


# (kind, label, parent kinds) in publication order.
TOKEN_PUBLICATION_ORDER: Final[tuple[tuple[str, str, tuple[str, ...]], ...]] = (
    ("model.token-inputs", "inputs", ()),
    ("model.token-supply-path", "supply-path", ("model.token-inputs",)),
    ("model.token-cashflows", "cashflows", ("model.token-inputs",)),
    ("model.token-peer-multiples", "peer-multiples", ("model.token-inputs",)),
    (
        "model.token-results",
        "results",
        (
            "model.token-supply-path",
            "model.token-cashflows",
            "model.token-peer-multiples",
        ),
    ),
    ("model.token-sensitivity", "sensitivity", ("model.token-results",)),
    ("model.token-reconciliation", "reconciliation", ("model.token-results",)),
)


def declared_token_input_hashes(
    case_dir: Path,
    records: tuple[InputFileHash, ...],
    *,
    verify_hashes: bool,
) -> tuple[InputFileHash, ...]:
    _validate_declared_input_files(case_dir, records, verify_hashes=verify_hashes)
    records = tuple(r for r in records if not r.name.startswith("artifact."))
    by_name = {record.name: record for record in records}
    if sum(r.name == "file.token-inputs" for r in records) != 1:
        raise CaseContractError(
            "token run must declare exactly one file.token-inputs hash"
        )
    expected = _expected_register_paths(case_dir)
    if set(by_name) != {"file.token-inputs", *expected}:
        raise CaseContractError("token input artifact has unexpected input file hashes")
    _require_register_paths(by_name, expected)
    return records


def authenticate_token_run(
    case_dir: Path,
    manifest: CaseManifest,
    artifacts: tuple[Artifact, ...],
    tables: tuple[tuple[str, pl.DataFrame], ...],
    run_id: str,
    *,
    verify_hashes: bool,
) -> date:
    """Reproduce a registered token run from its TOML, registers, and artifacts."""
    from finresearch.modeling import (
        _require_exact_model_inputs,
        _require_rebuilt_tables,
        _table_for_kind,
    )

    inputs = next(a for a in artifacts if a.kind == "model.token-inputs")
    record = next(
        (r for r in inputs.input_file_hashes if r.name == "file.token-inputs"), None
    )
    if record is None:
        raise CaseContractError(
            "token run must declare exactly one file.token-inputs hash"
        )
    input_path = resolve_relative_path(case_dir, record.path, "token run input")
    if not input_path.is_file():
        raise CaseContractError("token run input TOML is missing")
    input_bytes = input_path.read_bytes()
    if verify_hashes and hashlib.sha256(input_bytes).hexdigest() != record.sha256:
        raise CaseContractError(
            "token run input TOML sha256 does not match declaration"
        )
    config, parsed_bytes, parsed_path = load_token_input(case_dir, record.path)
    if parsed_path != input_path or parsed_bytes != input_bytes:
        raise CaseContractError(
            "token run input TOML bytes changed while authenticating"
        )
    extra = declared_token_input_hashes(
        case_dir, inputs.input_file_hashes, verify_hashes=verify_hashes
    )
    input_frame = _table_for_kind(tables, "model.token-inputs")
    scenarios = tuple(
        s for s in _SCENARIOS if s in set(input_frame["scenario"].to_list())
    )
    sens_frame = next((f for k, f in tables if k == "model.token-sensitivity"), None)
    sensitivity = None
    if sens_frame is not None:
        sensitivity = (
            tuple(sorted(set(sens_frame["discount_rate"].to_list()))),
            tuple(sorted(set(sens_frame["terminal_growth"].to_list()))),
        )
    data = load_token_data(case_dir, manifest, config, verify_hashes=verify_hashes)
    if (
        token_run_id(
            record.sha256,
            config.as_of,
            scenarios,
            sensitivity,
            extra,
            data.parent_bindings,
        )
        != run_id
    ):
        raise CaseContractError(
            "token run id does not match TOML, registers, artifacts, and outputs"
        )
    id_of = {a.kind: a.artifact_id for a in artifacts}
    expected_parents = {
        kind: (
            data.parents
            if kind == "model.token-inputs"
            else tuple(id_of[p] for p in parent_kinds)
        )
        for kind, _, parent_kinds in TOKEN_PUBLICATION_ORDER
    }
    _require_exact_model_inputs(
        case_dir,
        manifest,
        artifacts,
        expected_parents,
        extra,
        "token",
        verify_hashes=verify_hashes,
    )
    sources = load_model_sources(case_dir, as_of=config.as_of)
    validate_token_sources(config, sources)
    expected_frames = build_token_frames(config, data, run_id, scenarios, sensitivity)
    _require_rebuilt_tables(tables, expected_frames, "token")
    return config.as_of
