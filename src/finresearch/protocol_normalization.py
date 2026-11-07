"""Deterministic protocol-metric normalization from provider captures."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, urlsplit

import polars as pl

from finresearch.captures import LoadedCapture, case_context, load_capture
from finresearch.cases import CaseManifest, canonical_parameters_sha256
from finresearch.data_contracts import (
    NORMALIZED_PROTOCOL_METRICS_V1,
    NORMALIZED_PROVIDER_METRIC_DEFINITIONS_V1,
    DataContractError,
    DatasetContract,
)
from finresearch.ingestion import (
    IngestionError,
    IngestionReceipt,
    publish_snapshot,
    symbol_key,
)
from finresearch.json_values import json_number, json_object

PROTOCOL_PRODUCER: Final = "finresearch.data.normalize-protocol"
PROTOCOL_PRODUCER_VERSION: Final = "1"
SUPPORTED_ENDPOINTS: Final = (
    "defillama GET /summary/fees/{slug}?dataType=dailyFees|dailyRevenue|"
    "dailyHoldersRevenue",
    "defillama GET /summary/dexs/{slug}?dataType=dailyVolume",
    "defillama GET /summary/open-interest/{slug}",
    "hyperliquid POST /info type=metaAndAssetCtxs",
)
# (endpoint group, dataType) -> (metric, provider field, methodology keys)
_DEFILLAMA_SPECS: Final[
    dict[tuple[str, str | None], tuple[str, str, tuple[str, ...]]]
] = {
    ("fees", "dailyFees"): ("fees", "dailyFees", ("Fees", "UserFees")),
    ("fees", "dailyRevenue"): ("revenue", "dailyRevenue", ("Revenue",)),
    ("fees", "dailyHoldersRevenue"): (
        "holders_revenue",
        "dailyHoldersRevenue",
        ("HoldersRevenue",),
    ),
    ("dexs", "dailyVolume"): ("volume_spot", "dailyVolume", ("Volume",)),
    ("open-interest", None): ("open_interest", "openInterest", ("OpenInterest",)),
}
_HYPERLIQUID_DEFINITIONS: Final = {
    "open_interest": (
        "openInterest_x_markPx",
        "Notional open interest: openInterest times markPx from the same "
        "metaAndAssetCtxs response, in USD.",
    ),
    "funding_rate_1h": (
        "funding",
        "Hourly funding rate in basis points: funding times 10,000.",
    ),
    "volume_perp": (
        "dayNtlVlm",
        "Rolling 24 hour notional volume in USD at retrieval; a snapshot, not "
        "a calendar-day total.",
    ),
}


@dataclass(frozen=True)
class ProtocolNormalizationReceipt:
    """Artifact pair published for one set of captures."""

    metrics: IngestionReceipt
    definitions: IngestionReceipt


@dataclass(frozen=True)
class _Definition:
    provider: str
    protocol: str
    metric: str
    provider_field: str
    text: str | None
    url: str | None

    @property
    def sha256(self) -> str:
        canonical = json.dumps(
            {
                "provider_field": self.provider_field,
                "methodology_text": self.text,
                "methodology_url": self.url,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def identifier(self) -> str:
        return (
            f"{self.provider}.{self.protocol}.{self.provider_field}.{self.sha256[:12]}"
        )


Parsed = tuple[list[dict[str, object]], list[_Definition]]
Parser = Callable[[LoadedCapture, object], Parsed]


def normalize_protocol_metrics(
    workspace: Path,
    case_id: str,
    *,
    capture_artifact_ids: tuple[str, ...],
    entity_id: str,
    instrument_id: str | None = None,
) -> ProtocolNormalizationReceipt:
    """Parse captures into one protocol-metrics and definitions artifact pair."""
    if not capture_artifact_ids or len(set(capture_artifact_ids)) != len(
        capture_artifact_ids
    ):
        raise IngestionError("pass each --capture-artifact-id at least once, once")
    for label, value in (("entity id", entity_id), ("instrument id", instrument_id)):
        if value is not None and (not value or any(c.isspace() for c in value)):
            raise IngestionError(f"{label} must be non-empty without spaces")
    case_dir, manifest = case_context(workspace, case_id)

    metric_rows: list[dict[str, object]] = []
    definitions: dict[str, tuple[_Definition, LoadedCapture]] = {}
    captures: list[LoadedCapture] = []
    for capture_id in capture_artifact_ids:
        capture = load_capture(case_dir, manifest, capture_id)
        captures.append(capture)
        try:
            payload = json.loads(capture.body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise IngestionError(f"capture {capture_id} is not JSON: {exc}") from exc
        rows, found = _select_parser(capture)(capture, payload)
        retrieval_date = capture.retrieved_at.date()
        for row in rows:
            period_end = row["period_end"]
            if not isinstance(period_end, date):
                raise IngestionError("parser produced a row without period_end")
            row.update(
                {
                    "schema_version": NORMALIZED_PROTOCOL_METRICS_V1.version,
                    "entity_id": entity_id,
                    "instrument_id": instrument_id,
                    "is_complete": period_end < retrieval_date,
                    "capture_artifact_id": capture_id,
                    "source_artifact_id": capture_id,
                    "normalized_at": capture.retrieved_at,
                }
            )
        metric_rows.extend(rows)
        for definition in found:
            definitions.setdefault(definition.identifier, (definition, capture))
    if not metric_rows:
        raise IngestionError("the captures yield no protocol metric rows")

    definition_rows: list[dict[str, object]] = [
        {
            "schema_version": NORMALIZED_PROVIDER_METRIC_DEFINITIONS_V1.version,
            "provider": definition.provider,
            "provider_definition_id": identifier,
            "metric": definition.metric,
            "provider_field": definition.provider_field,
            "methodology_text": definition.text,
            "methodology_url": definition.url,
            "definition_sha256": definition.sha256,
            "observed_at": capture.retrieved_at,
            "source_artifact_id": capture.capture_id,
        }
        for identifier, (definition, capture) in sorted(definitions.items())
    ]
    declared = {str(row["provider_definition_id"]) for row in definition_rows}
    unresolved = sorted(
        {str(r["provider_definition_id"]) for r in metric_rows} - declared
    )
    if unresolved:
        raise IngestionError(f"metrics cite undefined definitions: {unresolved}")

    produced_at = max(capture.retrieved_at for capture in captures)
    parameters = {
        "entity_id": entity_id,
        "input_artifact_ids": list(capture_artifact_ids),
        "instrument_id": instrument_id,
        "produced_at": produced_at.isoformat(),
        "producer": PROTOCOL_PRODUCER,
        "producer_version": PROTOCOL_PRODUCER_VERSION,
    }
    entity = symbol_key(entity_id)
    definitions_receipt = _publish(
        case_dir,
        manifest,
        NORMALIZED_PROVIDER_METRIC_DEFINITIONS_V1,
        definition_rows,
        capture_artifact_ids,
        entity,
        parameters,
        produced_at,
    )
    metrics_receipt = _publish(
        case_dir,
        manifest,
        NORMALIZED_PROTOCOL_METRICS_V1,
        metric_rows,
        capture_artifact_ids,
        entity,
        parameters,
        produced_at,
    )
    return ProtocolNormalizationReceipt(metrics_receipt, definitions_receipt)


def _publish(
    case_dir: Path,
    manifest: CaseManifest,
    contract: DatasetContract,
    rows: list[dict[str, object]],
    parents: tuple[str, ...],
    entity: str,
    parameters: Mapping[str, object],
    produced_at: datetime,
) -> IngestionReceipt:
    try:
        frame = contract.canonical_sort(pl.DataFrame(rows, schema=contract.schema))
        contract.validate(frame)
    except (DataContractError, pl.exceptions.PolarsError) as exc:
        raise IngestionError(f"captures cannot be normalized: {exc}") from exc
    identity = canonical_parameters_sha256(
        {**parameters, "contract": contract.identifier}
    )
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=frame,
        contract=contract,
        path_parts=(contract.name, entity),
        entity_key=entity,
        identity=identity,
        producer=PROTOCOL_PRODUCER,
        producer_version=PROTOCOL_PRODUCER_VERSION,
        parameters_sha256=identity,
        input_artifact_ids=parents,
        produced_at=produced_at,
    )


def _select_parser(capture: LoadedCapture) -> Parser:
    if capture.provider == "defillama" and capture.metadata["request_method"] == "GET":
        url = urlsplit(capture.url)
        parts = url.path.strip("/").split("/")
        data_type = parse_qs(url.query).get("dataType", [None])[0]
        if (
            len(parts) == 3
            and parts[0] == "summary"
            and (parts[1], data_type) in _DEFILLAMA_SPECS
        ):
            return _parse_defillama
    if (
        capture.provider == "hyperliquid"
        and capture.request_body is not None
        and json.loads(capture.request_body).get("type") == "metaAndAssetCtxs"
    ):
        return _parse_hyperliquid_asset_ctxs
    raise IngestionError(
        f"capture {capture.capture_id} ({capture.provider} {capture.url}) has no "
        "protocol metric parser; supported endpoints: " + "; ".join(SUPPORTED_ENDPOINTS)
    )


def _bucket(value: object, label: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IngestionError(f"{label} must be a unix timestamp: {value!r}")
    try:
        return datetime.fromtimestamp(int(value), UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise IngestionError(f"{label} is not a valid timestamp: {value!r}") from exc


def _parse_defillama(capture: LoadedCapture, payload: object) -> Parsed:
    document = json_object(payload, "DefiLlama summary response")
    url = urlsplit(capture.url)
    group, slug = url.path.strip("/").split("/")[1:3]
    data_type = parse_qs(url.query).get("dataType", [None])[0]
    metric, provider_field, method_keys = _DEFILLAMA_SPECS[(group, data_type)]
    methodology = document.get("methodology")
    text: str | None = None
    if isinstance(methodology, dict):
        text = next(
            (
                methodology[key]
                for key in method_keys
                if isinstance(methodology.get(key), str)
            ),
            None,
        )
    method_url = document.get("methodologyURL")
    definition = _Definition(
        "defillama",
        slug,
        metric,
        provider_field,
        text,
        method_url if isinstance(method_url, str) else None,
    )
    instant = metric == "open_interest"

    def row(
        chain: str | None,
        sub_protocol: str | None,
        point_time: object,
        value: float,
        label: str,
    ) -> dict[str, object]:
        observed_at = _bucket(point_time, f"{label}[0]")
        return {
            "provider": "defillama",
            "provider_protocol_id": slug,
            "sub_protocol": sub_protocol,
            "chain": chain,
            "metric": metric,
            "period_type": "instant" if instant else "duration",
            "period_start": None if instant else observed_at.date(),
            "period_end": observed_at.date(),
            "value": value,
            "unit": "USD",
            "currency": "USD",
            "provider_definition_id": definition.identifier,
            "observed_at": observed_at,
        }

    chart = document.get("totalDataChart")
    if not isinstance(chart, list) or not chart:
        raise IngestionError("DefiLlama response has no totalDataChart")
    rows: list[dict[str, object]] = []
    for index, point in enumerate(chart):
        label = f"totalDataChart[{index}]"
        if not isinstance(point, list) or len(point) != 2:
            raise IngestionError(f"{label} must be [timestamp, value]")
        value = json_number(point[1], f"{label}[1]")
        if value is not None:
            rows.append(row(None, None, point[0], value, label))
    breakdown = document.get("totalDataChartBreakdown") or []
    if not isinstance(breakdown, list):
        raise IngestionError("totalDataChartBreakdown must be an array")
    for index, point in enumerate(breakdown):
        label = f"totalDataChartBreakdown[{index}]"
        if not (
            isinstance(point, list) and len(point) == 2 and isinstance(point[1], dict)
        ):
            raise IngestionError(f"{label} must be [timestamp, {{child: value}}]")
        for key, raw_value in sorted(point[1].items()):
            if isinstance(raw_value, dict):
                # chain -> sub-protocol -> value; chains are never summed.
                for child, inner in sorted(raw_value.items()):
                    if isinstance(inner, dict):
                        raise IngestionError(
                            f"{label}.{key}.{child} has an unsupported nested value"
                        )
                    value = json_number(inner, f"{label}.{key}.{child}")
                    if value is not None:
                        rows.append(row(str(key), str(child), point[0], value, label))
                continue
            value = json_number(raw_value, f"{label}.{key}")
            if value is not None:
                rows.append(row(None, str(key), point[0], value, label))
    return rows, [definition]


def _parse_hyperliquid_asset_ctxs(capture: LoadedCapture, payload: object) -> Parsed:
    if not (isinstance(payload, list) and len(payload) == 2):
        raise IngestionError("metaAndAssetCtxs response must be [meta, assetCtxs]")
    universe = json_object(payload[0], "metaAndAssetCtxs meta").get("universe")
    contexts = payload[1]
    if (
        not isinstance(universe, list)
        or not isinstance(contexts, list)
        or len(universe) != len(contexts)
    ):
        raise IngestionError("metaAndAssetCtxs universe and contexts must align")
    definitions = {
        metric: _Definition("hyperliquid", "hyperliquid", metric, field, text, None)
        for metric, (field, text) in _HYPERLIQUID_DEFINITIONS.items()
    }
    retrieved = capture.retrieved_at
    window_start = (retrieved - timedelta(hours=24)).date()
    rows: list[dict[str, object]] = []
    for index, (market, context) in enumerate(zip(universe, contexts, strict=True)):
        info = json_object(market, f"universe[{index}]")
        values = json_object(context, f"assetCtxs[{index}]")
        name = info.get("name")
        if not isinstance(name, str) or not name:
            raise IngestionError(f"universe[{index}].name must be a non-empty string")
        if info.get("isDelisted") is True:
            continue
        open_interest = json_number(values.get("openInterest"), f"{name}.openInterest")
        mark = json_number(values.get("markPx"), f"{name}.markPx")
        funding = json_number(values.get("funding"), f"{name}.funding")
        volume = json_number(values.get("dayNtlVlm"), f"{name}.dayNtlVlm")
        derived: list[tuple[str, float, str, bool]] = []
        if open_interest is not None and mark is not None:
            derived.append(("open_interest", open_interest * mark, "USD", True))
        if funding is not None:
            derived.append(("funding_rate_1h", funding * 10_000, "bps", True))
        if volume is not None:
            derived.append(("volume_perp", volume, "USD", False))
        for metric, value, unit, instant in derived:
            rows.append(
                {
                    "provider": "hyperliquid",
                    "provider_protocol_id": "hyperliquid",
                    "sub_protocol": name,
                    "chain": None,
                    "metric": metric,
                    "period_type": "instant" if instant else "duration",
                    "period_start": None if instant else window_start,
                    "period_end": retrieved.date(),
                    "value": value,
                    "unit": unit,
                    "currency": "USD" if unit == "USD" else None,
                    "provider_definition_id": definitions[metric].identifier,
                    "observed_at": retrieved,
                }
            )
    return rows, list(definitions.values())
