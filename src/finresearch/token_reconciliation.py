"""Cross-source token supply reconciliation under a strict case policy."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

import polars as pl

from finresearch.captures import case_context, require_manifest_v2
from finresearch.cases import (
    CaseContractError,
    InputFileHash,
    canonical_parameters_sha256,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1,
    NORMALIZED_TOKEN_SUPPLY_V1,
    SUPPLY_METRICS,
    DataContractError,
)
from finresearch.ingestion import (
    IngestionError,
    IngestionReceipt,
    publish_snapshot,
    symbol_key,
)
from finresearch.registers import load_model_sources

RECONCILE_PRODUCER: Final = "finresearch.data.reconcile-token-supply"
RECONCILE_PRODUCER_VERSION: Final = "1"
POLICY_INPUT_NAME: Final = "token-supply-policy"


@dataclass(frozen=True)
class Resolution:
    provider: str
    definition_id: str
    source_id: str


@dataclass(frozen=True)
class SupplyPolicy:
    provider_priority: tuple[str, ...]
    tolerance: dict[str, float]
    resolution: dict[str, Resolution]


def parse_supply_policy(content: bytes) -> SupplyPolicy:
    """Parse the strict policy TOML; unknown keys and bad types are errors."""
    try:
        data = tomllib.loads(content.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise IngestionError(f"invalid supply policy: {exc}") from exc
    unexpected = sorted(
        set(data) - {"version", "provider_priority", "tolerance", "resolution"}
    )
    if unexpected or "version" not in data or "provider_priority" not in data:
        raise IngestionError(
            "invalid supply policy: requires version and provider_priority and "
            f"allows only tolerance and resolution (unexpected: {unexpected})"
        )
    if data["version"] != 1 or isinstance(data["version"], bool):
        raise IngestionError("invalid supply policy: version must be 1")
    priority = data["provider_priority"]
    if (
        not isinstance(priority, list)
        or not priority
        or not all(isinstance(item, str) and item for item in priority)
        or len(set(priority)) != len(priority)
    ):
        raise IngestionError(
            "invalid supply policy: provider_priority must be unique non-empty strings"
        )
    tolerance_table = data.get("tolerance", {})
    resolution_table = data.get("resolution", {})
    if not isinstance(tolerance_table, dict) or not isinstance(resolution_table, dict):
        raise IngestionError(
            "invalid supply policy: tolerance and resolution are tables"
        )
    tolerance: dict[str, float] = {}
    for metric, value in tolerance_table.items():
        if (
            metric not in SUPPLY_METRICS
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value < 0
        ):
            raise IngestionError(
                f"invalid supply policy: tolerance.{metric} must be a nonnegative "
                "number for a supply metric"
            )
        tolerance[metric] = float(value)
    resolution: dict[str, Resolution] = {}
    for metric, entry in resolution_table.items():
        if (
            metric not in SUPPLY_METRICS
            or not isinstance(entry, dict)
            or set(entry) != {"provider", "definition_id", "source_id"}
            or not all(isinstance(value, str) and value for value in entry.values())
        ):
            raise IngestionError(
                f"invalid supply policy: resolution.{metric} needs exactly "
                "provider, definition_id, and source_id strings"
            )
        resolution[metric] = Resolution(
            entry["provider"], entry["definition_id"], entry["source_id"]
        )
    return SupplyPolicy(tuple(priority), tolerance, resolution)


def reconcile_token_supply(
    workspace: Path,
    case_id: str,
    *,
    instrument_id: str,
    as_of: date,
    policy_path: str,
    source_artifact_ids: tuple[str, ...] = (),
) -> IngestionReceipt:
    """Select one supply value per metric across providers, or record a conflict."""
    case_dir, manifest = case_context(workspace, case_id)
    require_manifest_v2(manifest, "reconcile-token-supply")
    try:
        policy_file = resolve_relative_path(case_dir, policy_path, "--policy")
        policy_bytes = policy_file.read_bytes()
    except (CaseContractError, OSError) as exc:
        raise IngestionError(
            f"cannot read supply policy {policy_path!r}: {exc}"
        ) from exc
    policy = parse_supply_policy(policy_bytes)
    policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()

    selected_sources = tuple(sorted(set(source_artifact_ids)))
    candidates = [
        artifact
        for artifact in manifest.artifacts
        if artifact.kind == NORMALIZED_TOKEN_SUPPLY_V1.name
        and artifact.schema_version == NORMALIZED_TOKEN_SUPPLY_V1.version
    ]
    declared = {artifact.artifact_id for artifact in candidates}
    for source_id in selected_sources:
        if source_id not in declared:
            raise IngestionError(
                f"not a declared normalized.token-supply.v1: {source_id}"
            )
    observations: list[dict[str, object]] = []
    for artifact in candidates:
        if selected_sources and artifact.artifact_id not in selected_sources:
            continue
        path = resolve_relative_path(case_dir, artifact.path, artifact.artifact_id)
        try:
            frame = pl.read_parquet(path)
            NORMALIZED_TOKEN_SUPPLY_V1.validate(frame)
        except (OSError, pl.exceptions.PolarsError, DataContractError) as exc:
            raise IngestionError(
                f"token-supply artifact {artifact.artifact_id} is invalid: {exc}"
            ) from exc
        for row in frame.iter_rows(named=True):
            if (
                row["instrument_id"] == instrument_id
                and row["holder_address"] is None
                and row["observed_at"].date() <= as_of
            ):
                observations.append({**row, "_artifact": artifact.artifact_id})
    if not observations:
        raise IngestionError(
            f"no token-supply observations for {instrument_id!r} on or before {as_of}"
        )

    latest = _latest_per_provider(observations)
    rows: list[dict[str, object]] = []
    used_resolutions: list[Resolution] = []
    for metric in sorted({metric for metric, _ in latest}):
        by_provider = {
            provider: row for (name, provider), row in latest.items() if name == metric
        }
        rows.extend(
            _reconcile_metric(
                metric, by_provider, policy, instrument_id, as_of, used_resolutions
            )
        )
    if used_resolutions:
        _check_sources(case_dir, as_of, used_resolutions)

    frame = pl.DataFrame(rows, schema=DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.schema)
    frame = DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.canonical_sort(frame)
    try:
        DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.validate(frame)
    except DataContractError as exc:
        raise IngestionError(f"reconciliation is invalid: {exc}") from exc
    parents = tuple(sorted({str(row["source_artifact_id"]) for row in rows}))
    parameters = {
        "as_of": as_of.isoformat(),
        "contract": DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.identifier,
        "input_artifact_ids": list(parents),
        "instrument_id": instrument_id,
        "policy_sha256": policy_sha256,
        "producer": RECONCILE_PRODUCER,
        "producer_version": RECONCILE_PRODUCER_VERSION,
    }
    identity = canonical_parameters_sha256(parameters)
    entity = symbol_key(instrument_id)
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="derived",
        frame=frame,
        contract=DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1,
        path_parts=(
            DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.name,
            entity,
            as_of.isoformat(),
        ),
        entity_key=entity,
        identity=identity,
        producer=RECONCILE_PRODUCER,
        producer_version=RECONCILE_PRODUCER_VERSION,
        parameters_sha256=identity,
        input_artifact_ids=parents,
        produced_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
        extra_input_file_hashes=(
            InputFileHash(
                name=POLICY_INPUT_NAME,
                path=policy_file.relative_to(case_dir.resolve()).as_posix(),
                sha256=policy_sha256,
            ),
        ),
    )


def _latest_per_provider(
    observations: list[dict[str, object]],
) -> dict[tuple[str, str], dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in observations:
        grouped.setdefault(
            (str(row["supply_metric"]), str(row["provider"])), []
        ).append(row)
    latest: dict[tuple[str, str], dict[str, object]] = {}
    for key, rows in grouped.items():
        newest = max(row["observed_at"] for row in rows)  # type: ignore[type-var]
        tied = [row for row in rows if row["observed_at"] == newest]
        if len({(row["amount"], row["definition_id"]) for row in tied}) > 1:
            raise IngestionError(
                f"equal-time token-supply conflict for {key[1]} {key[0]} at {newest}"
            )
        latest[key] = min(tied, key=lambda row: str(row["_artifact"]))
    return latest


def _reconcile_metric(
    metric: str,
    by_provider: dict[str, dict[str, object]],
    policy: SupplyPolicy,
    instrument_id: str,
    as_of: date,
    used_resolutions: list[Resolution],
) -> list[dict[str, object]]:
    amounts = {
        provider: float(str(row["amount"])) for provider, row in by_provider.items()
    }
    low, high = min(amounts.values()), max(amounts.values())
    if low == 0 and high > 0:
        raise IngestionError(
            f"cannot compute a relative spread for {metric}: minimum is zero"
        )
    spread = 0.0 if high == low else (high - low) / low
    tolerance = policy.tolerance.get(metric)
    selected_provider: str | None
    resolution: Resolution | None = None
    if len(by_provider) == 1:
        status, selected_provider = "single-source", next(iter(by_provider))
    else:
        if tolerance is None:
            raise IngestionError(
                f"{metric} is observed by {', '.join(sorted(by_provider))} but the "
                "policy has no tolerance entry for it"
            )
        if spread <= tolerance:
            status = "agree"
            selected_provider = next(
                (name for name in policy.provider_priority if name in by_provider), None
            )
            if selected_provider is None:
                raise IngestionError(
                    f"provider_priority names none of {', '.join(sorted(by_provider))} "
                    f"for {metric}"
                )
        elif metric in policy.resolution:
            resolution = policy.resolution[metric]
            chosen = by_provider.get(resolution.provider)
            if chosen is None or chosen["definition_id"] != resolution.definition_id:
                raise IngestionError(
                    f"resolution.{metric} names an absent observation: "
                    f"{resolution.provider} {resolution.definition_id}"
                )
            status, selected_provider = "resolved", resolution.provider
            used_resolutions.append(resolution)
        else:
            status, selected_provider = "conflict", None
    return [
        {
            "schema_version": DERIVED_TOKEN_SUPPLY_RECONCILIATION_V1.version,
            "instrument_id": instrument_id,
            "as_of": as_of,
            "supply_metric": metric,
            "provider": provider,
            "definition_id": row["definition_id"],
            "observed_at": row["observed_at"],
            "amount": amounts[provider],
            "min_amount": low,
            "max_amount": high,
            "spread": spread,
            "tolerance": 0.0 if tolerance is None else tolerance,
            "status": status,
            "selected": provider == selected_provider,
            "resolution_source_id": resolution.source_id if resolution else None,
            "source_artifact_id": row["_artifact"],
        }
        for provider, row in sorted(by_provider.items())
    ]


def _check_sources(case_dir: Path, as_of: date, resolutions: list[Resolution]) -> None:
    try:
        sources = load_model_sources(case_dir, as_of=as_of)
    except CaseContractError as exc:
        raise IngestionError(str(exc)) from exc
    for resolution in resolutions:
        source = sources.get(resolution.source_id)
        if source is None or source.effective_date > as_of:
            raise IngestionError(
                f"resolution source_id {resolution.source_id!r} is not an evidence "
                f"or assumption id dated on or before {as_of.isoformat()}"
            )
