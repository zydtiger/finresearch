"""Deterministic raw-to-normalized transformations with manifest lineage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, cast

import polars as pl

from finresearch.cases import (
    MANIFEST_V1,
    Artifact,
    CaseContractError,
    CaseManifest,
    canonical_parameters_sha256,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    DERIVED_INSTRUMENT_MASTER_CURRENT_V1,
    NORMALIZED_CORPORATE_ACTIONS_V1,
    NORMALIZED_DAILY_PRICES_V2,
    NORMALIZED_FUNDAMENTAL_FACTS_V2,
    NORMALIZED_INSTRUMENT_MASTER_V2,
    RAW_SEC_COMPANYFACTS_V1,
    RAW_YFINANCE_DAILY_PRICES_V1,
    DataContractError,
    DatasetContract,
)
from finresearch.ingestion import (
    IngestionError,
    IngestionReceipt,
    publish_snapshot,
    symbol_key,
)

# Currency is provider data captured at retrieval time; normalization only
# carries it from the raw snapshot.
DAILY_PRICES_KIND: Final = "raw.yfinance.daily-prices"
PRICE_BASIS: Final = "unadjusted"
COMPANYFACTS_KIND: Final = "raw.sec.companyfacts"
# value types that carry a numeric JSON scalar in value_text
_NUMERIC_VALUE_TYPES: Final = ("integer", "number")
NORMALIZATION_PRODUCER_VERSION: Final = "3"
_CANONICAL_METRICS: Final[dict[tuple[str, str], tuple[str, str]]] = {
    ("us-gaap", "Revenues"): ("income-statement", "revenue"),
    ("us-gaap", "SalesRevenueNet"): ("income-statement", "revenue"),
    ("us-gaap", "NetIncomeLoss"): ("income-statement", "net-income"),
    ("us-gaap", "Assets"): ("balance-sheet", "assets"),
    ("us-gaap", "Liabilities"): ("balance-sheet", "liabilities"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"): (
        "balance-sheet",
        "cash-and-equivalents",
    ),
}


@dataclass(frozen=True)
class NormalizationReceipt:
    """Persisted normalized artifacts for one raw snapshot."""

    instrument_master: IngestionReceipt
    daily_prices: IngestionReceipt
    corporate_actions: IngestionReceipt


def normalize_daily_prices(
    workspace: Path,
    case_id: str,
    symbol: str,
    *,
    raw_artifact_id: str | None = None,
    normalized_at: datetime | None = None,
) -> NormalizationReceipt:
    """Derive instrument-master and daily-prices artifacts from one raw snapshot."""
    requested_symbol = symbol.strip()
    if not requested_symbol:
        raise IngestionError("symbol must not be empty")

    case_dir, manifest, normalized_root = _case_normalized_context(workspace, case_id)
    raw_artifact = _select_raw_artifact(manifest, requested_symbol, raw_artifact_id)
    raw_frame = _read_raw_snapshot(case_dir, raw_artifact)
    RAW_YFINANCE_DAILY_PRICES_V1.validate(raw_frame)
    provider_symbol = _validate_raw_price_snapshot(
        raw_frame,
        requested_symbol,
        raw_artifact,
    )

    timestamp = _normalization_time(normalized_at, raw_artifact, raw_frame)
    instrument_id = f"yfinance.{symbol_key(provider_symbol)}"
    currency = _require_constant(raw_frame, "currency", raw_artifact.artifact_id)

    master_frame = _instrument_master_frame(
        raw_frame,
        instrument_id=instrument_id,
        provider_symbol=provider_symbol,
        currency=currency,
        source_artifact_id=raw_artifact.artifact_id,
        normalized_at=timestamp,
    )
    prices_frame = _daily_prices_frame(
        raw_frame,
        instrument_id=instrument_id,
        provider_symbol=provider_symbol,
        currency=currency,
        source_artifact_id=raw_artifact.artifact_id,
        normalized_at=timestamp,
    )
    actions_frame = _corporate_actions_frame(
        raw_frame,
        instrument_id=instrument_id,
        provider_symbol=provider_symbol,
        currency=currency,
        source_artifact_id=raw_artifact.artifact_id,
        normalized_at=timestamp,
    )
    master_frame = NORMALIZED_INSTRUMENT_MASTER_V2.canonical_sort(master_frame)
    prices_frame = NORMALIZED_DAILY_PRICES_V2.canonical_sort(prices_frame)
    actions_frame = NORMALIZED_CORPORATE_ACTIONS_V1.canonical_sort(actions_frame)
    try:
        NORMALIZED_INSTRUMENT_MASTER_V2.validate(master_frame)
        NORMALIZED_DAILY_PRICES_V2.validate(prices_frame)
        NORMALIZED_CORPORATE_ACTIONS_V1.validate(actions_frame)
    except DataContractError as exc:
        raise IngestionError(
            f"raw snapshot {raw_artifact.artifact_id} cannot be normalized: {exc}"
        ) from exc

    # The master registers first. If the price publication is interrupted, a
    # rerun recognizes the identical master receipt and completes the pair.
    master_receipt = _publish_normalized_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=master_frame,
        contract=NORMALIZED_INSTRUMENT_MASTER_V2,
        path_parts=("normalized.instrument-master", instrument_id),
        entity_key=instrument_id,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
    )
    prices_receipt = _publish_normalized_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=prices_frame,
        contract=NORMALIZED_DAILY_PRICES_V2,
        path_parts=("normalized.daily-prices", instrument_id),
        entity_key=instrument_id,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
    )
    actions_receipt = _publish_normalized_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=actions_frame,
        contract=NORMALIZED_CORPORATE_ACTIONS_V1,
        path_parts=("normalized.corporate-actions", instrument_id),
        entity_key=instrument_id,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
    )
    return NormalizationReceipt(
        instrument_master=master_receipt,
        daily_prices=prices_receipt,
        corporate_actions=actions_receipt,
    )


def normalize_fundamental_facts(
    workspace: Path,
    case_id: str,
    cik: str,
    *,
    raw_artifact_id: str | None = None,
    normalized_at: datetime | None = None,
) -> IngestionReceipt:
    """Derive one parsed fundamental-facts table from one raw SEC snapshot."""
    from finresearch.providers.sec import normalize_cik

    normalized_cik = normalize_cik(cik)
    case_dir, manifest, _ = _case_normalized_context(workspace, case_id)
    raw_artifact = _select_companyfacts_artifact(
        manifest,
        normalized_cik,
        raw_artifact_id,
    )
    raw_frame = _read_raw_snapshot(case_dir, raw_artifact)
    RAW_SEC_COMPANYFACTS_V1.validate(raw_frame)

    timestamp = _normalization_time(normalized_at, raw_artifact, raw_frame)
    facts_frame = _fundamental_facts_frame(
        raw_frame,
        normalized_cik=normalized_cik,
        source_artifact_id=raw_artifact.artifact_id,
        normalized_at=timestamp,
    )
    facts_frame = NORMALIZED_FUNDAMENTAL_FACTS_V2.canonical_sort(facts_frame)
    try:
        NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(facts_frame)
    except DataContractError as exc:
        raise IngestionError(
            f"raw snapshot {raw_artifact.artifact_id} cannot be normalized: {exc}"
        ) from exc
    return _publish_normalized_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=facts_frame,
        contract=NORMALIZED_FUNDAMENTAL_FACTS_V2,
        path_parts=("normalized.fundamental-facts", normalized_cik),
        entity_key=normalized_cik,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
    )


def reconcile_instrument_master(
    workspace: Path,
    case_id: str,
    *,
    as_of: date,
    source_artifact_ids: tuple[str, ...] = (),
) -> IngestionReceipt:
    """Derive one deterministic current-state master from v2 observations."""
    case_dir, manifest, _ = _case_normalized_context(workspace, case_id)
    canonical_sources = tuple(sorted(set(source_artifact_ids)))
    selected_sources = set(canonical_sources)
    observations: list[dict[str, object]] = []
    for artifact in manifest.artifacts:
        if (
            artifact.kind != NORMALIZED_INSTRUMENT_MASTER_V2.name
            or artifact.schema_version != NORMALIZED_INSTRUMENT_MASTER_V2.version
        ):
            continue
        frame = _read_raw_snapshot(case_dir, artifact)
        try:
            NORMALIZED_INSTRUMENT_MASTER_V2.validate(frame)
        except DataContractError as exc:
            raise IngestionError(
                f"instrument-master artifact {artifact.artifact_id} is invalid: {exc}"
            ) from exc
        rows = [
            row
            for row in frame.to_dicts()
            if not selected_sources or row["source_artifact_id"] in selected_sources
        ]
        if not rows:
            continue
        for row in rows:
            row["_master_artifact_id"] = artifact.artifact_id
        observations.extend(rows)
    if not observations:
        raise IngestionError(
            "no v2 instrument-master observations match reconciliation"
        )

    selected: list[dict[str, object]] = []
    contributing_parent_ids: set[str] = set()
    by_instrument: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in observations:
        valid_from = row["valid_from"]
        valid_to = row["valid_to"]
        observed_at = row["observed_at"]
        if (
            not isinstance(observed_at, datetime)
            or observed_at.date() > as_of
            or (isinstance(valid_from, date) and valid_from > as_of)
            or (isinstance(valid_to, date) and valid_to < as_of)
        ):
            continue
        key = (str(row["provider"]), str(row["instrument_id"]))
        by_instrument.setdefault(key, []).append(row)
    if not by_instrument:
        raise IngestionError(
            "no instrument-master observations are valid at requested as_of"
        )
    fields = tuple(NORMALIZED_INSTRUMENT_MASTER_V2.schema)
    for key, rows in by_instrument.items():
        latest = max(cast(datetime, row["observed_at"]) for row in rows)
        candidates = [row for row in rows if row["observed_at"] == latest]
        representative = dict(candidates[0])
        for field in fields:
            if field == "source_artifact_id":
                continue
            values = {row[field] for row in candidates if row[field] is not None}
            if len(values) > 1:
                raise IngestionError(
                    "equal-time instrument-master conflict for "
                    f"{key[0]}:{key[1]} field {field}"
                )
            if values:
                representative[field] = next(iter(values))
        source_ids = sorted(str(row["source_artifact_id"]) for row in candidates)
        representative["source_artifact_id"] = source_ids[0]
        for candidate in candidates:
            contributing_parent_ids.add(str(candidate["_master_artifact_id"]))
            contributing_parent_ids.add(str(candidate["source_artifact_id"]))
        representative.pop("_master_artifact_id", None)
        representative["schema_version"] = DERIVED_INSTRUMENT_MASTER_CURRENT_V1.version
        representative["as_of"] = as_of
        selected.append(representative)
    output = pl.DataFrame(
        selected,
        schema=DERIVED_INSTRUMENT_MASTER_CURRENT_V1.schema,
    )
    output = DERIVED_INSTRUMENT_MASTER_CURRENT_V1.canonical_sort(output)
    try:
        DERIVED_INSTRUMENT_MASTER_CURRENT_V1.validate(output)
    except DataContractError as exc:
        raise IngestionError(f"reconciled instrument-master is invalid: {exc}") from exc

    parent_ids = tuple(sorted(contributing_parent_ids))
    identity = canonical_parameters_sha256(
        {
            "as_of": as_of.isoformat(),
            "contract": DERIVED_INSTRUMENT_MASTER_CURRENT_V1.identifier,
            "input_artifact_ids": list(parent_ids),
            "producer": "finresearch.data.reconcile",
            "producer_version": "1",
            "source_artifact_ids": list(canonical_sources),
        }
    )
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="derived",
        frame=output,
        contract=DERIVED_INSTRUMENT_MASTER_CURRENT_V1,
        path_parts=("derived.instrument-master-current", as_of.isoformat()),
        entity_key="current",
        identity=identity,
        producer="finresearch.data.reconcile",
        producer_version="1",
        parameters_sha256=identity,
        input_artifact_ids=parent_ids,
        produced_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
        source=(parent_ids[0] if manifest.manifest_version == MANIFEST_V1 else None),
    )


def _publish_normalized_snapshot(
    *,
    case_dir: Path,
    manifest: CaseManifest,
    path_role: str,
    frame: pl.DataFrame,
    contract: DatasetContract,
    path_parts: tuple[str, ...],
    entity_key: str,
    raw_artifact: Artifact,
    normalized_at: datetime,
) -> IngestionReceipt:
    """Publish one normalized snapshot under a source-derived identity."""
    parameters_sha256 = canonical_parameters_sha256(
        {
            "contract": contract.name,
            "contract_version": contract.version,
            "input_artifact_ids": [raw_artifact.artifact_id],
            "normalized_at": _format_utc(normalized_at),
            "producer": "finresearch.data.normalize",
            "producer_version": NORMALIZATION_PRODUCER_VERSION,
        }
    )
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role=path_role,
        frame=frame,
        contract=contract,
        path_parts=path_parts,
        entity_key=entity_key,
        identity=parameters_sha256,
        producer="finresearch.data.normalize",
        producer_version=NORMALIZATION_PRODUCER_VERSION,
        parameters_sha256=parameters_sha256,
        input_artifact_ids=(raw_artifact.artifact_id,),
        produced_at=normalized_at,
        source=(
            raw_artifact.artifact_id
            if manifest.manifest_version == MANIFEST_V1
            else None
        ),
    )


def _select_companyfacts_artifact(
    manifest: CaseManifest,
    cik: str,
    raw_artifact_id: str | None,
) -> Artifact:
    prefix = f"{COMPANYFACTS_KIND}.{cik}."
    candidates = [
        artifact
        for artifact in manifest.artifacts
        if artifact.kind == COMPANYFACTS_KIND
        and artifact.artifact_id.startswith(prefix)
    ]
    if not candidates:
        raise IngestionError(
            f"no raw SEC companyfacts snapshot for CIK {cik}; "
            "run data ingest-sec-companyfacts first"
        )
    if raw_artifact_id is not None:
        for artifact in candidates:
            if artifact.artifact_id == raw_artifact_id:
                return artifact
        raise IngestionError(
            f"raw snapshot {raw_artifact_id!r} is not a companyfacts "
            f"artifact for CIK {cik}"
        )
    if len(candidates) > 1:
        ids = ", ".join(artifact.artifact_id for artifact in candidates)
        raise IngestionError(
            f"multiple raw snapshots for CIK {cik}; pass --raw-artifact-id: {ids}"
        )
    return candidates[0]


def _fundamental_facts_frame(
    frame: pl.DataFrame,
    *,
    normalized_cik: str,
    source_artifact_id: str,
    normalized_at: datetime,
) -> pl.DataFrame:
    if frame.filter(
        pl.col("value_type").is_in(_NUMERIC_VALUE_TYPES)
        & pl.col("value_text").cast(pl.Float64, strict=False).is_null()
    ).height:
        raise IngestionError(
            f"raw snapshot {source_artifact_id} contains an unparseable "
            "numeric fact value"
        )
    canonical_metric = pl.struct("taxonomy", "concept").map_elements(
        lambda item: _canonical_metric(item["taxonomy"], item["concept"]),
        return_dtype=pl.Struct(
            {
                "category": pl.String,
                "canonical_metric": pl.String,
            }
        ),
    )
    parsed = frame.select(
        pl.lit(NORMALIZED_FUNDAMENTAL_FACTS_V2.version, dtype=pl.UInt16).alias(
            "schema_version"
        ),
        pl.lit("sec").alias("provider"),
        pl.lit(normalized_cik).alias("entity_id"),
        pl.lit(None, dtype=pl.String).alias("instrument_id"),
        pl.lit(normalized_cik).alias("cik"),
        "taxonomy",
        "concept",
        pl.concat_str(["taxonomy", pl.lit(":"), "concept"]).alias("metric_id"),
        canonical_metric.struct.field("category"),
        canonical_metric.struct.field("canonical_metric"),
        "label",
        "unit",
        pl.col("unit")
        .map_elements(_unit_kind, return_dtype=pl.String)
        .alias("unit_kind"),
        pl.when(pl.col("unit").is_in(["USD", "EUR", "GBP", "JPY", "CNY", "HKD"]))
        .then(pl.col("unit"))
        .otherwise(None)
        .alias("currency"),
        "value_text",
        pl.when(pl.col("value_type").is_in(_NUMERIC_VALUE_TYPES))
        .then(pl.col("value_text").cast(pl.Float64, strict=False))
        .otherwise(None)
        .alias("value"),
        pl.when(pl.col("start_date").is_null())
        .then(pl.lit("instant"))
        .otherwise(pl.lit("duration"))
        .alias("period_type"),
        "start_date",
        "end_date",
        "fiscal_year",
        "fiscal_period",
        "form",
        "accession_number",
        "filed_date",
        pl.col("filed_date").alias("knowledge_date"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("available_at"),
        "frame",
        pl.lit(source_artifact_id).alias("source_artifact_id"),
        pl.lit(normalized_at).alias("normalized_at"),
        pl.lit(source_artifact_id).alias("_fact_source_artifact_id"),
        pl.struct(list(frame.schema))
        .map_elements(
            _fact_id,
            return_dtype=pl.String,
        )
        .alias("_source_observation_sha256"),
    )
    # SEC can publish exact duplicate observations; keep the first copy before
    # assigning an immutable identifier.  Restatements retain their accession
    # or value provenance and therefore get distinct fact ids.
    parsed = parsed.unique(maintain_order=True)
    fact_identity_fields = [name for name in parsed.schema if name != "normalized_at"]
    fact_id = pl.struct(fact_identity_fields).map_elements(
        _fact_id,
        return_dtype=pl.String,
    )
    return parsed.with_columns(fact_id.alias("fact_id")).select(
        NORMALIZED_FUNDAMENTAL_FACTS_V2.schema.keys()
    )


def _case_normalized_context(
    workspace: Path,
    case_id: str,
) -> tuple[Path, CaseManifest, Path]:
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    normalized_root = resolve_relative_path(
        case_dir,
        manifest.paths["normalized"],
        "paths.normalized",
    )
    if not normalized_root.is_dir():
        raise CaseContractError(
            f"required normalized directory missing: {normalized_root}"
        )
    return case_dir, manifest, normalized_root


def _select_raw_artifact(
    manifest: CaseManifest,
    symbol: str,
    raw_artifact_id: str | None,
) -> Artifact:
    key = symbol_key(symbol)
    prefix = f"{DAILY_PRICES_KIND}.{key}."
    candidates = [
        artifact
        for artifact in manifest.artifacts
        if artifact.kind == DAILY_PRICES_KIND
        and artifact.artifact_id.startswith(prefix)
    ]
    if not candidates:
        raise IngestionError(
            f"no raw yfinance daily-prices snapshot for symbol {symbol!r}; "
            "run data ingest-yfinance-prices first"
        )
    if raw_artifact_id is not None:
        for artifact in candidates:
            if artifact.artifact_id == raw_artifact_id:
                return artifact
        raise IngestionError(
            f"raw snapshot {raw_artifact_id!r} is not a {symbol!r} "
            "daily-prices artifact"
        )
    if len(candidates) > 1:
        ids = ", ".join(artifact.artifact_id for artifact in candidates)
        raise IngestionError(
            f"multiple raw snapshots for {symbol!r}; pass --raw-artifact-id: {ids}"
        )
    return candidates[0]


def _read_raw_snapshot(case_dir: Path, artifact: Artifact) -> pl.DataFrame:
    path = resolve_relative_path(
        case_dir,
        artifact.path,
        f"artifact {artifact.artifact_id}",
    )
    if not path.is_file():
        raise IngestionError(f"raw snapshot file missing: {artifact.path}")
    try:
        return pl.read_parquet(path)
    except Exception as exc:
        raise IngestionError(
            f"raw snapshot {artifact.artifact_id} is not readable: {exc}"
        ) from exc


def _validate_raw_price_snapshot(
    frame: pl.DataFrame,
    requested_symbol: str,
    artifact: Artifact,
) -> str:
    provider_symbol = _require_constant(
        frame,
        "provider_symbol",
        artifact.artifact_id,
    )
    if provider_symbol.lower() != requested_symbol.lower():
        raise IngestionError(
            f"raw snapshot {artifact.artifact_id} holds symbol "
            f"{provider_symbol!r}, not requested {requested_symbol!r}"
        )
    interval = _require_constant(frame, "interval", artifact.artifact_id)
    if interval != "1d":
        raise IngestionError(
            f"raw snapshot {artifact.artifact_id} uses interval {interval!r}; "
            "v1 normalizes 1d snapshots only"
        )
    if frame.get_column("session_date").is_duplicated().any():
        raise IngestionError(
            f"raw snapshot {artifact.artifact_id} contains duplicate session "
            "dates and cannot form daily bars"
        )
    return provider_symbol


def _instrument_master_frame(
    frame: pl.DataFrame,
    *,
    instrument_id: str,
    provider_symbol: str,
    currency: str | None,
    source_artifact_id: str,
    normalized_at: datetime,
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "schema_version": NORMALIZED_INSTRUMENT_MASTER_V2.version,
                "provider": "yfinance",
                "instrument_id": instrument_id,
                "entity_id": None,
                "provider_symbol": provider_symbol,
                "primary_symbol": None,
                "name": None,
                "asset_class": "unknown",
                "instrument_type": None,
                "venue_mic": None,
                "country_code": None,
                "trading_currency": currency,
                "valid_from": None,
                "valid_to": None,
                "observed_at": normalized_at,
                "source_artifact_id": source_artifact_id,
            }
        ],
        schema=NORMALIZED_INSTRUMENT_MASTER_V2.schema,
    )


def _daily_prices_frame(
    frame: pl.DataFrame,
    *,
    instrument_id: str,
    provider_symbol: str,
    currency: str | None,
    source_artifact_id: str,
    normalized_at: datetime,
) -> pl.DataFrame:
    return frame.select(
        pl.lit(NORMALIZED_DAILY_PRICES_V2.version, dtype=pl.UInt16).alias(
            "schema_version"
        ),
        pl.lit("yfinance").alias("provider"),
        pl.lit(instrument_id).alias("instrument_id"),
        pl.lit(provider_symbol).alias("provider_symbol"),
        pl.lit(currency, dtype=pl.String).alias("currency"),
        pl.lit(PRICE_BASIS).alias("price_basis"),
        "provider_timezone",
        "session_date",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        # Absent distribution events normalize to an explicit zero.
        pl.col("dividends").fill_null(0.0),
        pl.col("stock_splits").fill_null(0.0),
        pl.lit(source_artifact_id).alias("source_artifact_id"),
        pl.lit(normalized_at).alias("normalized_at"),
    )


def _corporate_actions_frame(
    frame: pl.DataFrame,
    *,
    instrument_id: str,
    provider_symbol: str,
    currency: str | None,
    source_artifact_id: str,
    normalized_at: datetime,
) -> pl.DataFrame:
    """Make explicit action rows without mixing them into price bars."""
    rows: list[dict[str, object | None]] = []
    for row in frame.iter_rows(named=True):
        ex_date = row["session_date"]
        for action_type, raw_field, cash in (
            ("dividend", "dividends", True),
            ("capital-gain", "capital_gains", True),
            ("split", "stock_splits", False),
        ):
            amount = row[raw_field]
            if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
                continue
            rows.append(
                {
                    "schema_version": NORMALIZED_CORPORATE_ACTIONS_V1.version,
                    "provider": "yfinance",
                    "instrument_id": instrument_id,
                    "provider_symbol": provider_symbol,
                    "currency": currency,
                    "action_type": action_type,
                    "ex_date": ex_date,
                    "pay_date": None,
                    "cash_amount": float(amount) if cash else None,
                    "ratio": float(amount) if not cash else None,
                    "source_artifact_id": source_artifact_id,
                    "normalized_at": normalized_at,
                }
            )
    return pl.DataFrame(rows, schema=NORMALIZED_CORPORATE_ACTIONS_V1.schema)


def _canonical_metric(taxonomy: object, concept: object) -> dict[str, str | None]:
    if not isinstance(taxonomy, str) or not isinstance(concept, str):
        return {"category": None, "canonical_metric": None}
    category, metric = _CANONICAL_METRICS.get(
        (taxonomy, concept),
        ("other", None),
    )
    return {"category": category, "canonical_metric": metric}


def _unit_kind(unit: object) -> str:
    if not isinstance(unit, str):
        return "other"
    if unit in {"USD", "EUR", "GBP", "JPY", "CNY", "HKD"}:
        return "currency"
    if unit == "shares":
        return "shares"
    if unit == "pure":
        return "ratio"
    if unit.endswith("/shares"):
        return "currency"
    return "other"


def _fact_id(row: dict[str, object]) -> str:
    """Hash all fact-defining fields without float conversion or wall time."""
    canonical = json.dumps(
        {
            name: value.isoformat() if isinstance(value, (date, datetime)) else value
            for name, value in sorted(row.items())
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_constant(frame: pl.DataFrame, column: str, label: str) -> str:
    values = frame.get_column(column).unique().to_list()
    if len(values) != 1 or not isinstance(values[0], str):
        raise IngestionError(
            f"raw snapshot {label} has inconsistent {column}: {values!r}"
        )
    return values[0]


def _normalization_time(
    value: datetime | None,
    raw_artifact: Artifact,
    raw_frame: pl.DataFrame,
) -> datetime:
    """Use explicit or immutable source provenance, never the wall clock."""
    timestamp = value
    if timestamp is None:
        if raw_artifact.retrieved_at is not None:
            try:
                timestamp = datetime.fromisoformat(
                    raw_artifact.retrieved_at.removesuffix("Z") + "+00:00"
                )
            except ValueError as exc:
                raise IngestionError(
                    f"raw artifact {raw_artifact.artifact_id} has invalid retrieved_at"
                ) from exc
        else:
            values = raw_frame.get_column("retrieved_at").unique().to_list()
            if len(values) != 1 or not isinstance(values[0], datetime):
                raise IngestionError(
                    f"raw snapshot {raw_artifact.artifact_id} has inconsistent "
                    f"retrieved_at: {values!r}"
                )
            timestamp = values[0]
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise IngestionError("normalized_at must be timezone-aware")
    return timestamp.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    """Render a normalized UTC timestamp for deterministic producer identity."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
