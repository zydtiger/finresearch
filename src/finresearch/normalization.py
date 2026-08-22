"""Deterministic raw-to-normalized transformations with manifest lineage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

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
    NORMALIZED_DAILY_PRICES_V1,
    NORMALIZED_FUNDAMENTAL_FACTS_V1,
    NORMALIZED_INSTRUMENT_MASTER_V1,
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
NORMALIZATION_PRODUCER_VERSION: Final = "2"


@dataclass(frozen=True)
class NormalizationReceipt:
    """Persisted normalized artifacts for one raw snapshot."""

    instrument_master: IngestionReceipt
    daily_prices: IngestionReceipt


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
    instrument_id = symbol_key(provider_symbol)
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
    try:
        NORMALIZED_INSTRUMENT_MASTER_V1.validate(master_frame)
        NORMALIZED_DAILY_PRICES_V1.validate(prices_frame)
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
        contract=NORMALIZED_INSTRUMENT_MASTER_V1,
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
        contract=NORMALIZED_DAILY_PRICES_V1,
        path_parts=("normalized.daily-prices", instrument_id),
        entity_key=instrument_id,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
    )
    return NormalizationReceipt(
        instrument_master=master_receipt,
        daily_prices=prices_receipt,
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
    try:
        NORMALIZED_FUNDAMENTAL_FACTS_V1.validate(facts_frame)
    except DataContractError as exc:
        raise IngestionError(
            f"raw snapshot {raw_artifact.artifact_id} cannot be normalized: {exc}"
        ) from exc
    return _publish_normalized_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=facts_frame,
        contract=NORMALIZED_FUNDAMENTAL_FACTS_V1,
        path_parts=("normalized.fundamental-facts", normalized_cik),
        entity_key=normalized_cik,
        raw_artifact=raw_artifact,
        normalized_at=timestamp,
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
    parsed = frame.select(
        pl.lit(NORMALIZED_FUNDAMENTAL_FACTS_V1.version, dtype=pl.UInt16).alias(
            "schema_version"
        ),
        pl.lit("sec").alias("provider"),
        pl.lit(normalized_cik).alias("cik"),
        "taxonomy",
        "concept",
        "label",
        "unit",
        "value_type",
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
        "filed_date",
        "frame",
        pl.lit(source_artifact_id).alias("source_artifact_id"),
        pl.lit(normalized_at).alias("normalized_at"),
    )
    if parsed.filter(
        pl.col("value_type").is_in(_NUMERIC_VALUE_TYPES) & pl.col("value").is_null()
    ).height:
        raise IngestionError(
            f"raw snapshot {source_artifact_id} contains an unparseable "
            "numeric fact value"
        )
    # SEC can publish exact duplicate observations; keep the first copy.
    return parsed.unique(maintain_order=True)


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
    session_dates = frame.get_column("session_date")
    return pl.DataFrame(
        [
            {
                "schema_version": NORMALIZED_INSTRUMENT_MASTER_V1.version,
                "provider": "yfinance",
                "instrument_id": instrument_id,
                "provider_symbol": provider_symbol,
                "currency": currency,
                "provider_timezone": _require_constant(
                    frame, "provider_timezone", source_artifact_id
                ),
                "first_session_date": session_dates.min(),
                "last_session_date": session_dates.max(),
                "observation_count": frame.height,
                "source_artifact_id": source_artifact_id,
                "normalized_at": normalized_at,
            }
        ],
        schema=NORMALIZED_INSTRUMENT_MASTER_V1.schema,
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
        pl.lit(NORMALIZED_DAILY_PRICES_V1.version, dtype=pl.UInt16).alias(
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
