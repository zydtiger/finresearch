"""Versioned tabular data contracts shared by ingestion and inspection."""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import polars as pl


class DataContractError(ValueError):
    """Raised when a table does not satisfy its declared contract."""


@dataclass(frozen=True)
class DatasetContract:
    """A versioned Polars schema plus structural validation rules."""

    name: str
    version: int
    schema: pl.Schema
    non_nullable: tuple[str, ...]
    unique_key: tuple[str, ...]
    sort_key: tuple[str, ...] = ()
    semantic_validator: Callable[[pl.DataFrame], None] | None = None
    allow_empty: bool = False

    @property
    def identifier(self) -> str:
        """Return the stable registry key for this contract version."""
        return f"{self.name}.v{self.version}"

    def validate(self, frame: pl.DataFrame) -> None:
        """Validate exact schema, required values, and row uniqueness."""
        if frame.schema != self.schema:
            raise DataContractError(
                f"schema mismatch for {self.identifier}: "
                f"expected {self.schema}, received {frame.schema}"
            )

        if frame.is_empty() and not self.allow_empty:
            raise DataContractError(f"{self.identifier} must contain at least one row")

        if "schema_version" in self.schema:
            mismatched_versions = frame.filter(
                pl.col("schema_version").is_null()
                | (pl.col("schema_version") != self.version)
            ).height
            if mismatched_versions:
                raise DataContractError(
                    f"{self.identifier} has {mismatched_versions} rows with "
                    f"schema_version other than {self.version}"
                )

        null_counts = frame.select(self.non_nullable).null_count().row(0)
        fields_with_nulls = [
            field
            for field, count in zip(self.non_nullable, null_counts, strict=True)
            if count
        ]
        if fields_with_nulls:
            fields = ", ".join(fields_with_nulls)
            raise DataContractError(
                f"{self.identifier} has nulls in required fields: {fields}"
            )

        duplicates = (
            frame.select(self.unique_key).is_duplicated().sum()
            if self.unique_key
            else 0
        )
        if duplicates:
            fields = ", ".join(self.unique_key)
            raise DataContractError(
                f"{self.identifier} has {duplicates} duplicate rows for key: {fields}"
            )
        if self.semantic_validator is not None:
            self.semantic_validator(frame)

    def canonical_sort(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Return the table in the immutable contract's deterministic order."""
        return frame.sort(self.sort_key) if self.sort_key else frame


RAW_YFINANCE_DAILY_PRICE_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "provider_symbol": pl.String,
    "currency": pl.String,
    "retrieved_at": pl.Datetime("us", "UTC"),
    "requested_start": pl.Date,
    "requested_end": pl.Date,
    "interval": pl.String,
    "provider_timezone": pl.String,
    "session_date": pl.Date,
    "timestamp": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Int64,
    "dividends": pl.Float64,
    "stock_splits": pl.Float64,
    "capital_gains": pl.Float64,
}

RAW_YFINANCE_DAILY_PRICES_V1: Final = DatasetContract(
    name="raw.yfinance.daily-prices",
    version=1,
    schema=pl.Schema(RAW_YFINANCE_DAILY_PRICE_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "provider_symbol",
        "currency",
        "retrieved_at",
        "requested_start",
        "requested_end",
        "interval",
        "provider_timezone",
        "session_date",
        "timestamp",
    ),
    unique_key=("provider_symbol", "interval", "timestamp"),
)

RAW_SEC_SUBMISSIONS_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "cik": pl.String,
    "retrieved_at": pl.Datetime("us", "UTC"),
    "source_url": pl.String,
    "entity_name": pl.String,
    "tickers": pl.List(pl.String),
    "exchanges": pl.List(pl.String),
    "sic": pl.String,
    "sic_description": pl.String,
    "accession_number": pl.String,
    "filing_date": pl.Date,
    "report_date": pl.Date,
    "acceptance_datetime": pl.String,
    "act": pl.String,
    "form": pl.String,
    "file_number": pl.String,
    "film_number": pl.String,
    "items": pl.String,
    "size": pl.Int64,
    "is_xbrl": pl.Boolean,
    "is_inline_xbrl": pl.Boolean,
    "primary_document": pl.String,
    "primary_doc_description": pl.String,
}

RAW_SEC_SUBMISSIONS_V1: Final = DatasetContract(
    name="raw.sec.submissions",
    version=1,
    schema=pl.Schema(RAW_SEC_SUBMISSIONS_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "cik",
        "retrieved_at",
        "source_url",
        "entity_name",
        "tickers",
        "exchanges",
        "accession_number",
        "filing_date",
        "form",
    ),
    unique_key=("cik", "accession_number"),
)

RAW_SEC_COMPANYFACTS_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "cik": pl.String,
    "retrieved_at": pl.Datetime("us", "UTC"),
    "source_url": pl.String,
    "entity_name": pl.String,
    "taxonomy": pl.String,
    "concept": pl.String,
    "label": pl.String,
    "description": pl.String,
    "unit": pl.String,
    "value_type": pl.String,
    "value_text": pl.String,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "accession_number": pl.String,
    "fiscal_year": pl.Int32,
    "fiscal_period": pl.String,
    "form": pl.String,
    "filed_date": pl.Date,
    "frame": pl.String,
}

RAW_SEC_COMPANYFACTS_V1: Final = DatasetContract(
    name="raw.sec.companyfacts",
    version=1,
    schema=pl.Schema(RAW_SEC_COMPANYFACTS_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "cik",
        "retrieved_at",
        "source_url",
        "entity_name",
        "taxonomy",
        "concept",
        "unit",
        "value_type",
        "value_text",
        "end_date",
        "accession_number",
        "form",
        "filed_date",
    ),
    # SEC can publish duplicate-looking facts; raw ingestion must preserve them.
    unique_key=(),
)

NORMALIZED_INSTRUMENT_MASTER_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "instrument_id": pl.String,
    "provider_symbol": pl.String,
    "currency": pl.String,
    "provider_timezone": pl.String,
    "first_session_date": pl.Date,
    "last_session_date": pl.Date,
    "observation_count": pl.Int64,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_INSTRUMENT_MASTER_V1: Final = DatasetContract(
    name="normalized.instrument-master",
    version=1,
    schema=pl.Schema(NORMALIZED_INSTRUMENT_MASTER_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "instrument_id",
        "provider_symbol",
        "currency",
        "provider_timezone",
        "first_session_date",
        "last_session_date",
        "observation_count",
        "source_artifact_id",
        "normalized_at",
    ),
    # One row per raw snapshot: a current-state master registry is a later
    # workflow that reconciles snapshots.
    unique_key=("instrument_id", "source_artifact_id"),
)

NORMALIZED_DAILY_PRICES_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "instrument_id": pl.String,
    "provider_symbol": pl.String,
    "currency": pl.String,
    "price_basis": pl.String,
    "provider_timezone": pl.String,
    "session_date": pl.Date,
    "timestamp": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "dividends": pl.Float64,
    "stock_splits": pl.Float64,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_DAILY_PRICES_V1: Final = DatasetContract(
    name="normalized.daily-prices",
    version=1,
    schema=pl.Schema(NORMALIZED_DAILY_PRICES_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "instrument_id",
        "provider_symbol",
        "currency",
        "price_basis",
        "provider_timezone",
        "session_date",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dividends",
        "stock_splits",
        "source_artifact_id",
        "normalized_at",
    ),
    unique_key=("instrument_id", "session_date"),
)

NORMALIZED_FUNDAMENTAL_FACTS_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "cik": pl.String,
    "taxonomy": pl.String,
    "concept": pl.String,
    "label": pl.String,
    "unit": pl.String,
    "value_type": pl.String,
    "value_text": pl.String,
    "value": pl.Float64,
    "period_type": pl.String,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "fiscal_year": pl.Int32,
    "fiscal_period": pl.String,
    "form": pl.String,
    "filed_date": pl.Date,
    "frame": pl.String,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_FUNDAMENTAL_FACTS_V1: Final = DatasetContract(
    name="normalized.fundamental-facts",
    version=1,
    schema=pl.Schema(NORMALIZED_FUNDAMENTAL_FACTS_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "cik",
        "taxonomy",
        "concept",
        "unit",
        "value_type",
        "value_text",
        "period_type",
        "end_date",
        "form",
        "filed_date",
        "source_artifact_id",
        "normalized_at",
    ),
    # Exact duplicate rows are removed during normalization; distinct
    # restatements of the same fact remain for analyst judgment, so a
    # unique key is intentionally not declared.
    unique_key=(),
)


# V2 canonical values deliberately describe what is known, rather than making
# provider-specific guesses.  The lists below are part of the on-disk contract.
CURRENCY_CODES: Final = frozenset(
    {"AUD", "CAD", "CHF", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD"}
)
ASSET_CLASSES: Final = frozenset(
    {
        "commodity",
        "crypto",
        "derivative",
        "equity",
        "etf",
        "fixed-income",
        "fund",
        "fx",
        "index",
        "other",
        "unknown",
    }
)
PRICE_BASES: Final = frozenset({"unadjusted", "split-adjusted", "total-return"})
UNIT_KINDS: Final = frozenset({"count", "currency", "ratio", "shares", "text", "other"})
FACT_CATEGORIES: Final = frozenset(
    {
        "balance-sheet",
        "cash-flow",
        "entity",
        "income-statement",
        "other",
        "share-data",
    }
)
FACT_UNITS: Final = frozenset(
    {
        "AUD",
        "CAD",
        "CHF",
        "CNY",
        "EUR",
        "GBP",
        "HKD",
        "JPY",
        "KRW",
        "SGD",
        "USD",
        "AUD/shares",
        "CAD/shares",
        "CHF/shares",
        "CNY/shares",
        "EUR/shares",
        "GBP/shares",
        "HKD/shares",
        "JPY/shares",
        "KRW/shares",
        "SGD/shares",
        "USD/shares",
        "pure",
        "shares",
    }
)
CORPORATE_ACTION_TYPES: Final = frozenset({"capital-gain", "dividend", "split"})
FX_RATE_KINDS: Final = frozenset({"close", "spot"})
PERIOD_TYPES: Final = frozenset({"duration", "instant"})


def _semantic_error(contract: str, message: str) -> DataContractError:
    return DataContractError(f"{contract} semantic violation: {message}")


def _validate_enum(
    frame: pl.DataFrame,
    contract: str,
    field: str,
    values: frozenset[str],
) -> None:
    unexpected = sorted(
        {
            value
            for value in frame.get_column(field).drop_nulls().to_list()
            if value not in values
        }
    )
    if unexpected:
        raise _semantic_error(
            contract, f"{field} has unsupported values: {unexpected!r}"
        )


def _validate_currency(frame: pl.DataFrame, contract: str, field: str) -> None:
    _validate_enum(frame, contract, field, CURRENCY_CODES)


def _validate_finite_positive(
    frame: pl.DataFrame,
    contract: str,
    field: str,
    *,
    allow_zero: bool = False,
) -> None:
    invalid = [
        value
        for value in frame.get_column(field).drop_nulls().to_list()
        if not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        or (not allow_zero and value == 0)
    ]
    if invalid:
        comparison = "nonnegative" if allow_zero else "positive"
        raise _semantic_error(
            contract, f"{field} must contain finite {comparison} values"
        )


def _validate_instrument_master_v2(frame: pl.DataFrame) -> None:
    contract = "normalized.instrument-master.v2"
    _validate_enum(frame, contract, "asset_class", ASSET_CLASSES)
    _validate_currency(frame, contract, "trading_currency")
    for row in frame.iter_rows(named=True):
        valid_from = row["valid_from"]
        valid_to = row["valid_to"]
        if valid_from is not None and valid_to is not None and valid_from > valid_to:
            raise _semantic_error(contract, "valid_from must not be after valid_to")
        country = row["country_code"]
        if country is not None and re.fullmatch(r"[A-Z]{2}", country) is None:
            raise _semantic_error(
                contract, "country_code must be ISO-3166 alpha-2 uppercase"
            )
        venue = row["venue_mic"]
        if venue is not None and re.fullmatch(r"[A-Z0-9]{4}", venue) is None:
            raise _semantic_error(contract, "venue_mic must be a four-character MIC")


def _validate_daily_prices_v2(frame: pl.DataFrame) -> None:
    contract = "normalized.daily-prices.v2"
    _validate_enum(frame, contract, "price_basis", PRICE_BASES)
    _validate_currency(frame, contract, "currency")
    bases = frame.get_column("price_basis").unique().to_list()
    if len(bases) != 1:
        raise _semantic_error(
            contract, "one artifact must contain exactly one price_basis"
        )
    _validate_finite_positive(frame, contract, "open")
    _validate_finite_positive(frame, contract, "high")
    _validate_finite_positive(frame, contract, "low")
    _validate_finite_positive(frame, contract, "close")
    _validate_finite_positive(frame, contract, "volume", allow_zero=True)
    _validate_finite_positive(frame, contract, "dividends", allow_zero=True)
    for row in frame.iter_rows(named=True):
        if row["low"] > min(row["open"], row["close"]):
            raise _semantic_error(contract, "low must not exceed open or close")
        if row["high"] < max(row["open"], row["close"]):
            raise _semantic_error(contract, "high must be at least open and close")
        split = row["stock_splits"]
        if not math.isfinite(split) or split < 0:
            raise _semantic_error(
                contract, "stock_splits must be finite and nonnegative"
            )


def _validate_fundamental_facts_v2(frame: pl.DataFrame) -> None:
    contract = "normalized.fundamental-facts.v2"
    _validate_enum(frame, contract, "unit", FACT_UNITS)
    _validate_enum(frame, contract, "unit_kind", UNIT_KINDS)
    _validate_enum(frame, contract, "category", FACT_CATEGORIES)
    _validate_enum(frame, contract, "period_type", PERIOD_TYPES)
    for row in frame.iter_rows(named=True):
        if row["currency"] is not None and row["currency"] not in CURRENCY_CODES:
            raise _semantic_error(contract, "currency has an unsupported value")
        numeric = row["value"]
        if numeric is not None and not math.isfinite(numeric):
            raise _semantic_error(contract, "value must be finite when present")
        if row["period_type"] == "instant" and row["start_date"] is not None:
            raise _semantic_error(contract, "instant facts must not set start_date")
        if row["period_type"] == "duration" and row["start_date"] is None:
            raise _semantic_error(contract, "duration facts must set start_date")
        if (
            row["start_date"] is not None
            and row["end_date"] is not None
            and row["start_date"] > row["end_date"]
        ):
            raise _semantic_error(contract, "start_date must not be after end_date")
        available_at = row["available_at"]
        filed_date = row["filed_date"]
        if (
            available_at is not None
            and filed_date is not None
            and available_at.date() < filed_date
        ):
            raise _semantic_error(contract, "available_at must not precede filed_date")


def _validate_estimates_v1(frame: pl.DataFrame) -> None:
    contract = "normalized.estimates.v1"
    _validate_enum(frame, contract, "unit_kind", UNIT_KINDS)
    for row in frame.iter_rows(named=True):
        if row["currency"] is not None and row["currency"] not in CURRENCY_CODES:
            raise _semantic_error(contract, "currency has an unsupported value")
        if not math.isfinite(row["value"]):
            raise _semantic_error(contract, "value must be finite")
        if row["availability_at"] > row["retrieved_at"]:
            raise _semantic_error(
                contract, "availability_at must not be after retrieved_at"
            )
        if row["estimate_as_of"] > row["availability_at"].date():
            raise _semantic_error(
                contract, "estimate_as_of must not be after availability_at"
            )


def _validate_corporate_actions_v1(frame: pl.DataFrame) -> None:
    contract = "normalized.corporate-actions.v1"
    _validate_enum(frame, contract, "action_type", CORPORATE_ACTION_TYPES)
    _validate_currency(frame, contract, "currency")
    for row in frame.iter_rows(named=True):
        cash = row["cash_amount"]
        ratio = row["ratio"]
        action = row["action_type"]
        if action in {"dividend", "capital-gain"}:
            if (
                cash is None
                or not math.isfinite(cash)
                or cash <= 0
                or ratio is not None
            ):
                raise _semantic_error(
                    contract, f"{action} requires positive cash_amount only"
                )
        elif (
            ratio is None or not math.isfinite(ratio) or ratio <= 0 or cash is not None
        ):
            raise _semantic_error(contract, "split requires positive ratio only")


def _validate_fx_rates_v1(frame: pl.DataFrame) -> None:
    contract = "normalized.fx-rates.v1"
    _validate_currency(frame, contract, "base_currency")
    _validate_currency(frame, contract, "quote_currency")
    _validate_enum(frame, contract, "rate_kind", FX_RATE_KINDS)
    _validate_finite_positive(frame, contract, "rate")
    if frame.filter(pl.col("base_currency") == pl.col("quote_currency")).height:
        raise _semantic_error(contract, "base_currency and quote_currency must differ")


NORMALIZED_INSTRUMENT_MASTER_V2_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "instrument_id": pl.String,
    "entity_id": pl.String,
    "provider_symbol": pl.String,
    "primary_symbol": pl.String,
    "name": pl.String,
    "asset_class": pl.String,
    "instrument_type": pl.String,
    "venue_mic": pl.String,
    "country_code": pl.String,
    "trading_currency": pl.String,
    "valid_from": pl.Date,
    "valid_to": pl.Date,
    "observed_at": pl.Datetime("us", "UTC"),
    "source_artifact_id": pl.String,
}

NORMALIZED_INSTRUMENT_MASTER_V2: Final = DatasetContract(
    name="normalized.instrument-master",
    version=2,
    schema=pl.Schema(NORMALIZED_INSTRUMENT_MASTER_V2_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "instrument_id",
        "asset_class",
        "trading_currency",
        "observed_at",
        "source_artifact_id",
    ),
    unique_key=("provider", "instrument_id", "source_artifact_id"),
    sort_key=("provider", "instrument_id", "observed_at", "source_artifact_id"),
    semantic_validator=_validate_instrument_master_v2,
)

NORMALIZED_DAILY_PRICES_V2_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "instrument_id": pl.String,
    "provider_symbol": pl.String,
    "currency": pl.String,
    "price_basis": pl.String,
    "provider_timezone": pl.String,
    "session_date": pl.Date,
    "timestamp": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "dividends": pl.Float64,
    "stock_splits": pl.Float64,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_DAILY_PRICES_V2: Final = DatasetContract(
    name="normalized.daily-prices",
    version=2,
    schema=pl.Schema(NORMALIZED_DAILY_PRICES_V2_FIELDS),
    non_nullable=tuple(NORMALIZED_DAILY_PRICES_V2_FIELDS),
    unique_key=("instrument_id", "session_date", "price_basis"),
    sort_key=("instrument_id", "session_date", "price_basis"),
    semantic_validator=_validate_daily_prices_v2,
)

NORMALIZED_FUNDAMENTAL_FACTS_V2_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "fact_id": pl.String,
    "entity_id": pl.String,
    "instrument_id": pl.String,
    "cik": pl.String,
    "taxonomy": pl.String,
    "concept": pl.String,
    "metric_id": pl.String,
    "category": pl.String,
    "canonical_metric": pl.String,
    "label": pl.String,
    "unit": pl.String,
    "unit_kind": pl.String,
    "currency": pl.String,
    "value_text": pl.String,
    "value": pl.Float64,
    "period_type": pl.String,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "fiscal_year": pl.Int32,
    "fiscal_period": pl.String,
    "form": pl.String,
    "accession_number": pl.String,
    "filed_date": pl.Date,
    "knowledge_date": pl.Date,
    "available_at": pl.Datetime("us", "UTC"),
    "frame": pl.String,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_FUNDAMENTAL_FACTS_V2: Final = DatasetContract(
    name="normalized.fundamental-facts",
    version=2,
    schema=pl.Schema(NORMALIZED_FUNDAMENTAL_FACTS_V2_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "fact_id",
        "unit",
        "unit_kind",
        "value_text",
        "period_type",
        "end_date",
        "source_artifact_id",
        "normalized_at",
    ),
    unique_key=("fact_id",),
    sort_key=("entity_id", "metric_id", "end_date", "filed_date", "fact_id"),
    semantic_validator=_validate_fundamental_facts_v2,
)

NORMALIZED_ESTIMATES_V1_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "entity_id": pl.String,
    "instrument_id": pl.String,
    "metric_id": pl.String,
    "period_end": pl.Date,
    "estimate_as_of": pl.Date,
    "availability_at": pl.Datetime("us", "UTC"),
    "retrieved_at": pl.Datetime("us", "UTC"),
    "value": pl.Float64,
    "unit": pl.String,
    "unit_kind": pl.String,
    "currency": pl.String,
    "source_artifact_id": pl.String,
}

NORMALIZED_ESTIMATES_V1: Final = DatasetContract(
    name="normalized.estimates",
    version=1,
    schema=pl.Schema(NORMALIZED_ESTIMATES_V1_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "metric_id",
        "period_end",
        "estimate_as_of",
        "availability_at",
        "retrieved_at",
        "value",
        "unit",
        "unit_kind",
        "source_artifact_id",
    ),
    unique_key=(
        "entity_id",
        "instrument_id",
        "provider",
        "metric_id",
        "period_end",
        "estimate_as_of",
        "source_artifact_id",
    ),
    sort_key=(
        "entity_id",
        "instrument_id",
        "provider",
        "metric_id",
        "period_end",
        "estimate_as_of",
        "source_artifact_id",
    ),
    semantic_validator=_validate_estimates_v1,
)

NORMALIZED_CORPORATE_ACTIONS_V1_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "instrument_id": pl.String,
    "provider_symbol": pl.String,
    "currency": pl.String,
    "action_type": pl.String,
    "ex_date": pl.Date,
    "pay_date": pl.Date,
    "cash_amount": pl.Float64,
    "ratio": pl.Float64,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_CORPORATE_ACTIONS_V1: Final = DatasetContract(
    name="normalized.corporate-actions",
    version=1,
    schema=pl.Schema(NORMALIZED_CORPORATE_ACTIONS_V1_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "instrument_id",
        "provider_symbol",
        "action_type",
        "ex_date",
        "source_artifact_id",
        "normalized_at",
    ),
    unique_key=("instrument_id", "ex_date", "action_type", "source_artifact_id"),
    sort_key=("instrument_id", "ex_date", "action_type"),
    semantic_validator=_validate_corporate_actions_v1,
    allow_empty=True,
)

NORMALIZED_FX_RATES_V1_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "base_currency": pl.String,
    "quote_currency": pl.String,
    "rate_date": pl.Date,
    "timestamp": pl.Datetime("us", "UTC"),
    "rate": pl.Float64,
    "rate_kind": pl.String,
    "source_artifact_id": pl.String,
    "normalized_at": pl.Datetime("us", "UTC"),
}

NORMALIZED_FX_RATES_V1: Final = DatasetContract(
    name="normalized.fx-rates",
    version=1,
    schema=pl.Schema(NORMALIZED_FX_RATES_V1_FIELDS),
    non_nullable=tuple(NORMALIZED_FX_RATES_V1_FIELDS),
    unique_key=("base_currency", "quote_currency", "rate_date", "rate_kind"),
    sort_key=("base_currency", "quote_currency", "rate_date", "rate_kind"),
    semantic_validator=_validate_fx_rates_v1,
)

DERIVED_INSTRUMENT_MASTER_CURRENT_V1_FIELDS: Final[
    dict[str, pl.DataType | type[pl.DataType]]
] = {
    **NORMALIZED_INSTRUMENT_MASTER_V2_FIELDS,
    "as_of": pl.Date,
}


def _validate_reconciled_instrument_master_v1(frame: pl.DataFrame) -> None:
    _validate_instrument_master_v2(
        frame.select(list(NORMALIZED_INSTRUMENT_MASTER_V2_FIELDS))
    )
    for row in frame.iter_rows(named=True):
        if row["observed_at"].date() > row["as_of"]:
            raise _semantic_error(
                "derived.instrument-master-current.v1",
                "observed_at must not be after as_of",
            )
        valid_from = row["valid_from"]
        valid_to = row["valid_to"]
        if valid_from is not None and valid_from > row["as_of"]:
            raise _semantic_error(
                "derived.instrument-master-current.v1",
                "valid_from must not be after as_of",
            )
        if valid_to is not None and valid_to < row["as_of"]:
            raise _semantic_error(
                "derived.instrument-master-current.v1",
                "valid_to must not be before as_of",
            )


DERIVED_INSTRUMENT_MASTER_CURRENT_V1: Final = DatasetContract(
    name="derived.instrument-master-current",
    version=1,
    schema=pl.Schema(DERIVED_INSTRUMENT_MASTER_CURRENT_V1_FIELDS),
    non_nullable=(
        "schema_version",
        "provider",
        "instrument_id",
        "asset_class",
        "trading_currency",
        "observed_at",
        "source_artifact_id",
        "as_of",
    ),
    unique_key=("provider", "instrument_id"),
    sort_key=("provider", "instrument_id"),
    semantic_validator=_validate_reconciled_instrument_master_v1,
)

CONTRACTS: Final = {
    RAW_YFINANCE_DAILY_PRICES_V1.identifier: RAW_YFINANCE_DAILY_PRICES_V1,
    RAW_SEC_SUBMISSIONS_V1.identifier: RAW_SEC_SUBMISSIONS_V1,
    RAW_SEC_COMPANYFACTS_V1.identifier: RAW_SEC_COMPANYFACTS_V1,
    NORMALIZED_INSTRUMENT_MASTER_V1.identifier: NORMALIZED_INSTRUMENT_MASTER_V1,
    NORMALIZED_DAILY_PRICES_V1.identifier: NORMALIZED_DAILY_PRICES_V1,
    NORMALIZED_FUNDAMENTAL_FACTS_V1.identifier: NORMALIZED_FUNDAMENTAL_FACTS_V1,
    NORMALIZED_INSTRUMENT_MASTER_V2.identifier: NORMALIZED_INSTRUMENT_MASTER_V2,
    NORMALIZED_DAILY_PRICES_V2.identifier: NORMALIZED_DAILY_PRICES_V2,
    NORMALIZED_FUNDAMENTAL_FACTS_V2.identifier: NORMALIZED_FUNDAMENTAL_FACTS_V2,
    NORMALIZED_ESTIMATES_V1.identifier: NORMALIZED_ESTIMATES_V1,
    NORMALIZED_CORPORATE_ACTIONS_V1.identifier: NORMALIZED_CORPORATE_ACTIONS_V1,
    NORMALIZED_FX_RATES_V1.identifier: NORMALIZED_FX_RATES_V1,
    DERIVED_INSTRUMENT_MASTER_CURRENT_V1.identifier: (
        DERIVED_INSTRUMENT_MASTER_CURRENT_V1
    ),
}


def get_contract(identifier: str) -> DatasetContract:
    """Return a registered dataset contract by versioned identifier."""
    try:
        return CONTRACTS[identifier]
    except KeyError as exc:
        raise DataContractError(f"unknown dataset contract: {identifier}") from exc
