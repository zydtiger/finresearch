"""Versioned tabular data contracts shared by ingestion and inspection."""

from __future__ import annotations

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

        if frame.is_empty():
            raise DataContractError(f"{self.identifier} must contain at least one row")

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

        duplicates = frame.select(self.unique_key).is_duplicated().sum()
        if duplicates:
            fields = ", ".join(self.unique_key)
            raise DataContractError(
                f"{self.identifier} has {duplicates} duplicate rows for key: {fields}"
            )


RAW_YFINANCE_DAILY_PRICE_FIELDS: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "schema_version": pl.UInt16,
    "provider": pl.String,
    "provider_symbol": pl.String,
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

CONTRACTS: Final = {
    RAW_YFINANCE_DAILY_PRICES_V1.identifier: RAW_YFINANCE_DAILY_PRICES_V1,
}


def get_contract(identifier: str) -> DatasetContract:
    """Return a registered dataset contract by versioned identifier."""
    try:
        return CONTRACTS[identifier]
    except KeyError as exc:
        raise DataContractError(f"unknown dataset contract: {identifier}") from exc
