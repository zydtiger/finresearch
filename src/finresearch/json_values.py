"""Strict readers for provider JSON values used by capture parsers."""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from finresearch.ingestion import IngestionError


def json_number(value: object, label: str) -> float | None:
    """Parse a JSON number or numeric string; null means absent."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise IngestionError(f"{label} must be a number: {value!r}")
    try:
        parsed = float(Decimal(str(value)))
    except (InvalidOperation, ValueError) as exc:
        raise IngestionError(f"{label} must be a number: {value!r}") from exc
    if not math.isfinite(parsed):
        raise IngestionError(f"{label} must be finite: {value!r}")
    return parsed


def json_object(value: object, label: str) -> Mapping[str, object]:
    """Require a JSON object."""
    if not isinstance(value, dict):
        raise IngestionError(f"{label} must be a JSON object")
    return value
