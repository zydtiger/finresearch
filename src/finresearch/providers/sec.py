"""SEC EDGAR adapters for submissions and company facts."""

from __future__ import annotations

import json
import re
import tempfile
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

import httpx
import polars as pl
from filelock import FileLock

from finresearch.data_contracts import (
    RAW_SEC_COMPANYFACTS_V1,
    RAW_SEC_SUBMISSIONS_V1,
    DatasetContract,
)
from finresearch.providers import ProviderError

SEC_BASE_URL: Final = "https://data.sec.gov"
USER_AGENT_PATTERN: Final = re.compile(r"\S+@\S+\.\S+")
DATE_PATTERN: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SEC_MINIMUM_REQUEST_INTERVAL_SECONDS: Final = 0.11


class SECProviderError(ProviderError):
    """Raised when SEC EDGAR cannot produce a valid raw snapshot."""


class _SECRequestRateLimiter:
    """Serialize SEC request starts across processes on the current host."""

    def __init__(
        self,
        lock_path: Path | None = None,
        minimum_interval_seconds: float = SEC_MINIMUM_REQUEST_INTERVAL_SECONDS,
    ) -> None:
        if minimum_interval_seconds <= 0:
            raise ValueError("minimum SEC request interval must be positive")
        if lock_path is None:
            lock_directory = Path(tempfile.gettempdir()) / "finresearch"
            lock_directory.mkdir(parents=True, exist_ok=True)
            lock_path = lock_directory / "sec-request-rate.lock"
        self._lock = FileLock(lock_path)
        self._minimum_interval_seconds = minimum_interval_seconds

    def wait(self) -> None:
        """Wait for a host-shared request slot."""
        with self._lock:
            time.sleep(self._minimum_interval_seconds)


class SECProvider:
    """Fetch SEC JSON endpoints with explicit fair-access identification."""

    def __init__(
        self,
        transport: httpx.BaseTransport | None = None,
        rate_limiter: _SECRequestRateLimiter | None = None,
    ) -> None:
        self._transport = transport
        self._rate_limiter = rate_limiter or _SECRequestRateLimiter()

    def fetch_submissions(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        """Fetch the current SEC submissions object and flatten recent filings."""
        normalized_cik = normalize_cik(cik)
        source_url = submissions_url(normalized_cik)
        payload = self._get_payload(source_url, user_agent)
        return submissions_to_frame(
            payload,
            expected_cik=normalized_cik,
            source_url=source_url,
            retrieved_at=retrieved_at,
        )

    def fetch_companyfacts(
        self,
        cik: str,
        user_agent: str,
        retrieved_at: datetime,
    ) -> pl.DataFrame:
        """Fetch and flatten the SEC companyfacts XBRL object."""
        normalized_cik = normalize_cik(cik)
        source_url = companyfacts_url(normalized_cik)
        payload = self._get_payload(source_url, user_agent)
        return companyfacts_to_frame(
            payload,
            expected_cik=normalized_cik,
            source_url=source_url,
            retrieved_at=retrieved_at,
        )

    def _get_payload(self, url: str, user_agent: str) -> dict[str, object]:
        declared_agent = validate_user_agent(user_agent)
        try:
            with httpx.Client(
                headers={
                    "User-Agent": declared_agent,
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                },
                timeout=30.0,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                self._rate_limiter.wait()
                response = client.get(url)
                response.raise_for_status()
                payload = response.json(parse_float=Decimal)
        except (httpx.HTTPError, json.JSONDecodeError, UnicodeError) as exc:
            raise SECProviderError(f"SEC request failed for {url}: {exc}") from exc
        if not isinstance(payload, dict):
            raise SECProviderError(f"SEC returned a non-object response for {url}")
        return {str(key): value for key, value in payload.items()}


def normalize_cik(cik: str) -> str:
    """Normalize a positive SEC CIK to its ten-digit API representation."""
    value = cik.strip()
    if re.fullmatch(r"[0-9]{1,10}", value) is None or int(value) <= 0:
        raise SECProviderError("CIK must be a positive number with at most 10 digits")
    return value.zfill(10)


def submissions_url(cik: str) -> str:
    """Return the canonical SEC submissions endpoint for a CIK."""
    return f"{SEC_BASE_URL}/submissions/CIK{normalize_cik(cik)}.json"


def companyfacts_url(cik: str) -> str:
    """Return the canonical SEC companyfacts endpoint for a CIK."""
    return f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{normalize_cik(cik)}.json"


def validate_user_agent(user_agent: str) -> str:
    """Require the organization/name and contact email expected by SEC policy."""
    value = user_agent.strip()
    match = USER_AGENT_PATTERN.search(value)
    identity = (
        "" if match is None else f"{value[: match.start()]}{value[match.end() :]}"
    )
    if match is None or not any(character.isalnum() for character in identity):
        raise SECProviderError(
            "SEC user agent must identify the requester and include a contact email"
        )
    return value


def submissions_to_frame(
    payload: Mapping[str, object],
    *,
    expected_cik: str,
    source_url: str,
    retrieved_at: datetime,
) -> pl.DataFrame:
    """Flatten SEC recent-submission columns without changing provider values."""
    retrieval_time = _utc_time(retrieved_at)
    cik = normalize_cik(_required_scalar_string(payload, "cik", "submissions"))
    if cik != expected_cik:
        raise SECProviderError(
            f"SEC submissions CIK {cik} does not match requested CIK {expected_cik}"
        )

    entity_name = _required_string(payload, "name", "submissions")
    tickers = _string_list(payload.get("tickers"), "submissions.tickers")
    exchanges = _string_list(payload.get("exchanges"), "submissions.exchanges")
    sic = _optional_string(payload.get("sic"), "submissions.sic")
    sic_description = _optional_string(
        payload.get("sicDescription"),
        "submissions.sicDescription",
    )
    filings = _required_mapping(payload, "filings", "submissions")
    recent = _required_mapping(filings, "recent", "submissions.filings")
    accessions = _required_list(recent, "accessionNumber", "submissions.recent")
    if not accessions:
        raise SECProviderError("SEC submissions contains no recent filings")

    columns = {
        name: _recent_column(recent, name, len(accessions))
        for name in (
            "filingDate",
            "reportDate",
            "acceptanceDateTime",
            "act",
            "form",
            "fileNumber",
            "filmNumber",
            "items",
            "size",
            "isXBRL",
            "isInlineXBRL",
            "primaryDocument",
            "primaryDocDescription",
        )
    }

    rows: list[dict[str, object]] = []
    for index, accession in enumerate(accessions):
        rows.append(
            {
                "schema_version": RAW_SEC_SUBMISSIONS_V1.version,
                "provider": "sec",
                "cik": cik,
                "retrieved_at": retrieval_time,
                "source_url": source_url,
                "entity_name": entity_name,
                "tickers": tickers,
                "exchanges": exchanges,
                "sic": sic,
                "sic_description": sic_description,
                "accession_number": _required_value_string(
                    accession,
                    f"submissions.recent.accessionNumber[{index}]",
                ),
                "filing_date": _required_date(
                    columns["filingDate"][index],
                    f"submissions.recent.filingDate[{index}]",
                ),
                "report_date": _optional_date(
                    columns["reportDate"][index],
                    f"submissions.recent.reportDate[{index}]",
                ),
                "acceptance_datetime": _optional_string(
                    columns["acceptanceDateTime"][index],
                    f"submissions.recent.acceptanceDateTime[{index}]",
                ),
                "act": _optional_string(
                    columns["act"][index], f"submissions.recent.act[{index}]"
                ),
                "form": _required_value_string(
                    columns["form"][index], f"submissions.recent.form[{index}]"
                ),
                "file_number": _optional_string(
                    columns["fileNumber"][index],
                    f"submissions.recent.fileNumber[{index}]",
                ),
                "film_number": _optional_string(
                    columns["filmNumber"][index],
                    f"submissions.recent.filmNumber[{index}]",
                ),
                "items": _optional_string(
                    columns["items"][index], f"submissions.recent.items[{index}]"
                ),
                "size": _optional_integer(
                    columns["size"][index], f"submissions.recent.size[{index}]"
                ),
                "is_xbrl": _optional_boolean(
                    columns["isXBRL"][index],
                    f"submissions.recent.isXBRL[{index}]",
                ),
                "is_inline_xbrl": _optional_boolean(
                    columns["isInlineXBRL"][index],
                    f"submissions.recent.isInlineXBRL[{index}]",
                ),
                "primary_document": _optional_string(
                    columns["primaryDocument"][index],
                    f"submissions.recent.primaryDocument[{index}]",
                ),
                "primary_doc_description": _optional_string(
                    columns["primaryDocDescription"][index],
                    f"submissions.recent.primaryDocDescription[{index}]",
                ),
            }
        )

    return _build_frame(rows, RAW_SEC_SUBMISSIONS_V1)


def companyfacts_to_frame(
    payload: Mapping[str, object],
    *,
    expected_cik: str,
    source_url: str,
    retrieved_at: datetime,
) -> pl.DataFrame:
    """Flatten SEC company facts while retaining filing and unit provenance."""
    retrieval_time = _utc_time(retrieved_at)
    cik = normalize_cik(_required_scalar_string(payload, "cik", "companyfacts"))
    if cik != expected_cik:
        raise SECProviderError(
            f"SEC companyfacts CIK {cik} does not match requested CIK {expected_cik}"
        )
    entity_name = _required_string(payload, "entityName", "companyfacts")
    taxonomies = _required_mapping(payload, "facts", "companyfacts")

    rows: list[dict[str, object]] = []
    for taxonomy, concepts_value in taxonomies.items():
        concepts = _mapping_value(concepts_value, f"companyfacts.facts.{taxonomy}")
        for concept, details_value in concepts.items():
            field = f"companyfacts.facts.{taxonomy}.{concept}"
            details = _mapping_value(details_value, field)
            label = _optional_string(details.get("label"), f"{field}.label")
            description = _optional_string(
                details.get("description"), f"{field}.description"
            )
            units = _required_mapping(details, "units", field)
            for unit, observations_value in units.items():
                observations = _list_value(
                    observations_value,
                    f"{field}.units.{unit}",
                )
                for index, observation_value in enumerate(observations):
                    observation_field = f"{field}.units.{unit}[{index}]"
                    observation = _mapping_value(
                        observation_value,
                        observation_field,
                    )
                    value_type, value_text = _fact_value(
                        observation.get("val"),
                        f"{observation_field}.val",
                    )
                    rows.append(
                        {
                            "schema_version": RAW_SEC_COMPANYFACTS_V1.version,
                            "provider": "sec",
                            "cik": cik,
                            "retrieved_at": retrieval_time,
                            "source_url": source_url,
                            "entity_name": entity_name,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "label": label,
                            "description": description,
                            "unit": unit,
                            "value_type": value_type,
                            "value_text": value_text,
                            "start_date": _optional_date(
                                observation.get("start"),
                                f"{observation_field}.start",
                            ),
                            "end_date": _required_date(
                                observation.get("end"),
                                f"{observation_field}.end",
                            ),
                            "accession_number": _required_value_string(
                                observation.get("accn"),
                                f"{observation_field}.accn",
                            ),
                            "fiscal_year": _optional_integer(
                                observation.get("fy"),
                                f"{observation_field}.fy",
                            ),
                            "fiscal_period": _optional_string(
                                observation.get("fp"),
                                f"{observation_field}.fp",
                            ),
                            "form": _required_value_string(
                                observation.get("form"),
                                f"{observation_field}.form",
                            ),
                            "filed_date": _required_date(
                                observation.get("filed"),
                                f"{observation_field}.filed",
                            ),
                            "frame": _optional_string(
                                observation.get("frame"),
                                f"{observation_field}.frame",
                            ),
                        }
                    )

    if not rows:
        raise SECProviderError("SEC companyfacts contains no facts")
    return _build_frame(rows, RAW_SEC_COMPANYFACTS_V1)


def _build_frame(
    rows: list[dict[str, object]],
    contract: DatasetContract,
) -> pl.DataFrame:
    """Construct and validate a Polars frame with provider-scoped errors."""
    try:
        frame = pl.DataFrame(rows, schema=contract.schema)
        contract.validate(frame)
    except (TypeError, ValueError, pl.exceptions.PolarsError) as exc:
        raise SECProviderError(
            f"SEC data does not satisfy {contract.identifier}: {exc}"
        ) from exc
    return frame


def _utc_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SECProviderError("retrieved_at must be timezone-aware")
    return value.astimezone(UTC)


def _required_mapping(
    data: Mapping[str, object], key: str, field: str
) -> Mapping[str, object]:
    return _mapping_value(data.get(key), f"{field}.{key}")


def _mapping_value(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise SECProviderError(f"{field} must be an object")
    return {str(key): item for key, item in value.items()}


def _required_list(data: Mapping[str, object], key: str, field: str) -> list[object]:
    return _list_value(data.get(key), f"{field}.{key}")


def _list_value(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise SECProviderError(f"{field} must be an array")
    return value


def _recent_column(
    recent: Mapping[str, object], key: str, expected_length: int
) -> list[object]:
    value = recent.get(key)
    if value is None:
        return [None] * expected_length
    column = _list_value(value, f"submissions.recent.{key}")
    if len(column) != expected_length:
        raise SECProviderError(
            f"submissions.recent.{key} length does not match accessionNumber"
        )
    return column


def _required_string(data: Mapping[str, object], key: str, field: str) -> str:
    return _required_value_string(data.get(key), f"{field}.{key}")


def _required_scalar_string(data: Mapping[str, object], key: str, field: str) -> str:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise SECProviderError(f"{field}.{key} must be a string or integer")
    return str(value)


def _required_value_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise SECProviderError(f"{field} must be a non-empty string")
    return value


def _optional_string(value: object, field: str) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise SECProviderError(f"{field} must be a string")
    return value


def _string_list(value: object, field: str) -> list[str]:
    items = _list_value(value, field) if value is not None else []
    if not all(isinstance(item, str) for item in items):
        raise SECProviderError(f"{field} must contain only strings")
    return [item for item in items if isinstance(item, str)]


def _required_date(value: object, field: str) -> date:
    result = _optional_date(value, field)
    if result is None:
        raise SECProviderError(f"{field} must be a YYYY-MM-DD date")
    return result


def _optional_date(value: object, field: str) -> date | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or DATE_PATTERN.fullmatch(value) is None:
        raise SECProviderError(f"{field} must be a YYYY-MM-DD date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SECProviderError(f"{field} must be a YYYY-MM-DD date") from exc


def _optional_integer(value: object, field: str) -> int | None:
    if value in (None, ""):
        return None
    if type(value) is not int:
        raise SECProviderError(f"{field} must be an integer")
    return value


def _optional_boolean(value: object, field: str) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise SECProviderError(f"{field} must be boolean or 0/1")


def _fact_value(value: object, field: str) -> tuple[str, str]:
    if value is None:
        return "null", "null"
    if isinstance(value, bool):
        value_type = "boolean"
    elif type(value) is int:
        value_type = "integer"
    elif isinstance(value, (float, Decimal)):
        value_type = "number"
    elif isinstance(value, str):
        value_type = "string"
    else:
        raise SECProviderError(f"{field} must be a JSON scalar")
    if isinstance(value, Decimal):
        return value_type, str(value)
    try:
        value_text = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except ValueError as exc:
        raise SECProviderError(f"{field} must be a finite JSON scalar") from exc
    return value_type, value_text
