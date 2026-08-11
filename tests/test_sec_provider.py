import json
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from finresearch.providers.sec import (
    SECProvider,
    SECProviderError,
    companyfacts_to_frame,
    normalize_cik,
    submissions_to_frame,
    validate_user_agent,
)

FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "sec"
RATE_LIMIT_WORKER = Path(__file__).parent / "process_sec_rate_limit_worker.py"
SUBMISSIONS = json.loads(
    (FIXTURE_DIRECTORY / "submissions.json").read_text(encoding="utf-8")
)
COMPANYFACTS = json.loads(
    (FIXTURE_DIRECTORY / "companyfacts.json").read_text(encoding="utf-8")
)
RETRIEVED_AT = datetime(2026, 8, 11, 4, 5, tzinfo=UTC)


def test_normalize_cik() -> None:
    assert normalize_cik("320193") == "0000320193"
    assert normalize_cik("0000320193") == "0000320193"


@pytest.mark.parametrize("cik", ["", "abc", "0", "12345678901", "٣٢٠١٩٣"])
def test_normalize_cik_rejects_invalid_value(cik: str) -> None:
    with pytest.raises(SECProviderError, match="CIK"):
        normalize_cik(cik)


def test_user_agent_requires_contact_email() -> None:
    assert (
        validate_user_agent("Finresearch Research user@example.com")
        == "Finresearch Research user@example.com"
    )

    with pytest.raises(SECProviderError, match="contact email"):
        validate_user_agent("finresearch")
    with pytest.raises(SECProviderError, match="contact email"):
        validate_user_agent("user@example.com")


def test_submissions_contract_flattens_columnar_recent_filings() -> None:
    frame = submissions_to_frame(
        SUBMISSIONS,
        expected_cik="0000320193",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        retrieved_at=RETRIEVED_AT,
    )

    assert frame.height == 2
    first = frame.row(0, named=True)
    assert first["cik"] == "0000320193"
    assert first["tickers"] == ["AAPL"]
    assert first["filing_date"] == date(2026, 8, 1)
    assert first["form"] == "10-Q"
    assert first["is_xbrl"] is True


def test_companyfacts_contract_preserves_fact_value_as_json_text() -> None:
    frame = companyfacts_to_frame(
        COMPANYFACTS,
        expected_cik="0000320193",
        source_url=("https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"),
        retrieved_at=RETRIEVED_AT,
    )

    assert frame.height == 2
    revenue = frame.filter(frame["concept"] == "Revenues").row(0, named=True)
    assert revenue["value_type"] == "integer"
    assert revenue["value_text"] == "300000000000"
    assert revenue["start_date"] == date(2025, 9, 28)
    public_float = frame.filter(frame["concept"] == "EntityPublicFloat").row(
        0, named=True
    )
    assert public_float["value_type"] == "number"
    assert public_float["value_text"] == "2500000000000.5"
    assert public_float["start_date"] is None


def test_sec_provider_declares_user_agent_and_uses_expected_endpoints() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["user-agent"] == "Finresearch user@example.com"
        timeout = request.extensions["timeout"]
        assert isinstance(timeout, dict)
        assert timeout["read"] == 30.0
        if request.url.path.startswith("/submissions/"):
            return httpx.Response(200, json=SUBMISSIONS)
        if request.url.path.startswith("/api/xbrl/companyfacts/"):
            return httpx.Response(200, json=COMPANYFACTS)
        return httpx.Response(404)

    provider = SECProvider(transport=httpx.MockTransport(handler))

    submissions = provider.fetch_submissions(
        "320193", "Finresearch user@example.com", RETRIEVED_AT
    )
    facts = provider.fetch_companyfacts(
        "320193", "Finresearch user@example.com", RETRIEVED_AT
    )

    assert submissions.height == 2
    assert facts.height == 2


def test_sec_provider_rejects_redirect_without_contact_leak() -> None:
    observed_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        return httpx.Response(
            302,
            headers={"location": "https://attacker.invalid/companyfacts.json"},
            request=request,
        )

    provider = SECProvider(transport=httpx.MockTransport(handler))

    with pytest.raises(SECProviderError, match="SEC request failed"):
        provider.fetch_companyfacts(
            "320193",
            "Finresearch user@example.com",
            RETRIEVED_AT,
        )

    assert len(observed_requests) == 1
    assert observed_requests[0].url.host == "data.sec.gov"


def test_sec_rate_limit_is_shared_across_processes(tmp_path: Path) -> None:
    coordination_directory = tmp_path / "coordination"
    coordination_directory.mkdir()
    lock_path = tmp_path / "sec-rate.lock"
    processes = [
        subprocess.Popen(  # noqa: S603 - fixed local interpreter and test helper.
            [
                sys.executable,
                str(RATE_LIMIT_WORKER),
                str(lock_path),
                str(coordination_directory),
                str(index),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index in range(2)
    ]
    deadline = time.monotonic() + 10
    while len(list(coordination_directory.glob("ready-*"))) < 2:
        if time.monotonic() >= deadline:
            for process in processes:
                process.terminate()
            pytest.fail("SEC rate-limit workers did not reach the barrier")
        time.sleep(0.01)
    (coordination_directory / "go").touch()

    outputs = [process.communicate(timeout=15) for process in processes]
    assert [process.returncode for process in processes] == [0, 0], outputs
    request_times = sorted(
        int(path.read_text(encoding="utf-8"))
        for path in coordination_directory.glob("request-*")
    )
    assert len(request_times) == 2
    assert request_times[1] - request_times[0] >= 100_000_000


def test_sec_provider_preserves_high_precision_json_number() -> None:
    raw_payload = (FIXTURE_DIRECTORY / "companyfacts.json").read_text(encoding="utf-8")
    raw_payload = raw_payload.replace(
        "2500000000000.5",
        "2500000000000.123456789",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=raw_payload.encode(),
            headers={"content-type": "application/json"},
            request=request,
        )

    provider = SECProvider(transport=httpx.MockTransport(handler))
    frame = provider.fetch_companyfacts(
        "320193",
        "Finresearch user@example.com",
        RETRIEVED_AT,
    )

    public_float = frame.filter(frame["concept"] == "EntityPublicFloat").row(
        0, named=True
    )
    assert public_float["value_text"] == "2500000000000.123456789"


@pytest.mark.parametrize(
    ("status_code", "content"),
    [(500, b"{}"), (200, b"not-json"), (200, b"\xff\xfe\xff")],
)
def test_sec_provider_wraps_http_and_json_failures(
    status_code: int,
    content: bytes,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            content=content,
            headers={"content-type": "application/json"},
            request=request,
        )

    provider = SECProvider(transport=httpx.MockTransport(handler))

    with pytest.raises(SECProviderError, match="SEC request failed"):
        provider.fetch_submissions(
            "320193",
            "Finresearch user@example.com",
            RETRIEVED_AT,
        )


def test_submissions_does_not_follow_supplemental_history_files() -> None:
    payload = json.loads(json.dumps(SUBMISSIONS))
    payload["filings"]["files"] = [
        {
            "name": "CIK0000320193-submissions-001.json",
            "filingCount": 1,
            "filingFrom": "1994-01-01",
            "filingTo": "2024-01-01",
        }
    ]
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json=payload, request=request)

    provider = SECProvider(transport=httpx.MockTransport(handler))
    frame = provider.fetch_submissions(
        "320193",
        "Finresearch user@example.com",
        RETRIEVED_AT,
    )

    assert requests == 1
    assert frame.height == 2


def test_submissions_rejects_misaligned_provider_columns() -> None:
    payload = json.loads(json.dumps(SUBMISSIONS))
    payload["filings"]["recent"]["form"] = ["10-Q"]

    with pytest.raises(SECProviderError, match="length does not match"):
        submissions_to_frame(
            payload,
            expected_cik="0000320193",
            source_url="https://data.sec.gov/submissions/CIK0000320193.json",
            retrieved_at=RETRIEVED_AT,
        )


@pytest.mark.parametrize("invalid_date", ["20260801", "2026-W31-6"])
def test_submissions_rejects_noncanonical_iso_dates(invalid_date: str) -> None:
    payload = json.loads(json.dumps(SUBMISSIONS))
    payload["filings"]["recent"]["filingDate"][0] = invalid_date

    with pytest.raises(SECProviderError, match="YYYY-MM-DD"):
        submissions_to_frame(
            payload,
            expected_cik="0000320193",
            source_url="https://data.sec.gov/submissions/CIK0000320193.json",
            retrieved_at=RETRIEVED_AT,
        )


@pytest.mark.parametrize("invalid_date", ["20260801", "2026-W31-6"])
def test_companyfacts_rejects_noncanonical_iso_dates(invalid_date: str) -> None:
    payload = json.loads(json.dumps(COMPANYFACTS))
    payload["facts"]["us-gaap"]["Revenues"]["units"]["USD"][0]["end"] = invalid_date

    with pytest.raises(SECProviderError, match="YYYY-MM-DD"):
        companyfacts_to_frame(
            payload,
            expected_cik="0000320193",
            source_url=(
                "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
            ),
            retrieved_at=RETRIEVED_AT,
        )


def test_submissions_wraps_fixed_width_schema_overflow() -> None:
    payload = json.loads(json.dumps(SUBMISSIONS))
    payload["filings"]["recent"]["size"][0] = 2**80

    with pytest.raises(SECProviderError, match="does not satisfy"):
        submissions_to_frame(
            payload,
            expected_cik="0000320193",
            source_url="https://data.sec.gov/submissions/CIK0000320193.json",
            retrieved_at=RETRIEVED_AT,
        )


def test_companyfacts_wraps_fixed_width_schema_overflow() -> None:
    payload = json.loads(json.dumps(COMPANYFACTS))
    payload["facts"]["us-gaap"]["Revenues"]["units"]["USD"][0]["fy"] = 2**40

    with pytest.raises(SECProviderError, match="does not satisfy"):
        companyfacts_to_frame(
            payload,
            expected_cik="0000320193",
            source_url=(
                "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
            ),
            retrieved_at=RETRIEVED_AT,
        )
