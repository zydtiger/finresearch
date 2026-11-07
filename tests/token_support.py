"""Shared fakes and fixtures for token provider tests (no network)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from finresearch.cases import initialize_case
from finresearch.providers.http import FetchedResponse

FIXTURES = Path(__file__).parent / "fixtures" / "token"
RETRIEVED_AT = datetime(2026, 11, 7, 20, 29, 28, tzinfo=UTC)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@dataclass
class FakeFetcher:
    """Return queued responses and record every request."""

    responses: list[FetchedResponse]
    requests: list[tuple[str, str, str | None]] = field(default_factory=list)

    def fetch(self, method: str, url: str, body: str | None) -> FetchedResponse:
        self.requests.append((method, url, body))
        return self.responses.pop(0)


def ok(content: bytes) -> FetchedResponse:
    return FetchedResponse(200, content, "application/json")


def new_case(workspace: Path, case_id: str = "hype") -> Path:
    return initialize_case(workspace, case_id)


def register_json(workspace: Path, tmp_path: Path, name: str, url: str) -> str:
    """Register a fixture as an agent capture and return its capture id."""
    from finresearch.captures import register_capture

    source = tmp_path / name
    source.write_bytes(fixture_bytes(name))
    return register_capture(
        workspace,
        "hype",
        source,
        provider="defillama",
        kind="api-response",
        url=url,
        retrieved_at=RETRIEVED_AT,
        http_status=200,
    ).capture_id
