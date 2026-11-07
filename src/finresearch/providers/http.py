"""Keyless JSON HTTP boundary shared by the capture-based provider adapters."""

from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol

import httpx
from filelock import FileLock

from finresearch.providers import ProviderError

MAX_ERROR_TEXT: Final = 500


@dataclass(frozen=True)
class FetchedResponse:
    """One HTTP response: status, exact body bytes, and media type."""

    status: int
    body: bytes
    media_type: str


class RequestFetcher(Protocol):
    """Small fetch boundary that tests replace with a fake."""

    def fetch(self, method: str, url: str, body: str | None) -> FetchedResponse: ...


class HostRequestRateLimiter:
    """Space request starts across processes on this host with a file lock."""

    def __init__(
        self,
        name: str,
        minimum_interval_seconds: float,
        lock_path: Path | None = None,
    ) -> None:
        if minimum_interval_seconds <= 0:
            raise ValueError("minimum request interval must be positive")
        if lock_path is None:
            directory = Path(tempfile.gettempdir()) / "finresearch"
            directory.mkdir(parents=True, exist_ok=True)
            lock_path = directory / f"{name}-request-rate.lock"
        self._lock_path = lock_path
        self._timestamp_path = lock_path.with_suffix(".last")
        self._lock = FileLock(lock_path)
        self._interval = minimum_interval_seconds

    def wait(self) -> None:
        """Block until a host-shared request slot opens, then claim it."""
        with self._lock:
            try:
                last = float(self._timestamp_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                last = 0.0
            remaining = last + self._interval - time.time()
            if remaining > 0:
                time.sleep(remaining)
            self._timestamp_path.write_text(repr(time.time()), encoding="utf-8")


class HttpxFetcher:
    """Fetch with a finite timeout, no redirects, and host-shared spacing."""

    def __init__(
        self,
        limiter: HostRequestRateLimiter,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._limiter = limiter
        self._transport = transport

    def fetch(self, method: str, url: str, body: str | None) -> FetchedResponse:
        try:
            with httpx.Client(
                headers={"Accept": "application/json"},
                timeout=30.0,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                self._limiter.wait()
                response = client.request(
                    method,
                    url,
                    content=None if body is None else body.encode("utf-8"),
                    headers=None
                    if body is None
                    else {"Content-Type": "application/json"},
                )
        except httpx.HTTPError as exc:
            raise ProviderError(f"request failed for {url}: {exc}") from exc
        return FetchedResponse(
            status=response.status_code,
            body=response.content,
            media_type=response.headers.get("content-type", "application/json")
            .split(";")[0]
            .strip()
            or "application/json",
        )


def fetch_json(
    fetcher: RequestFetcher,
    provider: str,
    method: str,
    url: str,
    body: str | None = None,
) -> bytes:
    """Return the exact body of a 2xx JSON response or fail verbatim."""
    response = fetcher.fetch(method, url, body)
    if not 200 <= response.status <= 299:
        text = response.body.decode("utf-8", errors="replace")[:MAX_ERROR_TEXT]
        raise ProviderError(
            f"{provider} request failed for {url}: HTTP {response.status}: {text}"
        )
    try:
        json.loads(response.body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProviderError(f"{provider} returned non-JSON for {url}: {exc}") from exc
    return response.body
