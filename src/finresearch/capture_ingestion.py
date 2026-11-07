"""CLI-recorded captures from keyless provider endpoints."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from finresearch.captures import CaptureReceipt, case_context, publish_capture
from finresearch.data_contracts import QUOTE_ASSETS
from finresearch.ingestion import IngestionError
from finresearch.providers import ProviderError, coingecko, hyperliquid
from finresearch.providers.http import RequestFetcher, fetch_json


def _retrieval_time(value: datetime | None) -> datetime:
    retrieval = value or datetime.now(UTC)
    if retrieval.utcoffset() is None:
        raise IngestionError("retrieved_at must be timezone-aware")
    return retrieval.astimezone(UTC)


def _quote(vs_currency: str) -> str:
    quote = vs_currency.strip().upper()
    if quote not in QUOTE_ASSETS:
        raise IngestionError(
            f"--vs-currency must be one of: {', '.join(sorted(QUOTE_ASSETS))}"
        )
    return quote.lower()


def _record(
    workspace: Path,
    case_id: str,
    *,
    provider: str,
    method: str,
    url: str,
    request_body: str | None,
    fetcher: RequestFetcher,
    retrieved_at: datetime,
) -> CaptureReceipt:
    """Fetch one response and publish it; failures write nothing."""
    content = fetch_json(fetcher, provider, method, url, request_body)
    case_dir, manifest = case_context(workspace, case_id)
    return publish_capture(
        case_dir,
        manifest,
        provider=provider,
        kind="api-response",
        recorded_by="cli",
        method=method,
        url=url,
        request_body=request_body,
        http_status=200,
        media_type="application/json",
        content=content,
        retrieved_at=retrieved_at,
    )


def ingest_coingecko_token(
    workspace: Path,
    case_id: str,
    coin_id: str,
    *,
    vs_currency: str = "usd",
    fetcher: RequestFetcher | None = None,
    retrieved_at: datetime | None = None,
) -> CaptureReceipt:
    """Capture CoinGecko `/coins/{id}` for one explicit coin id."""
    url = coingecko.coin_url(coin_id, _quote(vs_currency))
    case_context(workspace, case_id)
    return _record(
        workspace,
        case_id,
        provider="coingecko",
        method="GET",
        url=url,
        request_body=None,
        fetcher=fetcher or coingecko.default_fetcher(),
        retrieved_at=_retrieval_time(retrieved_at),
    )


def ingest_coingecko_markets(
    workspace: Path,
    case_id: str,
    coin_ids: tuple[str, ...],
    *,
    vs_currency: str = "usd",
    fetcher: RequestFetcher | None = None,
    retrieved_at: datetime | None = None,
) -> CaptureReceipt:
    """Capture CoinGecko `/coins/markets` for an explicit id list."""
    url = coingecko.markets_url(coin_ids, _quote(vs_currency))
    case_context(workspace, case_id)
    return _record(
        workspace,
        case_id,
        provider="coingecko",
        method="GET",
        url=url,
        request_body=None,
        fetcher=fetcher or coingecko.default_fetcher(),
        retrieved_at=_retrieval_time(retrieved_at),
    )


def ingest_hyperliquid_info(
    workspace: Path,
    case_id: str,
    request: str,
    *,
    token_id: str | None = None,
    address: str | None = None,
    coin: str | None = None,
    start: date | None = None,
    fetcher: RequestFetcher | None = None,
    retrieved_at: datetime | None = None,
) -> tuple[CaptureReceipt, ...]:
    """Capture one Hyperliquid info request; funding history is one per page."""
    start_ms = None if start is None else hyperliquid.utc_midnight_ms(start)
    body = hyperliquid.info_body(
        request, token_id=token_id, address=address, coin=coin, start_ms=start_ms
    )
    case_context(workspace, case_id)
    retrieval = _retrieval_time(retrieved_at)
    active = fetcher or hyperliquid.default_fetcher()

    def record(request_body: str) -> tuple[CaptureReceipt, bytes]:
        content = fetch_json(
            active,
            "hyperliquid",
            "POST",
            hyperliquid.HYPERLIQUID_INFO_URL,
            request_body,
        )
        case_dir, manifest = case_context(workspace, case_id)
        receipt = publish_capture(
            case_dir,
            manifest,
            provider="hyperliquid",
            kind="api-response",
            recorded_by="cli",
            method="POST",
            url=hyperliquid.HYPERLIQUID_INFO_URL,
            request_body=request_body,
            http_status=200,
            media_type="application/json",
            content=content,
            retrieved_at=retrieval,
        )
        return receipt, content

    if request != "funding-history":
        return (record(body)[0],)

    assert start_ms is not None and coin is not None
    retrieval_ms = int(retrieval.timestamp() * 1000)
    if start_ms > retrieval_ms:
        raise IngestionError("--start must not be after the retrieval time")
    receipts: list[CaptureReceipt] = []
    cursor = start_ms
    while True:
        receipt, content = record(
            hyperliquid.info_body(request, coin=coin, start_ms=cursor)
        )
        receipts.append(receipt)
        entries = json.loads(content)
        if not isinstance(entries, list):
            raise ProviderError("fundingHistory response must be a JSON array")
        if len(entries) < hyperliquid.FUNDING_PAGE_SIZE:
            break
        times: list[int] = []
        for entry in entries:
            if isinstance(entry, dict):
                moment = entry.get("time")
                if isinstance(moment, int):
                    times.append(moment)
        next_cursor = max(times) + 1 if times else cursor
        if next_cursor <= cursor or next_cursor > retrieval_ms:
            break
        cursor = next_cursor
    return tuple(receipts)
