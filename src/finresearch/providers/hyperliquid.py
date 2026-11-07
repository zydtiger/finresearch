"""Hyperliquid keyless info endpoint: request bodies and the throttled fetcher."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from typing import Final

from finresearch.data_contracts import canonical_request_body
from finresearch.providers import ProviderError
from finresearch.providers.http import (
    HostRequestRateLimiter,
    HttpxFetcher,
    RequestFetcher,
)

HYPERLIQUID_INFO_URL: Final = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_MINIMUM_REQUEST_INTERVAL_SECONDS: Final = 0.25
FUNDING_PAGE_SIZE: Final = 500
SIMPLE_REQUESTS: Final = {
    "validator-summaries": "validatorSummaries",
    "meta-and-asset-ctxs": "metaAndAssetCtxs",
    "spot-meta-and-asset-ctxs": "spotMetaAndAssetCtxs",
}
REQUEST_NAMES: Final = (
    "token-details",
    "validator-summaries",
    "meta-and-asset-ctxs",
    "spot-meta-and-asset-ctxs",
    "vault-details",
    "spot-clearinghouse-state",
    "funding-history",
)
_HEX = re.compile(r"0x[0-9a-fA-F]+")
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
_COIN = re.compile(r"[A-Za-z0-9:_.-]+")


def default_fetcher() -> RequestFetcher:
    """Return the host-throttled keyless Hyperliquid fetcher."""
    return HttpxFetcher(
        HostRequestRateLimiter(
            "hyperliquid", HYPERLIQUID_MINIMUM_REQUEST_INTERVAL_SECONDS
        )
    )


def info_body(
    request: str,
    *,
    token_id: str | None = None,
    address: str | None = None,
    coin: str | None = None,
    start_ms: int | None = None,
) -> str:
    """Return the canonical POST body, rejecting unsupported option mixes."""
    if request not in REQUEST_NAMES:
        raise ProviderError(
            f"unknown request {request!r}; expected one of: {', '.join(REQUEST_NAMES)}"
        )
    required = {
        "token-details": {"token_id"},
        "vault-details": {"address"},
        "spot-clearinghouse-state": {"address"},
        "funding-history": {"coin", "start"},
    }.get(request, set())
    given = {
        name
        for name, value in (
            ("token_id", token_id),
            ("address", address),
            ("coin", coin),
            ("start", start_ms),
        )
        if value is not None
    }
    if given != required:
        flags = {
            "token_id": "--token-id",
            "address": "--address",
            "coin": "--coin",
            "start": "--start",
        }
        wanted = ", ".join(sorted(flags[name] for name in required)) or "no options"
        raise ProviderError(f"--request {request} requires exactly {wanted}")
    payload: dict[str, object]
    if request == "token-details":
        assert token_id is not None
        if _HEX.fullmatch(token_id) is None:
            raise ProviderError("--token-id must be a 0x-prefixed hex id")
        payload = {"type": "tokenDetails", "tokenId": token_id.lower()}
    elif request in {"vault-details", "spot-clearinghouse-state"}:
        assert address is not None
        if _ADDRESS.fullmatch(address) is None:
            raise ProviderError("--address must be a 0x-prefixed 20-byte hex address")
        if request == "vault-details":
            payload = {"type": "vaultDetails", "vaultAddress": address.lower()}
        else:
            payload = {"type": "spotClearinghouseState", "user": address.lower()}
    elif request == "funding-history":
        assert coin is not None and start_ms is not None
        if _COIN.fullmatch(coin) is None:
            raise ProviderError("--coin must be a perp market name such as HYPE")
        payload = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
    else:
        payload = {"type": SIMPLE_REQUESTS[request]}
    return canonical_request_body(json.dumps(payload))


def utc_midnight_ms(day: date) -> int:
    """Return a UTC date's midnight as epoch milliseconds."""
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)
