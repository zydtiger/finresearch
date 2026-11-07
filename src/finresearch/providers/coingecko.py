"""CoinGecko keyless endpoints: URL construction and the rate-limited fetcher."""

from __future__ import annotations

import re
from typing import Final
from urllib.parse import quote, urlencode

from finresearch.providers import ProviderError
from finresearch.providers.http import (
    HostRequestRateLimiter,
    HttpxFetcher,
    RequestFetcher,
)

COINGECKO_BASE_URL: Final = "https://api.coingecko.com/api/v3"
# CoinGecko's keyless tier is throttled; request starts are at least 6 s apart.
COINGECKO_MINIMUM_REQUEST_INTERVAL_SECONDS: Final = 6.0
COIN_ID_PATTERN: Final = re.compile(r"[a-z0-9][a-z0-9._-]*")
MAX_MARKET_IDS: Final = 250


def default_fetcher() -> RequestFetcher:
    """Return the host-throttled keyless CoinGecko fetcher."""
    return HttpxFetcher(
        HostRequestRateLimiter("coingecko", COINGECKO_MINIMUM_REQUEST_INTERVAL_SECONDS)
    )


def validate_coin_id(coin_id: str) -> str:
    if COIN_ID_PATTERN.fullmatch(coin_id) is None:
        raise ProviderError(f"invalid CoinGecko coin id: {coin_id!r}")
    return coin_id


def coin_url(coin_id: str, vs_currency: str) -> str:
    """Return the `/coins/{id}` URL with the heavy sections disabled."""
    query = urlencode(
        {
            "community_data": "false",
            "developer_data": "false",
            "localization": "false",
            "tickers": "false",
            "vs_currency": vs_currency,
        }
    )
    return f"{COINGECKO_BASE_URL}/coins/{quote(validate_coin_id(coin_id))}?{query}"


def markets_url(coin_ids: tuple[str, ...], vs_currency: str) -> str:
    """Return the `/coins/markets` URL for a sorted, explicit id list."""
    ids = tuple(sorted({validate_coin_id(item) for item in coin_ids}))
    if not ids or len(ids) > MAX_MARKET_IDS:
        raise ProviderError(f"provide between 1 and {MAX_MARKET_IDS} distinct coin ids")
    query = urlencode(
        {
            "ids": ",".join(ids),
            "page": "1",
            "per_page": str(MAX_MARKET_IDS),
            "sparkline": "false",
            "vs_currency": vs_currency,
        },
        safe=",",
    )
    return f"{COINGECKO_BASE_URL}/coins/markets?{query}"
