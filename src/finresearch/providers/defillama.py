"""DefiLlama keyless per-protocol summary endpoints."""

from __future__ import annotations

import re
from typing import Final
from urllib.parse import quote

from finresearch.providers import ProviderError
from finresearch.providers.http import (
    HostRequestRateLimiter,
    HttpxFetcher,
    RequestFetcher,
)

DEFILLAMA_BASE_URL: Final = "https://api.llama.fi"
DEFILLAMA_MINIMUM_REQUEST_INTERVAL_SECONDS: Final = 0.5
SLUG_PATTERN: Final = re.compile(r"[a-z0-9][a-z0-9._-]*")
# dataset -> (endpoint group, dataType query value or None)
DATASETS: Final[dict[str, tuple[str, str | None]]] = {
    "fees": ("fees", "dailyFees"),
    "revenue": ("fees", "dailyRevenue"),
    "holders-revenue": ("fees", "dailyHoldersRevenue"),
    "dex-volume": ("dexs", "dailyVolume"),
    "open-interest": ("open-interest", None),
}


def default_fetcher() -> RequestFetcher:
    """Return the host-throttled keyless DefiLlama fetcher."""
    return HttpxFetcher(
        HostRequestRateLimiter("defillama", DEFILLAMA_MINIMUM_REQUEST_INTERVAL_SECONDS)
    )


def summary_url(slug: str, dataset: str) -> str:
    """Return the per-protocol summary URL for one dataset."""
    if SLUG_PATTERN.fullmatch(slug) is None:
        raise ProviderError(f"invalid DefiLlama protocol slug: {slug!r}")
    if dataset not in DATASETS:
        raise ProviderError(
            f"unknown dataset {dataset!r}; expected one of: {', '.join(DATASETS)}"
        )
    group, data_type = DATASETS[dataset]
    url = f"{DEFILLAMA_BASE_URL}/summary/{group}/{quote(slug)}"
    return url if data_type is None else f"{url}?dataType={data_type}"
