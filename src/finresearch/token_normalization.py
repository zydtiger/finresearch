"""Deterministic token supply and market normalization from captures."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, unquote, urlsplit

import polars as pl

from finresearch.captures import LoadedCapture, case_context, load_capture
from finresearch.cases import CaseManifest, canonical_parameters_sha256
from finresearch.data_contracts import (
    CURRENCY_CODES,
    NORMALIZED_TOKEN_MARKET_V1,
    NORMALIZED_TOKEN_SUPPLY_V1,
    DataContractError,
    DatasetContract,
)
from finresearch.ingestion import (
    IngestionError,
    IngestionReceipt,
    publish_snapshot,
    symbol_key,
)
from finresearch.json_values import json_number, json_object

TOKEN_PRODUCER: Final = "finresearch.data.normalize-token"
TOKEN_PRODUCER_VERSION: Final = "1"
# validatorSummaries reports stake in the staking token's wei (8 decimals).
HYPERLIQUID_STAKE_DECIMALS: Final = 8
HYPERLIQUID_STAKING_ASSET_ID: Final = "HYPE"
SUPPORTED_ENDPOINTS: Final = (
    "coingecko GET /api/v3/coins/{id}",
    "coingecko GET /api/v3/coins/markets",
    "hyperliquid POST /info type=tokenDetails",
    "hyperliquid POST /info type=validatorSummaries",
)


@dataclass
class ParsedAsset:
    """Provider-keyed rows of one asset, before an instrument id is applied."""

    supply: list[dict[str, object]] = field(default_factory=list)
    market: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class TokenNormalizationReceipt:
    """Artifacts published for one normalized capture."""

    supply: IngestionReceipt
    market: IngestionReceipt | None
    skipped_assets: tuple[str, ...]


Parser = Callable[[LoadedCapture, object], dict[str, ParsedAsset]]


def normalize_token_supply(
    workspace: Path,
    case_id: str,
    *,
    capture_artifact_id: str,
    instrument_id: str | None = None,
    instrument_map: Mapping[str, str] | None = None,
) -> TokenNormalizationReceipt:
    """Parse one capture into token-supply and token-market artifacts."""
    if (instrument_id is None) == (not instrument_map):
        raise IngestionError("pass exactly one of --instrument-id or --instrument-map")
    case_dir, manifest = case_context(workspace, case_id)
    capture = load_capture(case_dir, manifest, capture_artifact_id)
    parser = _select_parser(capture)
    try:
        payload = json.loads(capture.body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IngestionError(
            f"capture {capture_artifact_id} is not JSON: {exc}"
        ) from exc
    assets = parser(capture, payload)

    if instrument_id is not None:
        _require_instrument_id(instrument_id)
        if len(assets) != 1:
            raise IngestionError(
                f"capture holds {len(assets)} assets ({', '.join(sorted(assets))}); "
                "pass one --instrument-map PROVIDER_ASSET_ID=INSTRUMENT_ID per asset"
            )
        selection = {next(iter(assets)): instrument_id}
    else:
        selection = dict(instrument_map or {})
        for asset_id, mapped in selection.items():
            _require_instrument_id(mapped)
            if asset_id not in assets:
                raise IngestionError(
                    f"--instrument-map asset {asset_id!r} is not in the capture; "
                    f"present: {', '.join(sorted(assets))}"
                )
    skipped = tuple(sorted(set(assets) - set(selection)))

    supply_rows: list[dict[str, object]] = []
    market_rows: list[dict[str, object]] = []
    base = {
        "provider": capture.provider,
        "retrieved_at": capture.retrieved_at,
        "capture_artifact_id": capture.capture_id,
        "source_artifact_id": capture.capture_id,
        "normalized_at": capture.retrieved_at,
    }
    for asset_id in sorted(selection):
        identity = {
            "instrument_id": selection[asset_id],
            "provider_asset_id": asset_id,
        }
        supply_rows.extend(
            {
                "schema_version": NORMALIZED_TOKEN_SUPPLY_V1.version,
                **base,
                **identity,
                "unit": "tokens",
                "holder_address": None,
                "holder_label": None,
                **row,
            }
            for row in assets[asset_id].supply
        )
        market_rows.extend(
            {
                "schema_version": NORMALIZED_TOKEN_MARKET_V1.version,
                **base,
                **identity,
                "fdv_supply_basis": None,
                **row,
            }
            for row in assets[asset_id].market
        )
    if not supply_rows:
        raise IngestionError(f"capture {capture_artifact_id} yields no supply rows")

    parameters = {
        "input_artifact_ids": [capture.capture_id],
        "instrument_map": sorted(selection.items()),
        "normalized_at": capture.retrieved_at.isoformat(),
        "producer": TOKEN_PRODUCER,
        "producer_version": TOKEN_PRODUCER_VERSION,
    }
    entity = (
        symbol_key(next(iter(selection.values()))) if len(selection) == 1 else "multi"
    )
    supply_receipt = _publish(
        case_dir,
        manifest,
        NORMALIZED_TOKEN_SUPPLY_V1,
        supply_rows,
        capture,
        entity,
        parameters,
    )
    market_receipt = (
        _publish(
            case_dir,
            manifest,
            NORMALIZED_TOKEN_MARKET_V1,
            market_rows,
            capture,
            entity,
            parameters,
        )
        if market_rows
        else None
    )
    return TokenNormalizationReceipt(supply_receipt, market_receipt, skipped)


def _publish(
    case_dir: Path,
    manifest: CaseManifest,
    contract: DatasetContract,
    rows: list[dict[str, object]],
    capture: LoadedCapture,
    entity: str,
    parameters: dict[str, object],
) -> IngestionReceipt:
    try:
        frame = contract.canonical_sort(pl.DataFrame(rows, schema=contract.schema))
        contract.validate(frame)
    except (DataContractError, pl.exceptions.PolarsError) as exc:
        raise IngestionError(
            f"capture {capture.capture_id} cannot be normalized: {exc}"
        ) from exc
    identity = canonical_parameters_sha256(
        {**parameters, "contract": contract.identifier}
    )
    return publish_snapshot(
        case_dir=case_dir,
        manifest=manifest,
        path_role="normalized",
        frame=frame,
        contract=contract,
        path_parts=(contract.name, entity),
        entity_key=entity,
        identity=identity,
        producer=TOKEN_PRODUCER,
        producer_version=TOKEN_PRODUCER_VERSION,
        parameters_sha256=identity,
        input_artifact_ids=(capture.capture_id,),
        produced_at=capture.retrieved_at,
    )


def _require_instrument_id(value: str) -> None:
    if not value or value != value.strip() or any(c.isspace() for c in value):
        raise IngestionError(
            f"instrument id must be non-empty without spaces: {value!r}"
        )


def _select_parser(capture: LoadedCapture) -> Parser:
    url = urlsplit(capture.url)
    if capture.provider == "coingecko" and capture.metadata["request_method"] == "GET":
        if url.path == "/api/v3/coins/markets":
            return _parse_coingecko_markets
        if url.path.startswith("/api/v3/coins/") and url.path.count("/") == 4:
            return _parse_coingecko_coin
    if capture.provider == "hyperliquid" and capture.request_body is not None:
        request_type = json.loads(capture.request_body).get("type")
        if request_type == "tokenDetails":
            return _parse_hyperliquid_token_details
        if request_type == "validatorSummaries":
            return _parse_hyperliquid_validators
    raise IngestionError(
        f"capture {capture.capture_id} ({capture.provider} {capture.url}) has no "
        "token supply parser; supported endpoints: " + "; ".join(SUPPORTED_ENDPOINTS)
    )


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IngestionError(f"{label} must be an ISO 8601 timestamp: {value!r}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IngestionError(
            f"{label} must be an ISO 8601 timestamp: {value!r}"
        ) from exc
    if parsed.utcoffset() is None:
        raise IngestionError(f"{label} must carry a UTC offset: {value!r}")
    return parsed.astimezone(UTC)


def _observation(
    capture: LoadedCapture, provider_time: object, label: str
) -> tuple[datetime, str]:
    if provider_time is None:
        return capture.retrieved_at, "retrieval"
    return _timestamp(provider_time, label), "provider"


def _quote_from_url(url: str) -> tuple[str, str | None]:
    values = parse_qs(urlsplit(url).query).get("vs_currency", ["usd"])
    quote = values[0].upper()
    return quote, quote if quote in CURRENCY_CODES else None


_COINGECKO_SUPPLY: Final = (
    ("circulating", "circulating_supply"),
    ("total", "total_supply"),
    ("max", "max_supply"),
)


def _coingecko_asset(
    capture: LoadedCapture,
    *,
    supply_source: Mapping[str, object],
    value_of: Callable[[str], object],
    provider_time: object,
    label: str,
) -> ParsedAsset:
    observed_at, basis = _observation(capture, provider_time, f"{label}.last_updated")
    quote, _ = _quote_from_url(capture.url)
    asset = ParsedAsset()
    for metric, name in _COINGECKO_SUPPLY:
        amount = json_number(supply_source.get(name), f"{label}.{name}")
        if amount is not None:
            asset.supply.append(
                {
                    "supply_metric": metric,
                    "amount": amount,
                    "definition_id": f"coingecko.market_data.{name}",
                    "observed_at": observed_at,
                    "observed_at_basis": basis,
                }
            )
    for metric, name, basis_value in (
        ("price", "current_price", None),
        ("market_cap", "market_cap", None),
        ("fdv", "fully_diluted_valuation", "total"),
        ("volume_24h", "total_volume", None),
    ):
        value = json_number(value_of(name), f"{label}.{name}")
        if value is None or (value == 0 and metric in {"market_cap", "fdv"}):
            continue
        asset.market.append(
            {
                "metric": metric,
                "value": value,
                "quote_asset": quote,
                "currency": quote if quote in CURRENCY_CODES else None,
                "fdv_supply_basis": basis_value,
                "observed_at": observed_at,
            }
        )
    return asset


def _parse_coingecko_coin(
    capture: LoadedCapture, payload: object
) -> dict[str, ParsedAsset]:
    document = json_object(payload, "CoinGecko /coins/{id} response")
    coin_id = document.get("id")
    expected = unquote(urlsplit(capture.url).path.rsplit("/", 1)[1])
    if coin_id != expected:
        raise IngestionError(f"CoinGecko response id {coin_id!r} is not {expected!r}")
    market_data = json_object(document.get("market_data"), "market_data")
    quote = _quote_from_url(capture.url)[0].lower()

    def value_of(name: str) -> object:
        field_value = market_data.get(name)
        if isinstance(field_value, dict):
            return field_value.get(quote)
        return field_value

    return {
        expected: _coingecko_asset(
            capture,
            supply_source=market_data,
            value_of=value_of,
            provider_time=market_data.get("last_updated"),
            label="market_data",
        )
    }


def _parse_coingecko_markets(
    capture: LoadedCapture, payload: object
) -> dict[str, ParsedAsset]:
    if not isinstance(payload, list):
        raise IngestionError("CoinGecko /coins/markets response must be a JSON array")
    assets: dict[str, ParsedAsset] = {}
    for index, entry in enumerate(payload):
        item = json_object(entry, f"markets[{index}]")
        coin_id = item.get("id")
        if not isinstance(coin_id, str) or not coin_id:
            raise IngestionError(f"markets[{index}].id must be a non-empty string")
        if coin_id in assets:
            raise IngestionError(f"markets response repeats coin id {coin_id!r}")
        assets[coin_id] = _coingecko_asset(
            capture,
            supply_source=item,
            value_of=item.get,
            provider_time=item.get("last_updated"),
            label=f"markets[{index}]",
        )
    if not assets:
        raise IngestionError("CoinGecko /coins/markets response is empty")
    return assets


def _parse_hyperliquid_token_details(
    capture: LoadedCapture, payload: object
) -> dict[str, ParsedAsset]:
    document = json_object(payload, "Hyperliquid tokenDetails response")
    request = json.loads(capture.request_body or "{}")
    token_id = request.get("tokenId")
    if not isinstance(token_id, str):
        raise IngestionError("tokenDetails capture request has no tokenId")
    asset = ParsedAsset()
    common = {
        "observed_at": capture.retrieved_at,
        "observed_at_basis": "retrieval",
    }
    for metric, name in (
        ("circulating", "circulatingSupply"),
        ("total", "totalSupply"),
        ("max", "maxSupply"),
        ("future_emissions", "futureEmissions"),
    ):
        amount = json_number(document.get(name), f"tokenDetails.{name}")
        if amount is not None:
            asset.supply.append(
                {
                    "supply_metric": metric,
                    "amount": amount,
                    "definition_id": f"hyperliquid.tokenDetails.{name}",
                    **common,
                }
            )
    balances = document.get("nonCirculatingUserBalances") or []
    if not isinstance(balances, list):
        raise IngestionError("tokenDetails.nonCirculatingUserBalances must be an array")
    for index, entry in enumerate(balances):
        if not (
            isinstance(entry, list) and len(entry) == 2 and isinstance(entry[0], str)
        ):
            raise IngestionError(
                f"nonCirculatingUserBalances[{index}] must be [address, amount]"
            )
        amount = json_number(entry[1], f"nonCirculatingUserBalances[{index}]")
        if amount is None:
            raise IngestionError(f"nonCirculatingUserBalances[{index}] has no amount")
        asset.supply.append(
            {
                "supply_metric": "non_circulating",
                "amount": amount,
                "definition_id": "hyperliquid.tokenDetails.nonCirculatingUserBalances",
                "holder_address": entry[0],
                **common,
            }
        )
    return {token_id: asset}


def _parse_hyperliquid_validators(
    capture: LoadedCapture, payload: object
) -> dict[str, ParsedAsset]:
    if not isinstance(payload, list) or not payload:
        raise IngestionError("validatorSummaries response must be a non-empty array")
    total = 0
    for index, entry in enumerate(payload):
        stake = json_object(entry, f"validatorSummaries[{index}]").get("stake")
        if isinstance(stake, bool) or not isinstance(stake, int) or stake < 0:
            raise IngestionError(
                f"validatorSummaries[{index}].stake must be a nonnegative integer"
            )
        total += stake
    asset = ParsedAsset()
    asset.supply.append(
        {
            "supply_metric": "staked",
            "amount": total / 10**HYPERLIQUID_STAKE_DECIMALS,
            "definition_id": "hyperliquid.validatorSummaries.stake",
            "observed_at": capture.retrieved_at,
            "observed_at_basis": "retrieval",
        }
    )
    return {HYPERLIQUID_STAKING_ASSET_ID: asset}
