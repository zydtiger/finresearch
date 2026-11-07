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


MASTER_V3_HEADER = (
    "instrument_id,entity_id,provider_symbol,primary_symbol,name,asset_class,"
    "instrument_type,venue_mic,country_code,trading_currency,valid_from,valid_to,"
    "observed_at,chain,contract_address,token_standard,provider_ids,"
    "asset_class_source"
)


def import_csv_text(
    workspace: Path,
    tmp_path: Path,
    name: str,
    text: str,
    schema: str,
    provider: str = "manual",
    retrieved_at: str = "2026-11-07T00:00:00Z",
) -> None:
    from typer.testing import CliRunner

    from finresearch.cli import app

    source = tmp_path / name
    source.write_text(text)
    result = CliRunner().invoke(
        app,
        [
            "--workspace",
            str(workspace),
            "data",
            "import-csv",
            "hype",
            str(source),
            "--schema",
            schema,
            "--provider",
            provider,
            "--retrieved-at",
            retrieved_at,
        ],
    )
    assert result.exit_code == 0, result.output


def import_master_v3(
    workspace: Path,
    tmp_path: Path,
    *,
    instrument_id: str = "hype.hyperliquid.l1",
    asset_class: str = "crypto",
    provider_ids: str = "coingecko:hyperliquid;yfinance:HYPE32196-USD",
    name: str = "master.csv",
    observed_at: str = "2026-11-06T00:00:00Z",
) -> None:
    crypto = asset_class == "crypto"
    row = (
        f"{instrument_id},,HYPE,HYPE,Hyperliquid,{asset_class},,,,USD,,,{observed_at},"
        f"{'hyperliquid-l1' if crypto else ''},,{'native' if crypto else ''},"
        f"{provider_ids},import"
    )
    import_csv_text(
        workspace,
        tmp_path,
        name,
        MASTER_V3_HEADER + "\n" + row + "\n",
        "instrument-master.v3",
        retrieved_at=observed_at,
    )


def import_prices(
    workspace: Path,
    tmp_path: Path,
    sessions: list[tuple[str, float]],
    *,
    instrument_id: str = "manual.hype",
    symbol: str = "HYPE32196-USD",
    name: str = "prices.csv",
) -> None:
    header = (
        "instrument_id,provider_symbol,currency,price_basis,provider_timezone,"
        "session_date,timestamp,open,high,low,close,volume,dividends,stock_splits"
    )
    rows = [
        f"{instrument_id},{symbol},USD,unadjusted,UTC,{day},{day}T00:00:00Z,"
        f"{close},{close},{close},{close},1000,0.0,0.0"
        for day, close in sessions
    ]
    import_csv_text(
        workspace,
        tmp_path,
        name,
        header + "\n" + "\n".join(rows) + "\n",
        "daily-prices.v2",
    )
