"""Capture artifacts, register-capture, token units, and register references."""

from __future__ import annotations

import csv
import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner, Result

from finresearch.captures import load_capture
from finresearch.cases import initialize_case, read_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    CURRENCY_CODES,
    FACT_UNITS,
    NORMALIZED_FUNDAMENTAL_FACTS_V2,
    QUOTE_ASSETS,
    DataContractError,
    canonical_request_body,
    validate_capture_url,
)
from finresearch.registers import inspect_registers

runner = CliRunner()
URL = "https://api.coingecko.com/api/v3/coins/hyperliquid"
BODY = b'{"id":"hyperliquid"}'
RETRIEVED_AT = "2026-11-07T20:29:28Z"


def invoke(workspace: Path, *arguments: str) -> Result:
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    initialize_case(tmp_path, "hype")
    return tmp_path


def register(workspace: Path, tmp_path: Path, **overrides: str) -> Result:
    source = tmp_path / "capture.json"
    source.write_bytes(BODY)
    options = {
        "--provider": "coingecko",
        "--kind": "api-response",
        "--url": URL,
        "--retrieved-at": RETRIEVED_AT,
        "--http-status": "200",
        **overrides,
    }
    arguments = [item for pair in options.items() for item in pair]
    return invoke(
        workspace, "data", "register-capture", "hype", str(source), *arguments
    )


def test_register_capture_publishes_body_and_metadata(
    workspace: Path, tmp_path: Path
) -> None:
    result = register(workspace, tmp_path, **{"--expect-sha256": _sha(BODY)})

    assert result.exit_code == 0, result.output
    manifest = read_manifest(workspace / "cases" / "hype")
    body, metadata = sorted(manifest.artifacts, key=lambda item: item.kind)[::-1]
    assert (body.kind, metadata.kind) == ("raw.capture-body", "raw.capture")
    assert body.sha256 == _sha(BODY)
    assert metadata.input_artifact_ids == (body.artifact_id,)
    assert body.path.startswith("data/raw/captures/coingecko/api-coingecko-com-")
    assert str(tmp_path) not in (workspace / "cases/hype/manifest.toml").read_text()
    assert f"capture: {metadata.artifact_id}" in result.output
    assert invoke(workspace, "data", "validate", "hype").exit_code == 0

    loaded = load_capture(workspace / "cases" / "hype", manifest, metadata.artifact_id)
    assert loaded.body == BODY
    assert loaded.metadata["recorded_by"] == "agent"
    assert loaded.metadata["http_status"] == 200


def test_register_capture_rerun_is_idempotent_and_conflicts_fail(
    workspace: Path, tmp_path: Path
) -> None:
    first = register(workspace, tmp_path)
    manifest_bytes = (workspace / "cases/hype/manifest.toml").read_bytes()

    again = register(workspace, tmp_path)
    assert again.exit_code == 0
    assert again.output == first.output
    assert (workspace / "cases/hype/manifest.toml").read_bytes() == manifest_bytes

    conflict = register(workspace, tmp_path, **{"--http-status": "201"})
    assert conflict.exit_code == 1
    assert "conflict" in conflict.output
    assert (workspace / "cases/hype/manifest.toml").read_bytes() == manifest_bytes


def test_register_capture_checks_expected_hash_and_url(
    workspace: Path, tmp_path: Path
) -> None:
    mismatch = register(workspace, tmp_path, **{"--expect-sha256": "0" * 64})
    assert mismatch.exit_code == 1
    assert "differ from --expect-sha256" in mismatch.output

    for url in (
        "https://user:pw@api.coingecko.com/x",
        "https://api.coingecko.com/x?x_cg_demo_api_key=abc",
        "http://api.coingecko.com/x",
    ):
        rejected = register(workspace, tmp_path, **{"--url": url})
        assert rejected.exit_code == 1
    assert read_manifest(workspace / "cases" / "hype").artifacts == ()


def test_api_response_requires_status_and_post_body_is_canonical(
    workspace: Path, tmp_path: Path
) -> None:
    missing = register(workspace, tmp_path, **{"--http-status": ""})
    assert missing.exit_code != 0 or "http-status" in missing.output

    body_file = tmp_path / "body.json"
    body_file.write_text('{"tokenId":"0xabc","type":"tokenDetails"}')
    posted = register(
        workspace,
        tmp_path,
        **{
            "--provider": "hyperliquid",
            "--url": "https://api.hyperliquid.xyz/info",
            "--method": "POST",
            "--request-body": str(body_file),
        },
    )
    assert posted.exit_code == 0, posted.output
    capture = next(
        item
        for item in read_manifest(workspace / "cases/hype").artifacts
        if item.kind == "raw.capture"
    )
    loaded = load_capture(
        workspace / "cases/hype",
        read_manifest(workspace / "cases/hype"),
        capture.artifact_id,
    )
    assert loaded.request_body == '{"type":"tokenDetails","tokenId":"0xabc"}'


def test_capture_url_and_body_rules() -> None:
    validate_capture_url(
        "https://api.llama.fi/summary/fees/hyperliquid?dataType=dailyFees"
    )
    with pytest.raises(DataContractError, match="secret"):
        validate_capture_url("https://x.example/?Api-Secret=1")
    with pytest.raises(DataContractError, match="secret"):
        validate_capture_url("https://x.example/page#access_token=abc")
    assert (
        canonical_request_body('{"b":1,"type":"t","a":2}') == '{"type":"t","a":2,"b":1}'
    )


def test_new_fact_units_pair_with_unit_kind_and_currency() -> None:
    assert {"tokens", "bps", "USD/token", "EUR/token"} <= FACT_UNITS
    assert "USDT" in QUOTE_ASSETS and "USDT" not in CURRENCY_CODES

    def facts(**overrides: object) -> pl.DataFrame:
        row: dict[str, object] = {
            name: None for name in NORMALIZED_FUNDAMENTAL_FACTS_V2.schema
        }
        row.update(
            schema_version=2,
            provider="manual",
            fact_id="f1",
            unit="tokens",
            unit_kind="tokens",
            value_text="1",
            value=1.0,
            period_type="instant",
            end_date=date(2026, 1, 1),
            category="token-supply",
            source_artifact_id="raw",
            normalized_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        row.update(overrides)
        return pl.DataFrame([row], schema=NORMALIZED_FUNDAMENTAL_FACTS_V2.schema)

    NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(facts())
    NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(
        facts(unit="USD/token", unit_kind="currency", currency="USD")
    )
    NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(
        facts(unit="bps", unit_kind="ratio", category="protocol-metric")
    )
    for bad in (
        {"unit_kind": "count"},
        {"unit": "bps", "unit_kind": "currency"},
        {"unit": "USD/token", "unit_kind": "currency", "currency": "EUR"},
        {"unit": "EUR/token", "unit_kind": "currency"},
    ):
        with pytest.raises(DataContractError, match="requires unit_kind"):
            NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(facts(**bad))
    # Existing units keep their historical latitude.
    NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(facts(unit="USD", unit_kind="other"))


def write_registers(
    workspace: Path,
    evidence_ref: str,
    source_evidence: str,
    observed: str = "2026-11-07",
) -> None:
    registers = workspace / "cases" / "hype" / "registers"
    registers.mkdir(exist_ok=True)
    with (registers / "evidence.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["id", "claim", "source_type", "source_ref", "observed_at", "notes"]
        )
        writer.writerow(["e-a", "claim", "external", evidence_ref, observed, ""])
        writer.writerow(["e-b", "claim", "external", "text", "2026-11-07", ""])
        writer.writerow(["e-c", "claim", "external", "text", "2026-11-07", ""])
    with (registers / "assumptions.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "id",
                "parameter",
                "value",
                "unit",
                "rationale",
                "source_evidence",
                "updated_at",
            ]
        )
        writer.writerow(["a-1", "p", "1", "ratio", "r", source_evidence, "2026-11-07"])


def codes(workspace: Path) -> list[tuple[str, str]]:
    return [(i.code, i.message) for i in inspect_registers(workspace, "hype").issues]


def test_multi_id_source_evidence(workspace: Path) -> None:
    write_registers(workspace, "text", "e-a;e-b;e-c")
    assert codes(workspace) == []
    write_registers(workspace, "text", "e-b")
    assert codes(workspace) == []

    write_registers(workspace, "text", "e-a;e-zzz")
    [(code, message)] = codes(workspace)
    assert code == "register_dangling_reference"
    assert "'e-zzz'" in message

    for bad in ("e-b;e-a", "e-a;e-a", "e-a; e-b", "e-a;;e-b", "e-a;"):
        write_registers(workspace, "text", bad)
        assert [c for c, _ in codes(workspace)] == ["register_bad_source_evidence"]


def test_capture_references_in_evidence(workspace: Path, tmp_path: Path) -> None:
    assert register(workspace, tmp_path).exit_code == 0
    capture = next(
        item
        for item in read_manifest(workspace / "cases/hype").artifacts
        if item.kind == "raw.capture"
    )
    write_registers(workspace, f"see capture:{capture.artifact_id}.", "e-a")
    assert codes(workspace) == []

    write_registers(workspace, "capture:raw.capture.missing.2026", "e-a")
    [(code, message)] = codes(workspace)
    assert code == "register_capture_reference_invalid"
    assert "raw.capture.missing.2026" in message

    write_registers(
        workspace, f"capture:{capture.artifact_id}", "e-a", observed="2026-11-06"
    )
    assert [c for c, _ in codes(workspace)] == ["register_capture_after_observed"]


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
