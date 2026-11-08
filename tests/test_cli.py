import json
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path

import pytest
from typer.testing import CliRunner, Result

import finresearch
from finresearch.auditing import CaseAudit
from finresearch.captures import CaptureReceipt
from finresearch.cases import (
    DEFAULT_PATHS,
    CaseManifest,
    ValidationIssue,
    write_manifest,
)
from finresearch.cli import app
from finresearch.ingestion import IngestionReceipt
from finresearch.modeling import ModelRun
from finresearch.providers.sec import SECProviderError

runner = CliRunner()


def test_package_version_comes_from_installed_metadata() -> None:
    assert finresearch.__version__ == version("finresearch")


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


def invoke_json(workspace: Path, *arguments: str) -> tuple[Result, dict[str, object]]:
    """Invoke with --json and parse the single JSON object on stdout."""
    result = runner.invoke(app, ["--workspace", str(workspace), "--json", *arguments])
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict)
    return result, payload


def test_case_lifecycle(tmp_path: Path) -> None:
    initialized = invoke(
        tmp_path,
        "case",
        "init",
        "aapl-2026-08-11",
        "--title",
        "Apple valuation update",
    )

    assert initialized.exit_code == 0
    assert "created case: aapl-2026-08-11" in initialized.output

    status = invoke(tmp_path, "case", "status", "aapl-2026-08-11")
    assert status.exit_code == 0
    assert "manifest: valid" in status.output
    assert "status: active" in status.output
    assert "directories: 4/4 required" in status.output
    assert "artifacts: 0 declared, 0 present, 0 missing" in status.output
    assert "valid: yes" in status.output

    validated = invoke(tmp_path, "case", "validate", "aapl-2026-08-11")
    assert validated.exit_code == 0
    assert validated.output.strip() == "valid case: aapl-2026-08-11"


def test_case_init_does_not_overwrite_collision(tmp_path: Path) -> None:
    assert invoke(tmp_path, "case", "init", "aapl").exit_code == 0
    manifest = tmp_path / "cases" / "aapl" / "manifest.toml"
    original = manifest.read_bytes()

    repeated = invoke(tmp_path, "case", "init", "aapl")

    assert repeated.exit_code == 1
    assert "case already exists: aapl" in repeated.output
    assert manifest.read_bytes() == original


def test_case_migrate_upgrades_v1_and_is_idempotent(tmp_path: Path) -> None:
    case_dir = tmp_path / "cases" / "legacy"
    case_dir.mkdir(parents=True)
    write_manifest(
        case_dir,
        CaseManifest(
            manifest_version=1,
            case_id="legacy",
            title="Legacy case",
            status="active",
            paths=dict(DEFAULT_PATHS),
            artifacts=(),
        ),
    )

    migrated = invoke(tmp_path, "case", "migrate", "legacy")
    repeated = invoke(tmp_path, "case", "migrate", "legacy")

    assert migrated.exit_code == 0
    assert "migrated case manifest to v2: legacy" in migrated.output
    assert repeated.exit_code == 0
    assert "case manifest already at v2: legacy" in repeated.output


def test_case_status_reports_missing_case(tmp_path: Path) -> None:
    result = invoke(tmp_path, "case", "status", "missing")

    assert result.exit_code == 1
    assert "manifest: invalid" in result.output
    assert "error [case_missing]: case not found: missing" in result.output


def test_case_status_requires_workspace() -> None:
    result = runner.invoke(app, ["case", "status", "aapl"])

    assert result.exit_code == 2
    assert "Missing option '--workspace'" in result.output


def test_case_status_rejects_file_as_workspace(tmp_path: Path) -> None:
    workspace_file = tmp_path / "workspace.txt"
    workspace_file.write_text("not a directory", encoding="utf-8")

    result = runner.invoke(
        app,
        ["--workspace", str(workspace_file), "case", "status", "aapl"],
    )

    assert result.exit_code == 2
    assert "Directory" in result.output


def test_yfinance_ingestion_command_reports_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_ingestion(
        workspace: Path,
        case_id: str,
        symbol: str,
        start: date,
        end: date,
    ) -> IngestionReceipt:
        assert workspace == tmp_path
        assert (case_id, symbol) == ("aapl", "AAPL")
        assert (start, end) == (date(2026, 1, 2), date(2026, 1, 3))
        return IngestionReceipt(
            artifact_id="raw.yfinance.daily-prices.aapl.snapshot",
            path=tmp_path / "snapshot.parquet",
            row_count=1,
            sha256="a" * 64,
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    monkeypatch.setattr("finresearch.cli.ingest_yfinance_daily_prices", fake_ingestion)

    result = invoke(
        tmp_path,
        "data",
        "ingest-yfinance-prices",
        "aapl",
        "AAPL",
        "--start",
        "2026-01-02",
        "--end",
        "2026-01-03",
    )

    assert result.exit_code == 0
    assert "artifact: raw.yfinance.daily-prices.aapl.snapshot" in result.output
    assert "rows: 1" in result.output
    assert f"sha256: {'a' * 64}" in result.output


def test_yfinance_ingestion_command_rejects_invalid_date(tmp_path: Path) -> None:
    result = invoke(
        tmp_path,
        "data",
        "ingest-yfinance-prices",
        "aapl",
        "AAPL",
        "--start",
        "01/02/2026",
        "--end",
        "2026-01-03",
    )

    assert result.exit_code == 1
    assert "start must use YYYY-MM-DD format" in result.output


@pytest.mark.parametrize(
    ("command", "target"),
    [
        ("ingest-sec-submissions", "finresearch.cli.ingest_sec_submissions"),
        ("ingest-sec-companyfacts", "finresearch.cli.ingest_sec_companyfacts"),
    ],
)
def test_sec_ingestion_commands_report_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    target: str,
) -> None:
    def fake_ingestion(
        workspace: Path,
        case_id: str,
        cik: str,
        user_agent: str,
    ) -> IngestionReceipt:
        assert workspace == tmp_path
        assert (case_id, cik) == ("aapl", "320193")
        assert user_agent == "Finresearch user@example.com"
        return IngestionReceipt(
            artifact_id=f"raw.sec.{command}.snapshot",
            path=tmp_path / "sec.parquet",
            row_count=2,
            sha256="b" * 64,
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    monkeypatch.setattr(target, fake_ingestion)

    result = invoke(
        tmp_path,
        "data",
        command,
        "aapl",
        "320193",
        "--user-agent",
        "Finresearch user@example.com",
    )

    assert result.exit_code == 0
    assert f"artifact: raw.sec.{command}.snapshot" in result.output
    assert "rows: 2" in result.output


def test_sec_schema_failure_is_printed_as_concise_cli_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_ingestion(
        workspace: Path,
        case_id: str,
        cik: str,
        user_agent: str,
    ) -> IngestionReceipt:
        raise SECProviderError("SEC data does not satisfy raw.sec.companyfacts.v1")

    monkeypatch.setattr(
        "finresearch.cli.ingest_sec_companyfacts",
        fail_ingestion,
    )

    result = invoke(
        tmp_path,
        "data",
        "ingest-sec-companyfacts",
        "aapl",
        "320193",
        "--user-agent",
        "Finresearch user@example.com",
    )

    assert result.exit_code == 1
    assert (
        result.output.strip()
        == "error: SEC data does not satisfy raw.sec.companyfacts.v1"
    )


def test_case_status_reports_unused_raw_snapshots_without_failing(
    tmp_path: Path,
) -> None:
    from token_support import import_prices, new_case

    from finresearch.cases import read_manifest

    case_dir = new_case(tmp_path)
    import_prices(tmp_path, tmp_path, [("2026-11-06", 78.0)])
    used = invoke(tmp_path, "case", "status", "hype")
    assert used.exit_code == 0
    assert "unused raw snapshots: 0" in used.output

    manifest = read_manifest(case_dir)
    raw_ids = [a.artifact_id for a in manifest.artifacts if a.kind == "raw.import"]
    kept = tuple(a for a in manifest.artifacts if a.kind == "raw.import")
    write_manifest(case_dir, CaseManifest(**{**vars(manifest), "artifacts": kept}))
    unused = invoke(tmp_path, "case", "status", "hype")
    assert unused.exit_code == 0
    assert "unused raw snapshots: 1" in unused.output
    assert raw_ids[0] in unused.output


def _receipt(tmp_path: Path, name: str) -> IngestionReceipt:
    return IngestionReceipt(
        artifact_id=name,
        path=tmp_path / f"{name}.parquet",
        row_count=3,
        sha256="c" * 64,
        retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
    )


def test_json_ingestion_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "finresearch.cli.ingest_yfinance_daily_prices",
        lambda *args: _receipt(tmp_path, "raw.snap"),
    )

    result, payload = invoke_json(
        tmp_path,
        "data",
        "ingest-yfinance-prices",
        "aapl",
        "AAPL",
        "--start",
        "2026-01-02",
        "--end",
        "2026-01-03",
    )

    assert result.exit_code == 0
    assert payload == {
        "ok": True,
        "artifact_id": "raw.snap",
        "path": str(tmp_path / "raw.snap.parquet"),
        "row_count": 3,
        "sha256": "c" * 64,
    }


def test_json_normalize_receipt_nests_each_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from finresearch.normalization import NormalizationReceipt

    monkeypatch.setattr(
        "finresearch.cli.normalize_daily_prices",
        lambda *args, **kwargs: NormalizationReceipt(
            _receipt(tmp_path, "master"),
            _receipt(tmp_path, "prices"),
            _receipt(tmp_path, "actions"),
        ),
    )

    result, payload = invoke_json(tmp_path, "data", "normalize-daily-prices", "a", "X")

    assert result.exit_code == 0
    assert payload["ok"] is True
    daily = payload["daily_prices"]
    assert isinstance(daily, dict)
    assert daily["artifact_id"] == "prices"


def test_json_capture_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    capture = tmp_path / "body.json"
    capture.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "finresearch.cli.register_capture",
        lambda *args, **kwargs: CaptureReceipt(
            capture_id="capture.web.1",
            body_artifact_id="raw.capture.web.1.body",
            body_path=tmp_path / "body.json",
            metadata_path=tmp_path / "meta.json",
            content_sha256="d" * 64,
            content_bytes=2,
            retrieved_at=datetime(2026, 8, 11, tzinfo=UTC),
        ),
    )

    result, payload = invoke_json(
        tmp_path,
        "data",
        "register-capture",
        "aapl",
        str(capture),
        "--provider",
        "web",
        "--kind",
        "api-response",
        "--url",
        "https://example.com/x",
        "--retrieved-at",
        "2026-08-11T00:00:00Z",
    )

    assert result.exit_code == 0
    assert payload == {
        "ok": True,
        "capture_id": "capture.web.1",
        "body_artifact_id": "raw.capture.web.1.body",
        "path": str(tmp_path / "body.json"),
        "sha256": "d" * 64,
    }


def test_json_model_run_lists_run_and_artifact_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "finresearch.cli.run_dcf",
        lambda *args, **kwargs: ModelRun(
            "run-1", (_receipt(tmp_path, "a"), _receipt(tmp_path, "b"))
        ),
    )

    result, payload = invoke_json(tmp_path, "model", "dcf", "aapl")

    assert result.exit_code == 0
    assert payload == {"ok": True, "run_id": "run-1", "artifacts": ["a", "b"]}


def test_json_case_init_and_status(tmp_path: Path) -> None:
    created, init_payload = invoke_json(tmp_path, "case", "init", "aapl")
    status, status_payload = invoke_json(tmp_path, "case", "status", "aapl")

    assert created.exit_code == 0
    assert init_payload["case_id"] == "aapl"
    assert status.exit_code == 0
    assert status_payload["ok"] is True
    assert status_payload["issues"] == []
    assert status_payload["directories_present"] == 4


def test_json_audit_failure_reports_issues_and_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "finresearch.cli.audit_case",
        lambda *args, **kwargs: CaseAudit(
            "aapl",
            date(2026, 8, 11),
            (ValidationIssue("price_stale", "price is 9 days old"),),
        ),
    )

    result, payload = invoke_json(
        tmp_path,
        "case",
        "audit",
        "aapl",
        "--as-of",
        "2026-08-11",
        "--max-price-age-days",
        "3",
    )

    assert result.exit_code == 1
    assert payload == {
        "ok": False,
        "case_id": "aapl",
        "as_of": "2026-08-11",
        "issues": [{"code": "price_stale", "message": "price is 9 days old"}],
    }
    assert "error [price_stale]: price is 9 days old" in result.stderr


def test_json_error_prints_object_and_keeps_stderr(tmp_path: Path) -> None:
    result, payload = invoke_json(tmp_path, "case", "migrate", "missing")

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert isinstance(payload["error"], str)
    assert result.stderr.strip() == f"error: {payload['error']}"


def test_json_flag_absent_keeps_human_output(tmp_path: Path) -> None:
    result = invoke(tmp_path, "case", "init", "aapl")

    assert result.stdout.startswith("created case: aapl\nmanifest: ")
