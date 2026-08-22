"""One offline public-CLI case workflow from import through deterministic reports."""

from __future__ import annotations

import csv
import subprocess
from pathlib import Path

import polars as pl
from typer.testing import CliRunner, Result

from finresearch.cases import read_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    NORMALIZED_DAILY_PRICES_V2,
    NORMALIZED_FUNDAMENTAL_FACTS_V2,
)

runner = CliRunner()


def _invoke(workspace: Path, *arguments: str) -> Result:
    result = runner.invoke(app, ["--workspace", str(workspace), *arguments])
    assert result.exit_code == 0, result.output
    return result


def _write_csv(path: Path, header: list[str], row: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerow(row)


def _write_audited_registers(case_dir: Path) -> None:
    registers = case_dir / "registers"
    registers.mkdir()
    evidence = [
        "id",
        "claim",
        "source_type",
        "source_ref",
        "observed_at",
        "notes",
    ]
    assumptions = [
        "id",
        "parameter",
        "value",
        "unit",
        "rationale",
        "source_evidence",
        "updated_at",
    ]
    with (registers / "evidence.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(evidence)
        writer.writerows(
            [
                ["e1", "market capitalization", "filing", "fixture", "2026-06-30", ""],
                ["e2", "debt", "filing", "fixture", "2026-06-30", ""],
                ["e3", "cash", "filing", "fixture", "2026-06-30", ""],
                ["e4", "shares", "filing", "fixture", "2026-06-30", ""],
            ]
        )
    with (registers / "assumptions.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.writer(handle)
        writer.writerow(assumptions)
        writer.writerows(
            [
                ["a1", "cost_equity", "0.10", "ratio", "fixture", "e1", "2026-06-30"],
                ["a2", "cost_debt", "0.05", "ratio", "fixture", "e1", "2026-06-30"],
                ["a3", "tax_rate", "0.25", "ratio", "fixture", "e1", "2026-06-30"],
                ["a4", "debt_weight", "0.20", "ratio", "fixture", "e1", "2026-06-30"],
                ["a5", "forecast_fcf", "80", "USDm", "fixture", "e1", "2026-06-30"],
                [
                    "a6",
                    "terminal_growth",
                    "0.02",
                    "ratio",
                    "fixture",
                    "e1",
                    "2026-06-30",
                ],
            ]
        )


def _write_dcf_input(case_dir: Path) -> None:
    analysis = case_dir / "analysis"
    analysis.mkdir()
    (analysis / "dcf-inputs.toml").write_text(
        """version = 1
as_of = "2026-06-30"
currency = "USD"
value_unit = "USDm"
share_unit = "shares_m"
discount_convention = "year_end"
terminal_method = "gordon_growth"
projection_needs = []
[wacc]
cost_equity = { value=0.1, unit="ratio", source_id="a1" }
cost_debt = { value=0.05, unit="ratio", source_id="a2" }
tax_rate = { value=0.25, unit="ratio", source_id="a3" }
debt_weight = { value=0.2, unit="ratio", source_id="a4" }
[capitalization]
market_cap = { value=1000, unit="USDm", source_id="e1" }
debt = { value=200, unit="USDm", source_id="e2" }
cash = { value=50, unit="USDm", source_id="e3" }
diluted_shares = { value=100, unit="shares_m", source_id="e4" }
[scenario.bear]
forecast = [
  { period_end="2026-12-31", free_cash_flow={ value=70, unit="USDm", source_id="a5" } },
]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
[scenario.base]
forecast = [
  { period_end="2026-12-31", free_cash_flow={ value=80, unit="USDm", source_id="a5" } },
]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
[scenario.bull]
forecast = [
  { period_end="2026-12-31", free_cash_flow={ value=90, unit="USDm", source_id="a5" } },
]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
""",
        encoding="utf-8",
    )


def _run_id(result: Result) -> str:
    return next(
        line.removeprefix("run_id: ")
        for line in result.output.splitlines()
        if line.startswith("run_id: ")
    )


def test_repository_hygiene_ignores_only_root_local_state() -> None:
    repository = Path(__file__).resolve().parents[1]
    for directory in (
        ".finresearch-cache",
        "provider-cache",
        "research-workspaces",
        "licensed-documents",
        "generated-artifacts",
    ):
        ignored = subprocess.run(
            ["git", "check-ignore", "--no-index", "--quiet", f"{directory}/x"],
            cwd=repository,
            check=False,
        )
        assert ignored.returncode == 0
        for trackable in (f"tests/{directory}/x", f"docs/{directory}/x"):
            checked = subprocess.run(
                ["git", "check-ignore", "--no-index", "--quiet", trackable],
                cwd=repository,
                check=False,
            )
            assert checked.returncode == 1


def test_offline_cli_e2e_case_to_svg_reports(tmp_path: Path) -> None:
    case_id = "offline-demo"
    daily_source = tmp_path / "daily-prices.csv"
    facts_source = tmp_path / "fundamental-facts.csv"
    _write_csv(
        daily_source,
        [
            "instrument_id",
            "provider_symbol",
            "currency",
            "price_basis",
            "provider_timezone",
            "session_date",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividends",
            "stock_splits",
        ],
        [
            "manual.acme",
            "ACME",
            "USD",
            "unadjusted",
            "America/New_York",
            "2026-06-30",
            "2026-06-30T20:00:00Z",
            "100",
            "102",
            "99",
            "101",
            "1000",
            "0",
            "0",
        ],
    )
    _write_csv(
        facts_source,
        [
            "fact_id",
            "entity_id",
            "instrument_id",
            "cik",
            "taxonomy",
            "concept",
            "metric_id",
            "category",
            "canonical_metric",
            "label",
            "unit",
            "unit_kind",
            "currency",
            "value_text",
            "value",
            "period_type",
            "start_date",
            "end_date",
            "fiscal_year",
            "fiscal_period",
            "form",
            "accession_number",
            "filed_date",
            "knowledge_date",
            "available_at",
            "frame",
        ],
        [
            "fact-revenue-2026q2",
            "0000000001",
            "manual.acme",
            "0000000001",
            "us-gaap",
            "Revenue",
            "us-gaap:Revenue",
            "income-statement",
            "revenue",
            "Revenue",
            "USD",
            "currency",
            "USD",
            "100",
            "100",
            "duration",
            "2026-01-01",
            "2026-06-30",
            "2026",
            "Q2",
            "10-Q",
            "0000000001-26-000001",
            "2026-06-30",
            "2026-06-30",
            "2026-06-30T12:00:00Z",
            "",
        ],
    )

    initialized = _invoke(tmp_path, "case", "init", case_id, "--title", "Offline demo")
    assert "created case" in initialized.output
    case_dir = tmp_path / "cases" / case_id
    assert "manifest_version = 2" in (case_dir / "manifest.toml").read_text(
        encoding="utf-8"
    )

    daily_import = _invoke(
        tmp_path,
        "data",
        "import-csv",
        case_id,
        str(daily_source),
        "--schema",
        "daily-prices.v2",
        "--provider",
        "manual",
        "--retrieved-at",
        "2026-06-30T21:00:00Z",
    )
    facts_import = _invoke(
        tmp_path,
        "data",
        "import-csv",
        case_id,
        str(facts_source),
        "--schema",
        "fundamental-facts.v2",
        "--provider",
        "manual",
        "--retrieved-at",
        "2026-06-30T21:00:00Z",
    )
    assert "raw import:" in daily_import.output
    assert "normalized import:" in facts_import.output
    assert _invoke(tmp_path, "data", "validate", case_id).output.startswith("valid:")

    manifest = read_manifest(case_dir)
    daily = next(
        item for item in manifest.artifacts if item.kind == "normalized.daily-prices"
    )
    facts = next(
        item
        for item in manifest.artifacts
        if item.kind == "normalized.fundamental-facts"
    )
    assert daily.schema_version == 2 and facts.schema_version == 2
    assert len(daily.input_artifact_ids) == len(facts.input_artifact_ids) == 1
    NORMALIZED_DAILY_PRICES_V2.validate(pl.read_parquet(case_dir / daily.path))
    NORMALIZED_FUNDAMENTAL_FACTS_V2.validate(pl.read_parquet(case_dir / facts.path))

    _write_audited_registers(case_dir)
    _write_dcf_input(case_dir)
    dcf = _invoke(
        tmp_path,
        "model",
        "dcf",
        case_id,
        "--input",
        "analysis/dcf-inputs.toml",
        "--scenario",
        "all",
        "--sensitivity",
        "0.08,0.10;0.01,0.02",
    )
    run_id = _run_id(dcf)
    first_model_hashes = {
        item.artifact_id: item.sha256
        for item in read_manifest(case_dir).artifacts
        if item.kind.startswith("model.dcf-")
    }
    assert {
        item.kind
        for item in read_manifest(case_dir).artifacts
        if item.kind.startswith("model.dcf-")
    } == {
        "model.dcf-inputs",
        "model.dcf-cashflows",
        "model.dcf-results",
        "model.dcf-reconciliation",
        "model.dcf-sensitivity",
    }
    repeat_dcf = _invoke(
        tmp_path,
        "model",
        "dcf",
        case_id,
        "--input",
        "analysis/dcf-inputs.toml",
        "--scenario",
        "all",
        "--sensitivity",
        "0.08,0.10;0.01,0.02",
    )
    assert _run_id(repeat_dcf) == run_id
    assert dcf.output == repeat_dcf.output
    assert first_model_hashes == {
        item.artifact_id: item.sha256
        for item in read_manifest(case_dir).artifacts
        if item.kind.startswith("model.dcf-")
    }

    projection = _invoke(
        tmp_path,
        "model",
        "projection-assess",
        case_id,
        "--input",
        "analysis/dcf-inputs.toml",
    )
    projection_id = next(
        line.removeprefix("artifact: ")
        for line in projection.output.splitlines()
        if line.startswith("artifact: ")
    )
    manifest = read_manifest(case_dir)
    projection_artifact = next(
        item for item in manifest.artifacts if item.artifact_id == projection_id
    )
    assert pl.read_parquet(case_dir / projection_artifact.path)["status"].to_list() == [
        "not_required"
    ]

    assert _invoke(
        tmp_path,
        "case",
        "audit",
        case_id,
        "--as-of",
        "2026-06-30",
        "--max-price-age-days",
        "0",
        "--verify-hashes",
    ).output.endswith("valid: yes\n")

    markdown = _invoke(
        tmp_path, "report", "markdown", case_id, "--model-run-id", run_id
    )
    html = _invoke(tmp_path, "report", "html", case_id, "--model-run-id", run_id)
    first_report_manifest = read_manifest(case_dir)
    first_markdown_artifact = next(
        item
        for item in first_report_manifest.artifacts
        if item.kind == "report.markdown"
    )
    first_html_artifact = next(
        item for item in first_report_manifest.artifacts if item.kind == "report.html"
    )
    first_markdown_snapshot = (
        first_markdown_artifact.artifact_id,
        first_markdown_artifact.sha256,
        (case_dir / first_markdown_artifact.path).read_bytes(),
    )
    first_html_snapshot = (
        first_html_artifact.artifact_id,
        first_html_artifact.sha256,
        (case_dir / first_html_artifact.path).read_bytes(),
    )
    repeated_markdown = _invoke(
        tmp_path, "report", "markdown", case_id, "--model-run-id", run_id
    )
    repeated_html = _invoke(
        tmp_path, "report", "html", case_id, "--model-run-id", run_id
    )
    assert markdown.output == repeated_markdown.output
    assert html.output == repeated_html.output

    manifest = read_manifest(case_dir)
    results = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-results"
    )
    sensitivity = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-sensitivity"
    )
    markdown_artifact = next(
        item for item in manifest.artifacts if item.kind == "report.markdown"
    )
    html_artifact = next(
        item for item in manifest.artifacts if item.kind == "report.html"
    )
    results_frame = pl.read_parquet(case_dir / results.path)
    markdown_bytes = (case_dir / markdown_artifact.path).read_bytes()
    html_bytes = (case_dir / html_artifact.path).read_bytes()

    assert (
        markdown_artifact.artifact_id,
        markdown_artifact.sha256,
        markdown_bytes,
    ) == first_markdown_snapshot
    assert (
        html_artifact.artifact_id,
        html_artifact.sha256,
        html_bytes,
    ) == first_html_snapshot

    assert results_frame["scenario"].sort().to_list() == ["base", "bear", "bull"]
    assert sensitivity.artifact_id in html_bytes.decode("utf-8")
    assert b"Source IDs: a1, a2, a3, a4, a5, a6, e1, e2, e3, e4" in markdown_bytes
    assert b"Producing command" in markdown_bytes and b'<svg role="img"' in html_bytes
    assert all(
        marker not in html_bytes
        for marker in (b"<img", b"<link", b" src=", b"href=", b"http://", b"https://")
    )
    assert not any(
        Path(item.path).suffix in {".docx", ".xlsx"} for item in manifest.artifacts
    )
    assert (
        len([item for item in manifest.artifacts if item.kind.startswith("model.dcf-")])
        == 5
    )
    assert (
        len(
            [
                item
                for item in manifest.artifacts
                if item.kind in {"report.markdown", "report.html"}
            ]
        )
        == 2
    )
    assert _invoke(tmp_path, "data", "validate", case_id).output.startswith("valid:")
    assert _invoke(
        tmp_path,
        "case",
        "audit",
        case_id,
        "--as-of",
        "2026-06-30",
        "--max-price-age-days",
        "0",
        "--verify-hashes",
    ).output.endswith("valid: yes\n")
