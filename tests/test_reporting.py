"""Deterministic reporting and read-only case-audit coverage."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl
import pytest
from typer.testing import CliRunner

import finresearch.data_validation as data_validation_module
import finresearch.modeling as modeling_module
from finresearch.auditing import audit_case
from finresearch.cases import (
    CaseContractError,
    InputFileHash,
    initialize_case,
    read_manifest,
    write_manifest,
)
from finresearch.cli import app
from finresearch.data_validation import validate_artifact
from finresearch.ingestion import publish_artifact_bytes
from finresearch.local_import import IMPORT_SCHEMAS, import_parquet
from finresearch.modeling import resolve_model_run, run_comps, run_dcf
from finresearch.registers import load_model_sources
from finresearch.report_contract import (
    REPORT_HTML_LEGACY_PRODUCER_VERSION,
    REPORT_HTML_PRODUCER_VERSION,
    REPORT_PRODUCER,
    build_report_publication,
)
from finresearch.reporting import (
    ReportContext,
    ReportError,
    generate_report,
    load_report_context,
    render_html,
    render_markdown,
)

runner = CliRunner()


def _write_sources(case_dir: Path) -> None:
    registers = case_dir / "registers"
    registers.mkdir()
    registers.joinpath("evidence.csv").write_text(
        "id,claim,source_type,source_ref,observed_at,notes\n"
        "e1,x,filing,x,2026-01-01,\n"
        "e2,x,filing,x,2026-01-01,\n"
        "e3,x,filing,x,2026-01-01,\n"
        "e4,x,filing,x,2026-01-01,\n",
        encoding="utf-8",
    )
    registers.joinpath("assumptions.csv").write_text(
        "id,parameter,value,unit,rationale,source_evidence,updated_at\n"
        "a1,x,x,ratio,x,e1,2026-01-01\n"
        "a2,x,x,ratio,x,e1,2026-01-01\n"
        "a3,x,x,ratio,x,e1,2026-01-01\n"
        "a4,x,x,ratio,x,e1,2026-01-01\n"
        "a5,x,x,ratio,x,e1,2026-01-01\n"
        "a6,x,x,ratio,x,e1,2026-01-01\n",
        encoding="utf-8",
    )


def _write_dcf_input(case_dir: Path, *, as_of: str = "2026-06-30") -> None:
    analysis = case_dir / "analysis"
    analysis.mkdir()
    analysis.joinpath("dcf-inputs.toml").write_text(
        """version = 1
as_of = "{as_of}"
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
forecast = [{ period_end="2026-12-31", \
free_cash_flow={ value=70, unit="USDm", source_id="a5" } }]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
[scenario.base]
forecast = [{ period_end="2026-12-31", \
free_cash_flow={ value=80, unit="USDm", source_id="a5" } }]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
[scenario.bull]
forecast = [{ period_end="2026-12-31", \
free_cash_flow={ value=90, unit="USDm", source_id="a5" } }]
terminal = { terminal_growth={ value=0.02, unit="ratio", source_id="a6" } }
""".replace("{as_of}", as_of),
        encoding="utf-8",
    )


def _comps_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for company, name, role, market_cap, revenue, price, eps in (
        ("target", "Target", "target", 100.0, 20.0, 10.0, 2.0),
        ("<peer & one>", "<peer & one>", "peer", 120.0, 20.0, 12.0, 2.0),
        ("peer-b", "Peer B", "peer", 150.0, 30.0, 15.0, 3.0),
    ):
        for metric, value, unit in (
            ("market_cap", market_cap, "USDm"),
            ("net_debt", 10.0, "USDm"),
            ("revenue", revenue, "USDm"),
            ("share_price", price, "USD/share"),
            ("eps", eps, "USD/share"),
        ):
            rows.append(
                {
                    "company_id": company,
                    "company_name": name,
                    "role": role,
                    "metric": metric,
                    "period_basis": "LTM",
                    "period_end": date(2026, 6, 30),
                    "knowledge_date": date(2026, 6, 30),
                    "as_of": date(2026, 6, 30),
                    "value": value,
                    "unit": unit,
                    "currency": "USD",
                    "source_id": "e1",
                }
            )
    return rows


def _import_comps(
    workspace: Path,
    case_id: str,
    *,
    retrieved_at: datetime = datetime(2026, 6, 30, tzinfo=UTC),
) -> str:
    source = workspace / "comps.parquet"
    pl.DataFrame(
        _comps_rows(),
        schema=IMPORT_SCHEMAS["model.comps-observations.v1"].input_schema,
    ).write_parquet(source)
    return import_parquet(
        workspace,
        case_id,
        source,
        schema_name="model.comps-observations.v1",
        provider="manual",
        retrieved_at=retrieved_at,
    ).normalized.artifact_id


def _import_projection(
    workspace: Path,
    case_id: str,
    schema_name: str,
    rows: list[dict[str, object]],
    *,
    retrieved_at: datetime,
) -> None:
    source = workspace / f"{schema_name}.parquet"
    pl.DataFrame(rows, schema=IMPORT_SCHEMAS[schema_name].input_schema).write_parquet(
        source
    )
    import_parquet(
        workspace,
        case_id,
        source,
        schema_name=schema_name,
        provider="manual",
        retrieved_at=retrieved_at,
    )


def _rewrite_artifact(
    case_dir: Path,
    artifact_id: str,
    frame: pl.DataFrame,
) -> None:
    manifest = read_manifest(case_dir)
    artifact = next(
        item for item in manifest.artifacts if item.artifact_id == artifact_id
    )
    path = case_dir / artifact.path
    frame.write_parquet(path, compression="zstd", statistics=True)
    updated = replace(artifact, sha256=sha256(path.read_bytes()).hexdigest())
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                updated if item.artifact_id == artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )


def _rewrite_artifact_coherently(
    case_dir: Path,
    artifact_id: str,
    frame: pl.DataFrame,
) -> None:
    """Test-only byte/manifest tamper that preserves direct child hash records."""
    manifest = read_manifest(case_dir)
    artifact = next(
        item for item in manifest.artifacts if item.artifact_id == artifact_id
    )
    path = case_dir / artifact.path
    frame.write_parquet(path, compression="zstd", statistics=True)
    changed = replace(artifact, sha256=sha256(path.read_bytes()).hexdigest())
    write_manifest(
        case_dir,
        _replace_artifact_and_parent_hashes(manifest, changed),
    )


def _model_artifact_id(case_dir: Path, kind: str) -> str:
    return next(
        item.artifact_id
        for item in read_manifest(case_dir).artifacts
        if item.kind == kind
    )


def _replace_artifact_and_parent_hashes(manifest: Any, changed: Any) -> Any:
    """Make a test-only manifest mutation that preserves v2 parent bindings."""
    parent_name = f"artifact.{changed.artifact_id}"
    return replace(
        manifest,
        artifacts=tuple(
            changed
            if item.artifact_id == changed.artifact_id
            else replace(
                item,
                input_file_hashes=tuple(
                    replace(record, sha256=changed.sha256)
                    if record.name == parent_name
                    else record
                    for record in item.input_file_hashes
                ),
            )
            for item in manifest.artifacts
        ),
    )


def test_dcf_report_is_repeated_byte_identical_and_cli_visible(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo", sensitivity=((0.08,), (0.02,)))

    first = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    )
    second = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    )
    content = first.path.read_bytes()
    assert first == second
    assert b"Source IDs: a1, a2, a3, a4, a5, a6, e1, e2, e3, e4" in content
    assert b"Source artifact IDs: \\(none\\)" in content
    assert b"Model artifact IDs:" in content
    assert b"## Sensitivity" in content
    assert validate_artifact(tmp_path, "demo") == ()

    cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "report",
            "markdown",
            "demo",
            "--model-run-id",
            model.run_id,
        ],
    )
    assert cli.exit_code == 0, cli.output
    assert first.artifact_id in cli.output


def test_dcf_html_sensitivity_heatmaps_are_deterministic_and_traceable(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(
        tmp_path,
        "demo",
        sensitivity=((0.08, 0.1), (0.01, 0.02)),
    )
    context = load_report_context(tmp_path, "demo", model.run_id)
    sensitivity_id = _model_artifact_id(case_dir, "model.dcf-sensitivity")

    first_render = render_html(context)
    second_render = render_html(context)
    report = generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")
    repeated = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="html"
    )
    content = report.path.read_bytes()

    assert first_render == second_render == content
    assert report == repeated
    assert content.count(b'<figure class="sensitivity-heatmap">') == 3
    assert content.count(b'<svg role="img"') == 3
    assert b"Bear DCF per-share sensitivity heatmap" in content
    assert b"Base DCF per-share sensitivity heatmap" in content
    assert b"Bull DCF per-share sensitivity heatmap" in content
    assert sensitivity_id.encode() in content
    assert b"producing command: <code>finresearch --workspace PATH model dcf" in content
    assert b"--sensitivity 0.08,0.1;0.01,0.02" in content
    assert b"<img" not in content
    assert b"http://" not in content and b"https://" not in content
    assert audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    ).valid

    escaped = render_html(replace(context, case_id="demo<script>&"))
    assert b"demo&lt;script&gt;&amp;" in escaped
    assert b"<script>" not in escaped


def test_dcf_html_keeps_table_but_omits_one_dimensional_heatmap(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo", sensitivity=((0.08,), (0.02,)))

    content = generate_report(
        tmp_path,
        "demo",
        model_run_id=model.run_id,
        format="html",
    ).path.read_bytes()

    assert b"<h2>Sensitivity</h2>" in content
    assert b"model.dcf-sensitivity" in content
    assert b"<svg" not in content


def test_legacy_html_v1_audits_and_current_html_v2_is_distinct(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(
        tmp_path,
        "demo",
        sensitivity=((0.08, 0.1), (0.01, 0.02)),
    )
    manifest = read_manifest(case_dir)
    context = load_report_context(tmp_path, "demo", model.run_id)
    legacy = build_report_publication(
        manifest,
        context,
        "html",
        producer_version=REPORT_HTML_LEGACY_PRODUCER_VERSION,
    )
    legacy_receipt = publish_artifact_bytes(
        case_dir=case_dir,
        manifest=manifest,
        path_role="reports",
        kind=legacy.kind,
        schema_version=1,
        path_parts=(context.family, context.run_id),
        filename=Path(legacy.path).name,
        entity_key=context.run_id,
        identity=legacy.identity,
        content=legacy.content,
        producer=REPORT_PRODUCER,
        producer_version=legacy.producer_version,
        parameters_sha256=legacy.identity,
        input_artifact_ids=legacy.parents,
        produced_at=legacy.produced_at,
    )
    legacy_artifact = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.artifact_id == legacy_receipt.artifact_id
    )

    assert legacy_artifact.producer_version == REPORT_HTML_LEGACY_PRODUCER_VERSION
    assert legacy_receipt.path.read_bytes() == legacy.content
    assert b"sensitivity-heatmap" not in legacy.content
    assert audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    ).valid

    current = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="html"
    )
    repeated = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="html"
    )
    current_artifact = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.artifact_id == current.artifact_id
    )

    assert current == repeated
    assert current.artifact_id != legacy_receipt.artifact_id
    assert current.path != legacy_receipt.path
    assert current.path.name.startswith("v2.")
    assert current_artifact.producer_version == REPORT_HTML_PRODUCER_VERSION
    assert b"sensitivity-heatmap" in current.path.read_bytes()
    assert audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    ).valid


def test_comps_html_report_escapes_content_and_keeps_source_artifact(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    observation_id = _import_comps(tmp_path, "demo")
    model = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=observation_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue", "pe"),
    )

    context = load_report_context(tmp_path, "demo", model.run_id)
    report = generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")
    content = report.path.read_text(encoding="utf-8")
    assert context.source_artifact_ids
    assert context.source_artifact_ids[0] in content
    assert "&lt;peer &amp; one&gt;" in content
    assert "<peer & one>" not in content
    assert "<table>" in content
    assert validate_artifact(tmp_path, "demo") == ()
    assert case_dir.joinpath("manifest.toml").is_file()


def test_comps_source_retrieved_later_is_not_a_derived_model_run(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    observation_id = _import_comps(
        tmp_path,
        "demo",
        retrieved_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    model = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=observation_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )

    at_cutoff = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
    )
    assert at_cutoff.valid
    assert generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    ).path.is_file()

    before_run = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 29),
        max_price_age_days=1,
    )
    assert "model_run_after_as_of" in {issue.code for issue in before_run.issues}


def test_comps_run_cutoff_is_authenticated_not_observation_snapshot(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    observation_id = _import_comps(tmp_path, "demo")
    model = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=observation_id,
        as_of=date(2026, 7, 1),
        metrics=("ev_revenue",),
    )

    context = load_report_context(tmp_path, "demo", model.run_id)
    report = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    )
    assert context.as_of == date(2026, 7, 1)
    assert b"As of: 2026-07-01" in report.path.read_bytes()
    assert audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 7, 1),
        max_price_age_days=1,
    ).valid

    manifest = read_manifest(case_dir)
    rolled_back = tuple(
        replace(item, retrieved_at="2026-06-30T00:00:00Z")
        if item.artifact_id.endswith(f".{model.run_id}")
        else item
        for item in manifest.artifacts
    )
    write_manifest(case_dir, replace(manifest, artifacts=rolled_back))
    invalid = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 7, 1),
        max_price_age_days=1,
    )
    assert "model_run_identity_invalid" in {issue.code for issue in invalid.issues}
    with pytest.raises(ReportError, match="authentication failed"):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")
    assert not any(
        item.kind == "report.html" for item in read_manifest(case_dir).artifacts
    )


def test_selected_report_ignores_unrelated_model_runs_at_other_cutoffs(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    source_id = _import_comps(tmp_path, "demo")
    earlier = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 6, 30),
        metrics=("ev_revenue",),
    )
    selected = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 7, 1),
        metrics=("ev_revenue",),
    )
    future = run_comps(
        tmp_path,
        "demo",
        input_artifact_id=source_id,
        as_of=date(2026, 7, 2),
        metrics=("ev_revenue",),
    )

    whole_case = audit_case(
        tmp_path, "demo", as_of=date(2026, 7, 1), max_price_age_days=1
    )
    assert "model_run_after_as_of" in {issue.code for issue in whole_case.issues}
    receipt = generate_report(
        tmp_path, "demo", model_run_id=selected.run_id, format="markdown"
    )
    assert receipt.path.is_file()
    assert selected.run_id not in {earlier.run_id, future.run_id}
    assert selected.run_id.encode() in receipt.path.read_bytes()


@pytest.mark.parametrize(
    ("family", "kind"),
    (
        ("dcf", "model.dcf-results"),
        ("dcf", "model.dcf-reconciliation"),
        ("comps", "model.comps-inputs"),
        ("comps", "model.comps-results"),
    ),
)
def test_shared_resolver_rejects_coherent_frame_tampering(
    tmp_path: Path, family: str, kind: str
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    if family == "dcf":
        _write_dcf_input(case_dir)
        model = run_dcf(tmp_path, "demo")
    else:
        source_id = _import_comps(tmp_path, "demo")
        model = run_comps(
            tmp_path,
            "demo",
            input_artifact_id=source_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
    artifact_id = _model_artifact_id(case_dir, kind)
    artifact = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.artifact_id == artifact_id
    )
    frame = pl.read_parquet(case_dir / artifact.path)
    numeric = next(
        column for column, dtype in frame.schema.items() if dtype == pl.Float64
    )
    changed_frame = (
        frame.with_columns(
            (pl.col("actual") + 1.0).alias("actual"),
            (pl.col("expected") + 1.0).alias("expected"),
        )
        if kind == "model.dcf-reconciliation"
        else frame.with_columns((pl.col(numeric) + 1.0).alias(numeric))
    )
    _rewrite_artifact_coherently(case_dir, artifact_id, changed_frame)

    with pytest.raises(CaseContractError, match="does not match recomputed output"):
        resolve_model_run(
            case_dir,
            read_manifest(case_dir),
            family=cast(Literal["dcf", "comps"], family),
            run_id=model.run_id,
        )
    audit = audit_case(tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1)
    assert "model_run_identity_invalid" in {issue.code for issue in audit.issues}
    with pytest.raises(ReportError, match="authentication failed"):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")


def test_shared_resolver_rejects_extra_direct_parent_and_extra_input_hash(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    manifest = read_manifest(case_dir)
    inputs = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-inputs"
    )
    reconciliation = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-reconciliation"
    )
    with_parent = replace(
        reconciliation,
        input_artifact_ids=(*reconciliation.input_artifact_ids, inputs.artifact_id),
        input_file_hashes=(
            *reconciliation.input_file_hashes,
            InputFileHash(
                name=f"artifact.{inputs.artifact_id}",
                path=inputs.path,
                sha256=inputs.sha256 or "",
            ),
        ),
    )
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                with_parent if item.artifact_id == with_parent.artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )
    with pytest.raises(CaseContractError, match="lineage"):
        resolve_model_run(
            case_dir, read_manifest(case_dir), family="dcf", run_id=model.run_id
        )

    # Restore the declared parent set, then show a superfluous valid file hash
    # also cannot be smuggled into a model identity.
    extra_path = case_dir / "analysis" / "unrelated.txt"
    extra_path.write_text("unrelated", encoding="utf-8")
    extra = InputFileHash(
        name="file.unrelated",
        path="analysis/unrelated.txt",
        sha256=sha256(extra_path.read_bytes()).hexdigest(),
    )
    with_hash = replace(
        reconciliation,
        input_file_hashes=(*reconciliation.input_file_hashes, extra),
    )
    restored = read_manifest(case_dir)
    write_manifest(
        case_dir,
        replace(
            restored,
            artifacts=tuple(
                with_hash if item.artifact_id == with_hash.artifact_id else item
                for item in restored.artifacts
            ),
        ),
    )
    with pytest.raises(CaseContractError, match="exact authenticated case inputs"):
        resolve_model_run(
            case_dir, read_manifest(case_dir), family="dcf", run_id=model.run_id
        )


@pytest.mark.parametrize("filename", ("evidence.csv", "assumptions.csv"))
def test_extra_register_csv_field_is_stable_through_audit_and_report_cli(
    tmp_path: Path, filename: str
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    register = case_dir / "registers" / filename
    register.write_text(
        register.read_text(encoding="utf-8")
        + "too,many,unquoted,fields,for,this,row,extra\n",
        encoding="utf-8",
    )

    status = runner.invoke(
        app,
        ["--workspace", str(tmp_path), "data", "registers", "status", "demo"],
    )
    assert status.exit_code == 1
    assert "register_malformed" in status.output
    audit = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
        ],
    )
    assert audit.exit_code == 1
    assert "model_sources_invalid" in audit.output
    report = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "report",
            "markdown",
            "demo",
            "--model-run-id",
            model.run_id,
        ],
    )
    assert report.exit_code == 1
    assert "UnicodeDecodeError" not in report.output
    assert not any(
        artifact.kind == "report.markdown"
        for artifact in read_manifest(case_dir).artifacts
    )


def test_duplicate_evidence_header_is_stable_through_audit_and_report_cli(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    evidence = case_dir / "registers" / "evidence.csv"
    header, *rows = evidence.read_text(encoding="utf-8").splitlines()
    evidence.write_text(
        f"{header},id\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )

    status = runner.invoke(
        app,
        ["--workspace", str(tmp_path), "data", "registers", "status", "demo"],
    )
    assert status.exit_code == 1
    assert "duplicate columns: id" in status.output
    audit = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
        ],
    )
    assert audit.exit_code == 1
    assert "model_sources_invalid" in audit.output
    report = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "report",
            "markdown",
            "demo",
            "--model-run-id",
            model.run_id,
        ],
    )
    assert report.exit_code == 1
    assert "UnicodeDecodeError" not in report.output
    assert not any(
        artifact.kind == "report.markdown"
        for artifact in read_manifest(case_dir).artifacts
    )


def test_dcf_toml_cutoff_cannot_be_rolled_back_in_manifest(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir, as_of="2026-07-01")
    model = run_dcf(tmp_path, "demo")
    manifest = read_manifest(case_dir)
    original_bytes = {
        item.artifact_id: (case_dir / item.path).read_bytes()
        for item in manifest.artifacts
        if item.artifact_id.endswith(f".{model.run_id}")
    }
    rolled_back = tuple(
        replace(item, retrieved_at="2026-06-30T00:00:00Z")
        if item.artifact_id in original_bytes
        else item
        for item in manifest.artifacts
    )
    write_manifest(case_dir, replace(manifest, artifacts=rolled_back))

    audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
    )
    assert "model_run_identity_invalid" in {issue.code for issue in audit.issues}
    with pytest.raises(ReportError, match="authentication failed"):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="markdown")
    assert not any(
        item.kind.startswith("report.") for item in read_manifest(case_dir).artifacts
    )
    assert original_bytes == {
        artifact_id: (
            case_dir
            / next(
                item.path
                for item in read_manifest(case_dir).artifacts
                if item.artifact_id == artifact_id
            )
        ).read_bytes()
        for artifact_id in original_bytes
    }


def test_markdown_renderer_escapes_untrusted_cells_and_metadata() -> None:
    unsafe = "<script>alert(1)</script>|\\\r\nrow\x00\t\x1b\x7f\x80\x9f"
    context = ReportContext(
        case_id=f"case|\\\r\n{unsafe}",
        run_id=f"run|\\\r\n{unsafe}",
        family="comps",
        as_of=date(2026, 6, 30),
        artifacts=(),
        source_ids=(f"source|\\\r\n{unsafe}",),
        source_artifact_ids=(),
        results=pl.DataFrame(
            {
                "company_id": [unsafe],
                "value": [1.0],
            }
        ),
        summary=None,
        sensitivity=None,
    )

    markdown = render_markdown(context).decode("utf-8")
    table_lines = [line for line in markdown.splitlines() if line.startswith("|")]
    assert "<script>" not in markdown
    assert "&lt;script&gt;" in markdown
    assert "\r" not in markdown
    assert "\n<script>" not in markdown
    assert "\\|" in markdown
    assert "\\\\" in markdown
    assert all(
        escape in markdown
        for escape in {
            "\\[U+0000\\]",
            "\\[U+0009\\]",
            "\\[U+001B\\]",
            "\\[U+007F\\]",
            "\\[U+0080\\]",
            "\\[U+009F\\]",
        }
    )
    raw_bytes = markdown.encode("utf-8")
    assert not (
        set(raw_bytes)
        & (set(range(0, 10)) | set(range(11, 32)) | {0x7F} | set(range(0x80, 0xA0)))
    )
    assert len(table_lines) == 3
    assert "row" in table_lines[-1]


def test_markdown_renderer_neutralizes_untrusted_links_and_images() -> None:
    unsafe = (
        "[click](javascript:alert(1)) ![image](data:text/html,payload) "
        "[reference]: javascript:alert(1) <javascript:alert(1)> "
        "https://example.invalid http://example.invalid www.example.invalid "
        "person@example.invalid"
    )
    context = ReportContext(
        case_id=unsafe,
        run_id=unsafe,
        family="comps",
        as_of=date(2026, 6, 30),
        artifacts=(),
        source_ids=(unsafe,),
        source_artifact_ids=(),
        results=pl.DataFrame({"company_id": [unsafe], "value": [1.0]}),
        summary=None,
        sensitivity=None,
    )

    markdown = render_markdown(context).decode("utf-8")

    assert "[click](javascript:" not in markdown
    assert "![image](data:" not in markdown
    assert "[reference]: javascript:" not in markdown
    assert "<javascript:alert" not in markdown
    assert "https://" not in markdown
    assert "http://" not in markdown
    assert "www.example.invalid" not in markdown
    assert "person@example.invalid" not in markdown
    assert "\\[click\\]\\(javascript:alert\\(1\\)\\)" in markdown
    assert "\\!\\[image\\]\\(data:text/html,payload\\)" in markdown
    assert "&lt;javascript:alert\\(1\\)&gt;" in markdown
    assert "https:\\//example.invalid" in markdown
    assert "www\\.example.invalid" in markdown
    assert "person\\@example.invalid" in markdown


def test_case_audit_authenticates_coherently_redeclared_report(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    report = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    )
    manifest = read_manifest(case_dir)
    artifact = next(
        item for item in manifest.artifacts if item.artifact_id == report.artifact_id
    )
    report.path.write_bytes(b"coherently tampered report")
    reordered_parents = tuple(reversed(artifact.input_artifact_ids))
    hashes_by_name = {record.name: record for record in artifact.input_file_hashes}
    changed = replace(
        artifact,
        sha256=sha256(report.path.read_bytes()).hexdigest(),
        producer="adversarial.report",
        producer_version="999",
        parameters_sha256="0" * 64,
        input_artifact_ids=reordered_parents,
        input_file_hashes=tuple(
            hashes_by_name[f"artifact.{parent_id}"] for parent_id in reordered_parents
        ),
    )
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                changed if item.artifact_id == changed.artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )

    semantic_audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
    )
    audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert "report_identity_invalid" in {issue.code for issue in semantic_audit.issues}
    assert "report_identity_invalid" in {issue.code for issue in audit.issues}
    cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
            "--verify-hashes",
        ],
    )
    assert cli.exit_code == 1
    assert "error [report_identity_invalid]" in cli.output


def test_audit_verify_hashes_is_optional_for_unused_opaque_artifact(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    opaque = publish_artifact_bytes(
        case_dir=case_dir,
        manifest=read_manifest(case_dir),
        path_role="raw",
        kind="raw.opaque",
        schema_version=1,
        path_parts=("opaque",),
        filename="opaque.bin",
        entity_key="opaque",
        identity="opaque-identity",
        content=b"original opaque bytes",
        producer="test.opaque",
        producer_version="1",
        parameters_sha256="0" * 64,
        input_artifact_ids=(),
        produced_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    opaque.path.write_bytes(b"tampered opaque bytes")

    unchecked = audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    )
    checked = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert "checksum_mismatch" not in {issue.code for issue in unchecked.issues}
    assert unchecked.valid
    assert "checksum_mismatch" in {issue.code for issue in checked.issues}
    unchecked_cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
        ],
    )
    checked_cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
            "--verify-hashes",
        ],
    )
    assert unchecked_cli.exit_code == 0
    assert checked_cli.exit_code == 1
    assert "error [checksum_mismatch]" in checked_cli.output


@pytest.mark.parametrize("family", ("dcf", "comps"))
def test_audit_only_rehashes_model_inputs_when_requested(
    tmp_path: Path,
    family: Literal["dcf", "comps"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    if family == "dcf":
        _write_dcf_input(case_dir)
        model = run_dcf(tmp_path, "demo")
        changed_path = case_dir / "analysis" / "dcf-inputs.toml"
    else:
        input_artifact_id = _import_comps(tmp_path, "demo")
        model = run_comps(
            tmp_path,
            "demo",
            input_artifact_id=input_artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )
        changed_path = case_dir / "registers" / "assumptions.csv"
    generate_report(tmp_path, "demo", model_run_id=model.run_id, format="markdown")
    changed_path.write_bytes(changed_path.read_bytes() + b"\n")

    def unexpected_file_digest(_: Path) -> str:
        raise AssertionError("hashless audit must not digest a current file")

    with monkeypatch.context() as hashes:
        hashes.setattr(modeling_module, "_sha256", unexpected_file_digest)
        hashes.setattr(data_validation_module, "_sha256", unexpected_file_digest)
        unchecked = audit_case(
            tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
        )
        resolved = resolve_model_run(
            case_dir,
            read_manifest(case_dir),
            family=family,
            run_id=model.run_id,
            verify_hashes=False,
        )
        context = load_report_context(
            tmp_path, "demo", model.run_id, verify_hashes=False
        )
    checked = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert unchecked.valid
    assert resolved.run_id == model.run_id
    assert context.run_id == model.run_id
    assert "input_file_checksum_mismatch" in {issue.code for issue in checked.issues}
    with pytest.raises(ReportError, match="authentication failed"):
        load_report_context(tmp_path, "demo", model.run_id)


def test_audit_only_rehashes_model_artifact_bytes_when_requested(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    run_dcf(tmp_path, "demo")
    manifest = read_manifest(case_dir)
    reconciliation = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-reconciliation"
    )
    changed = replace(reconciliation, sha256="0" * 64)
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                changed if item.artifact_id == changed.artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )

    unchecked = audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    )
    checked = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert unchecked.valid
    assert "checksum_mismatch" in {issue.code for issue in checked.issues}
    assert "model_run_identity_invalid" in {issue.code for issue in checked.issues}


def test_dcf_sensitivity_numeric_identity_is_float_canonical(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)

    integer_grid = run_dcf(tmp_path, "demo", sensitivity=((1,), (0,)))
    float_grid = run_dcf(tmp_path, "demo", sensitivity=((1.0,), (0.0,)))
    sensitivity_artifact = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.kind == "model.dcf-sensitivity"
    )
    sensitivity = pl.read_parquet(case_dir / sensitivity_artifact.path)
    report = generate_report(
        tmp_path,
        "demo",
        model_run_id=integer_grid.run_id,
        format="markdown",
    )
    audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert integer_grid == float_grid
    assert sensitivity["wacc"].unique().to_list() == [1.0]
    assert sensitivity["terminal_growth"].unique().to_list() == [0.0]
    assert report.run_id == integer_grid.run_id
    assert audit.valid


def test_audit_always_checks_declared_input_file_locations(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    input_path = case_dir / "data" / "raw" / "opaque-input.txt"
    input_path.write_bytes(b"original input bytes")
    input_hash = sha256(input_path.read_bytes()).hexdigest()
    publish_artifact_bytes(
        case_dir=case_dir,
        manifest=read_manifest(case_dir),
        path_role="raw",
        kind="raw.opaque",
        schema_version=1,
        path_parts=("opaque",),
        filename="opaque.bin",
        entity_key="opaque",
        identity="opaque-with-input",
        content=b"opaque bytes",
        producer="test.opaque",
        producer_version="1",
        parameters_sha256="0" * 64,
        input_artifact_ids=(),
        produced_at=datetime(2026, 6, 30, tzinfo=UTC),
        extra_input_file_hashes=(
            InputFileHash(
                name="file.opaque-input",
                path="data/raw/opaque-input.txt",
                sha256=input_hash,
            ),
        ),
    )

    input_path.unlink()
    missing_default = audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    )
    missing_full = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert {issue.code for issue in missing_default.issues} == {"input_file_missing"}
    assert {issue.code for issue in missing_full.issues} == {"input_file_missing"}

    input_path.write_bytes(b"replacement input bytes")
    changed_default = audit_case(
        tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1
    )
    changed_full = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )

    assert changed_default.valid
    assert "input_file_checksum_mismatch" not in {
        issue.code for issue in changed_default.issues
    }
    assert {issue.code for issue in changed_full.issues} == {
        "input_file_checksum_mismatch"
    }


def test_report_rejects_failed_reconciliation_without_report_publication(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    reconciliation_id = _model_artifact_id(case_dir, "model.dcf-reconciliation")
    reconciliation = pl.read_parquet(
        case_dir
        / next(
            item.path
            for item in read_manifest(case_dir).artifacts
            if item.artifact_id == reconciliation_id
        )
    )
    _rewrite_artifact(
        case_dir,
        reconciliation_id,
        reconciliation.with_columns(
            (pl.col("actual") + 1.0).alias("actual"),
            (pl.col("difference") + 1.0).alias("difference"),
            pl.lit(False).alias("passed"),
            pl.lit("failed").alias("status"),
        ),
    )

    with pytest.raises(ReportError, match="does not match recomputed output"):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="markdown")
    assert not any(
        item.kind.startswith("report.") for item in read_manifest(case_dir).artifacts
    )


def test_reporting_and_audit_require_manifest_v2(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    write_manifest(case_dir, replace(read_manifest(case_dir), manifest_version=1))
    with pytest.raises(Exception, match="case migrate"):
        load_report_context(tmp_path, "demo", "missing")
    with pytest.raises(Exception, match="case migrate"):
        audit_case(
            tmp_path,
            "demo",
            as_of=date(2026, 6, 30),
            max_price_age_days=1,
        )


def test_report_rejects_missing_run_without_writing(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    with pytest.raises(ReportError, match="exactly one complete"):
        generate_report(tmp_path, "demo", model_run_id="not-a-run", format="html")
    assert read_manifest(case_dir).artifacts == ()


def test_report_rejects_cross_run_lineage_and_tampered_parent(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    first = run_dcf(tmp_path, "demo")
    input_path = case_dir / "analysis" / "dcf-inputs.toml"
    second_input = case_dir / "analysis" / "dcf-second.toml"
    second_input.write_text(
        input_path.read_text(encoding="utf-8").replace("value=80", "value=81"),
        encoding="utf-8",
    )
    second = run_dcf(tmp_path, "demo", input_path="analysis/dcf-second.toml")
    manifest = read_manifest(case_dir)
    old_cash = next(
        item
        for item in manifest.artifacts
        if item.kind == "model.dcf-cashflows" and first.run_id in item.artifact_id
    )
    second_results = next(
        item
        for item in manifest.artifacts
        if item.kind == "model.dcf-results" and second.run_id in item.artifact_id
    )
    old_parent = second_results.input_artifact_ids[0]
    rewritten_hashes = tuple(
        InputFileHash(
            name=f"artifact.{old_cash.artifact_id}",
            path=old_cash.path,
            sha256=old_cash.sha256 or "",
        )
        if record.name == f"artifact.{old_parent}"
        else record
        for record in second_results.input_file_hashes
    )
    cross_run = replace(
        second_results,
        input_artifact_ids=(old_cash.artifact_id,),
        input_file_hashes=rewritten_hashes,
    )
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                cross_run if item.artifact_id == second_results.artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )
    with pytest.raises(ReportError, match="cross-run"):
        load_report_context(tmp_path, "demo", second.run_id)

    # The first model run remains independently tamper-detectable and never
    # causes a report artifact to be silently regenerated over altered bytes.
    first_result = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.kind == "model.dcf-results" and first.run_id in item.artifact_id
    )
    first_path = case_dir / first_result.path
    first_path.write_bytes(b"tampered parent")
    with pytest.raises(ReportError, match="sha256 does not match"):
        generate_report(tmp_path, "demo", model_run_id=first.run_id, format="html")
    assert not any(
        item.kind.startswith("report.") for item in read_manifest(case_dir).artifacts
    )


def test_case_audit_reports_cutoff_stale_and_generated_hash_failures(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    report = generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")

    before_model = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 29),
        max_price_age_days=1,
    )
    assert "model_run_after_as_of" in {issue.code for issue in before_model.issues}

    report.path.write_bytes(b"tampered report")
    tampered = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
        verify_hashes=True,
    )
    assert "checksum_mismatch" in {issue.code for issue in tampered.issues}

    result = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
            "--verify-hashes",
        ],
    )
    assert result.exit_code == 1
    assert "error [checksum_mismatch]" in result.output


def test_case_audit_enforces_price_cutoff_and_staleness(tmp_path: Path) -> None:
    initialize_case(tmp_path, "demo")
    source = tmp_path / "prices.parquet"
    spec = IMPORT_SCHEMAS["daily-prices.v2"]

    def price_row(instrument_id: str, session: date) -> dict[str, object]:
        return {
            "instrument_id": instrument_id,
            "provider_symbol": instrument_id.rsplit(":", maxsplit=1)[-1],
            "currency": "USD",
            "price_basis": "unadjusted",
            "provider_timezone": "UTC",
            "session_date": session,
            "timestamp": datetime.combine(session, datetime.min.time(), tzinfo=UTC),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 10,
            "dividends": 0.0,
            "stock_splits": 0.0,
        }

    pl.DataFrame(
        [
            price_row("manual:ABC", date(2026, 6, 20)),
            price_row("manual:ABC", date(2026, 7, 1)),
            price_row("manual:FUT", date(2026, 7, 1)),
        ],
        schema=spec.input_schema,
    ).write_parquet(source)
    import_parquet(
        tmp_path,
        "demo",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=datetime(2026, 6, 20, tzinfo=UTC),
    )

    early = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 19),
        max_price_age_days=5,
    )
    assert {
        "price_session_after_as_of",
        "price_no_valid_session",
    }.issubset({issue.code for issue in early.issues})
    stale = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=2,
    )
    assert {
        "price_session_after_as_of",
        "price_stale",
        "price_no_valid_session",
    }.issubset({issue.code for issue in stale.issues})
    stale_issue = next(issue for issue in stale.issues if issue.code == "price_stale")
    assert "manual:ABC latest price 2026-06-20" in stale_issue.message

    case_dir = tmp_path / "cases" / "demo"
    normalized = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.kind == "normalized.daily-prices"
    )
    pl.DataFrame({"broken": [1]}).write_parquet(case_dir / normalized.path)
    malformed = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=2,
    )
    assert "contract_violation" in {issue.code for issue in malformed.issues}
    cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "2",
        ],
    )
    assert cli.exit_code == 1
    assert "error [contract_violation]" in cli.output


def test_case_audit_skips_malformed_model_source_selector(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    run_dcf(tmp_path, "demo")
    dcf_inputs = next(
        item
        for item in read_manifest(case_dir).artifacts
        if item.kind == "model.dcf-inputs"
    )
    pl.DataFrame({"source_id": [["a1"]]}).write_parquet(case_dir / dcf_inputs.path)

    audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
    )
    assert "contract_violation" in {issue.code for issue in audit.issues}


def test_case_audit_enforces_facts_estimates_and_master_cutoffs(tmp_path: Path) -> None:
    initialize_case(tmp_path, "demo")
    late = datetime(2026, 7, 1, tzinfo=UTC)
    _import_projection(
        tmp_path,
        "demo",
        "instrument-master.v2",
        [
            {
                "instrument_id": "manual:ABC",
                "entity_id": None,
                "provider_symbol": "ABC",
                "primary_symbol": "ABC",
                "name": None,
                "asset_class": "equity",
                "instrument_type": None,
                "venue_mic": None,
                "country_code": None,
                "trading_currency": "USD",
                "valid_from": None,
                "valid_to": None,
                "observed_at": late,
            }
        ],
        retrieved_at=late,
    )
    _import_projection(
        tmp_path,
        "demo",
        "fundamental-facts.v2",
        [
            {
                "fact_id": "fact-1",
                "entity_id": None,
                "instrument_id": None,
                "cik": "1",
                "taxonomy": "us-gaap",
                "concept": "Revenue",
                "metric_id": None,
                "category": "income-statement",
                "canonical_metric": None,
                "label": "Revenue",
                "unit": "USD",
                "unit_kind": "currency",
                "currency": "USD",
                "value_text": "100",
                "value": 100.0,
                "period_type": "duration",
                "start_date": date(2026, 1, 1),
                "end_date": date(2026, 6, 30),
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "form": "10-Q",
                "accession_number": "x",
                "filed_date": date(2026, 7, 1),
                "knowledge_date": date(2026, 7, 1),
                "available_at": late,
                "frame": None,
            }
        ],
        retrieved_at=late,
    )
    _import_projection(
        tmp_path,
        "demo",
        "estimates.v1",
        [
            {
                "entity_id": None,
                "instrument_id": "manual:ABC",
                "metric_id": "eps",
                "period_end": date(2026, 12, 31),
                "estimate_as_of": date(2026, 7, 1),
                "availability_at": late,
                "value": 1.0,
                "unit": "USD/shares",
                "unit_kind": "currency",
                "currency": "USD",
            }
        ],
        retrieved_at=late,
    )

    audit = audit_case(
        tmp_path,
        "demo",
        as_of=date(2026, 6, 30),
        max_price_age_days=1,
    )
    assert {
        "fact_knowledge_after_as_of",
        "estimate_as_of_after_as_of",
        "estimate_available_after_as_of",
        "master_observed_after_as_of",
    }.issubset({issue.code for issue in audit.issues})


def test_report_publication_interruption_recovers_one_identical_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    original = publish_artifact_bytes

    def interrupted(**kwargs: Any) -> object:
        original(**kwargs)
        raise OSError("injected report publication interruption")

    monkeypatch.setattr("finresearch.reporting.publish_artifact_bytes", interrupted)
    with pytest.raises(OSError, match="injected report publication interruption"):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="markdown")
    monkeypatch.setattr("finresearch.reporting.publish_artifact_bytes", original)

    recovered = generate_report(
        tmp_path, "demo", model_run_id=model.run_id, format="markdown"
    )
    reports = [
        item
        for item in read_manifest(case_dir).artifacts
        if item.kind == "report.markdown"
    ]
    assert len(reports) == 1
    assert recovered.artifact_id == reports[0].artifact_id
    assert validate_artifact(tmp_path, "demo") == ()


def test_report_and_audit_cli_help_describe_explicit_parameters() -> None:
    report_help = runner.invoke(app, ["--workspace", "/tmp", "report", "--help"])
    audit_help = runner.invoke(app, ["--workspace", "/tmp", "case", "audit", "--help"])
    assert report_help.exit_code == 0
    assert audit_help.exit_code == 0
    assert "markdown" in report_help.output and "html" in report_help.output
    assert (
        "--model-run-id"
        in runner.invoke(
            app, ["--workspace", "/tmp", "report", "markdown", "--help"]
        ).output
    )
    assert "--max-price-age-days" in audit_help.output
    assert "Additionally digest-check" in audit_help.output


def test_shared_resolver_rejects_dual_noncanonical_dcf_set(
    tmp_path: Path,
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    manifest = read_manifest(case_dir)
    result = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-results"
    )
    alternate_path = "data/derived/adversary/alternate-results.parquet"
    target = case_dir / alternate_path
    target.parent.mkdir(parents=True)
    target.write_bytes((case_dir / result.path).read_bytes())
    alternate = replace(
        result,
        artifact_id=f"model.dcf-results.adversary.{model.run_id}",
        path=alternate_path,
    )
    write_manifest(
        case_dir, replace(manifest, artifacts=(*manifest.artifacts, alternate))
    )

    with pytest.raises(CaseContractError, match="alternate noncanonical"):
        resolve_model_run(
            case_dir, read_manifest(case_dir), family="dcf", run_id=model.run_id
        )
    audit = audit_case(tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1)
    assert "model_run_identity_invalid" in {issue.code for issue in audit.issues}
    with pytest.raises(ReportError, match="authentication failed"):
        load_report_context(tmp_path, "demo", model.run_id)


@pytest.mark.parametrize(
    ("family", "kinds"),
    (
        (
            "dcf",
            (
                "model.dcf-inputs",
                "model.dcf-cashflows",
                "model.dcf-results",
                "model.dcf-reconciliation",
            ),
        ),
        (
            "comps",
            (
                "model.comps-inputs",
                "model.comps-results",
                "model.comps-summary",
                "model.comps-reconciliation",
            ),
        ),
    ),
)
def test_shared_resolver_rejects_wrong_run_id_in_every_model_table(
    tmp_path: Path, family: str, kinds: tuple[str, ...]
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    if family == "dcf":
        _write_dcf_input(case_dir)
        model = run_dcf(tmp_path, "demo")
    else:
        input_artifact_id = _import_comps(tmp_path, "demo")
        model = run_comps(
            tmp_path,
            "demo",
            input_artifact_id=input_artifact_id,
            as_of=date(2026, 6, 30),
            metrics=("ev_revenue",),
        )

    for kind in kinds:
        manifest = read_manifest(case_dir)
        artifact = next(item for item in manifest.artifacts if item.kind == kind)
        path = case_dir / artifact.path
        frame = pl.read_parquet(path).with_columns(
            pl.lit("wrong-run-id", dtype=pl.String).alias("run_id")
        )
        frame.write_parquet(path, compression="zstd", statistics=True)
        modified = replace(artifact, sha256=sha256(path.read_bytes()).hexdigest())
        write_manifest(
            case_dir, _replace_artifact_and_parent_hashes(manifest, modified)
        )
        with pytest.raises(CaseContractError, match="table run_id does not match"):
            resolve_model_run(
                case_dir,
                read_manifest(case_dir),
                family=cast(Literal["dcf", "comps"], family),
                run_id=model.run_id,
            )
        frame = pl.read_parquet(path).with_columns(
            pl.lit(model.run_id, dtype=pl.String).alias("run_id")
        )
        frame.write_parquet(path, compression="zstd", statistics=True)
        restored = replace(modified, sha256=sha256(path.read_bytes()).hexdigest())
        current = read_manifest(case_dir)
        write_manifest(case_dir, _replace_artifact_and_parent_hashes(current, restored))


def test_cross_run_lineage_is_rejected_by_shared_resolver(tmp_path: Path) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    manifest = read_manifest(case_dir)
    inputs = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-inputs"
    )
    reconciliation = next(
        item for item in manifest.artifacts if item.kind == "model.dcf-reconciliation"
    )
    changed = replace(
        reconciliation,
        input_artifact_ids=(inputs.artifact_id,),
        input_file_hashes=tuple(
            InputFileHash(
                name=f"artifact.{inputs.artifact_id}",
                path=inputs.path,
                sha256=inputs.sha256 or "",
            )
            if record.name == f"artifact.{reconciliation.input_artifact_ids[0]}"
            else record
            for record in reconciliation.input_file_hashes
        ),
    )
    write_manifest(
        case_dir,
        replace(
            manifest,
            artifacts=tuple(
                changed if item.artifact_id == changed.artifact_id else item
                for item in manifest.artifacts
            ),
        ),
    )
    with pytest.raises(CaseContractError, match="lineage is incomplete or cross-run"):
        resolve_model_run(
            case_dir, read_manifest(case_dir), family="dcf", run_id=model.run_id
        )


@pytest.mark.parametrize("filename", ("evidence.csv", "assumptions.csv"))
def test_invalid_utf8_model_registers_return_stable_audit_and_report_errors(
    tmp_path: Path, filename: str
) -> None:
    case_dir = initialize_case(tmp_path, "demo")
    _write_sources(case_dir)
    _write_dcf_input(case_dir)
    model = run_dcf(tmp_path, "demo")
    case_dir.joinpath("registers", filename).write_bytes(b"\xff\xfe")

    with pytest.raises(CaseContractError, match="model source registers are invalid"):
        load_model_sources(case_dir, as_of=date(2026, 6, 30))
    audit = audit_case(tmp_path, "demo", as_of=date(2026, 6, 30), max_price_age_days=1)
    assert "model_sources_invalid" in {issue.code for issue in audit.issues}
    cli = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "case",
            "audit",
            "demo",
            "--as-of",
            "2026-06-30",
            "--max-price-age-days",
            "1",
        ],
    )
    assert cli.exit_code == 1
    assert "UnicodeDecodeError" not in cli.output
    with pytest.raises(ReportError):
        generate_report(tmp_path, "demo", model_run_id=model.run_id, format="html")
    assert not any(
        item.kind == "report.html" for item in read_manifest(case_dir).artifacts
    )
