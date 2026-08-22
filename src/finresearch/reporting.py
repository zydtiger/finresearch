"""CLI-facing immutable report publication built on shared report contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from finresearch.auditing import audit_model_run
from finresearch.cases import CaseContractError, case_directory, read_manifest
from finresearch.ingestion import IngestionError, publish_artifact_bytes
from finresearch.report_contract import (
    REPORT_PRODUCER,
    REPORT_PRODUCER_VERSION,
    ReportContext,
    ReportFormat,
    build_report_publication,
    render_html,
    render_markdown,
    resolve_report_context,
)


class ReportError(IngestionError):
    """Raised when a model run is not safe or complete enough to report."""


@dataclass(frozen=True)
class ReportReceipt:
    """One immutable generated report receipt."""

    artifact_id: str
    path: Path
    sha256: str
    format: ReportFormat
    run_id: str


def generate_report(
    workspace: Path,
    case_id: str,
    *,
    model_run_id: str,
    format: ReportFormat,
) -> ReportReceipt:
    """Preflight, render, and register one immutable report artifact."""
    if format not in {"markdown", "html"}:
        raise ReportError("report format must be markdown or html")
    context = load_report_context(workspace, case_id, model_run_id)
    audit = audit_model_run(
        workspace,
        case_id,
        family=context.family,
        run_id=model_run_id,
        max_price_age_days=36_500,
        verify_hashes=True,
    )
    if not audit.valid:
        detail = "; ".join(f"[{item.code}] {item.message}" for item in audit.issues)
        raise ReportError(f"report audit gate failed: {detail}")
    case_dir = case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    publication = build_report_publication(manifest, context, format)
    receipt = publish_artifact_bytes(
        case_dir=case_dir,
        manifest=manifest,
        path_role="reports",
        kind=publication.kind,
        schema_version=1,
        path_parts=(context.family, model_run_id),
        filename=Path(publication.path).name,
        entity_key=model_run_id,
        identity=publication.identity,
        content=publication.content,
        producer=REPORT_PRODUCER,
        producer_version=REPORT_PRODUCER_VERSION,
        parameters_sha256=publication.identity,
        input_artifact_ids=publication.parents,
        produced_at=publication.produced_at,
    )
    return ReportReceipt(
        receipt.artifact_id, receipt.path, receipt.sha256, format, model_run_id
    )


def load_report_context(
    workspace: Path,
    case_id: str,
    run_id: str,
    *,
    verify_hashes: bool = True,
) -> ReportContext:
    """Resolve exactly one complete, coherent DCF or comps model run."""
    case_dir = case_directory(workspace, case_id)
    try:
        return resolve_report_context(
            case_dir,
            read_manifest(case_dir),
            case_id,
            run_id,
            verify_hashes=verify_hashes,
        )
    except CaseContractError as exc:
        raise ReportError(f"model run authentication failed: {exc}") from exc


__all__ = [
    "REPORT_PRODUCER",
    "REPORT_PRODUCER_VERSION",
    "ReportContext",
    "ReportError",
    "ReportFormat",
    "ReportReceipt",
    "generate_report",
    "load_report_context",
    "render_html",
    "render_markdown",
]
