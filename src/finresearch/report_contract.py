"""Shared deterministic report context, rendering, and authentication rules.

This module intentionally depends on model resolution but not on CLI or audit
code.  Both report publication and whole-case audit use the same builders so a
manifest-only rewrite cannot make altered report bytes look authentic.
"""

from __future__ import annotations

import hashlib
import html
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    InputFileHash,
    ValidationIssue,
    canonical_parameters_sha256,
    resolve_relative_path,
)
from finresearch.modeling import model_run_is_declared, resolve_model_run

REPORT_PRODUCER = "finresearch.report"
REPORT_PRODUCER_VERSION = "1"
ReportFormat = Literal["markdown", "html"]


@dataclass(frozen=True)
class ReportContext:
    """Typed, validated inputs needed by deterministic renderers."""

    case_id: str
    run_id: str
    family: Literal["dcf", "comps"]
    as_of: date
    artifacts: tuple[Artifact, ...]
    source_ids: tuple[str, ...]
    source_artifact_ids: tuple[str, ...]
    results: pl.DataFrame
    summary: pl.DataFrame | None
    sensitivity: pl.DataFrame | None


@dataclass(frozen=True)
class ReportPublication:
    """Complete canonical declaration and bytes for one report artifact."""

    format: ReportFormat
    content: bytes
    identity: str
    artifact_id: str
    kind: str
    path: str
    parents: tuple[str, ...]
    input_file_hashes: tuple[InputFileHash, ...]
    produced_at: datetime


def resolve_report_context(
    case_dir: Path,
    manifest: CaseManifest,
    case_id: str,
    run_id: str,
    *,
    verify_hashes: bool = True,
) -> ReportContext:
    """Resolve exactly one complete coherent DCF or comps model run."""
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "reporting requires manifest v2; run case migrate first"
        )
    dcf = _context_for_family(
        case_dir, manifest, case_id, run_id, "dcf", verify_hashes=verify_hashes
    )
    comps = _context_for_family(
        case_dir, manifest, case_id, run_id, "comps", verify_hashes=verify_hashes
    )
    found = [context for context in (dcf, comps) if context is not None]
    if len(found) != 1:
        raise CaseContractError(
            "model run must resolve to exactly one complete DCF or comps artifact set"
        )
    return found[0]


def build_report_publication(
    manifest: CaseManifest,
    context: ReportContext,
    format: ReportFormat,
) -> ReportPublication:
    """Build the exact immutable report declaration from one authenticated run."""
    if format not in {"markdown", "html"}:
        raise CaseContractError("report format must be markdown or html")
    parent_bindings = tuple(
        sorted(
            (artifact.artifact_id, artifact.sha256) for artifact in context.artifacts
        )
    )
    if any(sha256 is None for _, sha256 in parent_bindings):
        raise CaseContractError("report model parent is missing sha256")
    parents = tuple(artifact_id for artifact_id, _ in parent_bindings)
    identity = canonical_parameters_sha256(
        {
            "format": format,
            "model_run_id": context.run_id,
            "parents": parent_bindings,
            "producer": REPORT_PRODUCER,
            "producer_version": REPORT_PRODUCER_VERSION,
        }
    )
    content = render_markdown(context) if format == "markdown" else render_html(context)
    by_id = {artifact.artifact_id: artifact for artifact in context.artifacts}
    input_file_hashes = tuple(
        InputFileHash(
            name=f"artifact.{artifact_id}",
            path=by_id[artifact_id].path,
            sha256=by_id[artifact_id].sha256 or "",
        )
        for artifact_id in parents
    )
    kind = f"report.{format}"
    path = Path(
        manifest.paths["reports"],
        context.family,
        context.run_id,
        f"{identity}.{_extension(format)}",
    ).as_posix()
    return ReportPublication(
        format=format,
        content=content,
        identity=identity,
        artifact_id=f"{kind}.{context.run_id}.{identity}",
        kind=kind,
        path=path,
        parents=parents,
        input_file_hashes=input_file_hashes,
        produced_at=datetime.combine(context.as_of, datetime.min.time(), tzinfo=UTC),
    )


def authenticate_report_artifact(
    case_dir: Path,
    manifest: CaseManifest,
    artifact: Artifact,
    *,
    verify_hashes: bool = True,
) -> ValidationIssue | None:
    """Return one stable issue when a registered report is not canonical."""
    if artifact.kind not in {"report.markdown", "report.html"}:
        return None
    try:
        format = artifact.kind.removeprefix("report.")
        if format not in {"markdown", "html"}:
            raise CaseContractError("unsupported report format")
        run_id, identity = _report_identity_parts(artifact, format)
        context = resolve_report_context(
            case_dir,
            manifest,
            manifest.case_id,
            run_id,
            verify_hashes=verify_hashes,
        )
        expected = build_report_publication(
            manifest, context, cast_report_format(format)
        )
        _require_report_declaration(artifact, expected, identity)
    except CaseContractError as exc:
        return ValidationIssue(
            "report_identity_invalid",
            f"artifact {artifact.artifact_id}: {exc}",
        )
    path = resolve_relative_path(case_dir, artifact.path, "report artifact")
    if not path.is_file():
        return ValidationIssue(
            "report_content_mismatch",
            f"artifact {artifact.artifact_id}: report file is missing",
        )
    try:
        actual = path.read_bytes()
    except OSError as exc:
        return ValidationIssue(
            "report_content_mismatch",
            f"artifact {artifact.artifact_id}: report file is unreadable: {exc}",
        )
    expected_hash = (
        hashlib.sha256(expected.content).hexdigest() if verify_hashes else None
    )
    if actual != expected.content or (
        expected_hash is not None and artifact.sha256 != expected_hash
    ):
        return ValidationIssue(
            "report_content_mismatch",
            f"artifact {artifact.artifact_id}: bytes do not match "
            "deterministic report content",
        )
    return None


def _report_identity_parts(artifact: Artifact, format: str) -> tuple[str, str]:
    prefix = f"report.{format}."
    if not artifact.artifact_id.startswith(prefix):
        raise CaseContractError("report artifact id has an invalid prefix")
    parts = artifact.artifact_id.removeprefix(prefix).split(".")
    if len(parts) != 2 or not all(parts):
        raise CaseContractError("report artifact id must encode run id and identity")
    return parts[0], parts[1]


def _require_report_declaration(
    artifact: Artifact, expected: ReportPublication, identity: str
) -> None:
    expected_time = expected.produced_at.isoformat().replace("+00:00", "Z")
    if identity != expected.identity or artifact.artifact_id != expected.artifact_id:
        raise CaseContractError("report artifact id does not match canonical identity")
    if (
        artifact.kind != expected.kind
        or artifact.schema_version != 1
        or artifact.path != expected.path
        or artifact.producer != REPORT_PRODUCER
        or artifact.producer_version != REPORT_PRODUCER_VERSION
        or artifact.parameters_sha256 != expected.identity
        or artifact.retrieved_at != expected_time
        or artifact.row_count is not None
    ):
        raise CaseContractError(
            "report artifact metadata does not match canonical declaration"
        )
    if artifact.input_artifact_ids != expected.parents:
        raise CaseContractError("report artifact parent set does not match model run")
    if artifact.input_file_hashes != expected.input_file_hashes:
        raise CaseContractError(
            "report artifact input hashes do not match model parents"
        )


def _context_for_family(
    case_dir: Path,
    manifest: CaseManifest,
    case_id: str,
    run_id: str,
    family: Literal["dcf", "comps"],
    *,
    verify_hashes: bool,
) -> ReportContext | None:
    if not model_run_is_declared(manifest, family, run_id):
        return None
    resolved = resolve_model_run(
        case_dir,
        manifest,
        family=family,
        run_id=run_id,
        verify_hashes=verify_hashes,
    )
    reconciliation = resolved.table(f"model.{family}-reconciliation")
    if reconciliation.filter(pl.col("status") == "failed").height:
        raise CaseContractError(f"run {run_id} has failed reconciliation checks")
    inputs = resolved.table(f"model.{family}-inputs")
    source_ids = tuple(sorted(set(inputs["source_id"].to_list())))
    source_artifacts = (
        tuple(sorted(set(inputs["source_artifact_id"].to_list())))
        if "source_artifact_id" in inputs.columns
        else ()
    )
    return ReportContext(
        case_id,
        run_id,
        family,
        resolved.as_of,
        resolved.artifacts,
        source_ids,
        source_artifacts,
        resolved.table(f"model.{family}-results"),
        (resolved.table("model.comps-summary") if family == "comps" else None),
        (
            resolved.table("model.dcf-sensitivity")
            if any(kind == "model.dcf-sensitivity" for kind, _ in resolved.tables)
            else None
        ),
    )


def render_markdown(context: ReportContext) -> bytes:
    """Render portable deterministic Markdown without wall-clock content."""
    lines = [
        f"# {_markdown_text(context.family.upper())} report",
        "",
        f"- Run ID: {_markdown_text(context.run_id)}",
        f"- As of: {context.as_of.isoformat()}",
        "- Producing command: "
        f"finresearch --workspace PATH report markdown "
        f"{_markdown_text(context.case_id)} --model-run-id "
        f"{_markdown_text(context.run_id)}",
        f"- Source IDs: {_markdown_text(', '.join(context.source_ids) or '(none)')}",
        "- Source artifact IDs: "
        f"{_markdown_text(', '.join(context.source_artifact_ids) or '(none)')}",
        "- Model artifact IDs: "
        + _markdown_text(
            ", ".join(artifact.artifact_id for artifact in context.artifacts)
        ),
        "",
        "## Results",
        "",
        *_markdown_table(context.results),
    ]
    if context.summary is not None:
        lines.extend(["", "## Peer summary", "", *_markdown_table(context.summary)])
    if context.sensitivity is not None:
        lines.extend(["", "## Sensitivity", "", *_markdown_table(context.sensitivity)])
    return ("\n".join(lines) + "\n").encode("utf-8")


def render_html(context: ReportContext) -> bytes:
    """Render self-contained semantic HTML with all data escaped."""
    sections = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        f"<title>{html.escape(context.family.upper())} report</title>",
        "<style>body{font-family:system-ui,sans-serif;max-width:72rem;"
        "margin:2rem auto;}"
        "table{border-collapse:collapse;}th,td{border:1px solid #777;padding:.3rem;}"
        "th{background:#eee;}</style></head><body>",
        f"<h1>{html.escape(context.family.upper())} report</h1>",
        "<dl>",
        f"<dt>Run ID</dt><dd>{html.escape(context.run_id)}</dd>",
        f"<dt>As of</dt><dd>{context.as_of.isoformat()}</dd>",
        "<dt>Producing command</dt><dd><code>"
        + html.escape(
            f"finresearch --workspace PATH report html {context.case_id} "
            f"--model-run-id {context.run_id}"
        )
        + "</code></dd>",
        f"<dt>Source IDs</dt><dd>{html.escape(', '.join(context.source_ids))}</dd>",
        "<dt>Source artifact IDs</dt><dd>"
        f"{html.escape(', '.join(context.source_artifact_ids))}</dd>",
        "<dt>Model artifact IDs</dt><dd>"
        f"{html.escape(', '.join(item.artifact_id for item in context.artifacts))}"
        "</dd></dl>",
        "<h2>Results</h2>",
        _html_table(context.results),
    ]
    if context.summary is not None:
        sections.extend(["<h2>Peer summary</h2>", _html_table(context.summary)])
    if context.sensitivity is not None:
        sections.extend(["<h2>Sensitivity</h2>", _html_table(context.sensitivity)])
    sections.append("</body></html>\n")
    return "".join(sections).encode("utf-8")


def cast_report_format(value: str) -> ReportFormat:
    if value == "markdown":
        return "markdown"
    if value == "html":
        return "html"
    raise CaseContractError("unsupported report format")


def _markdown_table(frame: pl.DataFrame) -> list[str]:
    columns = frame.columns
    rows = frame.sort(columns).iter_rows()
    output = [
        "| " + " | ".join(_markdown_text(column) for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        output.append(
            "| " + " | ".join(_markdown_text(str(value)) for value in row) + " |"
        )
    return output


def _html_table(frame: pl.DataFrame) -> str:
    columns = frame.columns
    pieces = ["<table><thead><tr>"]
    pieces.extend(f"<th>{html.escape(column)}</th>" for column in columns)
    pieces.append("</tr></thead><tbody>")
    for row in frame.sort(columns).iter_rows():
        pieces.append("<tr>")
        pieces.extend(f"<td>{html.escape(str(value))}</td>" for value in row)
        pieces.append("</tr>")
    pieces.append("</tbody></table>")
    return "".join(pieces)


def _markdown_text(value: str) -> str:
    """Encode arbitrary text without allowing Markdown or HTML injection."""
    normalized = value.replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")
    normalized = "".join(
        f"[U+{ord(character):04X}]"
        if ord(character) < 32 or 0x7F <= ord(character) <= 0x9F
        else character
        for character in normalized
    )
    escaped = (
        html.escape(normalized, quote=True)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("`", "\\`")
        .replace("!", "\\!")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )
    return _break_bare_autolinks(escaped)


def _break_bare_autolinks(value: str) -> str:
    """Make GFM bare URL/e-mail triggers readable but non-clickable."""
    return value.replace("://", ":\\//").replace("www.", "www\\.").replace("@", "\\@")


def _extension(format: ReportFormat) -> str:
    return "md" if format == "markdown" else "html"
