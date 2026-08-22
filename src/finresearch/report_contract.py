"""Shared deterministic report context, rendering, and authentication rules.

This module intentionally depends on model resolution but not on CLI or audit
code.  Both report publication and whole-case audit use the same builders so a
manifest-only rewrite cannot make altered report bytes look authentic.
"""

from __future__ import annotations

import hashlib
import html
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, cast

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
REPORT_MARKDOWN_PRODUCER_VERSION = "1"
REPORT_HTML_LEGACY_PRODUCER_VERSION = "1"
REPORT_HTML_PRODUCER_VERSION = "2"
# Kept for callers that historically imported the v1 report producer constant.
REPORT_PRODUCER_VERSION = REPORT_MARKDOWN_PRODUCER_VERSION
ReportFormat = Literal["markdown", "html"]
SensitivityGrid = tuple[
    str,
    tuple[float, ...],
    tuple[float, ...],
    dict[tuple[float, float], float],
    str,
]


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
    producer_version: str
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
    *,
    producer_version: str | None = None,
) -> ReportPublication:
    """Build the exact immutable report declaration from one authenticated run."""
    if format not in {"markdown", "html"}:
        raise CaseContractError("report format must be markdown or html")
    producer_version = _report_producer_version(format, producer_version)
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
            "producer_version": producer_version,
        }
    )
    content = _render_report_content(context, format, producer_version)
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
    filename = f"{identity}.{_extension(format)}"
    if format == "html" and producer_version == REPORT_HTML_PRODUCER_VERSION:
        filename = f"v{producer_version}.{filename}"
    path = Path(
        manifest.paths["reports"],
        context.family,
        context.run_id,
        filename,
    ).as_posix()
    return ReportPublication(
        format=format,
        content=content,
        identity=identity,
        producer_version=producer_version,
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
        report_format = cast_report_format(format)
        producer_version = _report_producer_version(
            report_format, artifact.producer_version
        )
        run_id, identity = _report_identity_parts(artifact, format)
        context = resolve_report_context(
            case_dir,
            manifest,
            manifest.case_id,
            run_id,
            verify_hashes=verify_hashes,
        )
        expected = build_report_publication(
            manifest,
            context,
            report_format,
            producer_version=producer_version,
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
        or artifact.producer_version != expected.producer_version
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
    """Render the current v2 self-contained semantic HTML report."""
    return _render_html_v2(context)


def _render_report_content(
    context: ReportContext,
    format: ReportFormat,
    producer_version: str,
) -> bytes:
    """Select the frozen renderer matching one validated report declaration."""
    if format == "markdown":
        return render_markdown(context)
    if producer_version == REPORT_HTML_LEGACY_PRODUCER_VERSION:
        return _render_html_v1(context)
    return _render_html_v2(context)


def _render_html_v1(context: ReportContext) -> bytes:
    """Render the frozen Phase 3 HTML v1 byte contract exactly."""
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


def _render_html_v2(context: ReportContext) -> bytes:
    """Render the current HTML contract with deterministic DCF heatmaps."""
    sections = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        f"<title>{html.escape(context.family.upper())} report</title>",
        "<style>body{font-family:system-ui,sans-serif;max-width:72rem;"
        "margin:2rem auto;}"
        "table{border-collapse:collapse;}th,td{border:1px solid #777;padding:.3rem;}"
        "th{background:#eee;}figure.sensitivity-heatmap{margin:1rem 0;}"
        "figure.sensitivity-heatmap svg{border:1px solid #777;max-width:100%;}"
        "</style></head><body>",
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
        sections.extend(
            [
                "<h2>Sensitivity</h2>",
                *_html_sensitivity_heatmaps(context),
                _html_table(context.sensitivity),
            ]
        )
    sections.append("</body></html>\n")
    return "".join(sections).encode("utf-8")


def cast_report_format(value: str) -> ReportFormat:
    if value == "markdown":
        return "markdown"
    if value == "html":
        return "html"
    raise CaseContractError("unsupported report format")


def _report_producer_version(format: ReportFormat, producer_version: str | None) -> str:
    """Return a supported renderer version, defaulting to current generation."""
    if format == "markdown":
        version = producer_version or REPORT_MARKDOWN_PRODUCER_VERSION
        if version == REPORT_MARKDOWN_PRODUCER_VERSION:
            return version
    else:
        version = producer_version or REPORT_HTML_PRODUCER_VERSION
        if version in {
            REPORT_HTML_LEGACY_PRODUCER_VERSION,
            REPORT_HTML_PRODUCER_VERSION,
        }:
            return version
    raise CaseContractError(f"unsupported {format} report producer version {version!r}")


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


def _html_sensitivity_heatmaps(context: ReportContext) -> list[str]:
    """Render one deterministic accessible SVG heatmap per complete DCF grid."""
    if context.family != "dcf" or context.sensitivity is None:
        return []
    artifact = next(
        (item for item in context.artifacts if item.kind == "model.dcf-sensitivity"),
        None,
    )
    if artifact is None:
        return []
    grouped: dict[str, list[tuple[float, float, float, str]]] = {}
    for row in context.sensitivity.iter_rows(named=True):
        scenario = row.get("scenario")
        wacc = row.get("wacc")
        growth = row.get("terminal_growth")
        value = row.get("per_share_value")
        unit = row.get("per_share_unit")
        if (
            not isinstance(scenario, str)
            or not isinstance(unit, str)
            or any(
                isinstance(item, bool)
                or not isinstance(item, (int, float))
                or not math.isfinite(float(item))
                for item in (wacc, growth, value)
            )
        ):
            return []
        numeric_wacc = cast(int | float, wacc)
        numeric_growth = cast(int | float, growth)
        numeric_value = cast(int | float, value)
        grouped.setdefault(scenario, []).append(
            (
                float(numeric_wacc),
                float(numeric_growth),
                float(numeric_value),
                unit,
            )
        )
    grids = [
        _sensitivity_grid(scenario, rows) for scenario, rows in sorted(grouped.items())
    ]
    if not grids or any(grid is None for grid in grids):
        return []
    return [
        _render_sensitivity_heatmap(
            context,
            artifact.artifact_id,
            index,
            grid,
        )
        for index, grid in enumerate(grids, start=1)
        if grid is not None
    ]


def _sensitivity_grid(
    scenario: str,
    rows: list[tuple[float, float, float, str]],
) -> SensitivityGrid | None:
    """Return a complete finite two-dimensional sensitivity grid, if present."""
    waccs = tuple(sorted({wacc for wacc, _, _, _ in rows}))
    growths = tuple(sorted({growth for _, growth, _, _ in rows}, reverse=True))
    units = {unit for _, _, _, unit in rows}
    values = {(wacc, growth): value for wacc, growth, value, _ in rows}
    if (
        len(waccs) < 2
        or len(growths) < 2
        or len(units) != 1
        or len(values) != len(rows)
        or len(values) != len(waccs) * len(growths)
        or any(wacc <= 0 for wacc in waccs)
        or any(growth <= -1 or growth >= wacc for growth in growths for wacc in waccs)
    ):
        return None
    return scenario, waccs, growths, values, next(iter(units))


def _render_sensitivity_heatmap(
    context: ReportContext,
    artifact_id: str,
    index: int,
    grid: SensitivityGrid,
) -> str:
    """Render one dependency-free sensitivity SVG from an authenticated grid."""
    scenario, waccs, growths, values, unit = grid
    cell_width = 96
    cell_height = 42
    left = 132
    top = 56
    width = left + cell_width * len(waccs) + 20
    height = top + cell_height * len(growths) + 52
    figure_id = f"sensitivity-heatmap-{index}"
    minimum = min(values.values())
    maximum = max(values.values())
    command = _sensitivity_producing_command(context, waccs, tuple(reversed(growths)))
    scenario_label = scenario.replace("-", " ").title()
    pieces = [
        '<figure class="sensitivity-heatmap">',
        f'<svg role="img" aria-labelledby="{figure_id}-title {figure_id}-desc" '
        f'viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        f'<title id="{figure_id}-title">'
        f"{html.escape(scenario_label)} DCF per-share sensitivity heatmap</title>",
        f'<desc id="{figure_id}-desc">Rows are terminal growth and columns are '
        f"WACC. Values are per-share amounts in {html.escape(unit)}.</desc>",
        f'<text x="{left + cell_width * len(waccs) // 2}" y="18" '
        'text-anchor="middle" font-size="13">WACC</text>',
        f'<text x="16" y="{top + cell_height * len(growths) // 2}" '
        f'text-anchor="middle" font-size="13" transform="rotate(-90 16 '
        f'{top + cell_height * len(growths) // 2})">Terminal growth</text>',
    ]
    for column, wacc in enumerate(waccs):
        x = left + column * cell_width + cell_width // 2
        pieces.append(
            f'<text x="{x}" y="42" text-anchor="middle" font-size="12">'
            f"{_percent_label(wacc)}</text>"
        )
    for row, growth in enumerate(growths):
        y = top + row * cell_height
        pieces.append(
            f'<text x="{left - 8}" y="{y + 26}" text-anchor="end" '
            f'font-size="12">{_percent_label(growth)}</text>'
        )
        for column, wacc in enumerate(waccs):
            value = values[(wacc, growth)]
            x = left + column * cell_width
            color, text_color = _heatmap_colors(value, minimum, maximum)
            pieces.extend(
                [
                    f'<rect x="{x}" y="{y}" width="{cell_width}" '
                    f'height="{cell_height}" fill="{color}"/>',
                    f'<text x="{x + cell_width // 2}" y="{y + 26}" '
                    f'text-anchor="middle" font-size="12" fill="{text_color}">'
                    f"{_value_label(value)}</text>",
                ]
            )
    legend_y = top + cell_height * len(growths) + 30
    pieces.extend(
        [
            f'<text x="{left}" y="{legend_y}" font-size="11">'
            f"Low: {_value_label(minimum)}</text>",
            f'<text x="{left + cell_width * len(waccs)}" y="{legend_y}" '
            f'text-anchor="end" font-size="11">High: {_value_label(maximum)}</text>',
            "</svg>",
            "<figcaption>"
            f"{html.escape(scenario_label)} scenario; sensitivity artifact ID: "
            f"<code>{html.escape(artifact_id)}</code>; producing command: "
            f"<code>{html.escape(command)}</code>.</figcaption>",
            "</figure>",
        ]
    )
    return "".join(pieces)


def _sensitivity_producing_command(
    context: ReportContext,
    waccs: tuple[float, ...],
    growths: tuple[float, ...],
) -> str:
    """Return the canonical CLI command that produced this grid's model run."""
    input_artifact = next(
        item for item in context.artifacts if item.kind == "model.dcf-inputs"
    )
    input_path = next(
        item.path
        for item in input_artifact.input_file_hashes
        if item.name == "file.dcf-inputs"
    )
    scenarios = tuple(
        sorted(set(context.sensitivity["scenario"].to_list()))
        if context.sensitivity is not None
        else ()
    )
    scenario = "all" if scenarios == ("base", "bear", "bull") else scenarios[0]
    grid = ",".join(_number_label(value) for value in waccs)
    grid += ";" + ",".join(_number_label(value) for value in growths)
    return (
        f"finresearch --workspace PATH model dcf {context.case_id} --input "
        f"{input_path} --scenario {scenario} --sensitivity {grid}"
    )


def _heatmap_colors(value: float, minimum: float, maximum: float) -> tuple[str, str]:
    """Map one finite grid value to a stable dependency-free diverging color."""
    ratio = 0.5 if maximum == minimum else (value - minimum) / (maximum - minimum)
    ratio = max(0.0, min(1.0, ratio))
    low = (178, 24, 43)
    high = (33, 102, 172)
    color = tuple(
        round(start + (end - start) * ratio)
        for start, end in zip(low, high, strict=True)
    )
    return f"#{color[0]:02x}{color[1]:02x}{color[2]:02x}", (
        "#111" if ratio > 0.58 else "#fff"
    )


def _percent_label(value: float) -> str:
    return f"{value:.2%}"


def _number_label(value: float) -> str:
    return format(value, ".12g")


def _value_label(value: float) -> str:
    return format(value, ".3e") if abs(value) >= 1_000_000 else f"{value:.2f}"


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
