"""Command-line interface for finresearch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from finresearch.cases import (
    CaseContractError,
    CaseStatus,
    ValidationIssue,
    initialize_case,
    inspect_case,
)
from finresearch.data_contracts import DataContractError
from finresearch.data_validation import (
    MAX_PREVIEW_ROWS,
    ArtifactInspection,
    DataValidationError,
    inspect_artifact,
    validate_artifact,
)
from finresearch.ingestion import (
    IngestionError,
    IngestionReceipt,
    ingest_sec_companyfacts,
    ingest_sec_submissions,
    ingest_yfinance_daily_prices,
)
from finresearch.normalization import (
    normalize_daily_prices,
    normalize_fundamental_facts,
)
from finresearch.providers import ProviderError
from finresearch.registers import REGISTER_FILES, inspect_registers

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run deterministic investment-research workflows.",
)
case_app = typer.Typer(help="Inspect and manage research cases.")
data_app = typer.Typer(help="Ingest and inspect research data.")
register_app = typer.Typer(help="Validate and summarize research registers.")
app.add_typer(case_app, name="case")
app.add_typer(data_app, name="data")
data_app.add_typer(register_app, name="registers")

WorkspaceOption = Annotated[
    Path,
    typer.Option(
        "--workspace",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="Explicit research-artifact workspace.",
    ),
]
CaseIdArgument = Annotated[
    str,
    typer.Argument(help="Stable case identifier below WORKSPACE/cases."),
]
TitleOption = Annotated[
    str | None,
    typer.Option("--title", help="Human-readable case title."),
]
SymbolArgument = Annotated[
    str,
    typer.Argument(help="Provider symbol, such as AAPL, BTC-USD, or ^GSPC."),
]
StartOption = Annotated[
    str,
    typer.Option("--start", help="Inclusive start date in YYYY-MM-DD format."),
]
EndOption = Annotated[
    str,
    typer.Option("--end", help="Exclusive end date in YYYY-MM-DD format."),
]
CikArgument = Annotated[
    str,
    typer.Argument(help="SEC Central Index Key, with or without leading zeros."),
]
ArtifactIdArgument = Annotated[
    str | None,
    typer.Argument(help="Artifact id declared in the case manifest."),
]
RequiredArtifactIdArgument = Annotated[
    str,
    typer.Argument(help="Artifact id declared in the case manifest."),
]
LimitOption = Annotated[
    int,
    typer.Option(
        "--limit",
        min=0,
        max=MAX_PREVIEW_ROWS,
        help="Preview row limit for data inspect.",
    ),
]
SECUserAgentOption = Annotated[
    str,
    typer.Option(
        "--user-agent",
        help="SEC requester identity including a contact email.",
    ),
]
RawArtifactOption = Annotated[
    str | None,
    typer.Option(
        "--raw-artifact-id",
        help="Raw snapshot to normalize when multiple exist for the symbol.",
    ),
]


@dataclass(frozen=True)
class State:
    """State shared by commands for a single CLI invocation."""

    workspace: Path


@app.callback()
def configure(
    ctx: typer.Context,
    workspace: WorkspaceOption,
) -> None:
    """Require the explicit workspace shared by research commands."""
    ctx.obj = State(workspace=workspace)


def state_from_context(ctx: typer.Context) -> State:
    """Return validated CLI state from a Typer context."""
    state = ctx.obj
    if not isinstance(state, State):
        raise RuntimeError("finresearch CLI state was not initialized")
    return state


@case_app.command("init")
def initialize_case_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    title: TitleOption = None,
) -> None:
    """Initialize a case without overwriting existing state."""
    workspace = state_from_context(ctx).workspace
    try:
        case_dir = initialize_case(workspace, case_id, title)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))
    typer.echo(f"created case: {case_id}")
    typer.echo(f"manifest: {case_dir / 'manifest.toml'}")


@case_app.command("status")
def show_case_status(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Show manifest, directory, and artifact state for a case."""
    status = get_case_status(ctx, case_id)
    typer.echo(f"case: {status.case_id}")
    typer.echo(f"workspace: {state_from_context(ctx).workspace}")
    typer.echo(f"manifest: {'valid' if status.manifest_status else 'invalid'}")
    typer.echo(f"status: {status.manifest_status or 'unknown'}")
    typer.echo(
        "directories: "
        f"{status.required_directories_present}/"
        f"{status.required_directories_total} required"
    )
    typer.echo(
        "artifacts: "
        f"{status.artifacts_declared} declared, "
        f"{status.artifacts_present} present, "
        f"{status.artifacts_missing} missing"
    )
    typer.echo(f"valid: {'yes' if status.valid else 'no'}")
    if not status.valid:
        print_issues(status.issues)
        raise typer.Exit(1)


@case_app.command("validate")
def validate_case_command(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Validate a case against the complete v1 contract."""
    status = get_case_status(ctx, case_id)
    if not status.valid:
        print_issues(status.issues)
        raise typer.Exit(1)
    typer.echo(f"valid case: {case_id}")


@data_app.command("ingest-yfinance-prices")
def ingest_yfinance_prices_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    symbol: SymbolArgument,
    start: StartOption,
    end: EndOption,
) -> None:
    """Append one immutable raw daily-price snapshot from yfinance."""
    try:
        receipt = ingest_yfinance_daily_prices(
            state_from_context(ctx).workspace,
            case_id,
            symbol,
            parse_iso_date(start, "start"),
            parse_iso_date(end, "end"),
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    print_ingestion_receipt(receipt)


@data_app.command("ingest-sec-submissions")
def ingest_sec_submissions_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    cik: CikArgument,
    user_agent: SECUserAgentOption,
) -> None:
    """Append one SEC recent-submissions raw snapshot."""
    try:
        receipt = ingest_sec_submissions(
            state_from_context(ctx).workspace,
            case_id,
            cik,
            user_agent,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    print_ingestion_receipt(receipt)


@data_app.command("ingest-sec-companyfacts")
def ingest_sec_companyfacts_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    cik: CikArgument,
    user_agent: SECUserAgentOption,
) -> None:
    """Append one SEC companyfacts XBRL raw snapshot."""
    try:
        receipt = ingest_sec_companyfacts(
            state_from_context(ctx).workspace,
            case_id,
            cik,
            user_agent,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    print_ingestion_receipt(receipt)


@data_app.command("validate")
def validate_data_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    artifact_id: ArtifactIdArgument = None,
) -> None:
    """Deep-validate declared snapshots against their dataset contracts."""
    workspace = state_from_context(ctx).workspace
    try:
        issues = validate_artifact(workspace, case_id, artifact_id)
    except (CaseContractError, DataValidationError, OSError) as exc:
        fail(str(exc))
    if issues:
        print_issues(issues)
        raise typer.Exit(1)
    target = artifact_id or f"all declared artifacts of {case_id}"
    typer.echo(f"valid: {target}")


@data_app.command("inspect")
def inspect_data_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    artifact_id: RequiredArtifactIdArgument,
    limit: LimitOption = 5,
) -> None:
    """Report file, schema, provenance, and preview facts for one artifact."""
    workspace = state_from_context(ctx).workspace
    try:
        inspection = inspect_artifact(workspace, case_id, artifact_id, limit)
    except (CaseContractError, DataValidationError, OSError) as exc:
        fail(str(exc))
    print_artifact_inspection(inspection)


@data_app.command("normalize-daily-prices")
def normalize_daily_prices_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    symbol: SymbolArgument,
    raw_artifact_id: RawArtifactOption = None,
) -> None:
    """Derive instrument-master and daily-prices artifacts from one raw snapshot."""
    try:
        receipt = normalize_daily_prices(
            state_from_context(ctx).workspace,
            case_id,
            symbol,
            raw_artifact_id=raw_artifact_id,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))
    typer.echo("normalized instrument-master:")
    print_ingestion_receipt(receipt.instrument_master)
    typer.echo("normalized daily-prices:")
    print_ingestion_receipt(receipt.daily_prices)


@data_app.command("normalize-fundamental-facts")
def normalize_fundamental_facts_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    cik: CikArgument,
    raw_artifact_id: RawArtifactOption = None,
) -> None:
    """Derive parsed fundamental-facts from one raw SEC companyfacts snapshot."""
    try:
        receipt = normalize_fundamental_facts(
            state_from_context(ctx).workspace,
            case_id,
            cik,
            raw_artifact_id=raw_artifact_id,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))
    typer.echo("normalized fundamental-facts:")
    print_ingestion_receipt(receipt)


@register_app.command("status")
def show_registers_status(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Validate and summarize the case research registers."""
    workspace = state_from_context(ctx).workspace
    try:
        status = inspect_registers(workspace, case_id)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))
    typer.echo(f"case: {status.case_id}")
    typer.echo(
        f"registers: {status.registers_present}/{status.registers_total} present"
    )
    for filename in REGISTER_FILES:
        count = status.row_counts.get(filename, 0)
        if count:
            typer.echo(f"{filename}: {count} rows")
    typer.echo(f"valid: {'yes' if status.valid else 'no'}")
    if not status.valid:
        print_issues(status.issues)
        raise typer.Exit(1)


def get_case_status(ctx: typer.Context, case_id: str) -> CaseStatus:
    """Inspect a case and convert identifier errors to CLI failures."""
    try:
        return inspect_case(state_from_context(ctx).workspace, case_id)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))


def print_issues(issues: tuple[ValidationIssue, ...]) -> None:
    """Print validation issues in stable order."""
    for issue in issues:
        typer.echo(f"error [{issue.code}]: {issue.message}", err=True)


def print_ingestion_receipt(receipt: IngestionReceipt) -> None:
    """Print stable fields shared by raw ingestion commands."""
    typer.echo(f"artifact: {receipt.artifact_id}")
    typer.echo(f"path: {receipt.path}")
    typer.echo(f"rows: {receipt.row_count}")
    typer.echo(f"sha256: {receipt.sha256}")


def print_artifact_inspection(inspection: ArtifactInspection) -> None:
    """Print stable inspection facts in deterministic order."""
    typer.echo(f"artifact: {inspection.artifact_id}")
    typer.echo(f"contract: {inspection.contract_identifier}")
    typer.echo(f"path: {inspection.path}")
    typer.echo(f"size: {inspection.size} bytes")
    typer.echo(f"sha256: {inspection.sha256}")
    typer.echo(f"rows: {inspection.row_count}")
    if inspection.provider is not None:
        typer.echo(f"provider: {inspection.provider}")
    if inspection.provider_symbol is not None:
        typer.echo(f"provider_symbol: {inspection.provider_symbol}")
    if inspection.cik is not None:
        typer.echo(f"cik: {inspection.cik}")
    if inspection.source_url is not None:
        typer.echo(f"source_url: {inspection.source_url}")
    typer.echo(f"columns ({len(inspection.columns)}):")
    for column in inspection.columns:
        typer.echo(f"  {column.name}: {column.dtype}")
    if inspection.date_ranges:
        typer.echo("date ranges:")
        for item in inspection.date_ranges:
            typer.echo(
                f"  {item.name}: {item.minimum or 'n/a'} .. {item.maximum or 'n/a'}"
            )
    if inspection.nulls:
        typer.echo("nulls:")
        for null in inspection.nulls:
            typer.echo(f"  {null.name}: {null.count}")
    else:
        typer.echo("nulls: none")
    if inspection.duplicate_key_rows is not None:
        typer.echo(f"duplicate key rows: {inspection.duplicate_key_rows}")
    if inspection.preview_json is not None:
        typer.echo("preview:")
        typer.echo(inspection.preview_json)


def fail(message: str) -> NoReturn:
    """Exit a command with a concise contract error."""
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(1)


def parse_iso_date(value: str, field: str) -> date:
    """Parse a CLI date without accepting timestamps or locale variants."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        fail(f"{field} must use YYYY-MM-DD format: {value!r}")


def run() -> None:
    """Run the finresearch CLI."""
    app()
