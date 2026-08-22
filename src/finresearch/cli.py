"""Command-line interface for finresearch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Literal, NoReturn, cast

import typer

from finresearch.auditing import audit_case
from finresearch.cases import (
    CaseContractError,
    CaseStatus,
    ValidationIssue,
    initialize_case,
    inspect_case,
    migrate_case,
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
from finresearch.local_import import (
    LocalImportReceipt,
    import_csv,
    import_parquet,
    parse_import_timestamp,
)
from finresearch.modeling import projection_assessment, run_comps, run_dcf
from finresearch.normalization import (
    normalize_daily_prices,
    normalize_fundamental_facts,
    reconcile_instrument_master,
)
from finresearch.providers import ProviderError
from finresearch.registers import REGISTER_FILES, inspect_registers
from finresearch.reporting import generate_report

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run deterministic investment-research workflows.",
)
case_app = typer.Typer(help="Inspect and manage research cases.")
data_app = typer.Typer(help="Ingest and inspect research data.")
model_app = typer.Typer(help="Run auditable deterministic valuation models.")
report_app = typer.Typer(help="Render deterministic reports from validated model runs.")
register_app = typer.Typer(help="Validate and summarize research registers.")
app.add_typer(case_app, name="case")
app.add_typer(data_app, name="data")
app.add_typer(model_app, name="model")
app.add_typer(report_app, name="report")
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
ImportFileArgument = Annotated[
    Path,
    typer.Argument(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Explicit local source file to preserve and import.",
    ),
]
ImportSchemaOption = Annotated[
    str,
    typer.Option("--schema", help="Strict named source projection."),
]
ImportProviderOption = Annotated[
    str,
    typer.Option("--provider", help="Provider identifier recorded in canonical rows."),
]
ImportRetrievedAtOption = Annotated[
    str,
    typer.Option("--retrieved-at", help="Explicit RFC3339 UTC source retrieval time."),
]
AsOfOption = Annotated[
    str,
    typer.Option("--as-of", help="Current-state cutoff date in YYYY-MM-DD."),
]
SourceArtifactOption = Annotated[
    list[str] | None,
    typer.Option(
        "--source-artifact-id",
        help="Restrict reconciliation to raw source artifact ids; repeatable.",
    ),
]
DCFInputPathOption = Annotated[
    str,
    typer.Option("--input", help="Case-relative path to strict dcf-inputs.toml."),
]
CompsInputArtifactOption = Annotated[
    str,
    typer.Option("--input", help="Artifact id for model.comps-observations.v1."),
]
ScenarioOption = Annotated[
    str,
    typer.Option("--scenario", help="DCF scenario: bear, base, bull, or all."),
]
SensitivityOption = Annotated[
    str | None,
    typer.Option(
        "--sensitivity",
        help="WACC values; terminal-growth values, e.g. 0.08,0.1;0.01,0.02.",
    ),
]
MetricsOption = Annotated[
    str,
    typer.Option(
        "--metrics", help="Comma-separated multiples: ev_revenue,ev_ebitda,ev_ebit,pe."
    ),
]
TargetOption = Annotated[
    str | None, typer.Option("--target", help="Declared target company_id.")
]
ModelRunIdOption = Annotated[
    str,
    typer.Option("--model-run-id", help="Complete registered DCF or comps run id."),
]
MaxPriceAgeDaysOption = Annotated[
    int,
    typer.Option("--max-price-age-days", min=0, help="Maximum allowed price age."),
]
VerifyHashesOption = Annotated[
    bool,
    typer.Option(
        "--verify-hashes",
        help=(
            "Additionally digest-check every registered artifact and input file "
            "(full-case I/O)."
        ),
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
    """Validate a case against its versioned contract."""
    status = get_case_status(ctx, case_id)
    if not status.valid:
        print_issues(status.issues)
        raise typer.Exit(1)
    typer.echo(f"valid case: {case_id}")


@case_app.command("migrate")
def migrate_case_command(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Explicitly upgrade one v1 manifest to v2 without rewriting artifacts."""
    try:
        receipt = migrate_case(state_from_context(ctx).workspace, case_id)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))
    if receipt.migrated:
        typer.echo(f"migrated case manifest to v2: {case_id}")
    else:
        typer.echo(f"case manifest already at v2: {case_id}")


@case_app.command("audit")
def audit_case_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    as_of: AsOfOption,
    max_price_age_days: MaxPriceAgeDaysOption,
    verify_hashes: VerifyHashesOption = False,
) -> None:
    """Run read-only point-in-time and registered-byte audit gates."""
    try:
        audit = audit_case(
            state_from_context(ctx).workspace,
            case_id,
            as_of=parse_iso_date(as_of, "as_of"),
            max_price_age_days=max_price_age_days,
            verify_hashes=verify_hashes,
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))
    typer.echo(f"case: {audit.case_id}")
    typer.echo(f"as_of: {audit.as_of.isoformat()}")
    typer.echo(f"valid: {'yes' if audit.valid else 'no'}")
    if not audit.valid:
        print_issues(audit.issues)
        raise typer.Exit(1)


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


@data_app.command("import-csv")
def import_csv_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    source_file: ImportFileArgument,
    schema_name: ImportSchemaOption,
    provider: ImportProviderOption,
    retrieved_at: ImportRetrievedAtOption,
) -> None:
    """Preserve one strict UTF-8 CSV source and publish canonical Parquet."""
    try:
        receipt = import_csv(
            state_from_context(ctx).workspace,
            case_id,
            source_file,
            schema_name=schema_name,
            provider=provider,
            retrieved_at=parse_import_timestamp(retrieved_at),
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))
    print_local_import_receipt(receipt)


@data_app.command("import-parquet")
def import_parquet_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    source_file: ImportFileArgument,
    schema_name: ImportSchemaOption,
    provider: ImportProviderOption,
    retrieved_at: ImportRetrievedAtOption,
) -> None:
    """Preserve one exact-schema Parquet source and publish canonical Parquet."""
    try:
        receipt = import_parquet(
            state_from_context(ctx).workspace,
            case_id,
            source_file,
            schema_name=schema_name,
            provider=provider,
            retrieved_at=parse_import_timestamp(retrieved_at),
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))
    print_local_import_receipt(receipt)


@data_app.command("validate")
def validate_data_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    artifact_id: ArtifactIdArgument = None,
) -> None:
    """Validate declared artifact integrity and Parquet dataset contracts."""
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
    typer.echo("normalized corporate-actions:")
    print_ingestion_receipt(receipt.corporate_actions)


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


@data_app.command("reconcile-instrument-master")
def reconcile_instrument_master_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    as_of: AsOfOption,
    source_artifact_ids: SourceArtifactOption = None,
) -> None:
    """Derive an as-of current-state master from v2 observations."""
    try:
        receipt = reconcile_instrument_master(
            state_from_context(ctx).workspace,
            case_id,
            as_of=parse_iso_date(as_of, "as_of"),
            source_artifact_ids=tuple(source_artifact_ids or ()),
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))
    typer.echo("reconciled instrument-master:")
    print_ingestion_receipt(receipt)


@model_app.command("dcf")
def model_dcf_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    input_path: DCFInputPathOption = "analysis/dcf-inputs.toml",
    scenario: ScenarioOption = "all",
    sensitivity: SensitivityOption = None,
) -> None:
    """Run the strict case-backed DCF model and register all typed outputs."""
    try:
        receipt = run_dcf(
            state_from_context(ctx).workspace,
            case_id,
            input_path=input_path,
            scenario=cast_dcf_scenario(scenario),
            sensitivity=parse_dcf_sensitivity(sensitivity),
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        ValueError,
        OSError,
    ) as exc:
        fail(str(exc))
    print_model_run(receipt)


@model_app.command("comps")
def model_comps_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    input_artifact_id: CompsInputArtifactOption,
    as_of: AsOfOption,
    metrics: MetricsOption,
    target: TargetOption = None,
) -> None:
    """Compute declared comparable-company multiples without peer discovery."""
    try:
        receipt = run_comps(
            state_from_context(ctx).workspace,
            case_id,
            input_artifact_id=input_artifact_id,
            as_of=parse_iso_date(as_of, "as_of"),
            metrics=tuple(item.strip() for item in metrics.split(",") if item.strip()),
            target=target,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        ValueError,
        OSError,
    ) as exc:
        fail(str(exc))
    print_model_run(receipt)


@model_app.command("projection-assess")
def model_projection_assess_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    input_path: DCFInputPathOption = "analysis/dcf-inputs.toml",
) -> None:
    """Record whether explicit disclosed needs require later projections."""
    try:
        receipt = projection_assessment(
            state_from_context(ctx).workspace,
            case_id,
            input_path=input_path,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        ValueError,
        OSError,
    ) as exc:
        fail(str(exc))
    print_model_run(receipt)


@report_app.command("markdown")
def report_markdown_command(
    ctx: typer.Context, case_id: CaseIdArgument, model_run_id: ModelRunIdOption
) -> None:
    """Render an immutable portable Markdown report from one model run."""
    _render_report_command(ctx, case_id, model_run_id, "markdown")


@report_app.command("html")
def report_html_command(
    ctx: typer.Context, case_id: CaseIdArgument, model_run_id: ModelRunIdOption
) -> None:
    """Render an immutable self-contained HTML report from one model run."""
    _render_report_command(ctx, case_id, model_run_id, "html")


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


def print_local_import_receipt(receipt: LocalImportReceipt) -> None:
    """Report both immutable source and canonical local-import outputs."""
    typer.echo("raw import:")
    typer.echo(f"artifact: {receipt.raw.artifact_id}")
    typer.echo(f"path: {receipt.raw.path}")
    typer.echo(f"sha256: {receipt.raw.sha256}")
    typer.echo("normalized import:")
    typer.echo(f"artifact: {receipt.normalized.artifact_id}")
    typer.echo(f"path: {receipt.normalized.path}")
    typer.echo(f"rows: {receipt.normalized.row_count}")
    typer.echo(f"sha256: {receipt.normalized.sha256}")


def cast_dcf_scenario(value: str) -> Literal["bear", "base", "bull", "all"]:
    """Validate the compact CLI scenario token before model execution."""
    if value not in {"bear", "base", "bull", "all"}:
        raise IngestionError("scenario must be bear, base, bull, or all")
    return cast(Literal["bear", "base", "bull", "all"], value)


def parse_dcf_sensitivity(
    value: str | None,
) -> tuple[tuple[float, ...], tuple[float, ...]] | None:
    """Parse explicit WACC;growth grids without accepting inferred values."""
    if value is None:
        return None
    parts = value.split(";")
    if len(parts) != 2:
        raise IngestionError("sensitivity must be WACCS;GROWTHS")
    try:
        waccs = tuple(float(item) for item in parts[0].split(",") if item)
        growths = tuple(float(item) for item in parts[1].split(",") if item)
    except ValueError as exc:
        raise IngestionError("sensitivity values must be decimals") from exc
    if not waccs or not growths:
        raise IngestionError("sensitivity must contain WACCS and GROWTHS")
    return waccs, growths


def print_model_run(run: object) -> None:
    """Print only stable run/artifact identifiers for all model commands."""
    from finresearch.modeling import ModelRun

    if not isinstance(run, ModelRun):
        raise RuntimeError("unexpected model receipt")
    typer.echo(f"run_id: {run.run_id}")
    for receipt in run.receipts:
        typer.echo(f"artifact: {receipt.artifact_id}")


def _render_report_command(
    ctx: typer.Context,
    case_id: str,
    model_run_id: str,
    format: Literal["markdown", "html"],
) -> None:
    try:
        receipt = generate_report(
            state_from_context(ctx).workspace,
            case_id,
            model_run_id=model_run_id,
            format=format,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))
    typer.echo(f"artifact: {receipt.artifact_id}")
    typer.echo(f"path: {receipt.path}")
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
