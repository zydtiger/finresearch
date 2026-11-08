"""Command-line interface for finresearch."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Literal, NoReturn, cast

import typer

from finresearch.auditing import audit_case
from finresearch.capture_ingestion import (
    ingest_coingecko_markets,
    ingest_coingecko_token,
    ingest_defillama,
    ingest_hyperliquid_info,
)
from finresearch.captures import CaptureReceipt, register_capture
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
    normalize_fx_rates,
    reconcile_instrument_master,
)
from finresearch.protocol_normalization import normalize_protocol_metrics
from finresearch.providers import ProviderError
from finresearch.registers import REGISTER_FILES, inspect_registers
from finresearch.reporting import ReportReceipt, generate_report
from finresearch.token_model import run_token
from finresearch.token_normalization import normalize_token_supply
from finresearch.token_reconciliation import reconcile_token_supply

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
CaptureFileArgument = Annotated[
    Path,
    typer.Argument(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Explicit local file holding the exact fetched bytes.",
    ),
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
MaxPriceAgeClassOption = Annotated[
    list[str] | None,
    typer.Option(
        "--max-price-age",
        help="Per asset-class maximum price age as CLASS=N, e.g. crypto=0; repeatable.",
    ),
]
JsonOption = Annotated[
    bool,
    typer.Option(
        "--json",
        help="Print one JSON object per command instead of human-readable text.",
    ),
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


_json_output = False


@app.callback()
def configure(
    ctx: typer.Context,
    workspace: WorkspaceOption,
    json_output: JsonOption = False,
) -> None:
    """Require the explicit workspace shared by research commands."""
    global _json_output
    _json_output = json_output  # reset on every invocation
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

    def human() -> None:
        typer.echo(f"created case: {case_id}")
        typer.echo(f"manifest: {case_dir / 'manifest.toml'}")

    emit({"case_id": case_id, "manifest": str(case_dir / "manifest.toml")}, human)


@case_app.command("status")
def show_case_status(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Show manifest, directory, and artifact state for a case."""
    status = get_case_status(ctx, case_id)
    workspace = state_from_context(ctx).workspace

    def human() -> None:
        typer.echo(f"case: {status.case_id}")
        typer.echo(f"workspace: {workspace}")
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
        unused = status.unused_raw_snapshots
        typer.echo(f"unused raw snapshots: {len(unused)}")
        for artifact_id in unused:
            typer.echo(f"  {artifact_id}")
        typer.echo(f"valid: {'yes' if status.valid else 'no'}")

    emit_validation(
        {
            "case_id": status.case_id,
            "workspace": str(workspace),
            "manifest_valid": bool(status.manifest_status),
            "status": status.manifest_status or "unknown",
            "directories_present": status.required_directories_present,
            "directories_total": status.required_directories_total,
            "artifacts_declared": status.artifacts_declared,
            "artifacts_present": status.artifacts_present,
            "artifacts_missing": status.artifacts_missing,
            "unused_raw_snapshots": list(status.unused_raw_snapshots),
        },
        status.issues,
        human,
    )


@case_app.command("validate")
def validate_case_command(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Validate a case against its versioned contract."""
    status = get_case_status(ctx, case_id)
    emit_validation(
        {"case_id": case_id},
        status.issues,
        lambda: None if status.issues else typer.echo(f"valid case: {case_id}"),
    )


@case_app.command("migrate")
def migrate_case_command(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Explicitly upgrade one v1 manifest to v2 without rewriting artifacts."""
    try:
        receipt = migrate_case(state_from_context(ctx).workspace, case_id)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))
    emit(
        {"case_id": case_id, "migrated": receipt.migrated},
        lambda: typer.echo(
            f"migrated case manifest to v2: {case_id}"
            if receipt.migrated
            else f"case manifest already at v2: {case_id}"
        ),
    )


@case_app.command("audit")
def audit_case_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    as_of: AsOfOption,
    max_price_age_days: MaxPriceAgeDaysOption,
    verify_hashes: VerifyHashesOption = False,
    max_price_age: MaxPriceAgeClassOption = None,
) -> None:
    """Run read-only point-in-time and registered-byte audit gates."""
    try:
        limits = parse_class_limits(max_price_age)
        audit = audit_case(
            state_from_context(ctx).workspace,
            case_id,
            as_of=parse_iso_date(as_of, "as_of"),
            max_price_age_days=max_price_age_days,
            verify_hashes=verify_hashes,
            max_price_age=limits,
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))

    def human() -> None:
        typer.echo(f"case: {audit.case_id}")
        typer.echo(f"as_of: {audit.as_of.isoformat()}")
        typer.echo(f"valid: {'yes' if audit.valid else 'no'}")

    emit_validation(
        {"case_id": audit.case_id, "as_of": audit.as_of.isoformat()},
        audit.issues,
        human,
    )


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
    emit_ingestion_receipt(receipt)


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
    emit_ingestion_receipt(receipt)


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
    emit_ingestion_receipt(receipt)


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
    emit_local_import_receipt(receipt)


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
    emit_local_import_receipt(receipt)


@data_app.command("register-capture")
def register_capture_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    capture_file: CaptureFileArgument,
    provider: Annotated[
        str, typer.Option("--provider", help="web, coingecko, hyperliquid, defillama.")
    ],
    kind: Annotated[
        str,
        typer.Option(
            "--kind", help="api-response, web-page, document, or tool-summary."
        ),
    ],
    url: Annotated[str, typer.Option("--url", help="Absolute https source URL.")],
    retrieved_at: Annotated[
        str,
        typer.Option("--retrieved-at", help="RFC3339 UTC time the bytes were fetched."),
    ],
    method: Annotated[str, typer.Option("--method", help="GET or POST.")] = "GET",
    request_body: Annotated[
        Path | None,
        typer.Option(
            "--request-body",
            exists=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="JSON file holding the POST body.",
        ),
    ] = None,
    http_status: Annotated[
        int | None, typer.Option("--http-status", help="2xx status of the fetch.")
    ] = None,
    media_type: Annotated[
        str | None,
        typer.Option("--media-type", help="Defaults from the file extension."),
    ] = None,
    expect_sha256: Annotated[
        str | None,
        typer.Option("--expect-sha256", help="Fail unless the bytes hash to this."),
    ] = None,
) -> None:
    """Register fetched bytes as a hashed capture with request provenance."""
    try:
        receipt = register_capture(
            state_from_context(ctx).workspace,
            case_id,
            capture_file,
            provider=provider,
            kind=kind,
            url=url,
            retrieved_at=parse_import_timestamp(retrieved_at),
            method=method,
            request_body_file=request_body,
            http_status=http_status,
            media_type=media_type,
            expect_sha256=expect_sha256,
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))
    emit_capture_receipt(receipt)


VsCurrencyOption = Annotated[
    str, typer.Option("--vs-currency", help="Quote currency or stablecoin.")
]


@data_app.command("ingest-coingecko-token")
def ingest_coingecko_token_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    coin_id: Annotated[
        str, typer.Argument(help="CoinGecko coin id, e.g. hyperliquid.")
    ],
    vs_currency: VsCurrencyOption = "usd",
) -> None:
    """Capture CoinGecko /coins/{id} (keyless endpoint, throttled)."""
    try:
        receipt = ingest_coingecko_token(
            state_from_context(ctx).workspace,
            case_id,
            coin_id,
            vs_currency=vs_currency,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    emit_capture_receipt(receipt)


@data_app.command("ingest-coingecko-markets")
def ingest_coingecko_markets_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    ids: Annotated[
        str, typer.Option("--ids", help="Comma-separated explicit CoinGecko ids.")
    ],
    vs_currency: VsCurrencyOption = "usd",
) -> None:
    """Capture CoinGecko /coins/markets for an explicit id list."""
    try:
        receipt = ingest_coingecko_markets(
            state_from_context(ctx).workspace,
            case_id,
            tuple(item.strip() for item in ids.split(",") if item.strip()),
            vs_currency=vs_currency,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    emit_capture_receipt(receipt)


@data_app.command("ingest-hyperliquid-info")
def ingest_hyperliquid_info_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    request: Annotated[
        str,
        typer.Option(
            "--request",
            help=(
                "token-details, validator-summaries, meta-and-asset-ctxs, "
                "spot-meta-and-asset-ctxs, vault-details, "
                "spot-clearinghouse-state, or funding-history."
            ),
        ),
    ],
    token_id: Annotated[str | None, typer.Option("--token-id")] = None,
    address: Annotated[str | None, typer.Option("--address")] = None,
    coin: Annotated[str | None, typer.Option("--coin")] = None,
    start: Annotated[
        str | None, typer.Option("--start", help="UTC start date YYYY-MM-DD.")
    ] = None,
) -> None:
    """Capture one Hyperliquid info request (keyless endpoint)."""
    try:
        receipts = ingest_hyperliquid_info(
            state_from_context(ctx).workspace,
            case_id,
            request,
            token_id=token_id,
            address=address,
            coin=coin,
            start=None if start is None else parse_iso_date(start, "start"),
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    emit_capture_receipts(receipts)


@data_app.command("ingest-defillama")
def ingest_defillama_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    slug: Annotated[str, typer.Argument(help="DefiLlama protocol slug.")],
    dataset: Annotated[
        list[str],
        typer.Option(
            "--dataset",
            help=(
                "fees, revenue, holders-revenue, dex-volume, or open-interest; "
                "repeat to capture several, fetched in order."
            ),
        ),
    ],
) -> None:
    """Capture DefiLlama per-protocol summaries (keyless endpoints)."""
    json_output = json_mode()
    captured: list[CaptureReceipt] = []
    try:
        ingest_defillama(
            state_from_context(ctx).workspace,
            case_id,
            slug,
            tuple(dataset),
            report=captured.append if json_output else print_capture_receipt,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
        ProviderError,
    ) as exc:
        fail(str(exc))
    if json_output:
        emit_capture_receipts(captured)


@data_app.command("normalize-protocol-metrics")
def normalize_protocol_metrics_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    capture_artifact_ids: Annotated[
        list[str],
        typer.Option(
            "--capture-artifact-id", help="Declared raw.capture.v1 id; repeatable."
        ),
    ],
    entity_id: Annotated[str, typer.Option("--entity-id", help="Protocol entity id.")],
    instrument_id: Annotated[
        str | None,
        typer.Option("--instrument-id", help="Value-receiving token, if declared."),
    ] = None,
) -> None:
    """Parse captures into protocol-metrics and metric-definitions tables."""
    try:
        receipt = normalize_protocol_metrics(
            state_from_context(ctx).workspace,
            case_id,
            capture_artifact_ids=tuple(capture_artifact_ids),
            entity_id=entity_id,
            instrument_id=instrument_id,
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))

    def human() -> None:
        typer.echo("normalized provider-metric-definitions:")
        print_ingestion_receipt(receipt.definitions)
        typer.echo("normalized protocol-metrics:")
        print_ingestion_receipt(receipt.metrics)

    emit(
        {
            "definitions": ingestion_fields(receipt.definitions),
            "metrics": ingestion_fields(receipt.metrics),
        },
        human,
    )


@data_app.command("normalize-token-supply")
def normalize_token_supply_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    capture_artifact_id: Annotated[
        str, typer.Option("--capture-artifact-id", help="Declared raw.capture.v1 id.")
    ],
    instrument_id: Annotated[
        str | None, typer.Option("--instrument-id", help="For single-asset captures.")
    ] = None,
    instrument_map: Annotated[
        list[str] | None,
        typer.Option(
            "--instrument-map",
            help="PROVIDER_ASSET_ID=INSTRUMENT_ID; repeat per asset.",
        ),
    ] = None,
) -> None:
    """Parse one capture into token-supply and token-market tables."""
    try:
        mapping = parse_instrument_map(instrument_map)
        receipt = normalize_token_supply(
            state_from_context(ctx).workspace,
            case_id,
            capture_artifact_id=capture_artifact_id,
            instrument_id=instrument_id,
            instrument_map=mapping,
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))

    def human() -> None:
        typer.echo("normalized token-supply:")
        print_ingestion_receipt(receipt.supply)
        if receipt.market is not None:
            typer.echo("normalized token-market:")
            print_ingestion_receipt(receipt.market)
        if receipt.skipped_assets:
            typer.echo(f"skipped unmapped assets: {', '.join(receipt.skipped_assets)}")

    emit(
        {
            "supply": ingestion_fields(receipt.supply),
            "market": None
            if receipt.market is None
            else ingestion_fields(receipt.market),
            "skipped_assets": list(receipt.skipped_assets),
        },
        human,
    )


@data_app.command("reconcile-token-supply")
def reconcile_token_supply_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    instrument_id: Annotated[str, typer.Option("--instrument-id")],
    as_of: AsOfOption,
    policy: Annotated[
        str,
        typer.Option("--policy", help="Case-relative path to the strict policy TOML."),
    ],
    source_artifact_ids: SourceArtifactOption = None,
) -> None:
    """Reconcile token supply across providers under an explicit policy."""
    try:
        receipt = reconcile_token_supply(
            state_from_context(ctx).workspace,
            case_id,
            instrument_id=instrument_id,
            as_of=parse_iso_date(as_of, "as_of"),
            policy_path=policy,
            source_artifact_ids=tuple(source_artifact_ids or ()),
        )
    except (CaseContractError, DataContractError, IngestionError, OSError) as exc:
        fail(str(exc))
    emit_ingestion_receipt(receipt, "reconciled token-supply:")


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
    target = artifact_id or f"all declared artifacts of {case_id}"
    emit_validation(
        {"case_id": case_id, "target": target},
        issues,
        lambda: None if issues else typer.echo(f"valid: {target}"),
    )


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
    emit(inspection_fields(inspection), lambda: print_artifact_inspection(inspection))


@data_app.command("normalize-daily-prices")
def normalize_daily_prices_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    symbol: SymbolArgument,
    raw_artifact_id: RawArtifactOption = None,
    null_bars: Annotated[
        str,
        typer.Option(
            "--null-bars",
            help="fail (default) on bars with NULL OHLC, or drop them and "
            "publish normalized.price-gaps.",
        ),
    ] = "fail",
) -> None:
    """Derive instrument-master and daily-prices artifacts from one raw snapshot."""
    try:
        receipt = normalize_daily_prices(
            state_from_context(ctx).workspace,
            case_id,
            symbol,
            raw_artifact_id=raw_artifact_id,
            null_bars=null_bars,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))

    def human() -> None:
        typer.echo("normalized instrument-master:")
        print_ingestion_receipt(receipt.instrument_master)
        typer.echo("normalized daily-prices:")
        print_ingestion_receipt(receipt.daily_prices)
        typer.echo("normalized corporate-actions:")
        print_ingestion_receipt(receipt.corporate_actions)
        if receipt.price_gaps is not None:
            typer.echo("normalized price-gaps:")
            print_ingestion_receipt(receipt.price_gaps)

    payload: dict[str, object] = {
        "instrument_master": ingestion_fields(receipt.instrument_master),
        "daily_prices": ingestion_fields(receipt.daily_prices),
        "corporate_actions": ingestion_fields(receipt.corporate_actions),
    }
    if receipt.price_gaps is not None:
        payload["price_gaps"] = ingestion_fields(receipt.price_gaps)
    emit(payload, human)


@data_app.command("normalize-fx-rates")
def normalize_fx_rates_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    symbol: SymbolArgument,
    base: Annotated[str, typer.Option("--base", help="Base ISO currency code.")],
    quote: Annotated[str, typer.Option("--quote", help="Quote ISO currency code.")],
    raw_artifact_id: RawArtifactOption = None,
) -> None:
    """Derive close-only fx-rates and price-gaps artifacts from one raw snapshot."""
    try:
        receipt = normalize_fx_rates(
            state_from_context(ctx).workspace,
            case_id,
            symbol,
            base=base,
            quote=quote,
            raw_artifact_id=raw_artifact_id,
        )
    except (
        CaseContractError,
        DataContractError,
        IngestionError,
        OSError,
    ) as exc:
        fail(str(exc))

    def human() -> None:
        typer.echo("normalized fx-rates:")
        print_ingestion_receipt(receipt.fx_rates)
        typer.echo("normalized price-gaps:")
        print_ingestion_receipt(receipt.price_gaps)

    emit(
        {
            "fx_rates": ingestion_fields(receipt.fx_rates),
            "price_gaps": ingestion_fields(receipt.price_gaps),
        },
        human,
    )


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
    emit_ingestion_receipt(receipt, "normalized fundamental-facts:")


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
    emit_ingestion_receipt(receipt, "reconciled instrument-master:")


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
    emit_model_run(receipt)


@model_app.command("token")
def model_token_command(
    ctx: typer.Context,
    case_id: CaseIdArgument,
    input_path: Annotated[
        str,
        typer.Option("--input", help="Case-relative path to strict token-inputs.toml."),
    ] = "analysis/token-inputs.toml",
    scenario: ScenarioOption = "all",
    sensitivity: SensitivityOption = None,
) -> None:
    """Run the strict case-backed token model and register all typed outputs."""
    try:
        receipt = run_token(
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
    emit_model_run(receipt)


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
    emit_model_run(receipt)


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
    emit_model_run(receipt)


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

    def human() -> None:
        typer.echo(f"case: {status.case_id}")
        typer.echo(
            f"registers: {status.registers_present}/{status.registers_total} present"
        )
        for filename in REGISTER_FILES:
            count = status.row_counts.get(filename, 0)
            if count:
                typer.echo(f"{filename}: {count} rows")
        typer.echo(f"valid: {'yes' if status.valid else 'no'}")

    emit_validation(
        {
            "case_id": status.case_id,
            "registers_present": status.registers_present,
            "registers_total": status.registers_total,
            "row_counts": {
                name: status.row_counts[name]
                for name in REGISTER_FILES
                if status.row_counts.get(name, 0)
            },
        },
        status.issues,
        human,
    )


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


def print_capture_receipt(receipt: CaptureReceipt) -> None:
    """Print the stable identifiers of one registered capture."""
    typer.echo(f"capture: {receipt.capture_id}")
    typer.echo(f"body: {receipt.body_artifact_id}")
    typer.echo(f"path: {receipt.body_path}")
    typer.echo(f"sha256: {receipt.content_sha256}")


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


def ingestion_fields(receipt: IngestionReceipt) -> dict[str, object]:
    """Return the JSON form of the fields print_ingestion_receipt shows."""
    return {
        "artifact_id": receipt.artifact_id,
        "path": str(receipt.path),
        "row_count": receipt.row_count,
        "sha256": receipt.sha256,
    }


def capture_fields(receipt: CaptureReceipt) -> dict[str, object]:
    """Return the JSON form of the fields print_capture_receipt shows."""
    return {
        "capture_id": receipt.capture_id,
        "body_artifact_id": receipt.body_artifact_id,
        "path": str(receipt.body_path),
        "sha256": receipt.content_sha256,
    }


def inspection_fields(inspection: ArtifactInspection) -> dict[str, object]:
    """Return the JSON form of the facts print_artifact_inspection shows."""
    return {
        "artifact_id": inspection.artifact_id,
        "contract": inspection.contract_identifier,
        "path": inspection.path,
        "size": inspection.size,
        "sha256": inspection.sha256,
        "row_count": inspection.row_count,
        "provider": inspection.provider,
        "provider_symbol": inspection.provider_symbol,
        "cik": inspection.cik,
        "source_url": inspection.source_url,
        "columns": [
            {"name": column.name, "dtype": column.dtype}
            for column in inspection.columns
        ],
        "date_ranges": [
            {"name": item.name, "minimum": item.minimum, "maximum": item.maximum}
            for item in inspection.date_ranges
        ],
        "nulls": {null.name: null.count for null in inspection.nulls},
        "duplicate_key_rows": inspection.duplicate_key_rows,
        "preview": None
        if inspection.preview_json is None
        else json.loads(inspection.preview_json),
    }


def json_mode() -> bool:
    """Return whether the running command was invoked with the root --json flag."""
    return _json_output


def emit(payload: dict[str, object], human: Callable[[], object]) -> None:
    """Print one JSON object with --json, otherwise run the human printer."""
    if json_mode():
        typer.echo(json.dumps({"ok": True, **payload}))
    else:
        human()


def emit_validation(
    payload: dict[str, object],
    issues: tuple[ValidationIssue, ...],
    human: Callable[[], object],
) -> None:
    """Emit a validation result; issues also go to stderr and exit nonzero."""
    if json_mode():
        typer.echo(
            json.dumps(
                {
                    **payload,
                    "ok": not issues,
                    "issues": [
                        {"code": issue.code, "message": issue.message}
                        for issue in issues
                    ],
                }
            )
        )
    else:
        human()
    if issues:
        print_issues(issues)
        raise typer.Exit(1)


def emit_ingestion_receipt(
    receipt: IngestionReceipt, heading: str | None = None
) -> None:
    """Emit one ingestion receipt, optionally under a human-only heading."""

    def human() -> None:
        if heading is not None:
            typer.echo(heading)
        print_ingestion_receipt(receipt)

    emit(ingestion_fields(receipt), human)


def emit_capture_receipt(receipt: CaptureReceipt) -> None:
    """Emit one registered capture receipt."""
    emit(capture_fields(receipt), lambda: print_capture_receipt(receipt))


def emit_capture_receipts(receipts: Sequence[CaptureReceipt]) -> None:
    """Emit several capture receipts as one JSON object."""

    def human() -> None:
        for receipt in receipts:
            print_capture_receipt(receipt)

    emit({"captures": [capture_fields(receipt) for receipt in receipts]}, human)


def emit_local_import_receipt(receipt: LocalImportReceipt) -> None:
    """Emit the raw and normalized outputs of one local import."""
    emit(
        {
            "raw": {
                "artifact_id": receipt.raw.artifact_id,
                "path": str(receipt.raw.path),
                "sha256": receipt.raw.sha256,
            },
            "normalized": {
                "artifact_id": receipt.normalized.artifact_id,
                "path": str(receipt.normalized.path),
                "row_count": receipt.normalized.row_count,
                "sha256": receipt.normalized.sha256,
            },
        },
        lambda: print_local_import_receipt(receipt),
    )


def emit_report_receipt(receipt: ReportReceipt) -> None:
    """Emit one rendered report receipt."""

    def human() -> None:
        typer.echo(f"artifact: {receipt.artifact_id}")
        typer.echo(f"path: {receipt.path}")
        typer.echo(f"sha256: {receipt.sha256}")

    emit(
        {
            "artifact_id": receipt.artifact_id,
            "path": str(receipt.path),
            "sha256": receipt.sha256,
        },
        human,
    )


def parse_class_limits(values: list[str] | None) -> dict[str, int]:
    """Parse repeated CLASS=N price-age overrides."""
    limits: dict[str, int] = {}
    for item in values or ():
        name, separator, number = item.partition("=")
        if not separator or not name or not number.isdigit():
            raise IngestionError(f"--max-price-age must be CLASS=N: {item!r}")
        if name in limits:
            raise IngestionError(f"--max-price-age repeats class {name!r}")
        limits[name] = int(number)
    return limits


def parse_instrument_map(values: list[str] | None) -> dict[str, str] | None:
    """Parse repeated PROVIDER_ASSET_ID=INSTRUMENT_ID options."""
    if not values:
        return None
    mapping: dict[str, str] = {}
    for item in values:
        asset_id, separator, instrument = item.partition("=")
        if not separator or not asset_id or not instrument:
            raise IngestionError(
                f"--instrument-map must be PROVIDER_ASSET_ID=INSTRUMENT_ID: {item!r}"
            )
        if asset_id in mapping:
            raise IngestionError(f"--instrument-map repeats asset {asset_id!r}")
        mapping[asset_id] = instrument
    return mapping


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


def emit_model_run(run: object) -> None:
    """Emit only stable run/artifact identifiers for all model commands."""
    from finresearch.modeling import ModelRun

    if not isinstance(run, ModelRun):
        raise RuntimeError("unexpected model receipt")

    def human() -> None:
        typer.echo(f"run_id: {run.run_id}")
        for receipt in run.receipts:
            typer.echo(f"artifact: {receipt.artifact_id}")

    emit(
        {
            "run_id": run.run_id,
            "artifacts": [receipt.artifact_id for receipt in run.receipts],
        },
        human,
    )


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
    emit_report_receipt(receipt)


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
    if json_mode():
        typer.echo(json.dumps({"ok": False, "error": message}))
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
