"""Command-line interface for finresearch."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from finresearch.cases import (
    CaseContractError,
    CaseStatus,
    initialize_case,
    inspect_case,
)

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run deterministic investment-research workflows.",
)
case_app = typer.Typer(help="Inspect and manage research cases.")
app.add_typer(case_app, name="case")

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
        print_issues(status)
        raise typer.Exit(1)


@case_app.command("validate")
def validate_case_command(ctx: typer.Context, case_id: CaseIdArgument) -> None:
    """Validate a case against the complete v1 contract."""
    status = get_case_status(ctx, case_id)
    if not status.valid:
        print_issues(status)
        raise typer.Exit(1)
    typer.echo(f"valid case: {case_id}")


def get_case_status(ctx: typer.Context, case_id: str) -> CaseStatus:
    """Inspect a case and convert identifier errors to CLI failures."""
    try:
        return inspect_case(state_from_context(ctx).workspace, case_id)
    except (CaseContractError, OSError) as exc:
        fail(str(exc))


def print_issues(status: CaseStatus) -> None:
    """Print validation issues in stable order."""
    for issue in status.issues:
        typer.echo(f"error [{issue.code}]: {issue.message}", err=True)


def fail(message: str) -> NoReturn:
    """Exit a command with a concise contract error."""
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(1)


def run() -> None:
    """Run the finresearch CLI."""
    app()
