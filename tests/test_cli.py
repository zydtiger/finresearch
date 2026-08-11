from pathlib import Path

from typer.testing import CliRunner, Result

from finresearch.cli import app

runner = CliRunner()


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


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
