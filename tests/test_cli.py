from pathlib import Path

from typer.testing import CliRunner

from finresearch.cli import app

runner = CliRunner()


def test_root_prints_explicit_workspace(tmp_path: Path) -> None:
    result = runner.invoke(app, ["--workspace", str(tmp_path), "root"])

    assert result.exit_code == 0
    assert result.output.strip() == str(tmp_path.resolve())


def test_root_requires_workspace() -> None:
    result = runner.invoke(app, ["root"])

    assert result.exit_code == 2
    assert "Missing option '--workspace'" in result.output


def test_root_rejects_file_as_workspace(tmp_path: Path) -> None:
    workspace_file = tmp_path / "workspace.txt"
    workspace_file.write_text("not a directory", encoding="utf-8")

    result = runner.invoke(app, ["--workspace", str(workspace_file), "root"])

    assert result.exit_code == 2
    assert "Directory" in result.output
