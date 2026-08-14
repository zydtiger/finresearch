import csv
from pathlib import Path

import pytest
from typer.testing import CliRunner, Result

from finresearch.cases import initialize_case
from finresearch.cli import app
from finresearch.registers import REGISTER_FILES, inspect_registers

runner = CliRunner()

VALID_REGISTERS: dict[str, list[dict[str, str]]] = {
    "evidence.csv": [
        {
            "id": "evidence-001",
            "claim": "Revenue grows with search demand",
            "source_type": "filing",
            "source_ref": "0001652044-26-000071",
            "observed_at": "2026-07-23",
            "notes": "10-Q Q2",
        }
    ],
    "assumptions.csv": [
        {
            "id": "assumption-001",
            "parameter": "revenue_growth",
            "value": "0.10",
            "unit": "pct",
            "rationale": "Three-year average",
            "source_evidence": "evidence-001",
            "updated_at": "2026-08-01",
        }
    ],
    "scenarios.csv": [
        {
            "scenario": "bear",
            "parameter": "revenue_growth",
            "value": "0.05",
            "unit": "pct",
            "rationale": "Macro slowdown",
        },
        {
            "scenario": "base",
            "parameter": "revenue_growth",
            "value": "0.10",
            "unit": "pct",
            "rationale": "Three-year average",
        },
        {
            "scenario": "bull",
            "parameter": "revenue_growth",
            "value": "0.15",
            "unit": "pct",
            "rationale": "AI acceleration",
        },
    ],
    "catalysts.csv": [
        {
            "id": "catalyst-001",
            "event": "Q3 earnings",
            "expected_date": "2026-10-25",
            "impact": "positive",
            "notes": "",
        }
    ],
    "open_questions.csv": [
        {
            "id": "question-001",
            "question": "Is cloud margin improving?",
            "context": "Q2 call",
            "importance": "high",
            "status": "open",
            "answered_at": "",
        }
    ],
}


def write_registers(case_dir: Path, data: dict[str, list[dict[str, str]]]) -> None:
    """Write registers as real CSV files below the case directory."""
    registers_dir = case_dir / "registers"
    registers_dir.mkdir(exist_ok=True)
    for filename, rows in data.items():
        if rows is None:
            continue
        columns = list(rows[0]) if rows else ["id"]
        with (registers_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)


def invoke(workspace: Path, *arguments: str) -> Result:
    """Invoke the CLI with its required explicit workspace."""
    return runner.invoke(app, ["--workspace", str(workspace), *arguments])


@pytest.fixture
def register_case(tmp_path: Path) -> Path:
    """A workspace with one case carrying all five valid registers."""
    initialize_case(tmp_path, "aapl")
    write_registers(tmp_path / "cases" / "aapl", VALID_REGISTERS)
    return tmp_path


def test_missing_registers_are_valid(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")

    result = invoke(tmp_path, "data", "registers", "status", "aapl")

    assert result.exit_code == 0
    assert "registers: 0/5 present" in result.output
    assert "valid: yes" in result.output


def test_valid_registers_pass(register_case: Path) -> None:
    status = inspect_registers(register_case, "aapl")

    assert status.registers_present == 5
    assert status.row_counts["evidence.csv"] == 1
    assert status.row_counts["scenarios.csv"] == 3
    assert status.valid

    result = invoke(register_case, "data", "registers", "status", "aapl")
    assert result.exit_code == 0
    assert "registers: 5/5 present" in result.output
    assert "scenarios.csv: 3 rows" in result.output
    assert "valid: yes" in result.output


def test_missing_columns_are_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    write_registers(case_dir, {"catalysts.csv": [{"id": "c-1", "event": "x"}]})

    status = inspect_registers(register_case, "aapl")

    assert not status.valid
    codes = {issue.code for issue in status.issues}
    assert "register_schema" in codes


def test_duplicate_ids_are_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    rows = [dict(row) for row in VALID_REGISTERS["evidence.csv"]] * 2
    write_registers(case_dir, {"evidence.csv": rows})

    status = inspect_registers(register_case, "aapl")

    assert not status.valid
    assert any(issue.code == "register_duplicate_id" for issue in status.issues)


def test_bad_enum_and_date_are_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    write_registers(
        case_dir,
        {
            "open_questions.csv": [
                {
                    "id": "q-1",
                    "question": "?",
                    "context": "",
                    "importance": "critical",
                    "status": "open",
                    "answered_at": "not-a-date",
                }
            ]
        },
    )

    status = inspect_registers(register_case, "aapl")

    codes = {issue.code for issue in status.issues}
    assert "register_bad_enum" in codes
    assert "register_bad_date" in codes


def test_incomplete_scenarios_are_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    rows = VALID_REGISTERS["scenarios.csv"][:2]  # drop bull
    write_registers(case_dir, {"scenarios.csv": rows})

    status = inspect_registers(register_case, "aapl")

    assert not status.valid
    assert any(issue.code == "scenario_incomplete" for issue in status.issues)


def test_non_distinct_scenario_values_are_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    rows = [dict(row) for row in VALID_REGISTERS["scenarios.csv"]]
    rows[2]["value"] = "0.10"  # bull equals base
    write_registers(case_dir, {"scenarios.csv": rows})

    status = inspect_registers(register_case, "aapl")

    assert not status.valid
    assert any(issue.code == "scenario_not_distinct" for issue in status.issues)


def test_dangling_evidence_reference_is_rejected(register_case: Path) -> None:
    case_dir = register_case / "cases" / "aapl"
    rows = [dict(row) for row in VALID_REGISTERS["assumptions.csv"]]
    rows[0]["source_evidence"] = "evidence-ghost"
    write_registers(case_dir, {"assumptions.csv": rows})

    status = inspect_registers(register_case, "aapl")

    assert not status.valid
    assert any(issue.code == "register_dangling_reference" for issue in status.issues)


def test_missing_case_fails(tmp_path: Path) -> None:
    result = invoke(tmp_path, "data", "registers", "status", "missing")

    assert result.exit_code == 1
    assert "case not found: missing" in result.output


def test_empty_register_file_is_valid(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    case_dir = tmp_path / "cases" / "aapl"
    registers_dir = case_dir / "registers"
    registers_dir.mkdir()
    for filename in REGISTER_FILES:
        (registers_dir / filename).write_text("id\n", encoding="utf-8")

    status = inspect_registers(tmp_path, "aapl")

    assert status.registers_present == 5
    assert status.valid
