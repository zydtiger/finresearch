"""CSV research registers: evidence, assumptions, scenarios, catalysts, questions.

Registers store the explicit judgments and open items of a research process in
small human-auditable CSV files under ``registers/``. The CLI validates and
summarizes them; ordinary editing stays with the user's CSV tooling.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, Literal

from finresearch.cases import (
    CaseContractError,
    ValidationIssue,
    case_directory,
    read_manifest,
    resolve_relative_path,
)

REGISTER_FILES: Final = (
    "evidence.csv",
    "assumptions.csv",
    "scenarios.csv",
    "catalysts.csv",
    "open_questions.csv",
)

SCENARIOS: Final = ("bear", "base", "bull")
SOURCE_TYPES: Final = ("filing", "estimate", "external", "assumption")
IMPACTS: Final = ("positive", "negative", "neutral")
IMPORTANCES: Final = ("high", "medium", "low")
QUESTION_STATUSES: Final = ("open", "answered")


@dataclass(frozen=True)
class RegisterSpec:
    """One CSV register contract."""

    name: str
    columns: Mapping[str, Literal["text", "date"] | tuple[str, tuple[str, ...]]]


REGISTER_SPECS: Final = {
    "evidence.csv": RegisterSpec(
        name="evidence",
        columns={
            "id": "text",
            "claim": "text",
            "source_type": ("enum", SOURCE_TYPES),
            "source_ref": "text",
            "observed_at": "date",
            "notes": "text",
        },
    ),
    "assumptions.csv": RegisterSpec(
        name="assumptions",
        columns={
            "id": "text",
            "parameter": "text",
            "value": "text",
            "unit": "text",
            "rationale": "text",
            "source_evidence": "text",
            "updated_at": "date",
        },
    ),
    "scenarios.csv": RegisterSpec(
        name="scenarios",
        columns={
            "scenario": ("enum", SCENARIOS),
            "parameter": "text",
            "value": "text",
            "unit": "text",
            "rationale": "text",
        },
    ),
    "catalysts.csv": RegisterSpec(
        name="catalysts",
        columns={
            "id": "text",
            "event": "text",
            "expected_date": "date",
            "impact": ("enum", IMPACTS),
            "notes": "text",
        },
    ),
    "open_questions.csv": RegisterSpec(
        name="open_questions",
        columns={
            "id": "text",
            "question": "text",
            "context": "text",
            "importance": ("enum", IMPORTANCES),
            "status": ("enum", QUESTION_STATUSES),
            "answered_at": "date",
        },
    ),
}

REQUIRED_COLUMNS: Final = {
    "evidence.csv": ("id", "claim", "source_type", "source_ref", "observed_at"),
    "assumptions.csv": ("id", "parameter", "value", "updated_at"),
    "scenarios.csv": ("scenario", "parameter", "value"),
    "catalysts.csv": ("id", "event", "impact"),
    "open_questions.csv": ("id", "question", "importance", "status"),
}


@dataclass(frozen=True)
class RegisterStatus:
    """Observed state of the case registers."""

    case_id: str
    registers_present: int
    registers_total: int
    row_counts: Mapping[str, int]
    issues: tuple[ValidationIssue, ...]

    @property
    def valid(self) -> bool:
        """Return whether every present register satisfies its contract."""
        return not self.issues


@dataclass(frozen=True)
class ModelSource:
    """One date-bounded evidence or assumption record usable by a model."""

    source_id: str
    kind: str
    effective_date: date


def load_model_sources(case_dir: Path, *, as_of: date) -> dict[str, ModelSource]:
    """Load strict evidence/assumption ids and reject invalid records.

    This public loader deliberately uses the same register validation as the
    CLI instead of giving models a second, permissive CSV parser. Callers apply
    their model-specific cutoff to the returned effective dates.
    """
    manifest = read_manifest(case_dir)
    registers_dir = resolve_relative_path(
        case_dir, manifest.paths["registers"], "paths.registers"
    )
    status = inspect_registers(case_dir.parent.parent, manifest.case_id)
    relevant = [
        issue
        for issue in status.issues
        if issue.message.startswith("evidence.csv")
        or issue.message.startswith("assumptions.csv")
    ]
    if relevant:
        detail = "; ".join(issue.message for issue in relevant)
        raise CaseContractError(f"model source registers are invalid: {detail}")
    sources: dict[str, ModelSource] = {}
    for filename, kind, date_column in (
        ("evidence.csv", "evidence", "observed_at"),
        ("assumptions.csv", "assumption", "updated_at"),
    ):
        path = registers_dir / filename
        if not path.is_file():
            continue
        rows, issues = _validate_register(path)
        if issues:
            raise CaseContractError(f"model source register is invalid: {filename}")
        for row in rows:
            source_id = row.get("id", "")
            if not source_id:
                continue
            effective_date = date.fromisoformat(row[date_column])
            if source_id in sources:
                raise CaseContractError(f"duplicate model source id: {source_id!r}")
            sources[source_id] = ModelSource(source_id, kind, effective_date)
    return sources


def inspect_registers(workspace: Path, case_id: str) -> RegisterStatus:
    """Validate every present register; missing registers are not errors."""
    registers_dir = _registers_directory(workspace, case_id)
    row_counts: dict[str, int] = {}
    issues: list[ValidationIssue] = []
    if registers_dir.is_dir():
        for filename in REGISTER_FILES:
            path = registers_dir / filename
            if not path.is_file():
                continue
            rows, file_issues = _validate_register(path)
            row_counts[filename] = len(rows)
            issues.extend(file_issues)
        issues.extend(_validate_references(registers_dir))
        issues.extend(_validate_scenario_completeness(registers_dir))
    return RegisterStatus(
        case_id=case_id,
        registers_present=len(row_counts),
        registers_total=len(REGISTER_FILES),
        row_counts=row_counts,
        issues=tuple(issues),
    )


def _registers_directory(workspace: Path, case_id: str) -> Path:
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    manifest = read_manifest(case_dir)
    return resolve_relative_path(
        case_dir,
        manifest.paths["registers"],
        "paths.registers",
    )


def _validate_register(
    path: Path,
) -> tuple[list[dict[str, str]], list[ValidationIssue]]:
    spec = REGISTER_SPECS[path.name]
    issues: list[ValidationIssue] = []
    try:
        with path.open(newline="", encoding="utf-8") as file_handle:
            reader = csv.DictReader(file_handle)
            raw_rows = list(reader)
    except (OSError, csv.Error) as exc:
        return [], [ValidationIssue("register_unreadable", f"{path.name}: {exc}")]

    if raw_rows and any(row is None for row in raw_rows):
        return [], [
            ValidationIssue(
                "register_malformed",
                f"{path.name}: inconsistent column count in a row",
            )
        ]

    header = list(raw_rows[0]) if raw_rows else []
    if header:
        expected = set(spec.columns)
        actual = set(header)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"missing columns: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected columns: {', '.join(unexpected)}")
        if details:
            issues.append(
                ValidationIssue(
                    "register_schema",
                    f"{path.name} invalid columns ({'; '.join(details)})",
                )
            )

    rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for line_number, raw_row in enumerate(raw_rows, start=2):
        if not any(raw_row.values()):
            continue
        row = {key: (value or "").strip() for key, value in raw_row.items()}
        rows.append(row)
        row_issues = _validate_row(spec, row, path.name, line_number)
        issues.extend(row_issues)
        if "id" in spec.columns:
            identifier = row.get("id", "")
            if identifier:
                if identifier in seen_ids:
                    issues.append(
                        ValidationIssue(
                            "register_duplicate_id",
                            f"{path.name}:{line_number} duplicate id {identifier!r}",
                        )
                    )
                seen_ids.add(identifier)
    return rows, issues


def _validate_row(
    spec: RegisterSpec,
    row: dict[str, str],
    filename: str,
    line_number: int,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    required = REQUIRED_COLUMNS[filename]
    for column in required:
        if not row.get(column):
            issues.append(
                ValidationIssue(
                    "register_missing_field",
                    f"{filename}:{line_number} missing {column!r}",
                )
            )
    for column, kind in spec.columns.items():
        value = row.get(column, "")
        if not value:
            continue
        if kind == "date":
            try:
                date.fromisoformat(value)
            except ValueError:
                issues.append(
                    ValidationIssue(
                        "register_bad_date",
                        f"{filename}:{line_number} {column} is not "
                        f"YYYY-MM-DD: {value!r}",
                    )
                )
        elif isinstance(kind, tuple):
            _, allowed = kind
            if value not in allowed:
                choices = ", ".join(allowed)
                issues.append(
                    ValidationIssue(
                        "register_bad_enum",
                        f"{filename}:{line_number} {column} must be one of "
                        f"{choices}: {value!r}",
                    )
                )
    return issues


def _validate_references(registers_dir: Path) -> list[ValidationIssue]:
    evidence_path = registers_dir / "evidence.csv"
    assumptions_path = registers_dir / "assumptions.csv"
    if not evidence_path.is_file() or not assumptions_path.is_file():
        return []
    try:
        with evidence_path.open(newline="", encoding="utf-8") as file_handle:
            evidence_ids = {
                row["id"] for row in csv.DictReader(file_handle) if row.get("id")
            }
    except (OSError, csv.Error):
        return []
    issues: list[ValidationIssue] = []
    try:
        with assumptions_path.open(newline="", encoding="utf-8") as file_handle:
            for line_number, row in enumerate(
                csv.DictReader(file_handle),
                start=2,
            ):
                reference = (row.get("source_evidence") or "").strip()
                if reference and reference not in evidence_ids:
                    issues.append(
                        ValidationIssue(
                            "register_dangling_reference",
                            f"assumptions.csv:{line_number} source_evidence "
                            f"{reference!r} is not a declared evidence id",
                        )
                    )
    except (OSError, csv.Error):
        return []
    return issues


def _validate_scenario_completeness(registers_dir: Path) -> list[ValidationIssue]:
    path = registers_dir / "scenarios.csv"
    if not path.is_file():
        return []
    parameters: dict[str, dict[str, str]] = {}
    try:
        with path.open(newline="", encoding="utf-8") as file_handle:
            for _line_number, row in enumerate(csv.DictReader(file_handle), start=2):
                scenario = (row.get("scenario") or "").strip()
                parameter = (row.get("parameter") or "").strip()
                value = (row.get("value") or "").strip()
                if not scenario or not parameter:
                    continue
                if scenario not in SCENARIOS:
                    continue
                if scenario in parameters.setdefault(parameter, {}):
                    continue
                parameters[parameter][scenario] = value
    except (OSError, csv.Error):
        return []
    issues: list[ValidationIssue] = []
    for parameter, values in sorted(parameters.items()):
        missing = [s for s in SCENARIOS if s not in values]
        if missing:
            issues.append(
                ValidationIssue(
                    "scenario_incomplete",
                    f"scenarios.csv parameter {parameter!r} is missing "
                    f"{', '.join(missing)}",
                )
            )
            continue
        distinct = {values[s] for s in SCENARIOS}
        if len(distinct) != len(SCENARIOS):
            issues.append(
                ValidationIssue(
                    "scenario_not_distinct",
                    f"scenarios.csv parameter {parameter!r} repeats the same "
                    "value across bear/base/bull",
                )
            )
    return issues
