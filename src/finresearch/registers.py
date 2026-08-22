"""CSV research registers: evidence, assumptions, scenarios, catalysts, questions.

Registers store the explicit judgments and open items of a research process in
small human-auditable CSV files under ``registers/``. The CLI validates and
summarizes them; ordinary editing stays with the user's CSV tooling.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
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


@dataclass(frozen=True)
class _ParsedRegisterRow:
    """One private parsed CSV row paired with its physical file line number."""

    values: dict[str, str]
    line_number: int


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
        for parsed_row in rows:
            row = parsed_row.values
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
) -> tuple[list[_ParsedRegisterRow], list[ValidationIssue]]:
    spec = REGISTER_SPECS[path.name]
    issues: list[ValidationIssue] = []
    try:
        with path.open(newline="", encoding="utf-8") as file_handle:
            reader = csv.DictReader(file_handle, strict=True)
            fieldnames = list(reader.fieldnames or ())
            header_issue = _strict_header_issue(spec, path.name, fieldnames)
            if header_issue is not None:
                return [], [header_issue]
            raw_rows = [(reader.line_num, row) for row in reader]
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        return [], [ValidationIssue("register_unreadable", f"{path.name}: {exc}")]

    # DictReader represents an over-wide row as ``{None: [extra, ...]}`` and
    # a short row as a ``None`` value.  Validate those parser-level shapes
    # before string normalization so malformed user CSV cannot escape through
    # a permissive ``or ""`` conversion (or raise a TypeError in audit/report).
    malformed_line = next(
        (
            line_number
            for line_number, row in raw_rows
            if not isinstance(row, dict)
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in row.items()
            )
        ),
        None,
    )
    if malformed_line is not None:
        return [], [
            ValidationIssue(
                "register_malformed",
                f"{path.name}:{malformed_line} inconsistent column count in a row",
            )
        ]

    rows: list[_ParsedRegisterRow] = []
    seen_ids: set[str] = set()
    for line_number, raw_row in raw_rows:
        if not any(raw_row.values()):
            continue
        row = {key: value.strip() for key, value in raw_row.items()}
        rows.append(_ParsedRegisterRow(row, line_number))
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


def _strict_header_issue(
    spec: RegisterSpec,
    filename: str,
    fieldnames: Sequence[object],
) -> ValidationIssue | None:
    """Reject non-exact CSV projections before DictReader can drop fields."""
    if any(not isinstance(fieldname, str) for fieldname in fieldnames):
        return ValidationIssue("register_malformed", f"{filename}: invalid CSV header")
    header = tuple(str(fieldname) for fieldname in fieldnames)
    expected = tuple(spec.columns)
    duplicate_names = sorted(name for name in set(header) if header.count(name) > 1)
    if duplicate_names:
        return ValidationIssue(
            "register_schema",
            f"{filename} invalid header (duplicate columns: "
            f"{', '.join(duplicate_names)})",
        )
    if header != expected:
        return ValidationIssue(
            "register_schema",
            f"{filename} invalid header (expected ordered columns: "
            f"{', '.join(expected)})",
        )
    return None


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
    evidence_rows, evidence_issues = _validate_register(evidence_path)
    assumptions_rows, assumptions_issues = _validate_register(assumptions_path)
    if evidence_issues or assumptions_issues:
        return []
    evidence_ids = {
        parsed_row.values["id"]
        for parsed_row in evidence_rows
        if parsed_row.values.get("id")
    }
    issues: list[ValidationIssue] = []
    for parsed_row in assumptions_rows:
        row = parsed_row.values
        reference = row.get("source_evidence", "")
        if reference and reference not in evidence_ids:
            issues.append(
                ValidationIssue(
                    "register_dangling_reference",
                    f"assumptions.csv:{parsed_row.line_number} source_evidence "
                    f"{reference!r} is not a declared evidence id",
                )
            )
    return issues


def _validate_scenario_completeness(registers_dir: Path) -> list[ValidationIssue]:
    path = registers_dir / "scenarios.csv"
    if not path.is_file():
        return []
    parameters: dict[str, dict[str, str]] = {}
    rows, validation_issues = _validate_register(path)
    if validation_issues:
        return []
    for parsed_row in rows:
        row = parsed_row.values
        scenario = row.get("scenario", "")
        parameter = row.get("parameter", "")
        value = row.get("value", "")
        if not scenario or not parameter:
            continue
        if scenario not in SCENARIOS:
            continue
        if scenario in parameters.setdefault(parameter, {}):
            continue
        parameters[parameter][scenario] = value
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
