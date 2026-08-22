"""Read-only, deterministic whole-case audit checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    ValidationIssue,
    case_directory,
    inspect_case,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_validation import validate_artifact
from finresearch.modeling import (
    AuthenticatedModelRun,
    authenticate_case_model_runs,
    resolve_model_run,
)
from finresearch.registers import load_model_sources
from finresearch.report_contract import authenticate_report_artifact


@dataclass(frozen=True)
class CaseAudit:
    """Stable result from a complete read-only case audit."""

    case_id: str
    as_of: date
    issues: tuple[ValidationIssue, ...]

    @property
    def valid(self) -> bool:
        """Return whether no audit issues were found."""
        return not self.issues


def audit_case(
    workspace: Path,
    case_id: str,
    *,
    as_of: date,
    max_price_age_days: int,
    verify_hashes: bool = False,
) -> CaseAudit:
    """Audit registered artifacts and point-in-time constraints without writing."""
    if max_price_age_days < 0:
        raise CaseContractError("max_price_age_days must be non-negative")
    case_dir = case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "case audit requires manifest v2; run case migrate first"
        )

    issues = list(inspect_case(workspace, case_id).issues)
    # Structural and DatasetContract validation always run. Full-file digest
    # comparison is deliberately opt-in: it is materially more expensive on
    # large cases and is requested by report preflight.
    if manifest.artifacts:
        issues.extend(
            validate_artifact(workspace, case_id, verify_hashes=verify_hashes)
        )

    frame_issues, price_dates, price_instruments = _artifact_audit_issues(
        case_dir, manifest.artifacts, as_of
    )
    issues.extend(frame_issues)

    authenticated_runs, authentication_issues = authenticate_case_model_runs(
        case_dir, manifest, verify_hashes=verify_hashes
    )
    issues.extend(authentication_issues)
    for model_run in authenticated_runs:
        if model_run.as_of > as_of:
            issues.append(
                ValidationIssue(
                    "model_run_after_as_of",
                    f"{model_run.family} run {model_run.run_id} is dated after "
                    f"audit as_of {as_of.isoformat()}",
                )
            )
    for artifact in manifest.artifacts:
        report_issue = authenticate_report_artifact(
            case_dir,
            manifest,
            artifact,
            verify_hashes=verify_hashes,
        )
        if report_issue is not None:
            issues.append(report_issue)

    issues.extend(
        _price_freshness_issues(
            price_dates, price_instruments, as_of, max_price_age_days
        )
    )
    return CaseAudit(case_id, as_of, _sorted_issues(issues))


def audit_model_run(
    workspace: Path,
    case_id: str,
    *,
    family: Literal["dcf", "comps"],
    run_id: str,
    max_price_age_days: int,
    verify_hashes: bool = False,
) -> CaseAudit:
    """Audit one authenticated model run and only its transitive case inputs.

    Report preflight must not let unrelated historical, future, or legacy model
    runs veto a complete selected run.  The shared resolver authenticates the
    selected canonical set; this helper then applies the ordinary deep byte and
    PIT validators only to its transitive artifact graph.
    """
    if max_price_age_days < 0:
        raise CaseContractError("max_price_age_days must be non-negative")
    case_dir = case_directory(workspace, case_id)
    manifest = read_manifest(case_dir)
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(
            "case audit requires manifest v2; run case migrate first"
        )
    try:
        resolved = resolve_model_run(
            case_dir,
            manifest,
            family=family,
            run_id=run_id,
            verify_hashes=verify_hashes,
        )
    except CaseContractError as exc:
        return CaseAudit(
            case_id,
            date.min,
            (ValidationIssue("model_run_identity_invalid", str(exc)),),
        )
    artifacts = _transitive_artifacts(manifest, resolved)
    issues: list[ValidationIssue] = []
    # Calling the same public deep validator per selected node retains every
    # DatasetContract check without widening the report gate to unrelated
    # artifacts; callers may additionally request complete digest checks.
    if artifacts:
        for artifact in artifacts:
            issues.extend(
                validate_artifact(
                    workspace,
                    case_id,
                    artifact.artifact_id,
                    verify_hashes=verify_hashes,
                )
            )
    frame_issues, price_dates, price_instruments = _artifact_audit_issues(
        case_dir, artifacts, resolved.as_of
    )
    issues.extend(frame_issues)
    issues.extend(
        _price_freshness_issues(
            price_dates, price_instruments, resolved.as_of, max_price_age_days
        )
    )
    return CaseAudit(case_id, resolved.as_of, _sorted_issues(issues))


def _transitive_artifacts(
    manifest: CaseManifest, resolved: AuthenticatedModelRun
) -> tuple[Artifact, ...]:
    """Return a stable closure over one resolver-authenticated artifact set."""
    by_id = {artifact.artifact_id: artifact for artifact in manifest.artifacts}
    pending = [artifact.artifact_id for artifact in resolved.artifacts]
    selected: dict[str, Artifact] = {}
    while pending:
        artifact_id = pending.pop()
        artifact = by_id.get(artifact_id)
        if artifact is None or artifact_id in selected:
            continue
        selected[artifact_id] = artifact
        pending.extend(artifact.input_artifact_ids)
    return tuple(selected[key] for key in sorted(selected))


def _artifact_audit_issues(
    case_dir: Path,
    artifacts: tuple[Artifact, ...],
    as_of: date,
) -> tuple[list[ValidationIssue], dict[str, date], set[str]]:
    """Apply safe contract-dependent PIT checks to a declared artifact set."""
    issues: list[ValidationIssue] = []
    # Price freshness is point-in-time: future observations can prove a cutoff
    # violation but cannot make the historical price available at ``as_of``.
    price_dates: dict[str, date] = {}
    price_instruments: set[str] = set()
    for artifact in artifacts:
        path = resolve_relative_path(
            case_dir, artifact.path, f"artifact {artifact.artifact_id}"
        )
        if path.suffix != ".parquet" or not path.is_file():
            continue
        try:
            frame = pl.read_parquet(path)
        except Exception:
            continue
        issues.extend(_cutoff_issues(artifact.artifact_id, artifact.kind, frame, as_of))
        if artifact.kind == "normalized.daily-prices" and _has_schema(
            frame,
            instrument_id=pl.String,
            session_date=pl.Date,
        ):
            for row in frame.select("instrument_id", "session_date").iter_rows(
                named=True
            ):
                instrument = row["instrument_id"]
                session = row["session_date"]
                if isinstance(instrument, str) and isinstance(session, date):
                    price_instruments.add(instrument)
                    if session > as_of:
                        continue
                    current = price_dates.get(instrument)
                    if current is None or session > current:
                        price_dates[instrument] = session
        issues.extend(_model_lookahead_issues(case_dir, artifact.kind, frame, as_of))
    return issues, price_dates, price_instruments


def _price_freshness_issues(
    price_dates: dict[str, date],
    price_instruments: set[str],
    as_of: date,
    max_price_age_days: int,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for instrument, session in sorted(price_dates.items()):
        age = (as_of - session).days
        if age > max_price_age_days:
            issues.append(
                ValidationIssue(
                    "price_stale",
                    f"instrument {instrument} latest price {session.isoformat()} is "
                    f"{age} days old at {as_of.isoformat()} (maximum "
                    f"{max_price_age_days})",
                )
            )
    for instrument in sorted(price_instruments - set(price_dates)):
        issues.append(
            ValidationIssue(
                "price_no_valid_session",
                f"instrument {instrument} has no price session on or before "
                f"audit as_of {as_of.isoformat()}",
            )
        )
    return issues


def _cutoff_issues(
    artifact_id: str, kind: str, frame: pl.DataFrame, as_of: date
) -> list[ValidationIssue]:
    checks = {
        "normalized.fundamental-facts": (
            "knowledge_date",
            "fact_knowledge_after_as_of",
        ),
        "normalized.estimates": ("estimate_as_of", "estimate_as_of_after_as_of"),
        "normalized.instrument-master": ("observed_at", "master_observed_after_as_of"),
        "normalized.daily-prices": ("session_date", "price_session_after_as_of"),
    }
    issues: list[ValidationIssue] = []
    if kind == "normalized.estimates" and _has_schema(
        frame, availability_at=pl.Datetime("us", "UTC")
    ):
        issues.extend(
            _datetime_cutoff_issues(
                artifact_id,
                frame,
                "availability_at",
                as_of,
                "estimate_available_after_as_of",
            )
        )
    check = checks.get(kind)
    if check is None or not _has_date_schema(frame, check[0]):
        return issues
    field, code = check
    values = frame.get_column(field).drop_nulls().to_list()
    if any(
        observed is not None and observed > as_of
        for observed in (_value_date(value) for value in values)
    ):
        issues.append(
            ValidationIssue(
                code,
                f"artifact {artifact_id} has {field} after audit as_of "
                f"{as_of.isoformat()}",
            )
        )
    return issues


def _datetime_cutoff_issues(
    artifact_id: str,
    frame: pl.DataFrame,
    field: str,
    as_of: date,
    code: str,
) -> list[ValidationIssue]:
    values = frame.get_column(field).drop_nulls().to_list()
    if any(
        observed is not None and observed > as_of
        for observed in (_value_date(value) for value in values)
    ):
        return [
            ValidationIssue(
                code,
                f"artifact {artifact_id} has {field} after audit as_of "
                f"{as_of.isoformat()}",
            )
        ]
    return []


def _model_lookahead_issues(
    case_dir: Path, kind: str, frame: pl.DataFrame, as_of: date
) -> list[ValidationIssue]:
    if kind not in {"model.dcf-inputs", "model.comps-inputs"}:
        return []
    try:
        sources = load_model_sources(case_dir, as_of=as_of)
    except CaseContractError as exc:
        return [ValidationIssue("model_sources_invalid", str(exc))]
    issues: list[ValidationIssue] = []
    if _has_schema(frame, source_id=pl.String):
        for source_id in sorted(
            value
            for value in set(frame["source_id"].to_list())
            if isinstance(value, str)
        ):
            source = sources.get(source_id)
            if source is None:
                issues.append(
                    ValidationIssue(
                        "model_source_missing",
                        f"model source id is missing: {source_id}",
                    )
                )
            elif source.effective_date > as_of:
                issues.append(
                    ValidationIssue(
                        "model_source_after_as_of",
                        f"model source {source_id} is dated after audit as_of "
                        f"{as_of.isoformat()}",
                    )
                )
    return issues


def _value_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    # Dataset-contract validation reports the malformed column separately.  An
    # audit must still return its stable issue list instead of masking it with
    # an implementation exception while evaluating supplementary PIT checks.
    return None


def _has_schema(
    frame: pl.DataFrame,
    **expected: pl.DataType | type[pl.DataType],
) -> bool:
    """Return whether a malformed table safely supports a typed selector."""
    return all(frame.schema.get(name) == dtype for name, dtype in expected.items())


def _has_date_schema(frame: pl.DataFrame, field: str) -> bool:
    """Accept only a date or UTC timestamp for a point-in-time check."""
    return _has_schema(frame, **{field: pl.Date}) or _has_schema(
        frame, **{field: pl.Datetime("us", "UTC")}
    )


def _sorted_issues(issues: list[ValidationIssue]) -> tuple[ValidationIssue, ...]:
    unique = {(issue.code, issue.message): issue for issue in issues}
    return tuple(unique[key] for key in sorted(unique))
