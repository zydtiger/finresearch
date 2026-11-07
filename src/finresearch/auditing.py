"""Read-only, deterministic whole-case audit checks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
from finresearch.data_contracts import ASSET_CLASSES, provider_id_pairs
from finresearch.data_validation import validate_artifact
from finresearch.modeling import (
    AuthenticatedModelRun,
    authenticate_case_model_runs,
    resolve_model_run,
)
from finresearch.registers import capture_reference_issues, load_model_sources
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


@dataclass
class _PriceFacts:
    """Price and master observations collected for the freshness checks."""

    latest: dict[str, date] = field(default_factory=dict)
    prior: dict[str, date] = field(default_factory=dict)
    instruments: set[str] = field(default_factory=set)
    symbols: dict[str, set[str]] = field(default_factory=dict)
    # (instrument_id, asset_class, provider_ids, observed date)
    masters: list[tuple[str, str, str | None, date]] = field(default_factory=list)


def parse_max_price_age(values: Mapping[str, int] | None) -> dict[str, int]:
    """Validate per-asset-class price-age overrides."""
    limits = dict(values or {})
    for asset_class, limit in limits.items():
        if asset_class not in ASSET_CLASSES:
            raise CaseContractError(f"unknown asset class for price age: {asset_class}")
        if limit < 0:
            raise CaseContractError("per-class maximum price age must be non-negative")
    return limits


def audit_case(
    workspace: Path,
    case_id: str,
    *,
    as_of: date,
    max_price_age_days: int,
    verify_hashes: bool = False,
    max_price_age: Mapping[str, int] | None = None,
) -> CaseAudit:
    """Audit registered artifacts and point-in-time constraints without writing."""
    if max_price_age_days < 0:
        raise CaseContractError("max_price_age_days must be non-negative")
    class_limits = parse_max_price_age(max_price_age)
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

    frame_issues, facts = _artifact_audit_issues(case_dir, manifest.artifacts, as_of)
    issues.extend(frame_issues)
    issues.extend(capture_reference_issues(workspace, case_id))

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
        _price_freshness_issues(facts, as_of, max_price_age_days, class_limits)
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
    max_price_age: Mapping[str, int] | None = None,
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
    frame_issues, facts = _artifact_audit_issues(case_dir, artifacts, resolved.as_of)
    issues.extend(frame_issues)
    issues.extend(
        _price_freshness_issues(
            facts,
            resolved.as_of,
            max_price_age_days,
            parse_max_price_age(max_price_age),
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
) -> tuple[list[ValidationIssue], _PriceFacts]:
    """Apply safe contract-dependent PIT checks to a declared artifact set."""
    issues: list[ValidationIssue] = []
    # Price freshness is point-in-time: future observations can prove a cutoff
    # violation but cannot make the historical price available at ``as_of``.
    facts = _PriceFacts()
    by_id = {artifact.artifact_id: artifact for artifact in artifacts}
    # instrument -> (as_of, artifact id, conflicting metrics) of the latest
    # reconciliation dated on or before the cutoff.
    reconciliations: dict[str, tuple[date, str, list[str]]] = {}
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
            has_symbol = _has_schema(frame, provider_symbol=pl.String)
            columns = ["instrument_id", "session_date"] + (
                ["provider_symbol"] if has_symbol else []
            )
            for row in frame.select(columns).iter_rows(named=True):
                instrument = row["instrument_id"]
                session = row["session_date"]
                if isinstance(instrument, str) and isinstance(session, date):
                    facts.instruments.add(instrument)
                    symbol = row.get("provider_symbol")
                    if isinstance(symbol, str):
                        facts.symbols.setdefault(instrument, set()).add(symbol)
                    if session > as_of:
                        continue
                    for bucket, cutoff in (
                        (facts.latest, as_of),
                        (facts.prior, as_of - timedelta(days=1)),
                    ):
                        current = bucket.get(instrument)
                        if session <= cutoff and (current is None or session > current):
                            bucket[instrument] = session
        if artifact.kind == "normalized.instrument-master" and _has_schema(
            frame,
            instrument_id=pl.String,
            asset_class=pl.String,
            observed_at=pl.Datetime("us", "UTC"),
        ):
            has_ids = _has_schema(frame, provider_ids=pl.String)
            for row in frame.iter_rows(named=True):
                observed = row["observed_at"].date()
                if observed <= as_of:
                    facts.masters.append(
                        (
                            row["instrument_id"],
                            row["asset_class"],
                            row["provider_ids"] if has_ids else None,
                            observed,
                        )
                    )
        issues.extend(_model_lookahead_issues(case_dir, artifact.kind, frame, as_of))
        issues.extend(_capture_issues(artifact, frame, by_id))
        if artifact.kind == "derived.token-supply-reconciliation":
            _track_reconciliation(artifact.artifact_id, frame, as_of, reconciliations)
    for instrument, (_, artifact_id, metrics) in sorted(reconciliations.items()):
        issues.extend(
            ValidationIssue(
                "token_supply_conflict",
                f"instrument {instrument} supply metric {metric} is in conflict "
                f"in reconciliation {artifact_id}",
            )
            for metric in metrics
        )
    return issues, facts


def _capture_issues(
    artifact: Artifact,
    frame: pl.DataFrame,
    by_id: dict[str, Artifact],
) -> list[ValidationIssue]:
    """Bind a capture row to its own id and its body artifact's checksum."""
    if artifact.kind != "raw.capture" or not _has_schema(
        frame,
        capture_id=pl.String,
        body_artifact_id=pl.String,
        content_sha256=pl.String,
    ):
        return []
    issues: list[ValidationIssue] = []
    for row in frame.iter_rows(named=True):
        body = by_id.get(row["body_artifact_id"])
        if row["capture_id"] != artifact.artifact_id:
            problem = f"capture_id {row['capture_id']!r} is not the artifact id"
        elif artifact.input_artifact_ids != (row["body_artifact_id"],):
            problem = "body_artifact_id is not the artifact's only parent"
        elif body is not None and body.sha256 != row["content_sha256"]:
            problem = (
                f"content_sha256 {row['content_sha256']} differs from body "
                f"artifact checksum {body.sha256}"
            )
        else:
            continue
        issues.append(
            ValidationIssue(
                "capture_metadata_mismatch",
                f"capture {artifact.artifact_id}: {problem}",
            )
        )
    return issues


def _track_reconciliation(
    artifact_id: str,
    frame: pl.DataFrame,
    as_of: date,
    latest: dict[str, tuple[date, str, list[str]]],
) -> None:
    if not _has_schema(
        frame,
        instrument_id=pl.String,
        as_of=pl.Date,
        supply_metric=pl.String,
        status=pl.String,
    ):
        return
    rows = frame.select("instrument_id", "as_of", "supply_metric", "status").unique()
    for instrument in sorted(set(rows["instrument_id"].to_list())):
        mine = rows.filter(pl.col("instrument_id") == instrument)
        for cutoff in sorted(set(mine["as_of"].to_list())):
            if cutoff > as_of:
                continue
            current = latest.get(instrument)
            if current is not None and (current[0], current[1]) >= (
                cutoff,
                artifact_id,
            ):
                continue
            conflicts = sorted(
                mine.filter(
                    (pl.col("as_of") == cutoff) & (pl.col("status") == "conflict")
                )["supply_metric"].to_list()
            )
            latest[instrument] = (cutoff, artifact_id, conflicts)


def _price_asset_class(
    facts: _PriceFacts, instrument: str
) -> tuple[str | None, list[str]]:
    """Resolve one asset class from linked master observations.

    Returns the class (or None) and the distinct non-``unknown`` classes found.
    """
    symbols = {f"yfinance:{symbol}" for symbol in facts.symbols.get(instrument, ())}
    classes: set[str] = set()
    for master_id, asset_class, provider_ids, _ in facts.masters:
        linked = master_id == instrument
        if not linked and provider_ids is not None:
            try:
                linked = bool(
                    symbols & {f"{a}:{b}" for a, b in provider_id_pairs(provider_ids)}
                )
            except ValueError:
                linked = False
        if linked and asset_class != "unknown":
            classes.add(asset_class)
    found = sorted(classes)
    return (found[0] if len(found) == 1 else None), found


def _price_freshness_issues(
    facts: _PriceFacts,
    as_of: date,
    max_price_age_days: int,
    class_limits: Mapping[str, int],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for instrument in sorted(facts.instruments):
        asset_class, found = _price_asset_class(facts, instrument)
        if len(found) > 1:
            issues.append(
                ValidationIssue(
                    "asset_class_conflict",
                    f"instrument {instrument} resolves to conflicting asset "
                    f"classes: {', '.join(found)}",
                )
            )
        limit = class_limits.get(asset_class or "", max_price_age_days)
        latest = facts.latest.get(instrument)
        if asset_class == "crypto":
            # A UTC daily bar is complete at the end of its day, so the latest
            # complete bar at cutoff D is D - 1.
            if latest == as_of:
                issues.append(
                    ValidationIssue(
                        "price_bar_incomplete",
                        f"instrument {instrument} bar {as_of.isoformat()} is "
                        "incomplete at the audit as_of",
                    )
                )
            latest = facts.prior.get(instrument)
            reference = as_of - timedelta(days=1)
        else:
            reference = as_of
        if latest is None:
            if instrument not in facts.latest:
                issues.append(
                    ValidationIssue(
                        "price_no_valid_session",
                        f"instrument {instrument} has no price session on or "
                        f"before audit as_of {as_of.isoformat()}",
                    )
                )
            continue
        age = (reference - latest).days
        if age > limit:
            issues.append(
                ValidationIssue(
                    "price_stale",
                    f"instrument {instrument} latest price {latest.isoformat()} "
                    f"is {age} days old at {as_of.isoformat()} (maximum {limit})",
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
    for token_kind, token_code in (
        ("normalized.token-supply", "token_supply_after_as_of"),
        ("normalized.token-market", "token_market_after_as_of"),
    ):
        if kind == token_kind:
            issues.extend(
                _datetime_cutoff_issues(
                    artifact_id, frame, "observed_at", as_of, token_code
                )
                if _has_date_schema(frame, "observed_at")
                else []
            )
    if kind == "normalized.protocol-metrics":
        for field in ("period_end", "observed_at"):
            if _has_date_schema(frame, field):
                issues.extend(
                    _datetime_cutoff_issues(
                        artifact_id, frame, field, as_of, "protocol_metric_after_as_of"
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
