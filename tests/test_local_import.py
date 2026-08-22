"""Golden local-import projections and strict parsing coverage."""

from __future__ import annotations

import csv
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from typer.testing import CliRunner

import finresearch.local_import as local_import_module
from finresearch.cases import initialize_case, read_manifest
from finresearch.cli import app
from finresearch.data_contracts import (
    DERIVED_INSTRUMENT_MASTER_CURRENT_V1,
    DataContractError,
)
from finresearch.ingestion import ArtifactIntegrityError, IngestionError
from finresearch.local_import import (
    IMPORT_SCHEMAS,
    LocalImportReceipt,
    LocalImportSchema,
    import_csv,
    import_parquet,
)
from finresearch.normalization import reconcile_instrument_master

RETRIEVED_AT = datetime(2026, 8, 11, 4, 0, tzinfo=UTC)
runner = CliRunner()


def _value(name: str, dtype: pl.DataType) -> object | None:
    strings = {
        "instrument_id": "provider.aapl",
        "provider_symbol": "AAPL",
        "primary_symbol": "AAPL",
        "name": "Apple Inc.",
        "asset_class": "equity",
        "instrument_type": "common-stock",
        "venue_mic": "XNAS",
        "country_code": "US",
        "trading_currency": "USD",
        "currency": "USD",
        "price_basis": "unadjusted",
        "provider_timezone": "America/New_York",
        "fact_id": "not-generated",
        "entity_id": "0000320193",
        "cik": "0000320193",
        "taxonomy": "us-gaap",
        "concept": "Revenues",
        "metric_id": "us-gaap:Revenues",
        "category": "income-statement",
        "canonical_metric": "revenue",
        "label": "Revenue",
        "unit": "USD",
        "unit_kind": "currency",
        "value_text": "100.0",
        "period_type": "instant",
        "fiscal_period": "FY",
        "form": "10-K",
        "accession_number": "0000320193-26-000001",
        "frame": "CY2026Q1",
        "action_type": "dividend",
        "rate_kind": "spot",
        "base_currency": "USD",
        "quote_currency": "EUR",
        "company_id": "target",
        "company_name": "Target Co.",
        "role": "target",
        "metric": "revenue",
        "period_basis": "LTM",
        "source_id": "evidence-001",
    }
    if name in {"cash_amount"}:
        return 1.0
    if name in {"ratio"}:
        return None
    if name == "low":
        return 99.0
    if name == "high":
        return 102.0
    if name in {"open", "close"}:
        return 100.0
    if name in {"volume"}:
        return 0
    if name in {"dividends", "stock_splits"}:
        return 0.0
    if name in {"value", "rate"}:
        return 1.0
    if name in {"valid_from", "valid_to", "start_date"}:
        return None
    if dtype == pl.String:
        return strings.get(name, "value")
    if dtype == pl.Date:
        return date(2026, 1, 2)
    if dtype == pl.Datetime("us", "UTC"):
        return RETRIEVED_AT
    if dtype == pl.Int32:
        return 2026
    if dtype == pl.Int64:
        return 1
    if dtype == pl.Float64:
        return 1.0
    raise AssertionError(f"missing fixture value for {name}: {dtype}")


def source_frame(spec: LocalImportSchema) -> pl.DataFrame:
    return pl.DataFrame(
        [{name: _value(name, dtype) for name, dtype in spec.input_schema.items()}],
        schema=spec.input_schema,
    )


def write_csv(path: Path, frame: pl.DataFrame) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(frame.columns)
        for values in frame.iter_rows():
            row: list[str] = []
            for value in values:
                if value is None:
                    row.append("")
                elif isinstance(value, datetime):
                    row.append(value.isoformat().replace("+00:00", "Z"))
                elif isinstance(value, date):
                    row.append(value.isoformat())
                else:
                    row.append(str(value))
            writer.writerow(row)


def _seeded_import_outputs(
    tmp_path: Path,
) -> tuple[Path, Path, LocalImportReceipt]:
    """Produce expected immutable import bytes without declaring them in target."""
    source = tmp_path / "orphan-source.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["daily-prices.v2"]))
    seed_workspace = tmp_path / "seed"
    initialize_case(seed_workspace, "aapl")
    receipt = import_csv(
        seed_workspace,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    return source, seed_workspace / "cases" / "aapl", receipt


@pytest.mark.parametrize(
    "seed_raw, seed_normalized",
    [(True, False), (False, True), (True, True)],
)
def test_import_recovers_matching_undeclared_orphans(
    tmp_path: Path,
    seed_raw: bool,
    seed_normalized: bool,
) -> None:
    source, seed_case, expected = _seeded_import_outputs(tmp_path)
    target = tmp_path / "target"
    target_case = initialize_case(target, "aapl")
    for enabled, receipt in (
        (seed_raw, expected.raw),
        (seed_normalized, expected.normalized),
    ):
        if enabled:
            output = target_case / receipt.path.relative_to(seed_case)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(receipt.path.read_bytes())

    recovered = import_csv(
        target,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )

    assert recovered.raw.sha256 == expected.raw.sha256
    assert recovered.normalized.sha256 == expected.normalized.sha256
    assert len(read_manifest(target_case).artifacts) == 2


def test_import_rejects_mismatched_undeclared_orphan_without_mutation(
    tmp_path: Path,
) -> None:
    source, seed_case, expected = _seeded_import_outputs(tmp_path)
    target = tmp_path / "target"
    target_case = initialize_case(target, "aapl")
    orphan = target_case / expected.raw.path.relative_to(seed_case)
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"mismatched orphan")
    manifest_before = (target_case / "manifest.toml").read_bytes()
    orphan_before = orphan.read_bytes()

    with pytest.raises(
        ArtifactIntegrityError,
        match="raw orphan has conflicting bytes",
    ):
        import_csv(
            target,
            "aapl",
            source,
            schema_name="daily-prices.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )

    assert (target_case / "manifest.toml").read_bytes() == manifest_before
    assert orphan.read_bytes() == orphan_before
    assert not (target_case / expected.normalized.path.relative_to(seed_case)).exists()


@pytest.mark.parametrize("schema_name", sorted(IMPORT_SCHEMAS))
@pytest.mark.parametrize("source_format", ["csv", "parquet"])
def test_import_roundtrip_for_every_schema(
    tmp_path: Path,
    schema_name: str,
    source_format: str,
) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS[schema_name]
    source = tmp_path / f"golden.{source_format}"
    frame = source_frame(spec)
    if source_format == "csv":
        write_csv(source, frame)
        receipt = import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name=schema_name,
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    else:
        frame.write_parquet(source)
        receipt = import_parquet(
            tmp_path,
            "aapl",
            source,
            schema_name=schema_name,
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    assert receipt.raw.path.read_bytes() == source.read_bytes()
    output = pl.read_parquet(receipt.normalized.path)
    spec.contract.validate(output)
    assert output.equals(spec.contract.canonical_sort(output))
    assert (
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name=schema_name,
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
        == receipt
        if source_format == "csv"
        else import_parquet(
            tmp_path,
            "aapl",
            source,
            schema_name=schema_name,
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
        == receipt
    )
    assert len(read_manifest(tmp_path / "cases" / "aapl").artifacts) == 2


def test_import_csv_rejects_extra_header_and_null_alias(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["daily-prices.v2"]
    source = tmp_path / "bad.csv"
    write_csv(source, source_frame(spec))
    text = source.read_text(encoding="utf-8")
    source.write_text("extra," + text, encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "--workspace",
            str(tmp_path),
            "data",
            "import-csv",
            "aapl",
            str(source),
            "--schema",
            "daily-prices.v2",
            "--provider",
            "manual",
            "--retrieved-at",
            "2026-08-11T04:00:00Z",
        ],
    )
    assert result.exit_code == 1
    assert "CSV header" in result.output


def test_import_failure_rolls_back_both_new_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_case(tmp_path, "aapl")
    source = tmp_path / "prices.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["daily-prices.v2"]))

    def fail_manifest(*args: object, **kwargs: object) -> None:
        raise OSError("injected manifest interruption")

    monkeypatch.setattr(local_import_module, "write_manifest", fail_manifest)
    with pytest.raises(OSError, match="injected manifest interruption"):
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name="daily-prices.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )

    manifest = read_manifest(tmp_path / "cases" / "aapl")
    assert manifest.artifacts == ()
    assert not list((tmp_path / "cases" / "aapl" / "data").rglob("*.source"))
    assert not list((tmp_path / "cases" / "aapl" / "data").rglob("*.parquet"))


def test_import_parses_the_hashed_bytes_when_source_changes_mid_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["daily-prices.v2"]
    source = tmp_path / "prices.csv"
    original = source_frame(spec)
    write_csv(source, original)
    original_bytes = source.read_bytes()
    changed = original.with_columns(pl.lit(200.0).alias("close"))
    changed_source = tmp_path / "changed.csv"
    write_csv(changed_source, changed)
    parse = local_import_module._read_csv_projection

    def mutate_after_read(
        source_bytes: bytes, schema: LocalImportSchema
    ) -> pl.DataFrame:
        source.write_bytes(changed_source.read_bytes())
        return parse(source_bytes, schema)

    monkeypatch.setattr(local_import_module, "_read_csv_projection", mutate_after_read)
    receipt = import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )

    assert receipt.raw.path.read_bytes() == original_bytes
    assert pl.read_parquet(receipt.normalized.path).get_column("close").to_list() == [
        100.0
    ]


@pytest.mark.parametrize("interrupted_suffix", [".source", ".parquet"])
def test_import_link_window_interrupt_rolls_back_and_reruns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_suffix: str,
) -> None:
    initialize_case(tmp_path, "aapl")
    source = tmp_path / "prices.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["daily-prices.v2"]))
    original_link = os.link

    def link_then_interrupt(source_path: Any, target_path: Any) -> None:
        original_link(source_path, target_path)
        if str(target_path).endswith(interrupted_suffix):
            raise KeyboardInterrupt("injected link window")

    monkeypatch.setattr(os, "link", link_then_interrupt)
    with pytest.raises(KeyboardInterrupt, match="injected link window"):
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name="daily-prices.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    assert read_manifest(tmp_path / "cases" / "aapl").artifacts == ()
    assert not list((tmp_path / "cases" / "aapl" / "data").rglob("*.source"))
    assert not list((tmp_path / "cases" / "aapl" / "data").rglob("*.parquet"))

    monkeypatch.setattr(os, "link", original_link)
    receipt = import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    assert receipt.raw.path.is_file()
    assert receipt.normalized.path.is_file()


def test_import_producer_version_is_part_of_both_artifact_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_case(tmp_path, "aapl")
    source = tmp_path / "prices.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["daily-prices.v2"]))
    first = import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    monkeypatch.setattr(local_import_module, "IMPORT_PRODUCER_VERSION", "2")
    second = import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )

    assert first.raw.artifact_id != second.raw.artifact_id
    assert first.normalized.artifact_id != second.normalized.artifact_id
    manifest = read_manifest(tmp_path / "cases" / "aapl")
    assert len(manifest.artifacts) == 4
    assert {artifact.producer_version for artifact in manifest.artifacts} == {"1", "2"}


def test_currency_validation_matches_master_across_separate_import_lineage(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    master_source = tmp_path / "master.csv"
    daily_source = tmp_path / "daily.csv"
    write_csv(master_source, source_frame(IMPORT_SCHEMAS["instrument-master.v2"]))
    daily = source_frame(IMPORT_SCHEMAS["daily-prices.v2"]).with_columns(
        pl.lit("EUR").alias("currency")
    )
    write_csv(daily_source, daily)
    import_csv(
        tmp_path,
        "aapl",
        master_source,
        schema_name="instrument-master.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    import_csv(
        tmp_path,
        "aapl",
        daily_source,
        schema_name="daily-prices.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )

    result = runner.invoke(
        app, ["--workspace", str(tmp_path), "data", "validate", "aapl"]
    )
    assert result.exit_code == 1
    assert "[currency_mismatch]" in result.output
    assert "manual:provider.aapl" in result.output
    assert "expected 'USD', observed ['EUR']" in result.output


def test_currency_validation_checks_each_key_and_skips_null_action_currency(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    master_spec = IMPORT_SCHEMAS["instrument-master.v2"]
    first_master = source_frame(master_spec)
    second_master = first_master.with_columns(
        pl.lit("provider.msft").alias("instrument_id"),
        pl.lit("MSFT").alias("provider_symbol"),
        pl.lit("EUR").alias("trading_currency"),
    )
    masters = pl.concat([first_master, second_master])
    daily_spec = IMPORT_SCHEMAS["daily-prices.v2"]
    first_daily = source_frame(daily_spec)
    second_daily = first_daily.with_columns(
        pl.lit("provider.msft").alias("instrument_id"),
        pl.lit("MSFT").alias("provider_symbol"),
        pl.lit("USD").alias("currency"),
    )
    dailies = pl.concat([first_daily, second_daily])
    actions = source_frame(IMPORT_SCHEMAS["corporate-actions.v1"]).with_columns(
        pl.lit(None, dtype=pl.String).alias("currency")
    )
    for filename, frame, schema_name in (
        ("masters.csv", masters, "instrument-master.v2"),
        ("dailies.csv", dailies, "daily-prices.v2"),
        ("actions.csv", actions, "corporate-actions.v1"),
    ):
        source = tmp_path / filename
        write_csv(source, frame)
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name=schema_name,
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )

    result = runner.invoke(
        app, ["--workspace", str(tmp_path), "data", "validate", "aapl"]
    )
    assert result.exit_code == 1
    assert result.output.count("[currency_mismatch]") == 1
    assert "manual:provider.msft" in result.output


def test_estimates_identity_key_allows_distinct_entities_but_rejects_duplicates(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["estimates.v1"]
    first = source_frame(spec)
    second = first.with_columns(
        pl.lit("0000789019").alias("entity_id"),
        pl.lit("provider.msft").alias("instrument_id"),
    )
    multi_source = tmp_path / "multi-estimates.csv"
    write_csv(multi_source, pl.concat([first, second]))
    receipt = import_csv(
        tmp_path,
        "aapl",
        multi_source,
        schema_name="estimates.v1",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    assert receipt.normalized.row_count == 2

    duplicate_source = tmp_path / "duplicate-estimates.csv"
    write_csv(duplicate_source, pl.concat([first, first]))
    with pytest.raises(IngestionError, match="duplicate rows"):
        import_csv(
            tmp_path,
            "aapl",
            duplicate_source,
            schema_name="estimates.v1",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )


def test_reconciled_multi_source_rows_validate_and_undeclared_source_fails(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["instrument-master.v2"]
    first = source_frame(spec)
    second = first.with_columns(
        pl.lit("provider.msft").alias("instrument_id"),
        pl.lit("MSFT").alias("provider_symbol"),
    )
    for filename, frame in (("aapl-master.csv", first), ("msft-master.csv", second)):
        source = tmp_path / filename
        write_csv(source, frame)
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name="instrument-master.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    receipt = reconcile_instrument_master(
        tmp_path,
        "aapl",
        as_of=date(2026, 8, 11),
    )
    reconciled = pl.read_parquet(receipt.path)
    assert reconciled.height == 2
    assert reconciled.get_column("schema_version").unique().to_list() == [1]
    valid = runner.invoke(
        app,
        ["--workspace", str(tmp_path), "data", "validate", "aapl", receipt.artifact_id],
    )
    assert valid.exit_code == 0, valid.output

    tampered = pl.read_parquet(receipt.path).with_columns(
        pl.when(pl.col("instrument_id") == "provider.msft")
        .then(pl.lit("raw.undeclared"))
        .otherwise(pl.col("source_artifact_id"))
        .alias("source_artifact_id")
    )
    tampered.write_parquet(receipt.path)
    invalid = runner.invoke(
        app,
        ["--workspace", str(tmp_path), "data", "validate", "aapl", receipt.artifact_id],
    )
    assert invalid.exit_code == 1
    assert "[lineage_invalid]" in invalid.output
    assert "['raw.undeclared'] are not in manifest input_artifact_ids" in invalid.output


def test_contract_rejects_wrong_schema_version(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    source = tmp_path / "master.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["instrument-master.v2"]))
    receipt = import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="instrument-master.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    output = pl.read_parquet(receipt.normalized.path).with_columns(
        pl.lit(1, dtype=pl.UInt16).alias("schema_version")
    )

    with pytest.raises(DataContractError, match="schema_version other than 2"):
        IMPORT_SCHEMAS["instrument-master.v2"].contract.validate(output)


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("valid_from", date(2026, 8, 12), "valid_from must not be after as_of"),
        ("valid_to", date(2026, 8, 10), "valid_to must not be before as_of"),
    ],
)
def test_current_master_contract_enforces_as_of_validity(
    tmp_path: Path,
    column: str,
    value: date,
    message: str,
) -> None:
    initialize_case(tmp_path, "aapl")
    source = tmp_path / "master.csv"
    write_csv(source, source_frame(IMPORT_SCHEMAS["instrument-master.v2"]))
    import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="instrument-master.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    receipt = reconcile_instrument_master(
        tmp_path,
        "aapl",
        as_of=date(2026, 8, 11),
    )
    invalid = pl.read_parquet(receipt.path).with_columns(pl.lit(value).alias(column))

    with pytest.raises(DataContractError, match=message):
        DERIVED_INSTRUMENT_MASTER_CURRENT_V1.validate(invalid)


def test_reconcile_enforces_as_of_and_validity_ranges(tmp_path: Path) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["instrument-master.v2"]
    valid = source_frame(spec).with_columns(
        pl.lit(datetime(2026, 8, 10, tzinfo=UTC)).alias("observed_at")
    )
    future_observed = valid.with_columns(
        pl.lit("provider.future-observed").alias("instrument_id"),
        pl.lit(datetime(2026, 8, 12, tzinfo=UTC)).alias("observed_at"),
    )
    not_yet_valid = valid.with_columns(
        pl.lit("provider.not-yet").alias("instrument_id"),
        pl.lit(date(2026, 8, 12)).alias("valid_from"),
    )
    expired = valid.with_columns(
        pl.lit("provider.expired").alias("instrument_id"),
        pl.lit(date(2026, 8, 10)).alias("valid_to"),
    )
    source = tmp_path / "validity.csv"
    write_csv(source, pl.concat([valid, future_observed, not_yet_valid, expired]))
    import_csv(
        tmp_path,
        "aapl",
        source,
        schema_name="instrument-master.v2",
        provider="manual",
        retrieved_at=RETRIEVED_AT,
    )
    receipt = reconcile_instrument_master(
        tmp_path,
        "aapl",
        as_of=date(2026, 8, 11),
    )
    assert pl.read_parquet(receipt.path).get_column("instrument_id").to_list() == [
        "provider.aapl"
    ]


def test_reconcile_equal_time_conflict_and_sparse_merge_are_deterministic(
    tmp_path: Path,
) -> None:
    initialize_case(tmp_path, "aapl")
    spec = IMPORT_SCHEMAS["instrument-master.v2"]
    first = source_frame(spec)
    conflicting = first.with_columns(pl.lit("EUR").alias("trading_currency"))
    for filename, frame in (("first.csv", first), ("conflict.csv", conflicting)):
        source = tmp_path / filename
        write_csv(source, frame)
        import_csv(
            tmp_path,
            "aapl",
            source,
            schema_name="instrument-master.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    with pytest.raises(
        IngestionError, match="equal-time instrument-master conflict.*trading_currency"
    ):
        reconcile_instrument_master(tmp_path, "aapl", as_of=date(2026, 8, 11))

    clean = tmp_path / "clean"
    initialize_case(clean, "aapl")
    sparse = first.with_columns(pl.lit(None, dtype=pl.String).alias("name"))
    for filename, frame in (("named.csv", first), ("sparse.csv", sparse)):
        source = clean / filename
        write_csv(source, frame)
        import_csv(
            clean,
            "aapl",
            source,
            schema_name="instrument-master.v2",
            provider="manual",
            retrieved_at=RETRIEVED_AT,
        )
    first_receipt = reconcile_instrument_master(
        clean,
        "aapl",
        as_of=date(2026, 8, 11),
    )
    second_receipt = reconcile_instrument_master(
        clean,
        "aapl",
        as_of=date(2026, 8, 11),
    )
    merged = pl.read_parquet(first_receipt.path).row(0, named=True)
    assert first_receipt == second_receipt
    assert merged["name"] == "Apple Inc."
