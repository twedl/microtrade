"""Tests for the streaming FWF ingest layer."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from microtrade.discover import RawInput
from microtrade.excel_spec import read_workbook
from microtrade.ingest import (
    IngestError,
    QualityIssue,
    build_arrow_schema,
    iter_record_batches,
)
from microtrade.schema import Column, Spec
from tests._helpers import make_zip_input, render_fwf_lines


@pytest.fixture
def imports_spec(schema_workbook: Path, workbook_config):
    return read_workbook(schema_workbook, workbook_config)["imports"]


def _raw_input(tmp_path: Path, lines: list[str], *, year: int = 2024, month: int = 4) -> RawInput:
    zip_path = tmp_path / f"imports_{year}{month:02d}.zip"
    make_zip_input(zip_path, lines)
    return RawInput(trade_type="imports", year=year, month=month, path=zip_path)


def test_build_arrow_schema_contains_only_spec_columns(imports_spec) -> None:
    schema = build_arrow_schema(imports_spec)
    assert schema.names == [
        "period",
        "hs_code",
        "country_coo",
        "district_entry",
        "value_usd",
        "qty_kg",
    ]
    assert schema.field("value_usd").type == pa.int64()
    # Partition columns are encoded in the Hive path, not the parquet schema.
    assert "year" not in schema.names
    assert "month" not in schema.names


def test_ingest_round_trip_happy_path(imports_spec, tmp_path: Path) -> None:
    lines = render_fwf_lines(imports_spec, n_rows=7, seed=42)
    raw = _raw_input(tmp_path, lines)

    batches = list(iter_record_batches(raw, imports_spec, chunk_rows=1000))
    assert len(batches) == 1
    (batch,) = batches
    assert batch.num_rows == 7
    assert batch.schema == build_arrow_schema(imports_spec)

    # Numeric column casts to int64, strings strip trailing spaces.
    value_usd = batch.column("value_usd").to_pylist()
    assert all(isinstance(v, int) and v > 0 for v in value_usd)
    countries = batch.column("country_coo").to_pylist()
    assert all(v == v.strip() and len(v) <= 3 for v in countries)


def test_ingest_respects_chunk_rows(imports_spec, tmp_path: Path) -> None:
    lines = render_fwf_lines(imports_spec, n_rows=25, seed=1)
    raw = _raw_input(tmp_path, lines)

    batches = list(iter_record_batches(raw, imports_spec, chunk_rows=10))
    assert [b.num_rows for b in batches] == [10, 10, 5]
    assert sum(b.num_rows for b in batches) == 25


def test_ingest_nullable_blank_becomes_null(imports_spec, tmp_path: Path) -> None:
    # district_entry (col 4) is nullable; blank that column in one row.
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    col = {c.physical_name: c for c in imports_spec.columns}["district_entry"]
    blanked = (
        good_line[: col.start - 1] + (" " * col.length) + good_line[col.start - 1 + col.length :]
    )
    raw = _raw_input(tmp_path, [good_line, blanked])

    (batch,) = list(iter_record_batches(raw, imports_spec, chunk_rows=100))
    values = batch.column("district_entry").to_pylist()
    assert values[0] is not None
    assert values[1] is None


def test_ingest_blank_non_nullable_numeric_raises(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    col = {c.physical_name: c for c in imports_spec.columns}["value_usd"]
    blanked = (
        good_line[: col.start - 1] + (" " * col.length) + good_line[col.start - 1 + col.length :]
    )
    raw = _raw_input(tmp_path, [blanked])

    with pytest.raises(IngestError, match=r"value_usd.*non-nullable"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_garbage_in_numeric_raises_with_line_number(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=2, seed=0)[0]
    col = {c.physical_name: c for c in imports_spec.columns}["value_usd"]
    garbage = (
        good_line[: col.start - 1] + "ABCDEABCDEABCDE" + good_line[col.start - 1 + col.length :]
    )
    raw = _raw_input(tmp_path, [good_line, garbage])

    with pytest.raises(IngestError, match=r"line 2.*cannot parse"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_short_line(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw = _raw_input(tmp_path, [good_line, good_line[:-5]])

    with pytest.raises(IngestError, match="record truncated"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_tolerates_missing_trailing_filler(imports_spec, tmp_path: Path) -> None:
    """record_length is an upper bound. A record that's shorter than
    record_length but still covers every real column is valid - this is how
    datasets that omit optional trailing filler bytes ingest cleanly."""
    from dataclasses import replace

    padded_spec = replace(imports_spec, record_length=imports_spec.min_record_length + 3)
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw = _raw_input(tmp_path, [good_line])

    (batch,) = list(iter_record_batches(raw, padded_spec, chunk_rows=100))
    assert batch.num_rows == 1


def test_ingest_rejects_record_longer_than_record_length(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    too_long = good_line + "XXX"
    raw = _raw_input(tmp_path, [too_long])

    with pytest.raises(IngestError, match="longer than declared record_length"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_picks_largest_member_in_multi_member_zip(imports_spec, tmp_path: Path) -> None:
    """Multi-member zips: the largest member is the data file. Naming
    differences (case, extension, no extension) don't matter — small
    metadata/log siblings are skipped purely on size."""
    import zipfile

    zip_path = tmp_path / "imports_202404.zip"
    lines = render_fwf_lines(imports_spec, n_rows=50, seed=0)
    with zipfile.ZipFile(zip_path, "w") as zf:
        # Auxiliaries first to make sure order doesn't influence selection.
        zf.writestr("manifest.json", "{}\n")
        zf.writestr("delivery.log", "delivery notes\n")
        # Data file with a different stem and a lowercase ext — would have
        # missed any name-based rule.
        zf.writestr("imports_202404.txt", "\n".join(lines) + "\n")
    raw = RawInput("imports", 2024, 4, zip_path)

    batches = list(iter_record_batches(raw, imports_spec, chunk_rows=1000))
    assert sum(b.num_rows for b in batches) == 50


def test_ingest_raises_on_empty_zip(imports_spec, tmp_path: Path) -> None:
    import zipfile

    zip_path = tmp_path / "imports_202404.zip"
    with zipfile.ZipFile(zip_path, "w"):
        pass
    raw = RawInput("imports", 2024, 4, zip_path)

    with pytest.raises(IngestError, match="zip is empty"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_trade_type_mismatch(imports_spec, tmp_path: Path) -> None:
    line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw_path = tmp_path / "exports_us_202404.zip"
    make_zip_input(raw_path, [line])
    raw = RawInput("exports_us", 2024, 4, raw_path)

    with pytest.raises(IngestError, match="trade_type mismatch"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_period_before_effective_from(imports_spec, tmp_path: Path) -> None:
    # Rewrap the fixture spec as if its effective_from were 2024-01 so a 2023
    # input is explicitly out of window for this safety check.
    spec_2024 = Spec(
        trade_type=imports_spec.trade_type,
        version="2024-01",
        effective_from="2024-01",
        effective_to=imports_spec.effective_to,
        record_length=imports_spec.record_length,
        columns=imports_spec.columns,
        source=imports_spec.source,
        derived=imports_spec.derived,
        partition_by=imports_spec.partition_by,
    )
    line = render_fwf_lines(spec_2024, n_rows=1, seed=0)[0]
    raw = _raw_input(tmp_path, [line], year=2023, month=12)

    with pytest.raises(IngestError, match="does not apply"):
        list(iter_record_batches(raw, spec_2024, chunk_rows=100))


def test_ingest_date_column_parses_yyyymmdd(tmp_path: Path) -> None:
    spec = Spec(
        trade_type="imports",
        version="2024-01",
        effective_from="2024-01",
        record_length=13,
        columns=(
            Column(physical_name="ref", start=1, length=5, dtype="Utf8", nullable=False),
            Column(
                physical_name="entry_date",
                start=6,
                length=8,
                dtype="Date",
                nullable=False,
                parse="yyyymmdd_to_date",
            ),
        ),
    )
    line1 = "AAAAA" + "20240115"
    line2 = "BBBBB" + "20240229"
    raw = _raw_input(tmp_path, [line1, line2])

    (batch,) = list(iter_record_batches(raw, spec, chunk_rows=100))
    assert batch.schema.field("entry_date").type == pa.date32()
    assert batch.column("entry_date").to_pylist() == [date(2024, 1, 15), date(2024, 2, 29)]


def test_ingest_sink_captures_bad_row_and_continues(imports_spec, tmp_path: Path) -> None:
    good = render_fwf_lines(imports_spec, n_rows=3, seed=0)
    col = {c.physical_name: c for c in imports_spec.columns}["value_usd"]
    bad = good[0][: col.start - 1] + "ABCDEABCDEABCDE" + good[0][col.start - 1 + col.length :]
    raw = _raw_input(tmp_path, [good[0], bad, good[1]])

    captured: list[QualityIssue] = []
    (batch,) = list(
        iter_record_batches(raw, imports_spec, chunk_rows=100, on_quality_issue=captured.append)
    )
    assert batch.num_rows == 2
    assert len(captured) == 1
    issue = captured[0]
    assert issue.line_no == 2
    assert issue.column == "value_usd"
    assert "cannot parse" in issue.error
    assert issue.file == raw.path.name


def test_ingest_sink_captures_blank_non_nullable(imports_spec, tmp_path: Path) -> None:
    good = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    col = {c.physical_name: c for c in imports_spec.columns}["value_usd"]
    blanked = good[: col.start - 1] + (" " * col.length) + good[col.start - 1 + col.length :]
    raw = _raw_input(tmp_path, [blanked, good])

    captured: list[QualityIssue] = []
    (batch,) = list(
        iter_record_batches(raw, imports_spec, chunk_rows=100, on_quality_issue=captured.append)
    )
    assert batch.num_rows == 1
    assert len(captured) == 1
    assert captured[0].column == "value_usd"
    assert "non-nullable" in captured[0].error
