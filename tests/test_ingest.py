"""Tests for the streaming FWF ingest layer."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from microtrade.discover import RawInput
from microtrade.excel_spec import read_workbook
from microtrade.ingest import (
    IngestError,
    build_arrow_schema,
    iter_record_batches,
)
from tests._helpers import make_zip_input, render_fwf_lines


@pytest.fixture
def imports_spec(schema_workbook: Path):
    return read_workbook(schema_workbook, "2024-01")["imports"]


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
    col = {c.name: c for c in imports_spec.columns}["district_entry"]
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
    col = {c.name: c for c in imports_spec.columns}["value_usd"]
    blanked = (
        good_line[: col.start - 1] + (" " * col.length) + good_line[col.start - 1 + col.length :]
    )
    raw = _raw_input(tmp_path, [blanked])

    with pytest.raises(IngestError, match=r"value_usd.*non-nullable"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_garbage_in_numeric_raises_with_line_number(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=2, seed=0)[0]
    col = {c.name: c for c in imports_spec.columns}["value_usd"]
    garbage = (
        good_line[: col.start - 1] + "ABCDEABCDEABCDE" + good_line[col.start - 1 + col.length :]
    )
    raw = _raw_input(tmp_path, [good_line, garbage])

    with pytest.raises(IngestError, match=r"line 2.*cannot parse"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_short_line(imports_spec, tmp_path: Path) -> None:
    good_line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw = _raw_input(tmp_path, [good_line, good_line[:-5]])

    with pytest.raises(IngestError, match="expected record_length"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_zip_with_multiple_members(imports_spec, tmp_path: Path) -> None:
    import zipfile

    zip_path = tmp_path / "imports_202404.zip"
    line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("a.fwf", line + "\n")
        zf.writestr("b.fwf", line + "\n")
    raw = RawInput("imports", 2024, 4, zip_path)

    with pytest.raises(IngestError, match="exactly one inner file"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_trade_type_mismatch(imports_spec, tmp_path: Path) -> None:
    line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw_path = tmp_path / "exports_us_202404.zip"
    make_zip_input(raw_path, [line])
    raw = RawInput("exports_us", 2024, 4, raw_path)

    with pytest.raises(IngestError, match="trade_type mismatch"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))


def test_ingest_rejects_period_before_effective_from(imports_spec, tmp_path: Path) -> None:
    line = render_fwf_lines(imports_spec, n_rows=1, seed=0)[0]
    raw = _raw_input(tmp_path, [line], year=2023, month=12)

    with pytest.raises(IngestError, match="does not apply"):
        list(iter_record_batches(raw, imports_spec, chunk_rows=100))
