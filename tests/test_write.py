"""Tests for the atomic, idempotent Parquet PartitionWriter."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from microtrade.discover import RawInput
from microtrade.excel_spec import read_workbook
from microtrade.ingest import build_arrow_schema, iter_record_batches
from microtrade.write import PartitionWriter, WriteError
from tests._helpers import make_zip_input, render_fwf_lines


@pytest.fixture
def imports_spec(schema_workbook: Path, workbook_config):
    return read_workbook(schema_workbook, workbook_config)["imports"]


def _write_fixture_zip(tmp_path: Path, spec, n_rows: int = 10, month: int = 4) -> RawInput:
    lines = render_fwf_lines(spec, n_rows=n_rows, seed=month)
    zip_path = tmp_path / f"imports_2024{month:02d}.zip"
    make_zip_input(zip_path, lines)
    return RawInput("imports", 2024, month, zip_path)


def test_partition_writer_writes_hive_path(imports_spec, tmp_path: Path) -> None:
    raw = _write_fixture_zip(tmp_path, imports_spec, n_rows=5, month=4)
    dataset_root = tmp_path / "output"
    schema = build_arrow_schema(imports_spec)

    with PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w:
        for batch in iter_record_batches(raw, imports_spec, chunk_rows=2):
            w.write_batch(batch)
        assert w.rows_written == 5

    expected = dataset_root / "imports" / "year=2024" / "month=04" / "part-0.parquet"
    assert expected.exists()
    assert not expected.with_suffix(".parquet.tmp").exists()

    # Single-file read: only spec columns, no partition columns.
    file_schema = pq.ParquetFile(expected).schema_arrow
    assert file_schema.names == schema.names

    # Dataset read via Hive partitioning synthesizes year/month from the path.
    df = pl.scan_parquet(dataset_root / "imports", hive_partitioning=True).collect()
    assert df.height == 5
    assert df["year"].unique().to_list() == [2024]
    assert df["month"].unique().to_list() == [4]


def test_partition_writer_is_idempotent(imports_spec, tmp_path: Path) -> None:
    dataset_root = tmp_path / "output"
    schema = build_arrow_schema(imports_spec)

    # First pass: 3 rows.
    raw1 = _write_fixture_zip(tmp_path, imports_spec, n_rows=3, month=4)
    with PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w:
        for batch in iter_record_batches(raw1, imports_spec, chunk_rows=1000):
            w.write_batch(batch)

    first = pq.ParquetFile(
        dataset_root / "imports" / "year=2024" / "month=04" / "part-0.parquet"
    ).read()
    assert first.num_rows == 3

    # Second pass for the same partition: 5 rows. Must replace, not append.
    raw2 = _write_fixture_zip(tmp_path, imports_spec, n_rows=5, month=4)
    with PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w:
        for batch in iter_record_batches(raw2, imports_spec, chunk_rows=1000):
            w.write_batch(batch)

    second = pq.ParquetFile(
        dataset_root / "imports" / "year=2024" / "month=04" / "part-0.parquet"
    ).read()
    assert second.num_rows == 5


def test_partition_writer_cleans_up_on_exception(imports_spec, tmp_path: Path) -> None:
    dataset_root = tmp_path / "output"
    schema = build_arrow_schema(imports_spec)
    partition_dir = dataset_root / "imports" / "year=2024" / "month=04"

    with (
        pytest.raises(RuntimeError, match="boom"),
        PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w,
    ):
        batch = pa.record_batch([pa.array([], type=f.type) for f in schema], schema=schema)
        w.write_batch(batch)
        raise RuntimeError("boom")

    assert not (partition_dir / "part-0.parquet").exists()
    assert not (partition_dir / "part-0.parquet.tmp").exists()


def test_partition_writer_rejects_schema_mismatch(imports_spec, tmp_path: Path) -> None:
    dataset_root = tmp_path / "output"
    schema = build_arrow_schema(imports_spec)

    other_schema = pa.schema([pa.field("x", pa.int32())])
    bogus_batch = pa.record_batch([pa.array([1, 2, 3], type=pa.int32())], schema=other_schema)

    with (
        pytest.raises(WriteError, match="schema does not match"),
        PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w,
    ):
        w.write_batch(bogus_batch)


def test_partition_writer_rejects_use_without_context(imports_spec, tmp_path: Path) -> None:
    schema = build_arrow_schema(imports_spec)
    w = PartitionWriter(tmp_path / "output", "imports", 2024, 4, schema)

    empty = pa.record_batch([pa.array([], type=f.type) for f in schema], schema=schema)
    with pytest.raises(WriteError, match="not entered"):
        w.write_batch(empty)


def test_partition_writer_overwrites_stale_tmp(imports_spec, tmp_path: Path) -> None:
    """A leftover .tmp from a crashed prior run must not interfere."""
    dataset_root = tmp_path / "output"
    schema = build_arrow_schema(imports_spec)
    partition_dir = dataset_root / "imports" / "year=2024" / "month=04"
    partition_dir.mkdir(parents=True, exist_ok=True)
    stale = partition_dir / "part-0.parquet.tmp"
    stale.write_bytes(b"stale-bytes")

    raw = _write_fixture_zip(tmp_path, imports_spec, n_rows=2, month=4)
    with PartitionWriter(dataset_root, "imports", 2024, 4, schema) as w:
        for batch in iter_record_batches(raw, imports_spec, chunk_rows=100):
            w.write_batch(batch)

    final = partition_dir / "part-0.parquet"
    assert final.exists()
    assert pq.ParquetFile(final).read().num_rows == 2
    assert not stale.exists()
