"""Stream FWF records out of a raw trade zip as pyarrow RecordBatches.

The zip is opened read-only and decompressed on the fly via
`zipfile.ZipFile.open()`; we never extract the inner file to disk. Each line is
sliced by the resolved spec's `(start, length)` columns, cast to the canonical
dtype, and accumulated into per-column buffers; every `chunk_rows` lines we emit
a `pyarrow.RecordBatch` so memory stays bounded regardless of input size.

Emitted batches contain only the spec's columns. The `year`/`month` partition
values come from the raw filename (via `discover.RawInput`) and are encoded in
the Hive-partitioned output path by `PartitionWriter` - storing them *inside*
each parquet file would collide with `hive_partitioning=True` at read time.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import IO

import pyarrow as pa

from microtrade.discover import RawInput
from microtrade.schema import Column, Spec

DEFAULT_CHUNK_ROWS: int = 250_000

_TO_PYARROW: dict[str, pa.DataType] = {
    "Utf8": pa.string(),
    "Int64": pa.int64(),
    "Float64": pa.float64(),
}


class IngestError(RuntimeError):
    """Raised when a raw file is structurally incompatible with its spec."""


def build_arrow_schema(spec: Spec) -> pa.Schema:
    """Return the pyarrow schema that RecordBatches from this spec will use.

    Partition columns (`year`, `month`) are intentionally not included - they
    live in the Hive directory path only.
    """
    fields: list[pa.Field] = []
    for col in sorted(spec.columns, key=lambda c: c.start):
        if col.dtype not in _TO_PYARROW:
            raise IngestError(
                f"column {col.name!r} has dtype {col.dtype!r} with no pyarrow mapping"
            )
        fields.append(pa.field(col.name, _TO_PYARROW[col.dtype], nullable=col.nullable))
    return pa.schema(fields)


def iter_record_batches(
    raw: RawInput,
    spec: Spec,
    *,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    encoding: str = "utf-8",
) -> Iterator[pa.RecordBatch]:
    """Yield pyarrow RecordBatches of up to `chunk_rows` rows streamed from `raw.path`."""
    if chunk_rows <= 0:
        raise ValueError(f"chunk_rows must be positive, got {chunk_rows}")
    if raw.trade_type != spec.trade_type:
        raise IngestError(
            f"trade_type mismatch: input is {raw.trade_type!r}, spec is {spec.trade_type!r}"
        )
    if raw.period < spec.effective_from:
        raise IngestError(f"spec v{spec.effective_from} does not apply to period {raw.period}")

    columns_ordered = sorted(spec.columns, key=lambda c: c.start)
    arrow_schema = build_arrow_schema(spec)

    with zipfile.ZipFile(raw.path) as zf:
        members = [info for info in zf.infolist() if not info.is_dir()]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise IngestError(f"{raw.path.name}: expected exactly one inner file, found {names}")
        member = members[0]
        with zf.open(member) as binstream:
            yield from _stream_lines(
                binstream, raw, spec, columns_ordered, arrow_schema, chunk_rows, encoding
            )


def _stream_lines(
    binstream: IO[bytes],
    raw: RawInput,
    spec: Spec,
    columns_ordered: list[Column],
    arrow_schema: pa.Schema,
    chunk_rows: int,
    encoding: str,
) -> Iterator[pa.RecordBatch]:
    text = io.TextIOWrapper(binstream, encoding=encoding, newline="")
    buffers: list[list[str]] = [[] for _ in columns_ordered]
    first_line_in_batch = 1
    line_no = 0
    rows_in_batch = 0

    for line_no, raw_line in enumerate(text, start=1):
        line = raw_line.rstrip("\n").rstrip("\r")
        if len(line) != spec.record_length:
            raise IngestError(
                f"{raw.path.name} line {line_no}: expected record_length "
                f"{spec.record_length}, got {len(line)}"
            )
        for buf, col in zip(buffers, columns_ordered, strict=True):
            buf.append(line[col.start - 1 : col.start - 1 + col.length])
        rows_in_batch += 1

        if rows_in_batch >= chunk_rows:
            yield _build_batch(
                buffers, columns_ordered, raw, arrow_schema, raw_start_line=first_line_in_batch
            )
            buffers = [[] for _ in columns_ordered]
            first_line_in_batch = line_no + 1
            rows_in_batch = 0

    if rows_in_batch > 0:
        yield _build_batch(
            buffers, columns_ordered, raw, arrow_schema, raw_start_line=first_line_in_batch
        )


def _build_batch(
    buffers: list[list[str]],
    columns_ordered: list[Column],
    raw: RawInput,
    arrow_schema: pa.Schema,
    *,
    raw_start_line: int,
) -> pa.RecordBatch:
    arrays: list[pa.Array] = [
        _cast_column(values, col, raw, raw_start_line)
        for values, col in zip(buffers, columns_ordered, strict=True)
    ]
    return pa.record_batch(arrays, schema=arrow_schema)


def _cast_column(values: list[str], col: Column, raw: RawInput, start_line: int) -> pa.Array:
    if col.dtype == "Utf8":
        return _cast_utf8(values, col, raw, start_line)
    if col.dtype == "Int64":
        return _cast_numeric(values, col, raw, start_line, parse=int, pa_type=pa.int64())
    if col.dtype == "Float64":
        return _cast_numeric(values, col, raw, start_line, parse=float, pa_type=pa.float64())
    raise IngestError(f"column {col.name!r}: unsupported dtype {col.dtype!r}")


def _cast_utf8(values: list[str], col: Column, raw: RawInput, start_line: int) -> pa.Array:
    out: list[str | None] = []
    for i, v in enumerate(values):
        v_clean = v.rstrip()
        if not v_clean:
            if not col.nullable:
                raise IngestError(
                    _row_msg(raw, start_line + i, col, "blank value in non-nullable column")
                )
            out.append(None)
        else:
            out.append(v_clean)
    return pa.array(out, type=pa.string())


def _cast_numeric(
    values: list[str],
    col: Column,
    raw: RawInput,
    start_line: int,
    *,
    parse: type,
    pa_type: pa.DataType,
) -> pa.Array:
    out: list[object] = []
    for i, v in enumerate(values):
        v_clean = v.strip()
        if not v_clean:
            if not col.nullable:
                raise IngestError(
                    _row_msg(raw, start_line + i, col, "blank value in non-nullable numeric column")
                )
            out.append(None)
            continue
        try:
            out.append(parse(v_clean))
        except ValueError as exc:
            raise IngestError(
                _row_msg(raw, start_line + i, col, f"cannot parse {v_clean!r} as {col.dtype}")
            ) from exc
    return pa.array(out, type=pa_type)


def _row_msg(raw: RawInput, line_no: int, col: Column, detail: str) -> str:
    return f"{raw.path.name} line {line_no} column {col.name!r}: {detail}"


def iter_record_batches_from_path(
    path: Path,
    spec: Spec,
    *,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    encoding: str = "utf-8",
) -> Iterator[pa.RecordBatch]:
    """Convenience wrapper for tests: parse the filename to a RawInput then ingest."""
    from microtrade.discover import parse_filename

    raw = parse_filename(path)
    if raw is None:
        raise IngestError(f"{path.name}: filename does not match the expected pattern")
    yield from iter_record_batches(raw, spec, chunk_rows=chunk_rows, encoding=encoding)
