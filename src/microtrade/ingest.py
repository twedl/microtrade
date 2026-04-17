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
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
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
    "Date": pa.date32(),
}

_DATE_FORMATS: dict[str, str] = {
    "yyyymmdd_to_date": "%Y%m%d",
    "yyyymm_to_date": "%Y%m",
}


class IngestError(RuntimeError):
    """Raised when a raw file is structurally incompatible with its spec."""


class _CastError(ValueError):
    """Internal: a single value could not be coerced to its column dtype."""


@dataclass(frozen=True)
class QualityIssue:
    """One offending row that could not be ingested; logged and skipped."""

    file: str
    line_no: int
    column: str | None
    error: str
    raw_line: str


QualityIssueSink = Callable[[QualityIssue], None]


def build_arrow_schema(spec: Spec) -> pa.Schema:
    """Return the pyarrow schema that RecordBatches from this spec will use.

    Partition columns (`year`, `month`) are intentionally not included - they
    live in the Hive directory path only.
    """
    fields: list[pa.Field] = []
    for col in spec.ordered_columns:
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
    on_quality_issue: QualityIssueSink | None = None,
) -> Iterator[pa.RecordBatch]:
    """Yield pyarrow RecordBatches of up to `chunk_rows` rows streamed from `raw.path`.

    Row-level parse failures (bad numeric, blank non-nullable, bad date) are
    reported to `on_quality_issue` and the row is skipped. When the sink is
    `None` the row error is raised as `IngestError` (legacy behavior for
    direct callers that want fail-fast). Structural errors (unknown dtype,
    wrong record length, multiple zip members, trade_type mismatch) always
    raise regardless.
    """
    if chunk_rows <= 0:
        raise ValueError(f"chunk_rows must be positive, got {chunk_rows}")
    if raw.trade_type != spec.trade_type:
        raise IngestError(
            f"trade_type mismatch: input is {raw.trade_type!r}, spec is {spec.trade_type!r}"
        )
    if raw.period < spec.effective_from:
        raise IngestError(f"spec v{spec.effective_from} does not apply to period {raw.period}")

    columns_ordered = list(spec.ordered_columns)
    arrow_schema = build_arrow_schema(spec)

    with zipfile.ZipFile(raw.path) as zf:
        members = [info for info in zf.infolist() if not info.is_dir()]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise IngestError(f"{raw.path.name}: expected exactly one inner file, found {names}")
        member = members[0]
        with zf.open(member) as binstream:
            yield from _stream_lines(
                binstream,
                raw,
                spec,
                columns_ordered,
                arrow_schema,
                chunk_rows,
                encoding,
                on_quality_issue,
            )


def _stream_lines(
    binstream: IO[bytes],
    raw: RawInput,
    spec: Spec,
    columns_ordered: list[Column],
    arrow_schema: pa.Schema,
    chunk_rows: int,
    encoding: str,
    on_quality_issue: QualityIssueSink | None,
) -> Iterator[pa.RecordBatch]:
    text = io.TextIOWrapper(binstream, encoding=encoding, newline="")
    # Precompute field slice bounds once so the per-row loop is pure indexing.
    slices = [slice(c.start - 1, c.start - 1 + c.length) for c in columns_ordered]
    buffers: list[list[object]] = [[] for _ in columns_ordered]
    rows_in_batch = 0

    for line_no, raw_line in enumerate(text, start=1):
        line = raw_line.rstrip("\n").rstrip("\r")
        if len(line) != spec.record_length:
            raise IngestError(
                f"{raw.path.name} line {line_no}: expected record_length "
                f"{spec.record_length}, got {len(line)}"
            )

        row_values, bad_column, bad_error = _parse_row(line, columns_ordered, slices)
        if bad_error is not None:
            if on_quality_issue is None:
                raise IngestError(_row_msg(raw, line_no, bad_column, str(bad_error)))
            on_quality_issue(
                QualityIssue(
                    file=raw.path.name,
                    line_no=line_no,
                    column=bad_column,
                    error=str(bad_error),
                    raw_line=line,
                )
            )
            continue

        for buf, val in zip(buffers, row_values, strict=True):
            buf.append(val)
        rows_in_batch += 1

        if rows_in_batch >= chunk_rows:
            yield _build_batch(buffers, arrow_schema)
            buffers = [[] for _ in columns_ordered]
            rows_in_batch = 0

    if rows_in_batch > 0:
        yield _build_batch(buffers, arrow_schema)


def _parse_row(
    line: str, columns_ordered: list[Column], slices: list[slice]
) -> tuple[list[object], str | None, _CastError | None]:
    values: list[object] = []
    for col, sl in zip(columns_ordered, slices, strict=True):
        try:
            values.append(_parse_value(line[sl], col))
        except _CastError as exc:
            return values, col.name, exc
    return values, None, None


def _parse_value(raw_value: str, col: Column) -> object:
    if col.dtype == "Utf8":
        v_clean = raw_value.rstrip()
        if not v_clean:
            if not col.nullable:
                raise _CastError("blank value in non-nullable column")
            return None
        return v_clean

    v_clean = raw_value.strip()
    if not v_clean:
        if not col.nullable:
            raise _CastError(f"blank value in non-nullable {col.dtype} column")
        return None

    if col.dtype == "Int64":
        try:
            return int(v_clean)
        except ValueError as exc:
            raise _CastError(f"cannot parse {v_clean!r} as Int64") from exc
    if col.dtype == "Float64":
        try:
            return float(v_clean)
        except ValueError as exc:
            raise _CastError(f"cannot parse {v_clean!r} as Float64") from exc
    if col.dtype == "Date":
        return _parse_date(v_clean, col)
    raise _CastError(f"unsupported dtype {col.dtype!r}")


def _parse_date(v_clean: str, col: Column) -> date:
    parse = col.parse or "yyyymmdd_to_date"
    fmt = _DATE_FORMATS.get(parse)
    if fmt is None:
        raise _CastError(f"unknown parse {parse!r} for Date column")
    try:
        return datetime.strptime(v_clean, fmt).date()
    except ValueError as exc:
        raise _CastError(f"cannot parse {v_clean!r} as Date ({parse})") from exc


def _build_batch(buffers: list[list[object]], arrow_schema: pa.Schema) -> pa.RecordBatch:
    arrays = [pa.array(buf, type=arrow_schema.field(i).type) for i, buf in enumerate(buffers)]
    return pa.record_batch(arrays, schema=arrow_schema)


def _row_msg(raw: RawInput, line_no: int, column: str | None, detail: str) -> str:
    col_part = f" column {column!r}" if column is not None else ""
    return f"{raw.path.name} line {line_no}{col_part}: {detail}"


def iter_record_batches_from_path(
    path: Path,
    spec: Spec,
    *,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    encoding: str = "utf-8",
    on_quality_issue: QualityIssueSink | None = None,
) -> Iterator[pa.RecordBatch]:
    """Convenience wrapper for tests: parse the filename to a RawInput then ingest."""
    from microtrade.discover import parse_filename

    raw = parse_filename(path)
    if raw is None:
        raise IngestError(f"{path.name}: filename does not match the expected pattern")
    yield from iter_record_batches(
        raw,
        spec,
        chunk_rows=chunk_rows,
        encoding=encoding,
        on_quality_issue=on_quality_issue,
    )
