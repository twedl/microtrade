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
from datetime import datetime
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
    # Precompute slices and per-column parser closures once per stream so the
    # per-row loop does a single indirect call + no dtype branching.
    n = len(columns_ordered)
    slices = [slice(c.start - 1, c.start - 1 + c.length) for c in columns_ordered]
    parsers = [_make_parser(c) for c in columns_ordered]
    col_names = [c.name for c in columns_ordered]
    field_types = [arrow_schema.field(i).type for i in range(n)]

    buffers: list[list[object]] = [[] for _ in range(n)]
    rows_in_batch = 0

    for line_no, raw_line in enumerate(text, start=1):
        line = raw_line.rstrip("\n").rstrip("\r")
        if len(line) != spec.record_length:
            raise IngestError(
                f"{raw.path.name} line {line_no}: expected record_length "
                f"{spec.record_length}, got {len(line)}"
            )

        row_values: list[object] = [None] * n
        bad_column: str | None = None
        bad_error: _CastError | None = None
        for i in range(n):
            try:
                row_values[i] = parsers[i](line[slices[i]])
            except _CastError as exc:
                bad_column = col_names[i]
                bad_error = exc
                break

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
            yield _build_batch(buffers, arrow_schema, field_types)
            buffers = [[] for _ in range(n)]
            rows_in_batch = 0

    if rows_in_batch > 0:
        yield _build_batch(buffers, arrow_schema, field_types)


def _make_parser(col: Column) -> Callable[[str], object]:
    """Return a closure that casts a single FWF substring to `col`'s dtype.

    All dtype-specific branching happens here, once per column, rather than on
    every value during streaming. Closures raise `_CastError` on failure; the
    caller decides whether to log-and-skip or re-raise as `IngestError`.
    """
    nullable = col.nullable

    if col.dtype == "Utf8":

        def parse_utf8(raw_value: str) -> object:
            v = raw_value.rstrip()
            if not v:
                if not nullable:
                    raise _CastError("blank value in non-nullable column")
                return None
            return v

        return parse_utf8

    if col.dtype == "Int64":

        def parse_int(raw_value: str) -> object:
            v = raw_value.strip()
            if not v:
                if not nullable:
                    raise _CastError("blank value in non-nullable Int64 column")
                return None
            try:
                return int(v)
            except ValueError as exc:
                raise _CastError(f"cannot parse {v!r} as Int64") from exc

        return parse_int

    if col.dtype == "Float64":

        def parse_float(raw_value: str) -> object:
            v = raw_value.strip()
            if not v:
                if not nullable:
                    raise _CastError("blank value in non-nullable Float64 column")
                return None
            try:
                return float(v)
            except ValueError as exc:
                raise _CastError(f"cannot parse {v!r} as Float64") from exc

        return parse_float

    if col.dtype == "Date":
        parse_name = col.parse or "yyyymmdd_to_date"
        fmt = _DATE_FORMATS.get(parse_name)
        if fmt is None:
            raise IngestError(
                f"column {col.name!r}: unknown Date parse {parse_name!r}; "
                f"known: {sorted(_DATE_FORMATS)}"
            )

        def parse_date(raw_value: str) -> object:
            v = raw_value.strip()
            if not v:
                if not nullable:
                    raise _CastError("blank value in non-nullable Date column")
                return None
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError as exc:
                raise _CastError(f"cannot parse {v!r} as Date ({parse_name})") from exc

        return parse_date

    raise IngestError(f"column {col.name!r}: unsupported dtype {col.dtype!r}")


def _build_batch(
    buffers: list[list[object]],
    arrow_schema: pa.Schema,
    field_types: list[pa.DataType],
) -> pa.RecordBatch:
    arrays = [pa.array(buf, type=field_types[i]) for i, buf in enumerate(buffers)]
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
