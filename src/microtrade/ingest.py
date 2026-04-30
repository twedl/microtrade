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
import re
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import IO

import pyarrow as pa

from microtrade.discover import RawInput
from microtrade.schema import Column, ComputedColumn, Spec

DEFAULT_CHUNK_ROWS: int = 250_000

_TO_PYARROW: dict[str, pa.DataType] = {
    "Utf8": pa.string(),
    "Int64": pa.int64(),
    "Float64": pa.float64(),
    "Date": pa.date32(),
}

_WHITESPACE_RE = re.compile(r"\s+")

_DATE_FORMATS: dict[str, str] = {
    "yyyymmdd_to_date": "%Y%m%d",
    "yyyymm_to_date": "%Y%m",
}


class IngestError(RuntimeError):
    """Raised when a raw file is structurally incompatible with its spec."""


def _select_data_member(zf: zipfile.ZipFile, zip_path: Path) -> zipfile.ZipInfo:
    """Pick the data file from a possibly-multi-member zip.

    Convention: ``X.zip`` contains the data as ``X`` alongside any
    auxiliary metadata/log files. So:

    - empty zip -> raise
    - exactly one non-directory member -> use it
    - multiple members, exactly one named ``zip_path.name`` minus
      ``.zip`` -> use it (auxiliary members are ignored)
    - otherwise -> raise with the inner filename list so the user can
      see what was in the zip
    """
    members = [m for m in zf.infolist() if not m.is_dir()]
    if not members:
        raise IngestError(f"{zip_path.name}: zip is empty")
    if len(members) == 1:
        return members[0]
    expected = zip_path.name.removesuffix(".zip")
    matching = [m for m in members if m.filename == expected]
    if len(matching) == 1:
        return matching[0]
    names = [m.filename for m in members]
    raise IngestError(
        f"{zip_path.name}: cannot select data file from multi-member zip; "
        f"expected one matching {expected!r}, found {names}"
    )


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
    live in the Hive directory path only. Computed columns follow the FWF
    columns, in declaration order. Columns listed in `spec.dropped_columns`
    are omitted here but still sliced from the FWF in `_stream_lines`, so
    they can feed a computed column before disappearing.
    """
    dropped = set(spec.dropped_columns)
    fields: list[pa.Field] = []
    for col in spec.ordered_columns:
        if col.dtype not in _TO_PYARROW:
            raise IngestError(
                f"column {col.physical_name!r} has dtype {col.dtype!r} with no pyarrow mapping"
            )
        if col.effective_name in dropped:
            continue
        # Parquet field uses the logical name so merged datasets stay stable.
        fields.append(pa.field(col.effective_name, _TO_PYARROW[col.dtype], nullable=col.nullable))
    for comp in spec.computed_columns:
        if comp.dtype not in _TO_PYARROW:
            raise IngestError(
                f"computed column {comp.name!r} has dtype {comp.dtype!r} with no pyarrow mapping"
            )
        if comp.name in dropped:
            continue
        fields.append(pa.field(comp.name, _TO_PYARROW[comp.dtype], nullable=comp.nullable))
    return pa.schema(fields)


def iter_record_batches(
    raw: RawInput,
    spec: Spec,
    *,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    encoding: str = "utf-8",
    on_quality_issue: QualityIssueSink | None = None,
    max_skip_rate: float = 1.0,
) -> Iterator[pa.RecordBatch]:
    """Yield pyarrow RecordBatches of up to `chunk_rows` rows streamed from `raw.path`.

    Row-level parse failures (bad numeric, blank non-nullable, bad date) are
    reported to `on_quality_issue` and the row is skipped. When the sink is
    `None` the row error is raised as `IngestError` (legacy behavior for
    direct callers that want fail-fast). Structural errors (unknown dtype,
    wrong record length, multiple zip members, trade_type mismatch) always
    raise regardless.

    `max_skip_rate` (<1.0) aborts the stream with `IngestError` as soon as
    the per-row skip ratio exceeds the threshold, so a pathological file
    doesn't get parsed end-to-end just to fail at commit time.
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
        member = _select_data_member(zf, raw.path)
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
                max_skip_rate,
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
    max_skip_rate: float,
) -> Iterator[pa.RecordBatch]:
    text = io.TextIOWrapper(binstream, encoding=encoding, newline="")
    # Precompute slices and per-column parser closures once per stream so the
    # per-row loop does a single indirect call + no dtype branching.
    n = len(columns_ordered)
    slices = [slice(c.start - 1, c.start - 1 + c.length) for c in columns_ordered]
    parsers = [_make_parser(c) for c in columns_ordered]
    col_names = [c.effective_name for c in columns_ordered]

    # Computed columns: each runs after FWF parsing, indexing into the current
    # row's parsed values by source column position. Resolve source names to
    # indices once per stream.
    fwf_index = {name: i for i, name in enumerate(col_names)}
    computed_specs = [
        (comp, tuple(fwf_index[src] for src in comp.sources), _make_computer(comp))
        for comp in spec.computed_columns
    ]
    n_total = n + len(computed_specs)

    # Dropped columns are still parsed (computed may depend on them) but never
    # land in a buffer. `kept_indices` picks the row_values slots that survive.
    dropped = set(spec.dropped_columns)
    all_output_names = col_names + [c.name for c in spec.computed_columns]
    kept_indices = tuple(i for i, name in enumerate(all_output_names) if name not in dropped)
    field_types = [arrow_schema.field(j).type for j in range(len(kept_indices))]

    # Per-line comparisons resolve against locals; `record_length` is an upper
    # bound and `min_required` is the last real-column byte.
    min_required = spec.min_record_length
    max_allowed = spec.record_length

    buffers: list[list[object]] = [[] for _ in kept_indices]
    rows_in_batch = 0
    # Per-row skip-rate check: abort as soon as ratio crosses threshold.
    rows_skipped = 0

    for line_no, raw_line in enumerate(text, start=1):
        line = raw_line.rstrip("\n").rstrip("\r")
        line_len = len(line)
        if line_len < min_required:
            raise IngestError(
                f"{raw.path.name} line {line_no}: record truncated (need at least "
                f"{min_required} bytes to cover all columns, got {line_len})"
            )
        if line_len > max_allowed:
            raise IngestError(
                f"{raw.path.name} line {line_no}: record longer than declared "
                f"record_length {max_allowed} (got {line_len})"
            )

        row_values: list[object] = [None] * n_total
        bad_column: str | None = None
        bad_error: _CastError | None = None
        for i in range(n):
            try:
                row_values[i] = parsers[i](line[slices[i]])
            except _CastError as exc:
                bad_column = col_names[i]
                bad_error = exc
                break

        if bad_error is None:
            for offset, (comp, src_indices, computer) in enumerate(computed_specs):
                try:
                    row_values[n + offset] = computer(tuple(row_values[s] for s in src_indices))
                except _CastError as exc:
                    bad_column = comp.name
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
            rows_skipped += 1
            if max_skip_rate < 1.0 and rows_skipped / line_no > max_skip_rate:
                raise skip_rate_error(rows_skipped, line_no, max_skip_rate)
            continue

        for buf, src in zip(buffers, kept_indices, strict=True):
            buf.append(row_values[src])
        rows_in_batch += 1

        if rows_in_batch >= chunk_rows:
            yield _build_batch(buffers, arrow_schema, field_types)
            buffers = [[] for _ in kept_indices]
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
                f"column {col.physical_name!r}: unknown Date parse {parse_name!r}; "
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

    raise IngestError(f"column {col.physical_name!r}: unsupported dtype {col.dtype!r}")


def _make_computer(comp: ComputedColumn) -> Callable[[tuple[object, ...]], object]:
    """Return a closure that computes one row's value from its source values.

    Raises `IngestError` up front if the `kind` is unknown; per-row parse-
    style failures raise `_CastError` which the stream loop routes to the
    quality log.
    """
    if comp.kind == "concat_to_date":

        def concat(values: tuple[object, ...]) -> object:
            base, day = values
            if base is None or day is None:
                return None
            if not isinstance(base, date):
                raise _CastError(
                    f"computed {comp.name!r}: first source must be Date, got {type(base).__name__}"
                )
            if isinstance(day, str):
                stripped = day.strip()
                if not stripped:
                    return None
                try:
                    day_int = int(stripped)
                except ValueError as exc:
                    raise _CastError(
                        f"computed {comp.name!r}: cannot parse day source {day!r} as int"
                    ) from exc
            elif isinstance(day, int):
                day_int = day
            else:
                raise _CastError(
                    f"computed {comp.name!r}: second source must be Int64 or Utf8, "
                    f"got {type(day).__name__}"
                )
            try:
                return date(base.year, base.month, day_int)
            except ValueError as exc:
                raise _CastError(
                    f"computed {comp.name!r}: cannot build date({base.year}, "
                    f"{base.month}, {day_int}): {exc}"
                ) from exc

        return concat

    if comp.kind == "concat_text":
        separator = comp.separator

        def concat_text(values: tuple[object, ...]) -> object:
            parts: list[str] = []
            for v in values:
                if v is None:
                    continue
                if not isinstance(v, str):
                    raise _CastError(
                        f"computed {comp.name!r}: concat_text source must be Utf8, "
                        f"got {type(v).__name__}"
                    )
                stripped = v.strip()
                if stripped:
                    parts.append(stripped)
            if not parts:
                return None
            return _WHITESPACE_RE.sub(" ", separator.join(parts)).strip()

        return concat_text

    raise IngestError(f"computed column {comp.name!r}: unknown kind {comp.kind!r}")


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


def skip_rate_error(skipped: int, total: int, threshold: float) -> IngestError:
    """Build the shared 'too many rows skipped' IngestError used by ingest and pipeline."""
    return IngestError(
        f"{skipped}/{total} rows failed parsing ({skipped / total:.1%}); "
        f"exceeds max_skip_rate {threshold:.1%}"
    )
