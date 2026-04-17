"""One-shot converter: Excel schema workbook -> versioned YAML specs.

Real workbooks supplied upstream use a layout that is loose by spreadsheet
standards: sheets are ordered (sheet 1 = imports, 2 = exports_us, 3 =
exports_nonus) regardless of name, the field table sits below a few preamble
rows, and the table itself reads `Position | Description | Length | Type` with
``Blank`` rows interleaved as FWF filler bytes that are not real fields.

This module:

- maps the first three sheets positionally onto :data:`TRADE_TYPES`,
- autodetects the header row by looking for `Position`, `Description`,
  `Length`, `Type` (case-insensitive) in the same row,
- skips rows whose Description is `Blank` (FWF padding),
- normalizes the `Type` token to one of the canonical Polars dtype names
  (Utf8, Int64, Float64, Date),
- preserves the rightmost extent (Blank or real) as the spec's
  ``record_length`` so FWF line-length validation matches the source layout.

Optional `Nullable` and `Parse` columns are honored when present; otherwise
columns default to nullable + the dtype's default parse string. This module
runs only via `microtrade import-spec PATH.xlsx --effective-from YYYY-MM` and
never on the ingest hot path.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from microtrade.schema import (
    TRADE_TYPES,
    Column,
    Spec,
    SpecError,
    SpecSource,
    file_sha256,
    now_iso,
    validate_period,
    validate_spec,
)

REQUIRED_HEADERS: tuple[str, ...] = ("position", "description", "length", "type")
OPTIONAL_HEADERS: tuple[str, ...] = ("nullable", "parse")
_HEADER_TOKENS: frozenset[str] = frozenset(REQUIRED_HEADERS)

_DTYPE_ALIASES: Mapping[str, str] = {
    "string": "Utf8",
    "str": "Utf8",
    "text": "Utf8",
    "char": "Utf8",
    "varchar": "Utf8",
    "utf8": "Utf8",
    "int": "Int64",
    "integer": "Int64",
    "bigint": "Int64",
    "long": "Int64",
    "int64": "Int64",
    # Real workbooks tag numeric columns as `Num`; default to Int64. If a
    # specific column needs Float64 the user can override the YAML by hand.
    "num": "Int64",
    "number": "Int64",
    "numeric": "Int64",
    "float": "Float64",
    "double": "Float64",
    "decimal": "Float64",
    "float64": "Float64",
    "date": "Date",
    "yyyymmdd": "Date",
}

_PARSE_FOR_DTYPE: Mapping[str, str | None] = {
    "Utf8": None,
    "Int64": None,
    "Float64": None,
    "Date": "yyyymmdd_to_date",
}

_BLANK_FIELD: str = "blank"


def normalize_dtype(raw: str) -> str:
    key = raw.strip().lower()
    if key in _DTYPE_ALIASES:
        return _DTYPE_ALIASES[key]
    raise SpecError(f"unrecognized dtype {raw!r}; known: {sorted(set(_DTYPE_ALIASES.values()))}")


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"y", "yes", "true", "t", "1"}:
        return True
    if text in {"n", "no", "false", "f", "0"}:
        return False
    raise SpecError(f"cannot interpret {value!r} as boolean for 'nullable'")


def _cell_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _cell_int(value: Any, *, field: str) -> int:
    text = _cell_str(value)
    if not text:
        raise SpecError(f"{field}: missing value")
    try:
        # Cast through float to tolerate Excel's numeric coercion ("345.0").
        return int(float(text))
    except ValueError as exc:
        raise SpecError(f"{field}: cannot read {value!r} as integer") from exc


def _find_header_row(df: pl.DataFrame, sheet: str) -> int:
    for i, row in enumerate(df.iter_rows()):
        tokens = {str(c).strip().lower() for c in row if c is not None}
        if _HEADER_TOKENS.issubset(tokens):
            return i
    raise SpecError(
        f"sheet {sheet!r}: could not find a header row containing {sorted(_HEADER_TOKENS)}"
    )


def _header_index(row: tuple[Any, ...]) -> dict[str, int]:
    out: dict[str, int] = {}
    for i, cell in enumerate(row):
        key = _cell_str(cell).lower()
        if key:
            out.setdefault(key, i)
    return out


@dataclass(frozen=True)
class _ColumnIndex:
    """Resolved column positions for one sheet's header row."""

    position: int
    description: int
    length: int
    type_: int
    nullable: int | None
    parse: int | None

    @classmethod
    def from_header(cls, header: dict[str, int], sheet: str) -> _ColumnIndex:
        missing = [h for h in REQUIRED_HEADERS if h not in header]
        if missing:
            raise SpecError(f"sheet {sheet!r}: header row missing columns {missing}")
        return cls(
            position=header["position"],
            description=header["description"],
            length=header["length"],
            type_=header["type"],
            nullable=header.get("nullable"),
            parse=header.get("parse"),
        )


def _row_to_column(row: tuple[Any, ...], idx: _ColumnIndex, *, sheet: str, line_no: int) -> Column:
    where = f"sheet {sheet!r} row {line_no}"
    start = _cell_int(row[idx.position], field=f"{where} Position")
    length = _cell_int(row[idx.length], field=f"{where} Length")
    name = _cell_str(row[idx.description])
    if not name:
        raise SpecError(f"{where}: Description is empty")
    dtype = normalize_dtype(_cell_str(row[idx.type_]) or "")
    nullable = (
        _coerce_bool(row[idx.nullable])
        if idx.nullable is not None and row[idx.nullable] is not None
        else True
    )
    parse_raw = row[idx.parse] if idx.parse is not None else None
    parse = _cell_str(parse_raw) if parse_raw not in (None, "") else _PARSE_FOR_DTYPE.get(dtype)
    return Column(
        name=name,
        start=start,
        length=length,
        dtype=dtype,
        nullable=nullable,
        parse=parse,
        description=None,
    )


def _sheet_to_layout(df: pl.DataFrame, sheet: str) -> tuple[tuple[Column, ...], int]:
    """Parse a sheet into (real columns, record_length).

    `record_length` is the rightmost extent across every parsable row in the
    field table - including `Blank` filler rows - so it matches the actual
    FWF line length even when filler trails the last real column.
    """
    header_idx = _find_header_row(df, sheet)
    idx = _ColumnIndex.from_header(_header_index(df.row(header_idx)), sheet)

    columns: list[Column] = []
    max_end = 0
    for raw_offset in range(header_idx + 1, df.height):
        row = df.row(raw_offset)
        if all(c is None or _cell_str(c) == "" for c in row):
            continue
        # Footer rows (totals, signatures, etc.) often have non-numeric
        # Position; skip them quietly so layouts can carry trailing notes.
        try:
            start = _cell_int(row[idx.position], field="position")
            length = _cell_int(row[idx.length], field="length")
        except SpecError:
            continue
        max_end = max(max_end, start + length - 1)

        if _cell_str(row[idx.description]).lower() == _BLANK_FIELD:
            continue

        columns.append(_row_to_column(row, idx, sheet=sheet, line_no=raw_offset + 1))

    if not columns:
        raise SpecError(f"sheet {sheet!r}: no column rows found below header row {header_idx + 1}")
    return tuple(columns), max_end


def _derived_for(columns: tuple[Column, ...]) -> tuple[tuple[str, str], ...]:
    for col in columns:
        if col.name == "period" and col.parse in {"yyyymm_to_date", "yyyymmdd_to_date"}:
            return (("year", "year(period)"), ("month", "month(period)"))
    return ()


def read_workbook(workbook: Path, effective_from: str) -> dict[str, Spec]:
    """Parse the workbook into one Spec per trade type. Sheets map by position
    (1 -> imports, 2 -> exports_us, 3 -> exports_nonus); names are ignored."""
    validate_period(effective_from)
    workbook = workbook.resolve()
    sha = file_sha256(workbook)
    imported_at = now_iso()

    sheets = pl.read_excel(workbook, sheet_id=0, has_header=False)
    if not isinstance(sheets, dict):
        raise SpecError("polars.read_excel did not return a sheet dict")

    sheet_items = list(sheets.items())
    if len(sheet_items) < len(TRADE_TYPES):
        raise SpecError(
            f"workbook {workbook.name} has {len(sheet_items)} sheet(s); "
            f"need at least {len(TRADE_TYPES)} (one per trade type, in order: "
            f"{list(TRADE_TYPES)})"
        )

    out: dict[str, Spec] = {}
    for trade_type, (sheet_name, df) in zip(TRADE_TYPES, sheet_items, strict=False):
        columns, record_length = _sheet_to_layout(df, sheet_name)
        spec = Spec(
            trade_type=trade_type,
            version=effective_from,
            effective_from=effective_from,
            record_length=record_length,
            columns=columns,
            source=SpecSource(
                workbook=workbook.name,
                sha256=sha,
                sheet=sheet_name,
                imported_at=imported_at,
            ),
            derived=_derived_for(columns),
        )
        validate_spec(spec)
        out[trade_type] = spec
    return out
