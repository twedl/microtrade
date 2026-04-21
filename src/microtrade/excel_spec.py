"""One-shot converter: Excel schema workbook -> versioned YAML specs.

`read_workbook(path, WorkbookConfig)` reads the sheets named in the config,
parses each one's `Position | Description | Length | Type` table (case-
insensitive; `Blank` rows are FWF padding that extend record_length but
do not become columns), normalizes the `Type` token to a canonical Polars
dtype (Utf8, Int64, Float64, Date), and emits one `Spec` per sheet with
`effective_from`/`effective_to`/`workbook_id`/`filename_pattern` baked in
from the config. Optional `Nullable` and `Parse` columns are honored when
present; otherwise columns default to nullable + the dtype's default
parse string. Only called by `microtrade import-spec`; never on the ingest
hot path.
"""

from __future__ import annotations

import difflib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import polars as pl

from microtrade.config import WorkbookConfig
from microtrade.schema import (
    TRADE_TYPES,
    Column,
    Spec,
    SpecError,
    SpecSource,
    file_sha256,
    now_iso,
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
    if isinstance(value, int | float):
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
    physical_name = _cell_str(row[idx.description])
    if not physical_name:
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
        physical_name=physical_name,
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
        except SpecError:
            continue

        # Sentinel row: Position parses but Length is blank. Workbook authors
        # use this to mark "the record extends to byte N"; treat Position as
        # the last byte so record_length picks up trailing filler the schema
        # sheet would otherwise drop.
        if _cell_str(row[idx.length]) == "":
            max_end = max(max_end, start)
            continue

        try:
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


def _apply_rename(
    columns: tuple[Column, ...], rename: Mapping[str, str], *, sheet: str
) -> tuple[Column, ...]:
    """Stamp `logical_name` on each column whose `physical_name` is in `rename`.

    A rename key that matches no column is a stale config entry (usually a
    leftover from a workbook change); we raise so it surfaces at import
    time, not silently at ingest.
    """
    physical_names = {c.physical_name for c in columns}
    unknown = sorted(set(rename) - physical_names)
    if unknown:
        suggestions = {
            name: difflib.get_close_matches(name, physical_names, n=1) for name in unknown
        }
        hints = ", ".join(
            f"{name!r} (did you mean {matches[0]!r}?)" if matches else repr(name)
            for name, matches in suggestions.items()
        )
        raise SpecError(f"sheet {sheet!r}: rename refers to unknown physical column(s): {hints}")
    return tuple(
        replace(col, logical_name=rename[col.physical_name]) if col.physical_name in rename else col
        for col in columns
    )


def _derived_for(columns: tuple[Column, ...]) -> tuple[tuple[str, str], ...]:
    for col in columns:
        if col.effective_name == "period" and col.parse in {"yyyymm_to_date", "yyyymmdd_to_date"}:
            return (("year", "year(period)"), ("month", "month(period)"))
    return ()


def derive_workbook_id(workbook_filename: str) -> str:
    """Pull the stable identifier off the front of a workbook filename.

    The convention upstream is `<workbook_id>_<rest>.<ext>`, e.g.
    `XYZ12345_Record_Layout.xls` -> `XYZ12345`,
    `ABC-1234567_Record_Layout.xls` -> `ABC-1234567`. Falls back to the
    stem if there is no `_` to split on.
    """
    stem = Path(workbook_filename).stem
    head, sep, _ = stem.partition("_")
    return head if sep else stem


def read_workbook(workbook: Path, workbook_config: WorkbookConfig) -> dict[str, Spec]:
    """Parse only the sheets listed in `workbook_config.sheets` into Specs.

    Each listed sheet maps to a trade type (explicit via `SheetConfig.trade_type`,
    or positional via TRADE_TYPES when omitted). `effective_from`, `effective_to`,
    `workbook_id`, and per-sheet `filename_pattern` all come from the config and
    are baked into the emitted Specs so discovery can run without the config.
    """
    effective_from = workbook_config.effective_from
    workbook = workbook.resolve()
    sha = file_sha256(workbook)
    imported_at = now_iso()
    resolved_workbook_id = (
        workbook_config.workbook_id
        if workbook_config.workbook_id is not None
        else derive_workbook_id(workbook.name)
    )

    sheets = pl.read_excel(workbook, sheet_id=0, has_header=False)
    if not isinstance(sheets, dict):
        raise SpecError("polars.read_excel did not return a sheet dict")

    missing = [name for name in workbook_config.sheets if name not in sheets]
    if missing:
        raise SpecError(
            f"workbook {workbook.name} does not contain sheet(s) {missing}; "
            f"available: {list(sheets)}"
        )

    out: dict[str, Spec] = {}
    for sheet_idx, (sheet_name, sheet_config) in enumerate(workbook_config.sheets.items()):
        if sheet_config.trade_type is not None:
            trade_type = sheet_config.trade_type
        elif sheet_idx < len(TRADE_TYPES):
            trade_type = TRADE_TYPES[sheet_idx]
        else:
            raise SpecError(
                f"workbook {workbook.name}: sheet {sheet_name!r} at position "
                f"{sheet_idx} has no trade_type and no positional fallback "
                f"(only {len(TRADE_TYPES)} positional slots: {list(TRADE_TYPES)})"
            )
        df = sheets[sheet_name]
        columns, record_length = _sheet_to_layout(df, sheet_name)
        if sheet_config.rename:
            columns = _apply_rename(columns, sheet_config.rename, sheet=sheet_name)
        if trade_type in out:
            raise SpecError(
                f"workbook {workbook.name}: sheets map multiple entries to trade_type "
                f"{trade_type!r}"
            )
        spec = Spec(
            trade_type=trade_type,
            version=effective_from,
            effective_from=effective_from,
            effective_to=workbook_config.effective_to,
            record_length=record_length,
            columns=columns,
            source=SpecSource(
                workbook=workbook.name,
                sha256=sha,
                sheet=sheet_name,
                imported_at=imported_at,
                filename_pattern=sheet_config.filename_pattern,
                workbook_id=resolved_workbook_id,
            ),
            derived=_derived_for(columns),
        )
        validate_spec(spec)
        out[trade_type] = spec
    return out
