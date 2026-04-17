"""One-shot converter: Excel schema workbook -> versioned YAML specs.

The workbook is expected to have one sheet per trade type
(`imports`, `exports_us`, `exports_nonus`) with a header row containing
the following columns (case-insensitive, order-agnostic):

    name | start | length | dtype | nullable | description

Only `name`, `start`, `length`, `dtype` are required; `nullable` defaults to
True and `description` to None. `dtype` is normalized to one of the canonical
Polars dtype names (Utf8, Int64, Float64, Date).

This module is invoked via `microtrade import-spec PATH.xlsx --effective-from YYYY-MM`
and never runs on the ingest hot path.
"""

from __future__ import annotations

from collections.abc import Mapping
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

REQUIRED_HEADERS: tuple[str, ...] = ("name", "start", "length", "dtype")
OPTIONAL_HEADERS: tuple[str, ...] = ("nullable", "description", "parse")

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
    "float": "Float64",
    "double": "Float64",
    "decimal": "Float64",
    "numeric": "Float64",
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


def _row_to_column(row: dict[str, Any]) -> Column:
    try:
        name = str(row["name"]).strip()
        start = int(row["start"])
        length = int(row["length"])
        dtype = normalize_dtype(str(row["dtype"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise SpecError(f"invalid column row {row!r}: {exc}") from exc

    nullable_raw = row.get("nullable")
    nullable = _coerce_bool(nullable_raw) if nullable_raw is not None else True

    parse_raw = row.get("parse")
    parse = str(parse_raw).strip() if parse_raw not in (None, "") else _PARSE_FOR_DTYPE.get(dtype)

    description_raw = row.get("description")
    description = str(description_raw).strip() if description_raw not in (None, "") else None

    if not name:
        raise SpecError(f"column name missing in row {row!r}")

    return Column(
        name=name,
        start=start,
        length=length,
        dtype=dtype,
        nullable=nullable,
        parse=parse,
        description=description,
    )


def _sheet_to_columns(df: pl.DataFrame, sheet: str) -> tuple[Column, ...]:
    headers = {h.lower(): h for h in df.columns}
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        raise SpecError(f"sheet {sheet!r}: missing required columns {missing}")

    renamed = df.rename(
        {headers[h]: h for h in headers if h in REQUIRED_HEADERS + OPTIONAL_HEADERS}
    )
    keep = [h for h in REQUIRED_HEADERS + OPTIONAL_HEADERS if h in renamed.columns]
    trimmed = renamed.select(keep)

    columns: list[Column] = []
    for row in trimmed.iter_rows(named=True):
        if row.get("name") in (None, "") and row.get("start") in (None, ""):
            continue  # skip fully blank rows
        columns.append(_row_to_column(row))
    if not columns:
        raise SpecError(f"sheet {sheet!r}: no column rows found")
    return tuple(columns)


def _derived_for(columns: tuple[Column, ...]) -> tuple[tuple[str, str], ...]:
    for col in columns:
        if col.name == "period" and col.parse in {"yyyymm_to_date", "yyyymmdd_to_date"}:
            return (("year", "year(period)"), ("month", "month(period)"))
    return ()


def read_workbook(workbook: Path, effective_from: str) -> dict[str, Spec]:
    """Parse every known trade-type sheet into a Spec. Returns {trade_type: Spec}."""
    validate_period(effective_from)
    workbook = workbook.resolve()
    sha = file_sha256(workbook)
    imported_at = now_iso()

    sheets = pl.read_excel(workbook, sheet_id=0)
    if not isinstance(sheets, dict):
        raise SpecError("polars.read_excel did not return a sheet dict")

    lowered = {name.lower(): name for name in sheets}
    missing = [t for t in TRADE_TYPES if t not in lowered]
    if missing:
        raise SpecError(
            f"workbook {workbook.name} missing required sheets: {missing}; "
            f"found {sorted(sheets.keys())}"
        )

    out: dict[str, Spec] = {}
    for trade_type in TRADE_TYPES:
        sheet_name = lowered[trade_type]
        columns = _sheet_to_columns(sheets[sheet_name], sheet_name)
        record_length = max(c.start + c.length - 1 for c in columns)
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
