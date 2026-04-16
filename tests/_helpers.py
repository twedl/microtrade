"""Test helpers: synthetic schema-workbook builder and FWF data generator.

Kept out of `src/microtrade/` so it never ships with the package. These utilities
let us drive every downstream test (excel_spec, schema, ingest, pipeline) from a
single in-tree synthetic source of truth, matching the real-world workflow
(workbook -> YAML -> FWF zips -> parquet) without any binary fixtures in git.
"""

from __future__ import annotations

import random
import string
import zipfile
from dataclasses import dataclass
from pathlib import Path

from openpyxl import Workbook

from microtrade.schema import Column, Spec


@dataclass(frozen=True)
class SheetSpec:
    """In-test description of a workbook sheet to synthesize."""

    trade_type: str
    rows: tuple[tuple[str, int, int, str, bool, str, str | None], ...]
    # (name, start, length, dtype, nullable, description, parse)


SHEET_HEADER: tuple[str, ...] = (
    "name",
    "start",
    "length",
    "dtype",
    "nullable",
    "description",
    "parse",
)

DEFAULT_SHEETS: tuple[SheetSpec, ...] = (
    SheetSpec(
        trade_type="imports",
        rows=(
            ("period", 1, 6, "string", False, "YYYYMM", "yyyymm_to_date"),
            ("hs_code", 7, 10, "string", False, "HTS code", None),
            ("country_coo", 17, 3, "string", False, "country of origin", None),
            ("district_entry", 20, 4, "string", True, "port district", None),
            ("value_usd", 24, 15, "int", False, "customs value in USD", None),
            ("qty_kg", 39, 15, "int", True, "net weight kilograms", None),
        ),
    ),
    SheetSpec(
        trade_type="exports_us",
        rows=(
            ("period", 1, 6, "string", False, "YYYYMM", "yyyymm_to_date"),
            ("schedule_b", 7, 10, "string", False, "Schedule B code", None),
            ("country_dest", 17, 3, "string", False, "destination country", None),
            ("value_fas", 20, 15, "int", False, "F.A.S. value", None),
            ("transport_mode", 35, 2, "string", True, "mode code", None),
        ),
    ),
    SheetSpec(
        trade_type="exports_nonus",
        rows=(
            ("period", 1, 6, "string", False, "YYYYMM", "yyyymm_to_date"),
            ("hs_code", 7, 10, "string", False, "HS code", None),
            ("country_dest", 17, 3, "string", False, "destination", None),
            ("value_usd", 20, 15, "float", True, "declared value", None),
        ),
    ),
)


def build_workbook(path: Path, sheets: tuple[SheetSpec, ...] = DEFAULT_SHEETS) -> Path:
    """Write a synthetic schema workbook with one sheet per trade type."""
    wb = Workbook()
    wb.remove(wb.active)  # drop the default sheet openpyxl creates
    for sheet in sheets:
        ws = wb.create_sheet(title=sheet.trade_type)
        ws.append(list(SHEET_HEADER))
        for row in sheet.rows:
            name, start, length, dtype, nullable, description, parse = row
            ws.append([name, start, length, dtype, "y" if nullable else "n", description, parse])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


# --- FWF data generation ---------------------------------------------------


def _gen_string(col: Column, rng: random.Random, row_idx: int) -> str:
    if col.name == "period":
        year = rng.choice([2023, 2024, 2025])
        month = rng.randint(1, 12)
        return f"{year}{month:02d}"
    if col.name.startswith("country") or col.name == "country_dest":
        return rng.choice(["USA", "CHN", "MEX", "CAN", "DEU", "JPN", "KOR"])
    if col.name in {"hs_code", "schedule_b"}:
        return "".join(rng.choices(string.digits, k=min(col.length, 10)))
    if col.name == "transport_mode":
        return rng.choice(["10", "20", "30", "40", "50"])
    if col.name == "district_entry":
        return f"{rng.randint(1000, 9999)}"
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=min(col.length, 8)))


def _gen_int(col: Column, rng: random.Random, row_idx: int) -> int:
    if col.name == "value_usd" or col.name.startswith("value"):
        return rng.randint(100, 10_000_000)
    if col.name.startswith("qty"):
        return rng.randint(1, 100_000)
    return rng.randint(0, 10**9)


def _gen_float(col: Column, rng: random.Random, row_idx: int) -> float:
    return round(rng.uniform(1.0, 1_000_000.0), 2)


def _format_field(col: Column, value: str) -> str:
    """Pad/truncate to exactly col.length; numerics right-aligned, strings left."""
    text = value.rjust(col.length) if col.dtype in {"Int64", "Float64"} else value.ljust(col.length)
    return text[: col.length]


def _render_value(col: Column, rng: random.Random, row_idx: int) -> str:
    if col.dtype == "Utf8":
        return _gen_string(col, rng, row_idx)
    if col.dtype == "Int64":
        return str(_gen_int(col, rng, row_idx))
    if col.dtype == "Float64":
        return f"{_gen_float(col, rng, row_idx):.2f}"
    if col.dtype == "Date":
        return "20240101"
    raise ValueError(f"unhandled dtype {col.dtype!r}")


def render_fwf_lines(
    spec: Spec,
    n_rows: int,
    *,
    seed: int = 0,
    include_bad: bool = False,
) -> list[str]:
    """Generate `n_rows` well-formed FWF lines plus (optional) a few bad rows."""
    rng = random.Random(seed)
    ordered = sorted(spec.columns, key=lambda c: c.start)
    lines: list[str] = []
    for i in range(n_rows):
        lines.append("".join(_format_field(col, _render_value(col, rng, i)) for col in ordered))
    if include_bad:
        lines.extend(_bad_rows(spec, rng, ordered))
    return lines


def _bad_rows(spec: Spec, rng: random.Random, ordered: list[Column]) -> list[str]:
    """Produce realistic imperfections: null-like fillers, short lines, garbage."""
    bad: list[str] = []

    # Nullable column blanked (whitespace).
    nullable_cols = [c for c in ordered if c.nullable]
    if nullable_cols:
        target = nullable_cols[0]
        parts = []
        for col in ordered:
            if col.name == target.name:
                parts.append(" " * col.length)
            else:
                parts.append(_format_field(col, _render_value(col, rng, -1)))
        bad.append("".join(parts))

    # Line truncated by 5 characters (should be rejected by record_length check).
    full = "".join(_format_field(col, _render_value(col, rng, -2)) for col in ordered)
    bad.append(full[:-5])

    # Garbage in a numeric field (non-digit chars) - should fail dtype cast.
    numeric_cols = [c for c in ordered if c.dtype in {"Int64", "Float64"}]
    if numeric_cols:
        target = numeric_cols[0]
        parts = []
        for col in ordered:
            if col.name == target.name:
                parts.append(_format_field(col, "ABCDE"))
            else:
                parts.append(_format_field(col, _render_value(col, rng, -3)))
        bad.append("".join(parts))

    return bad


def make_zip_input(
    path: Path,
    lines: list[str],
    inner_name: str = "data.fwf",
) -> Path:
    """Write lines into a zip file, matching the pipeline's expected input shape."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(inner_name, "\n".join(lines) + "\n")
    return path
