"""Test helpers: synthetic schema-workbook builder and FWF data generator.

Kept out of `src/microtrade/` so it never ships with the package. These utilities
let us drive every downstream test (excel_spec, schema, ingest, pipeline) from a
single in-tree synthetic source of truth, matching the real-world workflow
(workbook -> YAML -> FWF zips -> parquet) without any binary fixtures in git.
"""

from __future__ import annotations

import random
import re
import string
import zipfile
from dataclasses import dataclass
from pathlib import Path

import yaml
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

SHEET_TITLES: dict[str, str] = {
    "imports": "ImportsSheet",
    "exports_us": "USExportsSheet",
    "exports_nonus": "NonUSExportsSheet",
}


def default_filename_pattern(sheet_title: str) -> str:
    """Synthetic filename pattern used by tests for a given sheet title."""
    return (
        f"^{re.escape(sheet_title)}"
        r"_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$"
    )


def input_filename(sheet_title: str, year: int, month: int, flag: str = "N") -> str:
    """Filename that matches `default_filename_pattern(sheet_title)`."""
    return f"{sheet_title}_{year}{month:02d}{flag}.TXT.zip"


def build_project_config(
    config_path: Path,
    workbook_path: Path,
    effective_from: str,
    *,
    effective_to: str | None = None,
    workbook_id: str | None = None,
    sheet_titles: dict[str, str] | None = None,
) -> Path:
    """Write a `microtrade.yaml` referencing `workbook_path` with default patterns.

    Produces one entry per (trade_type, sheet_title) pair in `sheet_titles`
    (defaulting to `SHEET_TITLES`). The synthetic workbook's `period` column
    is declared Date + yyyymm_to_date at the workbook level, so no cast or
    parse overrides are needed here.
    """
    titles = sheet_titles if sheet_titles is not None else SHEET_TITLES
    sheets_cfg: dict[str, object] = {}
    for trade_type, sheet_title in titles.items():
        entry: dict[str, object] = {
            "trade_type": trade_type,
            "filename_pattern": default_filename_pattern(sheet_title),
        }
        sheets_cfg[sheet_title] = entry
    workbook_entry: dict[str, object] = {
        "effective_from": effective_from,
        "sheets": sheets_cfg,
    }
    if effective_to is not None:
        workbook_entry["effective_to"] = effective_to
    if workbook_id is not None:
        workbook_entry["workbook_id"] = workbook_id
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump({"workbooks": {workbook_path.name: workbook_entry}}, sort_keys=False),
        encoding="utf-8",
    )
    return config_path


DEFAULT_SHEETS: tuple[SheetSpec, ...] = (
    SheetSpec(
        trade_type="imports",
        rows=(
            ("period", 1, 6, "date", False, "YYYYMM", "yyyymm_to_date"),
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
            ("period", 1, 6, "date", False, "YYYYMM", "yyyymm_to_date"),
            ("schedule_b", 7, 10, "string", False, "Schedule B code", None),
            ("country_dest", 17, 3, "string", False, "destination country", None),
            ("value_fas", 20, 15, "int", False, "F.A.S. value", None),
            ("transport_mode", 35, 2, "string", True, "mode code", None),
        ),
    ),
    SheetSpec(
        trade_type="exports_nonus",
        rows=(
            ("period", 1, 6, "date", False, "YYYYMM", "yyyymm_to_date"),
            ("hs_code", 7, 10, "string", False, "HS code", None),
            ("country_dest", 17, 3, "string", False, "destination", None),
            ("value_usd", 20, 15, "float", True, "declared value", None),
        ),
    ),
)


def build_workbook(path: Path, sheets: tuple[SheetSpec, ...] = DEFAULT_SHEETS) -> Path:
    """Write a synthetic schema workbook in the real upstream layout.

    Sheets are positional (`SHEET001`, `SHEET002`, ...): the spec parser maps
    sheet index -> trade_type, ignoring names. Each sheet carries a few
    preamble rows (so we exercise the parser's header autodetection) and a
    `Position | Description | Length | Type | Nullable | Parse` table.
    """
    wb = Workbook()
    wb.remove(wb.active)  # drop the default sheet openpyxl creates
    for sheet in sheets:
        # Distinct first-two-chars per sheet so the discover old-pattern
        # `(workbook_id, sheet[:2])` lookup doesn't collide on the fixture.
        ws = wb.create_sheet(title=SHEET_TITLES[sheet.trade_type])
        ws.append([f"Synthetic layout for {sheet.trade_type}", None, None, None, None, None])
        ws.append(["CONFIDENTIAL", None, None, None, None, None])
        ws.append([None, None, None, None, None, None])
        ws.append(["Position", "Description", "Length", "Type", "Nullable", "Parse"])
        for row in sheet.rows:
            name, start, length, dtype, nullable, _description, parse = row
            ws.append([start, name, length, dtype, "y" if nullable else "n", parse])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


# --- FWF data generation ---------------------------------------------------


def _gen_string(col: Column, rng: random.Random, row_idx: int) -> str:
    if col.physical_name == "period":
        year = rng.choice([2023, 2024, 2025])
        month = rng.randint(1, 12)
        return f"{year}{month:02d}"
    if col.physical_name.startswith("country") or col.physical_name == "country_dest":
        return rng.choice(["USA", "CHN", "MEX", "CAN", "DEU", "JPN", "KOR"])
    if col.physical_name in {"hs_code", "schedule_b"}:
        return "".join(rng.choices(string.digits, k=min(col.length, 10)))
    if col.physical_name == "transport_mode":
        return rng.choice(["10", "20", "30", "40", "50"])
    if col.physical_name == "district_entry":
        return f"{rng.randint(1000, 9999)}"
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=min(col.length, 8)))


def _gen_int(col: Column, rng: random.Random, row_idx: int) -> int:
    if col.physical_name == "value_usd" or col.physical_name.startswith("value"):
        return rng.randint(100, 10_000_000)
    if col.physical_name.startswith("qty"):
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
        year = rng.choice([2023, 2024, 2025])
        month = rng.randint(1, 12)
        if col.parse == "yyyymm_to_date":
            return f"{year}{month:02d}"
        day = rng.randint(1, 28)
        return f"{year}{month:02d}{day:02d}"
    raise ValueError(f"unhandled dtype {col.dtype!r}")


def render_fwf_lines(
    spec: Spec,
    n_rows: int,
    *,
    seed: int = 0,
    include_bad: bool = False,
) -> list[str]:
    """Generate `n_rows` well-formed FWF lines plus (optional) a few bad rows.

    Lines are sized to `spec.record_length`; bytes not covered by any column
    (the gaps a real workbook fills with ``Blank`` padding rows) are left as
    spaces, so the output round-trips cleanly through `iter_record_batches`
    even for specs with non-contiguous column layouts.
    """
    rng = random.Random(seed)
    ordered = list(spec.ordered_columns)
    lines = [_render_line(spec, ordered, rng, i) for i in range(n_rows)]
    if include_bad:
        lines.extend(_bad_rows(spec, rng, ordered))
    return lines


def render_ytd_fwf_lines(
    spec: Spec,
    *,
    snapshot_year: int,
    snapshot_month: int,
    rows_per_month: int,
    seed: int = 0,
    start_month: int = 1,
) -> list[str]:
    """Generate YTD-cumulative FWF lines: `rows_per_month` rows for each month
    from `snapshot_year`-`start_month` through `snapshot_year`-`snapshot_month`.

    The `period` column (physical_name=="period", length 6) is pinned to the
    `YYYYMM` of the row's month; the pipeline then routes each row to the
    correct per-month partition via `MultiPartitionWriter`. `start_month`
    lets a fixture skip earlier months (e.g. represent a dataset that only
    has a December snapshot for a prior year).
    """
    rng = random.Random(seed)
    ordered = list(spec.ordered_columns)
    period_col = next((c for c in ordered if c.physical_name == "period"), None)
    lines: list[str] = []
    for month in range(start_month, snapshot_month + 1):
        period_raw = f"{snapshot_year:04d}{month:02d}"
        overrides: dict[str, str] | None = None
        if period_col is not None:
            overrides = {"period": period_raw.ljust(period_col.length)[: period_col.length]}
        for i in range(rows_per_month):
            lines.append(_render_line(spec, ordered, rng, i, overrides))
    return lines


def _render_line(
    spec: Spec,
    ordered: list[Column],
    rng: random.Random,
    row_idx: int,
    overrides: dict[str, str] | None = None,
) -> str:
    """Render a single FWF line sized to `spec.record_length`.

    `overrides` replaces the normally-rendered chunk for the named columns -
    used by `_bad_rows` to inject blanks into a nullable column or garbage
    into a numeric column without having to reconstruct the whole line.
    """
    buf = bytearray(b" " * spec.record_length)
    for col in ordered:
        if overrides is not None and col.physical_name in overrides:
            chunk = overrides[col.physical_name]
        else:
            chunk = _format_field(col, _render_value(col, rng, row_idx))
        buf[col.start - 1 : col.start - 1 + col.length] = chunk.encode("utf-8")
    return buf.decode("utf-8")


def _bad_rows(spec: Spec, rng: random.Random, ordered: list[Column]) -> list[str]:
    """Produce realistic imperfections: null-like fillers, short lines, garbage."""
    bad: list[str] = []

    # Nullable column blanked (whitespace).
    nullable_cols = [c for c in ordered if c.nullable]
    if nullable_cols:
        target = nullable_cols[0]
        bad.append(
            _render_line(spec, ordered, rng, -1, {target.physical_name: " " * target.length})
        )

    # Line truncated by 5 characters (should be rejected by record_length check).
    bad.append(_render_line(spec, ordered, rng, -2)[:-5])

    # Garbage in a numeric field (non-digit chars) - should fail dtype cast.
    numeric_cols = [c for c in ordered if c.dtype in {"Int64", "Float64"}]
    if numeric_cols:
        target = numeric_cols[0]
        bad.append(
            _render_line(
                spec, ordered, rng, -3, {target.physical_name: _format_field(target, "ABCDE")}
            )
        )

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
