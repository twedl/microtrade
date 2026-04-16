"""Sanity checks for the test helpers themselves.

We exercise `render_fwf_lines` and `make_zip_input` here so downstream tests
can rely on them without re-validating basic invariants.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from microtrade.excel_spec import read_workbook
from tests._helpers import make_zip_input, render_fwf_lines


def test_render_fwf_lines_produces_fixed_width(schema_workbook: Path) -> None:
    specs = read_workbook(schema_workbook, "2024-01")
    imports = specs["imports"]
    lines = render_fwf_lines(imports, n_rows=10, seed=42)

    assert len(lines) == 10
    for line in lines:
        assert len(line) == imports.record_length


def test_render_fwf_lines_with_bad_rows_includes_truncated_and_null(
    schema_workbook: Path,
) -> None:
    specs = read_workbook(schema_workbook, "2024-01")
    imports = specs["imports"]
    lines = render_fwf_lines(imports, n_rows=5, seed=1, include_bad=True)

    assert len(lines) >= 6  # 5 good + bad rows appended
    widths = {len(line) for line in lines}
    # At least one line is the wrong width (the truncated bad row).
    assert any(w != imports.record_length for w in widths)


def test_render_fwf_lines_is_deterministic(schema_workbook: Path) -> None:
    spec = read_workbook(schema_workbook, "2024-01")["exports_us"]
    a = render_fwf_lines(spec, n_rows=20, seed=7)
    b = render_fwf_lines(spec, n_rows=20, seed=7)
    assert a == b


def test_make_zip_input_roundtrip(tmp_path: Path, schema_workbook: Path) -> None:
    spec = read_workbook(schema_workbook, "2024-01")["exports_nonus"]
    lines = render_fwf_lines(spec, n_rows=3, seed=0)
    zip_path = make_zip_input(tmp_path / "exports_nonus_202401.zip", lines)

    with zipfile.ZipFile(zip_path) as zf:
        members = zf.namelist()
        assert members == ["data.fwf"]
        text = zf.read(members[0]).decode("utf-8")

    roundtrip = text.splitlines()
    assert roundtrip == lines
