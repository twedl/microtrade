"""Sanity checks for the test helpers themselves.

We exercise `render_fwf_lines` and `make_zip_input` here so downstream tests
can rely on them without re-validating basic invariants.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from microtrade.excel_spec import read_workbook
from microtrade.ingest import QualityIssue, iter_record_batches_from_path
from microtrade.schema import Column, Spec, load_all
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


def test_render_fwf_lines_honors_record_length_gaps() -> None:
    """A spec with non-contiguous columns (gap bytes between start positions)
    produces lines of exactly `record_length` characters, with the gap bytes
    left as spaces so real-workbook ``Blank`` filler rows round-trip cleanly."""
    spec = Spec(
        trade_type="imports",
        version="2024-01",
        effective_from="2024-01",
        record_length=20,
        columns=(
            Column(name="a", start=1, length=5, dtype="Utf8"),
            # bytes 6-7 are a Blank-style filler gap
            Column(name="b", start=8, length=5, dtype="Utf8"),
            # bytes 13-20 are a trailing filler gap
        ),
    )

    lines = render_fwf_lines(spec, n_rows=3, seed=0)
    assert len(lines) == 3
    for line in lines:
        assert len(line) == spec.record_length
        assert line[5:7] == "  "  # inter-column gap
        assert line[12:20] == " " * 8  # trailing gap


def test_render_fwf_lines_roundtrips_against_shipping_spec(tmp_path: Path) -> None:
    """Lines generated from the committed real-workbook `imports` spec ingest
    cleanly - no quality issues, no record_length mismatch, every row counted."""
    spec_dir = Path("src/microtrade/specs")
    spec = load_all(spec_dir, "imports")[0]

    lines = render_fwf_lines(spec, n_rows=5, seed=0)
    for line in lines:
        assert len(line) == spec.record_length

    zip_path = make_zip_input(tmp_path / "imports_202404.zip", lines)

    captured: list[QualityIssue] = []
    batches = list(iter_record_batches_from_path(zip_path, spec, on_quality_issue=captured.append))
    assert captured == [], f"unexpected quality issues: {captured}"
    assert sum(b.num_rows for b in batches) == 5
