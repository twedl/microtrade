"""Tests for the Excel -> Spec converter and the `import-spec` CLI."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from microtrade import schema
from microtrade.cli import app
from microtrade.excel_spec import normalize_dtype, read_workbook
from microtrade.schema import TRADE_TYPES, SpecError, load_spec


def test_normalize_dtype_handles_common_aliases() -> None:
    assert normalize_dtype("string") == "Utf8"
    assert normalize_dtype(" INT ") == "Int64"
    assert normalize_dtype("float64") == "Float64"
    assert normalize_dtype("date") == "Date"
    # Real-workbook tokens.
    assert normalize_dtype("Char") == "Utf8"
    assert normalize_dtype("Num") == "Int64"


def test_normalize_dtype_rejects_unknown() -> None:
    with pytest.raises(SpecError, match="unrecognized dtype"):
        normalize_dtype("chronology")


def test_read_workbook_produces_spec_per_trade_type(schema_workbook: Path) -> None:
    specs = read_workbook(schema_workbook, "2024-01")
    assert set(specs) == set(TRADE_TYPES)

    imports = specs["imports"]
    assert imports.trade_type == "imports"
    assert imports.effective_from == "2024-01"
    assert imports.record_length == 53  # 39 + 15 - 1 per synthetic sheet
    assert [c.name for c in imports.columns] == [
        "period",
        "hs_code",
        "country_coo",
        "district_entry",
        "value_usd",
        "qty_kg",
    ]
    assert imports.source is not None
    assert imports.source.workbook == schema_workbook.name
    # Sheets map positionally now; the synthetic builder uses `SHEET001` etc.
    assert imports.source.sheet == "SHEET001"
    assert imports.derived == (("year", "year(period)"), ("month", "month(period)"))

    exports_nonus = specs["exports_nonus"]
    assert [c.dtype for c in exports_nonus.columns] == ["Utf8", "Utf8", "Utf8", "Float64"]


def test_read_workbook_rejects_invalid_period(schema_workbook: Path) -> None:
    with pytest.raises(SpecError):
        read_workbook(schema_workbook, "2024/01")


def test_read_workbook_rejects_too_few_sheets(tmp_path: Path) -> None:
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet("only_one")
    ws.append(["Position", "Description", "Length", "Type"])
    ws.append([1, "a", 5, "Char"])
    path = tmp_path / "wb.xlsx"
    wb.save(path)

    with pytest.raises(SpecError, match="need at least"):
        read_workbook(path, "2024-01")


def test_read_workbook_skips_blank_filler_rows(tmp_path: Path) -> None:
    """`Blank` rows are FWF padding bytes - they do not become columns, but
    they extend `record_length` so it matches the actual line width."""
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    for sheet_idx, name in enumerate(("imports", "exports_us", "exports_nonus"), start=1):
        ws = wb.create_sheet(f"SHEET{sheet_idx:03d}")
        ws.append([f"layout {name}", None, None, None])
        ws.append(["Position", "Description", "Length", "Type"])
        ws.append([1, "code", 5, "Char"])
        ws.append([6, "Blank", 1, "Char"])  # filler
        ws.append([7, "value", 10, "Num"])
        ws.append([17, "Blank", 3, "Char"])  # trailing filler extends record_length
    path = tmp_path / "wb.xlsx"
    wb.save(path)

    specs = read_workbook(path, "2024-01")
    imports = specs["imports"]
    assert [c.name for c in imports.columns] == ["code", "value"]
    assert [c.dtype for c in imports.columns] == ["Utf8", "Int64"]
    assert imports.record_length == 19  # trailing Blank pushes it past `value`'s end (16)


def test_import_spec_cli_writes_yaml(schema_workbook: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "specs"
    result = CliRunner().invoke(
        app,
        [
            "import-spec",
            str(schema_workbook),
            "--effective-from",
            "2024-01",
            "--out",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    for trade_type in TRADE_TYPES:
        path = out_dir / trade_type / "v2024-01.yaml"
        assert path.exists()
        spec = load_spec(path)
        assert spec.trade_type == trade_type
        assert spec.effective_from == "2024-01"


def test_import_spec_cli_prints_diff_against_previous(
    schema_workbook: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "specs"
    runner = CliRunner()

    first = runner.invoke(
        app,
        ["import-spec", str(schema_workbook), "--effective-from", "2024-01", "--out", str(out_dir)],
    )
    assert first.exit_code == 0

    # Re-run for a later effective-from; should succeed and report no diff.
    second = runner.invoke(
        app,
        ["import-spec", str(schema_workbook), "--effective-from", "2025-01", "--out", str(out_dir)],
    )
    assert second.exit_code == 0, second.output
    assert "diff vs v2024-01" in second.output


def test_import_spec_cli_refuses_overwrite_without_force(
    schema_workbook: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "specs"
    runner = CliRunner()

    runner.invoke(
        app,
        ["import-spec", str(schema_workbook), "--effective-from", "2024-01", "--out", str(out_dir)],
    )
    result = runner.invoke(
        app,
        ["import-spec", str(schema_workbook), "--effective-from", "2024-01", "--out", str(out_dir)],
    )
    assert result.exit_code == 1
    assert "already exists" in result.output

    forced = runner.invoke(
        app,
        [
            "import-spec",
            str(schema_workbook),
            "--effective-from",
            "2024-01",
            "--out",
            str(out_dir),
            "--force",
        ],
    )
    assert forced.exit_code == 0


# --- validate-specs ---------------------------------------------------------


def _seed_valid_specs(spec_dir: Path, schema_workbook: Path) -> None:
    """Helper: drop v2020-01 specs for all three trade types into `spec_dir`."""
    specs = read_workbook(schema_workbook, "2020-01")
    for trade_type, spec in specs.items():
        schema.save_spec(spec, spec_dir / trade_type / "v2020-01.yaml")


def test_validate_specs_ok_on_clean_tree(schema_workbook: Path, tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook)
    # Add a second version so the command exercises the diff path.
    v2_specs = read_workbook(schema_workbook, "2024-06")
    for trade_type, spec in v2_specs.items():
        schema.save_spec(spec, spec_dir / trade_type / "v2024-06.yaml")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 0, result.output
    assert "OK" in result.output
    assert "imports:" in result.output
    assert "v2020-01" in result.output
    assert "v2024-06" in result.output
    # Identical specs => "no column changes" in the diff section.
    assert "no column changes" in result.output


def test_validate_specs_reports_invalid_yaml(schema_workbook: Path, tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook)
    # Overwrite the imports spec with one that has overlapping columns.
    bad = spec_dir / "imports" / "v2020-01.yaml"
    bad.write_text(
        "trade_type: imports\n"
        "version: '2020-01'\n"
        "effective_from: '2020-01'\n"
        "record_length: 10\n"
        "columns:\n"
        "  - {name: a, start: 1, length: 5, dtype: Utf8}\n"
        "  - {name: b, start: 4, length: 5, dtype: Utf8}\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "FAIL" in result.output
    assert "overlaps" in result.output
    assert str(bad) in result.output


def test_validate_specs_rejects_filename_version_mismatch(
    schema_workbook: Path, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook)
    # Rename the imports YAML so the filename version disagrees with effective_from.
    (spec_dir / "imports" / "v2020-01.yaml").rename(spec_dir / "imports" / "v2020-02.yaml")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "does not match effective_from" in result.output


def test_validate_specs_reports_dtype_conflict_across_versions(
    schema_workbook: Path, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook)
    # Write a v2024-06 for imports that conflicts on `value_usd` dtype (Int64 -> Float64).
    v2 = read_workbook(schema_workbook, "2024-06")["imports"]
    new_cols = tuple(
        schema.Column(
            name=c.name,
            start=c.start,
            length=c.length,
            dtype="Float64" if c.name == "value_usd" else c.dtype,
            nullable=c.nullable,
            parse=c.parse,
            description=c.description,
        )
        for c in v2.columns
    )
    v2_conflicting = schema.Spec(
        trade_type=v2.trade_type,
        version=v2.version,
        effective_from=v2.effective_from,
        record_length=v2.record_length,
        columns=new_cols,
        source=v2.source,
        derived=v2.derived,
        partition_by=v2.partition_by,
    )
    schema.save_spec(v2_conflicting, spec_dir / "imports" / "v2024-06.yaml")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "canonical-schema conflict" in result.output
    assert "value_usd" in result.output


def test_validate_specs_empty_tree_exits_nonzero(tmp_path: Path) -> None:
    spec_dir = tmp_path / "empty-specs"
    spec_dir.mkdir()
    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "no specs found" in result.output
