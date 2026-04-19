"""Tests for the Excel -> Spec converter and the `import-spec` CLI."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from microtrade import schema
from microtrade.cli import app
from microtrade.config import WorkbookConfig, load_config
from microtrade.excel_spec import derive_workbook_id, normalize_dtype, read_workbook
from microtrade.schema import TRADE_TYPES, SpecError, load_spec
from tests._helpers import (
    build_project_config,
    build_workbook,
    default_filename_pattern,
)


def test_normalize_dtype_handles_common_aliases() -> None:
    assert normalize_dtype("string") == "Utf8"
    assert normalize_dtype(" INT ") == "Int64"
    assert normalize_dtype("float64") == "Float64"
    assert normalize_dtype("date") == "Date"
    assert normalize_dtype("Char") == "Utf8"
    assert normalize_dtype("Num") == "Int64"


def test_normalize_dtype_rejects_unknown() -> None:
    with pytest.raises(SpecError, match="unrecognized dtype"):
        normalize_dtype("chronology")


def test_read_workbook_produces_spec_per_trade_type(
    schema_workbook: Path, workbook_config: WorkbookConfig
) -> None:
    specs = read_workbook(schema_workbook, workbook_config)
    assert set(specs) == set(TRADE_TYPES)

    imports = specs["imports"]
    assert imports.trade_type == "imports"
    assert imports.effective_from == "2020-01"
    assert imports.record_length == 53
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
    assert imports.source.sheet == "ImportsSheet"
    assert imports.source.filename_pattern == default_filename_pattern("ImportsSheet")
    assert imports.derived == (("year", "year(period)"), ("month", "month(period)"))

    exports_nonus = specs["exports_nonus"]
    assert [c.dtype for c in exports_nonus.columns] == ["Utf8", "Utf8", "Utf8", "Float64"]


def test_read_workbook_rejects_missing_sheet(tmp_path: Path) -> None:
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet("only_one")
    ws.append(["Position", "Description", "Length", "Type"])
    ws.append([1, "a", 5, "Char"])
    wb_path = tmp_path / "wb.xlsx"
    wb.save(wb_path)

    # Config references a sheet that isn't in the workbook.
    config_path = build_project_config(tmp_path / "microtrade.yaml", wb_path, "2024-01")
    workbook_config = load_config(config_path).get_workbook(wb_path)

    with pytest.raises(SpecError, match="does not contain sheet"):
        read_workbook(wb_path, workbook_config)


def test_read_workbook_skips_blank_filler_rows(tmp_path: Path) -> None:
    """`Blank` rows are FWF padding bytes - they do not become columns, but
    they extend `record_length` so it matches the actual line width."""
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    sheet_title = "ImportsSheet"
    ws = wb.create_sheet(sheet_title)
    ws.append(["layout imports", None, None, None])
    ws.append(["Position", "Description", "Length", "Type"])
    ws.append([1, "code", 5, "Char"])
    ws.append([6, "Blank", 1, "Char"])  # filler
    ws.append([7, "value", 10, "Num"])
    ws.append([17, "Blank", 3, "Char"])  # trailing filler extends record_length
    wb_path = tmp_path / "wb.xlsx"
    wb.save(wb_path)

    config_path = build_project_config(
        tmp_path / "microtrade.yaml",
        wb_path,
        "2024-01",
        sheet_titles={"imports": sheet_title},
    )
    workbook_config = load_config(config_path).get_workbook(wb_path)

    specs = read_workbook(wb_path, workbook_config)
    imports = specs["imports"]
    assert [c.name for c in imports.columns] == ["code", "value"]
    assert [c.dtype for c in imports.columns] == ["Utf8", "Int64"]
    assert imports.record_length == 19


def test_read_workbook_bakes_effective_to(tmp_path: Path) -> None:
    workbook = build_workbook(tmp_path / "wb.xlsx")
    config_path = build_project_config(
        tmp_path / "microtrade.yaml", workbook, "2020-01", effective_to="2023-12"
    )
    workbook_config = load_config(config_path).get_workbook(workbook)

    specs = read_workbook(workbook, workbook_config)
    for spec in specs.values():
        assert spec.effective_to == "2023-12"


def test_read_workbook_default_workbook_id_derives_from_filename(
    schema_workbook: Path, workbook_config: WorkbookConfig
) -> None:
    specs = read_workbook(schema_workbook, workbook_config)
    for spec in specs.values():
        assert spec.source is not None
        # `schema_workbook.xlsx` -> "schema" (config does not set workbook_id).
        assert spec.source.workbook_id == "schema"


def test_read_workbook_config_workbook_id_wins(tmp_path: Path) -> None:
    workbook = build_workbook(tmp_path / "wb.xlsx")
    config_path = build_project_config(
        tmp_path / "microtrade.yaml", workbook, "2020-01", workbook_id="XYZ12345"
    )
    workbook_config = load_config(config_path).get_workbook(workbook)

    specs = read_workbook(workbook, workbook_config)
    for spec in specs.values():
        assert spec.source is not None
        assert spec.source.workbook_id == "XYZ12345"


# --- import-spec CLI --------------------------------------------------------


def _invoke_import(
    runner: CliRunner, workbook: Path, config_path: Path, out_dir: Path, *extra: str
):
    return runner.invoke(
        app,
        [
            "import-spec",
            str(workbook),
            "--config",
            str(config_path),
            "--out",
            str(out_dir),
            *extra,
        ],
    )


def test_import_spec_cli_writes_yaml(
    schema_workbook: Path, project_config_path: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "specs"
    result = _invoke_import(CliRunner(), schema_workbook, project_config_path, out_dir)
    assert result.exit_code == 0, result.output

    for trade_type in TRADE_TYPES:
        path = out_dir / trade_type / "v2020-01.yaml"
        assert path.exists()
        spec = load_spec(path)
        assert spec.trade_type == trade_type
        assert spec.effective_from == "2020-01"
        assert spec.source is not None
        assert spec.source.filename_pattern is not None


def test_import_spec_cli_prints_diff_against_previous(
    schema_workbook: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "specs"
    runner = CliRunner()

    cfg_v1 = build_project_config(tmp_path / "v1.yaml", schema_workbook, "2020-01")
    first = _invoke_import(runner, schema_workbook, cfg_v1, out_dir)
    assert first.exit_code == 0

    cfg_v2 = build_project_config(tmp_path / "v2.yaml", schema_workbook, "2025-01")
    second = _invoke_import(runner, schema_workbook, cfg_v2, out_dir)
    assert second.exit_code == 0, second.output
    assert "diff vs v2020-01" in second.output


def test_import_spec_cli_refuses_overwrite_without_force(
    schema_workbook: Path, project_config_path: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "specs"
    runner = CliRunner()

    first = _invoke_import(runner, schema_workbook, project_config_path, out_dir)
    assert first.exit_code == 0

    result = _invoke_import(runner, schema_workbook, project_config_path, out_dir)
    assert result.exit_code == 1
    assert "already exists" in result.output

    forced = _invoke_import(runner, schema_workbook, project_config_path, out_dir, "--force")
    assert forced.exit_code == 0


def test_import_spec_cli_errors_when_workbook_missing_from_config(
    schema_workbook: Path, tmp_path: Path
) -> None:
    """If the config lists no entry for this workbook, fail with a clear message."""
    other_workbook = build_workbook(tmp_path / "other.xlsx")
    config_path = build_project_config(tmp_path / "microtrade.yaml", other_workbook, "2020-01")
    out_dir = tmp_path / "specs"

    result = _invoke_import(CliRunner(), schema_workbook, config_path, out_dir)
    assert result.exit_code == 2
    assert "not listed in the project config" in result.output


# --- validate-specs ---------------------------------------------------------


def _seed_valid_specs(
    spec_dir: Path, schema_workbook: Path, workbook_config: WorkbookConfig
) -> None:
    specs = read_workbook(schema_workbook, workbook_config)
    for trade_type, spec in specs.items():
        schema.save_spec(spec, spec_dir / trade_type / f"v{spec.effective_from}.yaml")


def test_validate_specs_ok_on_clean_tree(schema_workbook: Path, tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    cfg_v1 = build_project_config(
        tmp_path / "v1.yaml", schema_workbook, "2020-01", effective_to="2023-12"
    )
    cfg_v2 = build_project_config(tmp_path / "v2.yaml", schema_workbook, "2024-01")
    _seed_valid_specs(spec_dir, schema_workbook, load_config(cfg_v1).get_workbook(schema_workbook))
    _seed_valid_specs(spec_dir, schema_workbook, load_config(cfg_v2).get_workbook(schema_workbook))

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 0, result.output
    assert "OK (3 trade types, 6 specs)" in result.output
    assert "imports:" in result.output
    assert "v2020-01" in result.output
    assert "v2024-01" in result.output
    assert "no column changes" in result.output


def test_validate_specs_reports_invalid_yaml(
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook, workbook_config)
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
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook, workbook_config)
    (spec_dir / "imports" / "v2020-01.yaml").rename(spec_dir / "imports" / "v2020-02.yaml")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "does not match effective_from" in result.output


def test_validate_specs_flags_window_overlap(schema_workbook: Path, tmp_path: Path) -> None:
    """Two specs whose [effective_from, effective_to] windows overlap must fail."""
    spec_dir = tmp_path / "specs"
    cfg_early = build_project_config(
        tmp_path / "early.yaml", schema_workbook, "2020-01", effective_to="2024-12"
    )
    cfg_late = build_project_config(tmp_path / "late.yaml", schema_workbook, "2024-06")
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_early).get_workbook(schema_workbook)
    )
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_late).get_workbook(schema_workbook)
    )

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "overlapping" in result.output.lower()


def test_validate_specs_flags_gap_between_windows(schema_workbook: Path, tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    cfg_early = build_project_config(
        tmp_path / "early.yaml", schema_workbook, "2020-01", effective_to="2022-12"
    )
    cfg_late = build_project_config(tmp_path / "late.yaml", schema_workbook, "2024-01")
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_early).get_workbook(schema_workbook)
    )
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_late).get_workbook(schema_workbook)
    )

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "gap" in result.output.lower()


def test_validate_specs_flags_open_ended_before_later_spec(
    schema_workbook: Path, tmp_path: Path
) -> None:
    """An earlier spec without `effective_to` must not coexist with a later spec -
    the active window would be ambiguous."""
    spec_dir = tmp_path / "specs"
    cfg_early = build_project_config(tmp_path / "early.yaml", schema_workbook, "2020-01")
    cfg_late = build_project_config(tmp_path / "late.yaml", schema_workbook, "2024-01")
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_early).get_workbook(schema_workbook)
    )
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_late).get_workbook(schema_workbook)
    )

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 1
    assert "open-ended" in result.output.lower()


def test_validate_specs_reports_dtype_conflict_across_versions(
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    # First spec with a closed window.
    cfg_early = build_project_config(
        tmp_path / "early.yaml", schema_workbook, "2020-01", effective_to="2024-05"
    )
    _seed_valid_specs(
        spec_dir, schema_workbook, load_config(cfg_early).get_workbook(schema_workbook)
    )
    # Second spec, later window, but we rewrite its `value_usd` dtype.
    cfg_late = build_project_config(tmp_path / "late.yaml", schema_workbook, "2024-06")
    wbcfg_late = load_config(cfg_late).get_workbook(schema_workbook)
    v2 = read_workbook(schema_workbook, wbcfg_late)["imports"]
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
        effective_to=v2.effective_to,
        record_length=v2.record_length,
        columns=new_cols,
        source=v2.source,
        derived=v2.derived,
        partition_by=v2.partition_by,
    )
    schema.save_spec(v2_conflicting, spec_dir / "imports" / "v2024-06.yaml")
    # Also seed the rest so validate-specs has something to compare.
    for trade_type, spec in read_workbook(schema_workbook, wbcfg_late).items():
        if trade_type == "imports":
            continue
        schema.save_spec(spec, spec_dir / trade_type / "v2024-06.yaml")

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


def test_validate_specs_continues_across_trade_types(
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook, workbook_config)
    (spec_dir / "imports" / "v2020-01.yaml").write_text(
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
    assert "overlaps" in result.output
    assert "exports_us:" in result.output
    assert "exports_nonus:" in result.output


def test_validate_specs_ignores_non_v_prefixed_yaml(
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    _seed_valid_specs(spec_dir, schema_workbook, workbook_config)
    (spec_dir / "imports" / "backup.yaml").write_text("not a spec\n", encoding="utf-8")
    (spec_dir / "imports" / "README.yaml").write_text("also not a spec\n", encoding="utf-8")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 0, result.output
    assert "OK" in result.output


def test_validate_specs_pluralizes_summary_for_singletons(
    schema_workbook: Path, workbook_config: WorkbookConfig, tmp_path: Path
) -> None:
    spec_dir = tmp_path / "specs"
    imports_spec = read_workbook(schema_workbook, workbook_config)["imports"]
    schema.save_spec(imports_spec, spec_dir / "imports" / "v2020-01.yaml")

    result = CliRunner().invoke(app, ["validate-specs", "--spec-dir", str(spec_dir)])
    assert result.exit_code == 0, result.output
    assert "OK (1 trade type, 1 spec)" in result.output


def test_derive_workbook_id_strips_first_underscore_chunk() -> None:
    assert derive_workbook_id("XYZ12345_Record_Layout.xls") == "XYZ12345"
    assert derive_workbook_id("ABC-1234567_Record_Layout.xls") == "ABC-1234567"
    assert derive_workbook_id("plainname.xlsx") == "plainname"
