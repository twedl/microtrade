"""Tests for the `microtrade inspect` CLI subcommand.

These drive the command end-to-end through `CliRunner` against a small
`inspect_env` fixture (spec dir + a single raw zip), separate from the
full ingest fixture so the failure modes exercised here stay focused.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from microtrade import excel_spec, schema
from microtrade.cli import app
from microtrade.config import load_config
from tests._helpers import build_project_config, build_workbook, make_zip_input, render_fwf_lines


@pytest.fixture
def inspect_env(tmp_path: Path) -> dict[str, Path]:
    """Minimal setup for inspect tests: specs for all trade types + one raw zip."""
    spec_dir = tmp_path / "specs"
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    workbook_path = tmp_path / "schema_workbook.xlsx"
    build_workbook(workbook_path)
    cfg_path = build_project_config(tmp_path / "microtrade.yaml", workbook_path, "2020-01")
    workbook_config = load_config(cfg_path).get_workbook(workbook_path)

    specs = excel_spec.read_workbook(workbook_path, workbook_config)
    for trade_type, spec in specs.items():
        schema.save_spec(spec, spec_dir / trade_type / "v2020-01.yaml")

    imports_spec = specs["imports"]
    lines = render_fwf_lines(imports_spec, n_rows=5, seed=0)
    zip_path = make_zip_input(input_dir / "ImportsSheet_202401N.TXT.zip", lines)

    return {"tmp": tmp_path, "spec": spec_dir, "input": input_dir, "zip": zip_path}


def _invoke(args: list[str]) -> object:
    return CliRunner().invoke(app, args)


def test_inspect_annotates_first_rows(inspect_env) -> None:
    result = _invoke(
        ["inspect", str(inspect_env["zip"]), "--spec-dir", str(inspect_env["spec"]), "--rows", "2"]
    )
    assert result.exit_code == 0, result.output
    assert "imports 2024-01" in result.output
    assert "v2020-01" in result.output
    for col_name in ("period", "hs_code", "value_usd"):
        assert col_name in result.output
    assert "line 1" in result.output
    assert "line 2" in result.output


def test_inspect_raw_skips_annotation(inspect_env) -> None:
    result = _invoke(
        [
            "inspect",
            str(inspect_env["zip"]),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--rows",
            "1",
            "--raw",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "line 1" in result.output
    # Raw mode skips the `[start..end] dtype` annotation table.
    assert "Utf8" not in result.output
    assert "Int64" not in result.output


def test_inspect_accepts_plain_fwf_with_overrides(inspect_env, tmp_path: Path) -> None:
    workbook = inspect_env["tmp"] / "schema_workbook.xlsx"
    cfg_path = build_project_config(tmp_path / "cfg.yaml", workbook, "2020-01")
    workbook_config = load_config(cfg_path).get_workbook(workbook)
    imports_spec = excel_spec.read_workbook(workbook, workbook_config)["imports"]
    lines = render_fwf_lines(imports_spec, n_rows=2, seed=0)
    fwf_path = tmp_path / "imports.fwf"
    fwf_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = _invoke(
        [
            "inspect",
            str(fwf_path),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--type",
            "imports",
            "--period",
            "2024-01",
            "--rows",
            "1",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "imports 2024-01" in result.output
    assert "v2020-01" in result.output


def test_inspect_rejects_unresolvable_filename(inspect_env, tmp_path: Path) -> None:
    weird = tmp_path / "not_a_trade_drop.fwf"
    weird.write_text("whatever\n", encoding="utf-8")

    result = _invoke(["inspect", str(weird), "--spec-dir", str(inspect_env["spec"])])
    assert result.exit_code == 2
    assert "filename does not match" in result.output


def test_inspect_errors_when_no_spec(inspect_env) -> None:
    for yaml_file in (inspect_env["spec"] / "imports").rglob("*.yaml"):
        yaml_file.unlink()

    # With imports' spec gone, the ImportsSheet prefix has no mapping, so the
    # filename can't resolve to a (trade_type, period) pair without --type/--period.
    result = _invoke(["inspect", str(inspect_env["zip"]), "--spec-dir", str(inspect_env["spec"])])
    assert result.exit_code == 2
    assert "filename does not match" in result.output

    # With explicit overrides, the resolution path reaches `load_all` and
    # surfaces the missing-spec condition cleanly.
    result = _invoke(
        [
            "inspect",
            str(inspect_env["zip"]),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--type",
            "imports",
            "--period",
            "2024-01",
        ]
    )
    assert result.exit_code == 2
    assert "no specs found" in result.output


def test_inspect_rejects_unknown_trade_type(inspect_env) -> None:
    result = _invoke(
        [
            "inspect",
            str(inspect_env["zip"]),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--type",
            "pretend",
        ]
    )
    assert result.exit_code == 2
    assert "unknown trade_type" in result.output


def test_inspect_rejects_malformed_period_override(inspect_env) -> None:
    result = _invoke(
        [
            "inspect",
            str(inspect_env["zip"]),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--period",
            "2024/01",
        ]
    )
    assert result.exit_code == 2
    assert "period" in result.output.lower()


def test_inspect_rejects_zip_with_multiple_members(inspect_env, tmp_path: Path) -> None:
    bad_zip = tmp_path / "ImportsSheet_202401N.TXT.zip"
    with zipfile.ZipFile(bad_zip, "w") as zf:
        zf.writestr("a.fwf", "first\n")
        zf.writestr("b.fwf", "second\n")

    result = _invoke(["inspect", str(bad_zip), "--spec-dir", str(inspect_env["spec"])])
    assert result.exit_code == 2
    assert "exactly one inner file" in result.output


def test_inspect_rows_zero_prints_spec_only(inspect_env) -> None:
    result = _invoke(
        [
            "inspect",
            str(inspect_env["zip"]),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--rows",
            "0",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "v2020-01" in result.output
    assert "imports 2024-01" in result.output
    assert "line 1" not in result.output


def test_inspect_reports_decode_error(inspect_env, tmp_path: Path) -> None:
    """Non-UTF-8 bytes in a plain FWF exit cleanly rather than dumping a traceback,
    and the error message points at the `--encoding` flag."""
    bad_fwf = tmp_path / "imports.fwf"
    bad_fwf.write_bytes(b"\xff\xfe\xfd invalid bytes for utf-8\n")

    result = _invoke(
        [
            "inspect",
            str(bad_fwf),
            "--spec-dir",
            str(inspect_env["spec"]),
            "--type",
            "imports",
            "--period",
            "2024-01",
            "--rows",
            "1",
            "--encoding",
            "utf-8",
        ]
    )
    assert result.exit_code == 2
    assert "decode" in result.output.lower()
    assert "--encoding" in result.output
