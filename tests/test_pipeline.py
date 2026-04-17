"""End-to-end tests for the ingest pipeline and the `microtrade ingest` CLI.

Each test builds the full upstream chain (synthetic workbook -> YAML specs ->
synthetic FWF zips) in a temp dir so the exercised paths match the real
production workflow.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from microtrade import excel_spec, pipeline, schema
from microtrade.cli import app
from tests._helpers import build_workbook, make_zip_input, render_fwf_lines


@pytest.fixture
def prepared_env(tmp_path: Path) -> dict[str, Path]:
    """Build workbook, YAML specs, and a small tree of raw zips covering 2024+2025."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    spec_dir = tmp_path / "specs"
    workbook_path = tmp_path / "schema_workbook.xlsx"

    input_dir.mkdir()
    build_workbook(workbook_path)

    # Generate and commit YAML specs covering everything 2020+, so a 2023 input
    # is still processable when YTD is disabled.
    specs_by_type = excel_spec.read_workbook(workbook_path, "2020-01")
    for trade_type, spec in specs_by_type.items():
        schema.save_spec(spec, spec_dir / trade_type / "v2020-01.yaml")

    # Synthetic raw zips: 2024 (current YTD), 2023 (prior year, should be ignored).
    rows_per_month = {"imports": 12, "exports_us": 9, "exports_nonus": 6}
    for trade_type, spec in specs_by_type.items():
        for year, month in [(2024, 1), (2024, 2), (2023, 12)]:
            lines = render_fwf_lines(spec, n_rows=rows_per_month[trade_type], seed=month)
            make_zip_input(input_dir / f"{trade_type}_{year}{month:02d}.zip", lines)

    return {
        "tmp": tmp_path,
        "input": input_dir,
        "output": output_dir,
        "spec": spec_dir,
        "rows_per_month": rows_per_month,  # type: ignore[dict-item]
    }


def _config(
    env: dict[str, Path],
    **overrides: object,
) -> pipeline.PipelineConfig:
    base = dict(
        input_dir=env["input"],
        output_dir=env["output"],
        spec_dir=env["spec"],
        ytd=True,
        current_year=2024,
    )
    base.update(overrides)
    return pipeline.PipelineConfig(**base)  # type: ignore[arg-type]


def test_pipeline_ytd_processes_only_current_year(prepared_env) -> None:
    env = prepared_env
    summary = pipeline.run(_config(env))

    # 3 trade types x 2 months in 2024; 2023 is excluded by YTD.
    assert summary.ok_count == 6
    assert summary.failed_count == 0
    for r in summary.results:
        assert r.year == 2024

    # Expected rows per (trade_type, year, month):
    rows_per_month = env["rows_per_month"]  # type: ignore[index]
    for trade_type in schema.TRADE_TYPES:
        for month in (1, 2):
            partition = (
                env["output"] / trade_type / "year=2024" / f"month={month:02d}" / "part-0.parquet"
            )
            assert partition.exists()
        assert not (env["output"] / trade_type / "year=2023").exists()

        df = pl.scan_parquet(
            env["output"] / trade_type / "**/*.parquet", hive_partitioning=True
        ).collect()
        assert df.height == 2 * rows_per_month[trade_type]
        assert set(df["year"].unique().to_list()) == {2024}
        assert set(df["month"].unique().to_list()) == {1, 2}


def test_pipeline_writes_one_manifest_line_per_partition(prepared_env) -> None:
    env = prepared_env
    summary = pipeline.run(_config(env))

    for trade_type in schema.TRADE_TYPES:
        manifests_dir = env["output"] / "_manifests" / trade_type
        manifest_files = list(manifests_dir.glob("*.jsonl"))
        assert len(manifest_files) == 1
        lines = manifest_files[0].read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2  # two months per trade type

        records = [json.loads(line) for line in lines]
        for record in records:
            assert record["trade_type"] == trade_type
            assert record["status"] == "ok"
            assert record["run_id"] == summary.run_id
            assert record["spec_version"] == "2020-01"
            assert record["input_sha256"]
            assert record["rows_written"] > 0


def test_pipeline_rerun_is_idempotent_and_creates_new_manifest(prepared_env) -> None:
    env = prepared_env

    first = pipeline.run(_config(env))
    assert first.failed_count == 0

    before = {p: p.stat().st_size for p in env["output"].rglob("part-0.parquet")}

    second = pipeline.run(_config(env))
    assert second.failed_count == 0
    assert second.run_id != first.run_id

    # Partition files are the same (no duplicates, same content).
    after_files = list(env["output"].rglob("part-0.parquet"))
    assert set(after_files) == set(before.keys())

    # Each trade type now has two manifest files.
    for trade_type in schema.TRADE_TYPES:
        manifests = list((env["output"] / "_manifests" / trade_type).glob("*.jsonl"))
        assert len(manifests) == 2


def test_pipeline_explicit_year_overrides_ytd(prepared_env) -> None:
    env = prepared_env
    summary = pipeline.run(_config(env, ytd=False, year=2023))

    # 3 trade types x 1 month in 2023.
    assert summary.ok_count == 3
    for r in summary.results:
        assert r.year == 2023 and r.month == 12


def test_pipeline_type_filter_limits_work(prepared_env) -> None:
    env = prepared_env
    summary = pipeline.run(_config(env, trade_types=("imports",)))
    assert {r.trade_type for r in summary.results} == {"imports"}
    assert summary.ok_count == 2


def test_pipeline_records_failure_and_continues(prepared_env) -> None:
    env = prepared_env

    # Corrupt one imports zip with a truncated (wrong-length) line - that's a
    # structural error that still fails the whole partition, unlike row-level
    # parse errors which are routed to the quality-issues log.
    target = env["input"] / "imports_202401.zip"
    target.unlink()
    spec = excel_spec.read_workbook(env["tmp"] / "schema_workbook.xlsx", "2020-01")["imports"]
    good = render_fwf_lines(spec, n_rows=3, seed=0)
    make_zip_input(target, [good[0], good[1][:-5]])

    summary = pipeline.run(_config(env))
    # 6 total (3 types x 2 months); exactly 1 failed; others complete.
    assert len(summary.results) == 6
    assert summary.failed_count == 1
    failed = next(r for r in summary.results if r.status == "failed")
    assert failed.trade_type == "imports"
    assert failed.year == 2024 and failed.month == 1
    assert "record_length" in (failed.error or "")

    # Failure is recorded in the manifest.
    manifest = next((env["output"] / "_manifests" / "imports").glob("*.jsonl")).read_text(
        encoding="utf-8"
    )
    records = [json.loads(line) for line in manifest.strip().splitlines()]
    statuses = {(r["year"], r["month"]): r["status"] for r in records}
    assert statuses[(2024, 1)] == "failed"
    assert statuses[(2024, 2)] == "ok"


def test_pipeline_row_level_error_logged_to_quality_issues(prepared_env) -> None:
    """A bad numeric row is skipped, logged to the quality-issues JSONL, and the
    partition still writes successfully for the remaining rows."""
    env = prepared_env

    target = env["input"] / "imports_202401.zip"
    target.unlink()
    spec = excel_spec.read_workbook(env["tmp"] / "schema_workbook.xlsx", "2020-01")["imports"]
    good = render_fwf_lines(spec, n_rows=3, seed=0)
    col = {c.name: c for c in spec.columns}["value_usd"]
    bad_line = good[0][: col.start - 1] + "ABCDEABCDEABCDE" + good[0][col.start - 1 + col.length :]
    make_zip_input(target, [good[0], bad_line, good[1]])

    summary = pipeline.run(_config(env))
    assert summary.failed_count == 0
    partition = next(
        r for r in summary.results if r.trade_type == "imports" and (r.year, r.month) == (2024, 1)
    )
    assert partition.status == "ok"
    assert partition.rows_written == 2
    assert partition.rows_skipped == 1

    issues_file = env["output"] / "_quality_issues" / "imports" / f"{summary.run_id}.jsonl"
    assert issues_file.exists()
    records = [json.loads(line) for line in issues_file.read_text().strip().splitlines()]
    assert len(records) == 1
    assert records[0]["column"] == "value_usd"
    assert records[0]["line_no"] == 2
    assert "cannot parse" in records[0]["error"]


def test_pipeline_writes_canonical_dataset_schema(prepared_env) -> None:
    env = prepared_env
    pipeline.run(_config(env))

    for trade_type in schema.TRADE_TYPES:
        schema_path = env["output"] / trade_type / "_dataset_schema.json"
        assert schema_path.exists()
        payload = json.loads(schema_path.read_text())
        assert payload["trade_type"] == trade_type
        assert payload["spec_versions"] == ["2020-01"]
        names = [c["name"] for c in payload["columns"]]
        # Spec-defined columns present; partition keys not mixed in.
        assert "period" in names
        assert "year" not in names and "month" not in names


def test_pipeline_missing_spec_is_recorded_as_failure(tmp_path: Path) -> None:
    """A raw zip whose trade_type has no YAML spec fails cleanly, not crashes."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    spec_dir = tmp_path / "specs"
    input_dir.mkdir()
    spec_dir.mkdir()

    # Build a workbook + spec only for exports_us, then drop a raw imports zip.
    workbook = tmp_path / "wb.xlsx"
    build_workbook(workbook)
    specs = excel_spec.read_workbook(workbook, "2024-01")
    schema.save_spec(specs["exports_us"], spec_dir / "exports_us" / "v2024-01.yaml")

    imports_spec = specs["imports"]
    lines = render_fwf_lines(imports_spec, n_rows=1, seed=0)
    make_zip_input(input_dir / "imports_202404.zip", lines)

    summary = pipeline.run(
        pipeline.PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            trade_types=("imports",),
            ytd=True,
            current_year=2024,
        )
    )
    assert summary.failed_count == 1
    assert "no specs found" in (summary.results[0].error or "")


def test_cli_ingest_end_to_end(prepared_env) -> None:
    env = prepared_env
    result = CliRunner().invoke(
        app,
        [
            "ingest",
            "--input",
            str(env["input"]),
            "--output",
            str(env["output"]),
            "--spec-dir",
            str(env["spec"]),
            "--current-year",
            "2024",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "6 ok, 0 failed" in result.output

    # Round-trip via polars to confirm the dataset is readable end-to-end.
    # The glob filters out the sibling `_dataset_schema.json` that the pipeline
    # also writes at the trade-type root.
    df = pl.scan_parquet(
        env["output"] / "imports" / "**/*.parquet", hive_partitioning=True
    ).collect()
    assert df.height > 0
    assert {"year", "month"}.issubset(df.columns)


def test_cli_ingest_exits_nonzero_on_failure(prepared_env) -> None:
    env = prepared_env
    # Remove all specs so every partition fails with "no specs found".
    for yaml_file in env["spec"].rglob("*.yaml"):
        yaml_file.unlink()

    result = CliRunner().invoke(
        app,
        [
            "ingest",
            "--input",
            str(env["input"]),
            "--output",
            str(env["output"]),
            "--spec-dir",
            str(env["spec"]),
            "--current-year",
            "2024",
        ],
    )
    assert result.exit_code == 1
    assert "6 failed" in result.output


def test_cli_inspect_annotates_first_rows(prepared_env) -> None:
    env = prepared_env
    zip_path = env["input"] / "imports_202401.zip"

    result = CliRunner().invoke(
        app,
        ["inspect", str(zip_path), "--spec-dir", str(env["spec"]), "--rows", "2"],
    )
    assert result.exit_code == 0, result.output
    assert "imports 2024-01" in result.output
    assert "v2020-01" in result.output
    # Every spec column name appears in the annotated dump.
    for col_name in ("period", "hs_code", "value_usd"):
        assert col_name in result.output
    assert "line 1" in result.output
    assert "line 2" in result.output


def test_cli_inspect_raw_skips_annotation(prepared_env) -> None:
    env = prepared_env
    zip_path = env["input"] / "imports_202401.zip"

    result = CliRunner().invoke(
        app,
        ["inspect", str(zip_path), "--spec-dir", str(env["spec"]), "--rows", "1", "--raw"],
    )
    assert result.exit_code == 0, result.output
    assert "line 1" in result.output
    # Raw mode does not emit the `[start..end] dtype` annotation table.
    assert "Utf8" not in result.output
    assert "Int64" not in result.output


def test_cli_inspect_accepts_plain_fwf_with_overrides(prepared_env, tmp_path: Path) -> None:
    env = prepared_env
    workbook = env["tmp"] / "schema_workbook.xlsx"
    imports_spec = excel_spec.read_workbook(workbook, "2020-01")["imports"]
    lines = render_fwf_lines(imports_spec, n_rows=2, seed=0)
    fwf_path = tmp_path / "imports.fwf"
    fwf_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "inspect",
            str(fwf_path),
            "--spec-dir",
            str(env["spec"]),
            "--type",
            "imports",
            "--period",
            "2024-01",
            "--rows",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "imports 2024-01" in result.output
    assert "v2020-01" in result.output


def test_cli_inspect_rejects_unresolvable_filename(tmp_path: Path, prepared_env) -> None:
    env = prepared_env
    weird = tmp_path / "not_a_trade_drop.fwf"
    weird.write_text("whatever\n", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        ["inspect", str(weird), "--spec-dir", str(env["spec"])],
    )
    assert result.exit_code == 2
    assert "filename does not match" in result.output


def test_cli_inspect_errors_when_no_spec(prepared_env) -> None:
    env = prepared_env
    zip_path = env["input"] / "imports_202401.zip"
    for yaml_file in (env["spec"] / "imports").rglob("*.yaml"):
        yaml_file.unlink()

    result = CliRunner().invoke(
        app,
        ["inspect", str(zip_path), "--spec-dir", str(env["spec"])],
    )
    assert result.exit_code == 2
    assert "no specs found" in result.output
