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
from microtrade.config import load_config
from tests._helpers import (
    SHEET_TITLES,
    build_project_config,
    build_workbook,
    input_filename,
    make_zip_input,
    render_fwf_lines,
)

SHEET_FOR_TRADE_TYPE: dict[str, str] = SHEET_TITLES


def _input_filename(trade_type: str, year: int, month: int, flag: str = "N") -> str:
    return input_filename(SHEET_FOR_TRADE_TYPE[trade_type], year, month, flag)


def _workbook_config(tmp_path: Path, workbook: Path, effective_from: str, **kwargs):
    """Build a microtrade.yaml next to `workbook` and return its WorkbookConfig."""
    cfg_path = build_project_config(
        tmp_path / f"config_{effective_from}.yaml", workbook, effective_from, **kwargs
    )
    return load_config(cfg_path).get_workbook(workbook)


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
    workbook_cfg = _workbook_config(tmp_path, workbook_path, "2020-01")
    specs_by_type = excel_spec.read_workbook(workbook_path, workbook_cfg)
    for trade_type, spec in specs_by_type.items():
        schema.save_spec(spec, spec_dir / trade_type / "v2020-01.yaml")

    # Synthetic raw zips: 2024 (current YTD), 2023 (prior year, should be ignored).
    rows_per_month = {"imports": 12, "exports_us": 9, "exports_nonus": 6}
    for trade_type, spec in specs_by_type.items():
        for year, month in [(2024, 1), (2024, 2), (2023, 12)]:
            lines = render_fwf_lines(spec, n_rows=rows_per_month[trade_type], seed=month)
            make_zip_input(input_dir / _input_filename(trade_type, year, month), lines)

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
    target = env["input"] / _input_filename("imports", 2024, 1)
    target.unlink()
    workbook = env["tmp"] / "schema_workbook.xlsx"
    spec = excel_spec.read_workbook(workbook, _workbook_config(env["tmp"], workbook, "2020-01"))[
        "imports"
    ]
    good = render_fwf_lines(spec, n_rows=3, seed=0)
    make_zip_input(target, [good[0], good[1][:-5]])

    summary = pipeline.run(_config(env))
    # 6 total (3 types x 2 months); exactly 1 failed; others complete.
    assert len(summary.results) == 6
    assert summary.failed_count == 1
    failed = next(r for r in summary.results if r.status == "failed")
    assert failed.trade_type == "imports"
    assert failed.year == 2024 and failed.month == 1
    assert "truncated" in (failed.error or "")

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

    target = env["input"] / _input_filename("imports", 2024, 1)
    target.unlink()
    workbook = env["tmp"] / "schema_workbook.xlsx"
    spec = excel_spec.read_workbook(workbook, _workbook_config(env["tmp"], workbook, "2020-01"))[
        "imports"
    ]
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


def _make_bad_imports_zip(env: dict[str, Path], n_good: int, n_bad: int) -> Path:
    """Replace the 2024-01 imports zip with `n_good` valid rows and `n_bad`
    rows that have garbage bytes in the non-nullable `value_usd` column."""
    target = env["input"] / _input_filename("imports", 2024, 1)
    target.unlink()
    workbook = env["tmp"] / "schema_workbook.xlsx"
    spec = excel_spec.read_workbook(workbook, _workbook_config(env["tmp"], workbook, "2020-01"))[
        "imports"
    ]
    col = {c.name: c for c in spec.columns}["value_usd"]
    good = render_fwf_lines(spec, n_rows=n_good + n_bad, seed=0)
    lines = list(good[:n_good])
    for src in good[n_good : n_good + n_bad]:
        lines.append(src[: col.start - 1] + "ABCDEABCDEABCDE" + src[col.start - 1 + col.length :])
    make_zip_input(target, lines)
    return target


def test_pipeline_quality_log_honors_cap(prepared_env) -> None:
    """With `max_quality_issues=3`, only the first 3 bad rows land in the
    JSONL file; `rows_skipped` in the manifest still reflects the full count."""
    env = prepared_env
    _make_bad_imports_zip(env, n_good=2, n_bad=10)

    summary = pipeline.run(_config(env, max_quality_issues=3, max_skip_rate=1.0))
    partition = next(
        r for r in summary.results if r.trade_type == "imports" and (r.year, r.month) == (2024, 1)
    )
    assert partition.status == "ok"
    assert partition.rows_written == 2
    assert partition.rows_skipped == 10
    assert partition.rows_skipped_logged == 3

    issues_file = env["output"] / "_quality_issues" / "imports" / f"{summary.run_id}.jsonl"
    records = [json.loads(line) for line in issues_file.read_text().strip().splitlines()]
    assert len(records) == 3

    manifest = next((env["output"] / "_manifests" / "imports").glob("*.jsonl")).read_text()
    entries = [json.loads(line) for line in manifest.strip().splitlines()]
    imports_jan = next(e for e in entries if (e["year"], e["month"]) == (2024, 1))
    assert imports_jan["rows_skipped"] == 10
    assert imports_jan["rows_skipped_logged"] == 3


def test_pipeline_aborts_when_skip_rate_exceeded(prepared_env) -> None:
    """Partition with > max_skip_rate bad rows fails with an IngestError,
    the temp parquet is cleaned up, and the quality log still records the bad rows."""
    env = prepared_env
    _make_bad_imports_zip(env, n_good=1, n_bad=9)

    summary = pipeline.run(_config(env, max_skip_rate=0.5))
    failed = next(r for r in summary.results if r.status == "failed")
    assert failed.trade_type == "imports"
    assert failed.year == 2024 and failed.month == 1
    assert "max_skip_rate" in (failed.error or "")

    # The aborted partition's temp parquet is not left behind.
    partition_dir = env["output"] / "imports" / "year=2024" / "month=01"
    if partition_dir.exists():
        assert list(partition_dir.iterdir()) == []

    # Quality log still has the skipped rows for post-mortem.
    issues_file = env["output"] / "_quality_issues" / "imports" / f"{summary.run_id}.jsonl"
    assert issues_file.exists()
    records = [json.loads(line) for line in issues_file.read_text().strip().splitlines()]
    assert len(records) == 9


def test_pipeline_skip_rate_disabled_with_one_point_zero(prepared_env) -> None:
    """`max_skip_rate=1.0` preserves pre-cap behavior - even mostly-bad
    partitions still write successfully (modulo the quality log)."""
    env = prepared_env
    _make_bad_imports_zip(env, n_good=1, n_bad=9)

    summary = pipeline.run(_config(env, max_skip_rate=1.0))
    partition = next(
        r for r in summary.results if r.trade_type == "imports" and (r.year, r.month) == (2024, 1)
    )
    assert partition.status == "ok"
    assert partition.rows_written == 1
    assert partition.rows_skipped == 9


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
    """A raw zip whose period predates every committed spec fails cleanly,
    not crashes. (Files whose sheet has no spec at all are silently skipped
    by discover - that's covered by the discover tests.)"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    spec_dir = tmp_path / "specs"
    input_dir.mkdir()
    spec_dir.mkdir()

    # Spec only kicks in from 2025-01, so a 2024-04 input has no applicable spec.
    workbook = tmp_path / "wb.xlsx"
    build_workbook(workbook)
    specs = excel_spec.read_workbook(workbook, _workbook_config(tmp_path, workbook, "2025-01"))
    schema.save_spec(specs["imports"], spec_dir / "imports" / "v2025-01.yaml")

    imports_spec = specs["imports"]
    lines = render_fwf_lines(imports_spec, n_rows=1, seed=0)
    make_zip_input(input_dir / _input_filename("imports", 2024, 4), lines)

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
    assert "no spec applies" in (summary.results[0].error or "")


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
    # Bump every spec's effective_from past the input year so discover still
    # finds the files (patterns intact) but `resolve` rejects each partition.
    workbook = env["tmp"] / "schema_workbook.xlsx"
    future_specs = excel_spec.read_workbook(
        workbook, _workbook_config(env["tmp"], workbook, "2099-01")
    )
    for yaml_file in env["spec"].rglob("*.yaml"):
        yaml_file.unlink()
    for trade_type, spec in future_specs.items():
        schema.save_spec(spec, env["spec"] / trade_type / "v2099-01.yaml")

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
