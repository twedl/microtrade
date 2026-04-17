# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

`microtrade` turns monthly drops of zipped fixed-width (FWF) trade microdata into per-type Hive-partitioned Parquet datasets. Three trade types with distinct schemas: **imports**, **exports_us**, **exports_nonus**. Raw files arrive as one zip per `(trade_type, year, month)` (e.g. `imports_202404.zip`). Monthly runs reprocess all months YTD of the current year; prior years are frozen.

## Common commands

Project tooling is `uv` on Python 3.12+.

- `uv sync` — install runtime + dev deps from `uv.lock`.
- `uv run microtrade --help` — CLI entry point.
- `uv run pytest` — full test suite with coverage (`--cov=microtrade`).
- `uv run pytest tests/test_pipeline.py::test_name` — single test.
- `uv run ruff format` / `uv run ruff check` — format and lint.
- `uv run mypy src` — strict type check on the package.
- `uv run pre-commit run --all-files` — run all pre-commit hooks locally.

## Architecture (target state — phased build)

Runtime pipeline (`src/microtrade/`):

- `discover.py` — scan input dir, parse `(type, year, month)` from filenames via regex.
- `schema.py` — load versioned YAML specs from `src/microtrade/specs/<type>/v<effective>.yaml`; `resolve(trade_type, period)` picks the spec whose `effective_from` is the latest ≤ `period`.
- `excel_spec.py` — one-shot converter: Excel workbook (one sheet per trade type) → YAML specs. Never called on the hot path; driven by `microtrade import-spec PATH.xlsx --effective-from YYYY-MM`.
- `ingest.py` — streams FWF from the zip via `zipfile.ZipFile.open()` (no extraction), slices lines by `(start, length)`, casts to spec dtypes, yields `pyarrow.RecordBatch`es in `chunk_rows` batches.
- `write.py` — `PartitionWriter` opens `pyarrow.parquet.ParquetWriter` on `year=YYYY/month=MM/part-0.parquet.tmp`, flushes batches one row-group at a time, then atomic-renames on success (idempotent + crash-safe).
- `pipeline.py` — orchestrates discover → schema → ingest → write, appending a per-run JSONL manifest under `output/_manifests/<trade_type>/` (kept outside the dataset root so `pl.scan_parquet(..., hive_partitioning=True)` doesn't trip on non-parquet siblings).
- `cli.py` — Typer app: `ingest`, `import-spec`, `validate-specs`, `inspect`, `version`.

Key invariants:

- Excel is the **upstream** source of truth; committed YAML under `specs/` is the **runtime** contract. Regenerate YAML with `import-spec` when a new workbook lands.
- Three separate datasets on disk (one per trade type). Each Hive-partitioned by `year=/month=/`.
- Per-partition write is idempotent: YTD re-runs overwrite the current year's partitions cleanly.
- Canonical per-dataset schema is the union of all committed specs for that trade type, stored at `output/<trade_type>/_dataset_schema.json`; partitions whose spec lacks a column write typed nulls.
- **Never delete or drop any rows or columns when processing raw data.** If any column cannot be processed, raise an exception and fail the partition. If a single row cannot be processed, write the offending row (with filename, line number, and error) to a separate quality-issue log and continue processing the rest of the file.

## Conventions

- Python 3.12+, PEP 621 metadata in `pyproject.toml`, `uv.lock` checked in.
- `src/` layout. Package code is fully typed (`py.typed` marker shipped).
- Ruff (format + lint) and strict mypy configured in `pyproject.toml`; both must pass before commits.
- Tests under `tests/` mirror the module layout; integration tests build synthetic fixture zips in `tests/conftest.py` rather than checking in binary fixtures.

## Current build state

Core pipeline is feature-complete and covers every documented invariant:

- scaffolding, Excel→YAML converter + `import-spec` CLI, `discover`/`ingest`/`write` (streaming FWF → atomic Parquet), and `pipeline.run` + `microtrade ingest` CLI with per-run JSONL manifests;
- Date dtype (pyarrow `date32`) with `yyyymmdd_to_date` / `yyyymm_to_date` parsers;
- row-level parse failures logged to `<output>/_quality_issues/<trade_type>/<run_id>.jsonl` and skipped, while the surrounding partition still writes;
- canonical per-dataset schema refreshed at `<output>/<trade_type>/_dataset_schema.json` (union of every committed spec, latest dtype wins, nullability widens);
- `excel_spec.read_workbook` handles the real upstream layout: positional sheet→trade_type mapping, autodetected `Position | Description | Length | Type` header, skipped `Blank` filler rows, `Char`/`Num` dtype tokens (Num defaults to Int64);
- reference YAML specs generated from `examples/microdata-layout.xls` ship under `src/microtrade/specs/`.

Remaining: `validate-specs` and `inspect` CLI subcommands are still stubs (exit code 2). Consumers that hive-scan the dataset should use a `**/*.parquet` glob because `_dataset_schema.json` lives at the dataset root per CLAUDE.md's path.
