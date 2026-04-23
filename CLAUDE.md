# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

Context and conventions for this project. Read before generating code.

## Project purpose

Thin wrapper around the `microtrade` Python library. Runs as a Kubernetes
`CronJob`. Each run:

1. Generates microtrade spec YAMLs from workbook files (stage 1).
2. Ingests raw zipped data files into hive-partitioned parquet (stage 2).
3. Records state to local disk so subsequent runs skip already-done work.

All heavy lifting (parsing workbooks, reading raw files, writing parquet) is
done by `microtrade`. This project is planning, dispatch, and state tracking.

**Raw drop semantics (microtrade's model):** each raw file is a YTD snapshot
for its `(trade_type, year)`. A single `..._202406N.TXT.zip` file covers
Jan–Jun of 2024; microtrade partitions rows internally via each sheet's
`routing_column` (a row-level Date). One raw file therefore produces many
month partitions, and the latest snapshot per `(trade_type, year)` wins at
ingest time. The unit of reprocessing in this project is
`(trade_type, year)`, not a single month.

## Stack

- Python 3.12+ (microtrade requires `>=3.12`)
- `loguru` for logging
- `pydantic-settings` for project config (env + `.env`)
- `pydantic` v2 for validating `microtrade.yaml`
- `pyyaml` for YAML parsing
- `microtrade` (imported as a library, not shelled out)
- No orchestrator — k8s `CronJob` is the scheduler

No Airflow, Prefect, Dagster, Datadog, or any long-running server. No network
services. State lives on disk only.

## Code conventions

- Type hints on all public functions.
- Functions over classes unless state or a protocol is needed.
- Keep modules small and single-purpose.
- Fail fast: validate config at startup, raise on unexpected input.
- No silent excepts. Log and re-raise or log and continue with explicit intent.

## Logging

Use `loguru` with its defaults. No custom sinks, no structured JSON, no
aggregator integration.

```python
from loguru import logger
logger.info("message")
```

Add a rotating file sink only if the user asks for persistent logs.

## Config

Two separate YAMLs, two roles:

- **`config.yaml` + env vars** — *where*: paths, directories.
  Loaded via `pydantic-settings`. Changes per environment.
- **`microtrade.yaml`** — *how*: microtrade's own domain config (workbooks,
  sheets, filename patterns, column casts/parses/renames). Read directly by
  microtrade. This project *also* parses it with pydantic models, but only
  for planning (matching raw files to partitions).

Environment variable prefix: `MT_`. Example: `MT_RAW_DIR=/data/raw`.

### `config.yaml` fields

```
microtrade_yaml: path to microtrade.yaml
workbooks_dir: directory of .xls workbook files (stage 1 input)
raw_dir: directory of raw zipped data files (stage 2 input)
specs_dir: directory where microtrade writes generated spec YAMLs
processed_dir: directory for hive-partitioned parquet output
spec_manifests_dir: state dir for stage 1
raw_manifests_dir: state dir for stage 2 (one JSON per raw file)
upstream_raw_dir: remote source (provider drops here, periodically deletes)
raw_remote_dir: our permanent archive (mirror of upstream + version history)
```

## State tracking

All state lives on disk as JSON files (one per tracked item). No SQLite, no
JSONL append log, no database. Rationale: simplest possible model, easy to
inspect with `cat`/`jq`, no concurrency concerns for a single-pod cronjob.

Write atomically: write to `path.tmp`, then `os.replace(tmp, path)`.

### Two manifest directories

```
data/manifests/
  specs/        # one JSON per workbook file
  raw/          # one JSON per raw file
```

### Spec manifest (stage 1) fields

- `workbook_name`
- `workbook_hash` (content hash of the workbook file)
- `microtrade_hash` (content hash of `microtrade.yaml` at time of generation)
- `specs_written` (list of output spec file paths)
- `processed_at` (ISO-8601 UTC)

### Raw manifest (stage 2) fields

- `raw_name`
- `raw_hash` (content hash of the raw zip)
- `microtrade_hash` (content hash of `microtrade.yaml` at time of ingest)
- `trade_type`, `year`, `month`, `flag` (extracted from filename via
  `filename_pattern` — `month` is the snapshot month, not a partition key)
- `processed_at` (ISO-8601 UTC)

Rows written, per-partition paths, and per-row quality issues are owned by
microtrade's own manifest under `processed_dir/_manifests/<trade_type>/`.
This project does not duplicate them.

## Planning (dirty-check) logic

### Stage 1 — spec generation

A workbook is dirty if:
- no manifest exists, OR
- `workbook_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash.

### Stage 2 — year ingest

A raw file is dirty if:
- no manifest exists, OR
- `raw_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash.

If any raw file mapping to a given `(trade_type, year)` is dirty, the whole
year is dirty for that trade type. Grouping key: `(trade_type, year)`.
Rationale: microtrade reprocesses at year granularity (one YTD snapshot
drives Jan..snapshot-month); there is no "single month" unit to reprocess.

## Year reprocessing model

Output layout is hive-partitioned:
`<trade_type>/year=YYYY/month=MM/part-N.parquet`

When `(trade_type=T, year=Y)` is dirty:

1. Call `microtrade.pipeline.run(PipelineConfig(input_dir=raw_dir,
   output_dir=processed_dir, spec_dir=specs_dir, trade_types=(T,), year=Y,
   ytd=False))`.
2. Microtrade handles discovery, latest-snapshot-per-year selection,
   partition atomicity (`.tmp` + rename), and delete-before-rewrite
   internally. This project does not touch `processed_dir` directly.
3. On success, write raw manifests for every raw file that maps to
   `(T, Y)`. On failure (non-zero `failed_count` in `RunSummary`), skip
   manifest updates so the year replans next run.

Self-healing on partial failure: failed years have no manifest updates and
replan automatically.

## Matching raw files to partitions

`microtrade.yaml` declares workbooks, each with sheets, each with a
`filename_pattern` (regex with named groups `year`, `month`, `flag`) and an
`effective_from`/`effective_to` date window.

To match a raw file: iterate workbooks and sheets, test the filename regex,
extract `year`/`month`/`flag` from the match, check the date falls within the
workbook's effective window. First match wins. Date windows do not overlap
(guaranteed by the project spec), so first-match is deterministic.

A raw file with no matching sheet is logged as a warning and skipped.

## Microtrade adapter

Microtrade is called through a thin adapter class so the pipeline code
doesn't depend on microtrade's exact API shape. The adapter exposes two
methods:

```python
class MicrotradeAdapter:
    def import_spec(self, workbook: Path, microtrade_yaml: Path,
                    specs_out: Path) -> list[Path]: ...
    def ingest_year(self, trade_type: str, year: int, raw_dir: Path,
                    specs_dir: Path, out_dir: Path) -> RunSummary: ...
```

`import_spec` returns paths of written spec YAMLs. `ingest_year` returns
microtrade's `RunSummary` so callers can inspect `failed_count`. Keep
microtrade imports inside this module only.

## Pipeline entry point

`pipeline.py` has a `main()` that:

1. Loads `config.yaml` via pydantic-settings.
2. Plans stage 1 (dirty workbooks). Runs stage 1 if any.
3. Loads `microtrade.yaml` into pydantic models.
4. Plans stage 2 (dirty `(trade_type, year)` pairs). Runs stage 2 if any.
5. On per-year failure: log with `logger.exception` and continue with other
   years. Failed year has no raw-manifest updates, so it replans next run.

## Kubernetes deployment notes

- Single `CronJob`. One pod per run. No concurrency within a run.
- All state dirs must be on a PersistentVolume that persists across pod
  restarts.
- `microtrade.yaml` and `config.yaml` ship in the image or mount as a
  ConfigMap.
- Set `concurrencyPolicy: Forbid` on the CronJob so two runs can't race on
  the same state directory.
- Pod exit code: 0 on clean completion (including "nothing to do"), non-zero
  if any year failed. The CronJob's failure metrics then reflect real
  failures.

## Module layout

```
project/
  pyproject.toml
  config.yaml
  microtrade.yaml
  CLAUDE.md
  src/
    config.py              # pydantic-settings: load_config()
    microtrade_config.py   # pydantic models over microtrade.yaml, match_raw()
    manifest.py            # read/write manifests atomically
    hashing.py             # hash_file()
    adapter.py             # MicrotradeAdapter
    pipeline.py            # plan + run both stages, main()
  tests/
    ...
  data/
    workbooks/
    raw/
    specs/
    processed/
    manifests/
      specs/
      raw/
```

## Things this project explicitly does NOT do

- No orchestration framework.
- No long-running services.
- No remote state (database, cloud storage, API).
- No structured/JSON logging.
- No parsing of raw data files (microtrade does it).
- No schema validation of parquet output (microtrade's concern).
- No row-level routing or YTD logic (microtrade owns both).
- No retry logic beyond "next cronjob run replans dirty items".
- No per-row provenance tracking; `(trade_type, year)` is the unit of
  reprocessing.

---

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

`microtrade` turns monthly drops of zipped fixed-width (FWF) trade microdata into per-type Hive-partitioned Parquet datasets. Three trade types with distinct schemas: **imports**, **exports_us**, **exports_nonus**. Raw files arrive as one zip per `(trade_type, year, month)`; their filenames vary by generation (e.g. `SHEET002_202404N.TXT.zip` for the current generation, `XYZ12345_Im202404.zip` for older workbooks), and each Spec carries its own `filename_pattern` regex so discovery routes files without any hardcoded naming convention. Monthly runs reprocess all months YTD of the current year; prior years are frozen.

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

- `config.py` — loads the project config (`microtrade.yaml` by default) listing each workbook, its `effective_from`/`effective_to` window, `workbook_id`, and per-sheet `filename_pattern` + `routing_column`. Only consulted by `import-spec`; never at ingest time.
- `discover.py` — scan input dir, match each file against every committed spec's `filename_pattern`, route to the matching Spec. Ambiguous matches (one file matching multiple specs) raise; flags `N`/`C` dedupe in favor of `N`.
- `schema.py` — load versioned YAML specs from `<spec_dir>/<type>/v<effective>.yaml`; `resolve(trade_type, period)` picks the spec whose `[effective_from, effective_to]` window contains `period` (latest effective_from wins on ties).
- `excel_spec.py` — one-shot converter: Excel workbook → one YAML spec per configured sheet. Never called on the hot path; driven by `microtrade import-spec PATH.xlsx --config microtrade.yaml`. Reads only the sheets the config names and bakes the config's period window + `filename_pattern` into each Spec.
- `ingest.py` — streams FWF from the zip via `zipfile.ZipFile.open()` (no extraction), slices lines by `(start, length)`, casts to spec dtypes, yields `pyarrow.RecordBatch`es in `chunk_rows` batches.
- `write.py` — `PartitionWriter` opens `pyarrow.parquet.ParquetWriter` on `year=YYYY/month=MM/part-0.parquet.tmp`, flushes batches one row-group at a time, then atomic-renames on success (idempotent + crash-safe). `MultiPartitionWriter` routes each row to its partition by reading the spec's declared `routing_column` (a Date column — upstream schemas call this `period`, `year_month`, etc.).
- `pipeline.py` — orchestrates discover → schema → ingest → write, appending a per-run JSONL manifest under `output/_manifests/<trade_type>/` (kept outside the dataset root so `pl.scan_parquet(..., hive_partitioning=True)` doesn't trip on non-parquet siblings).
- `cli.py` — Typer app: `ingest`, `import-spec`, `validate-specs`, `inspect`, `version`.

Key invariants:

- Excel + project config are the **upstream** source of truth; committed YAML under `specs/` is the **runtime** contract. Regenerate YAML with `import-spec` when a new workbook lands or the config changes.
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
- `excel_spec.read_workbook(path, WorkbookConfig)` reads only the sheets named in the config, autodetects the `Position | Description | Length | Type` header, skips `Blank` filler rows, handles `Char`/`Num` dtype tokens (Num defaults to Int64), and bakes the config's `effective_from`/`effective_to`/`workbook_id`/per-sheet `filename_pattern` into each Spec.
- reference YAML specs generated from `examples/microdata-layout.xls` ship under `src/microtrade/specs/` (tracked for migration in issue #16 — see note there before relying on them in consumer projects).

- `microtrade inspect PATH` dumps the resolved spec plus the first N rows of a raw zip (or plain FWF via `--type` / `--period`), with per-column `[start..end] dtype 'value'` annotation; `--raw` prints full lines unannotated.
- `microtrade validate-specs` walks `<spec_dir>/<trade_type>/v*.yaml`, runs `validate_spec` on each, checks the filename version matches `effective_from`, prints per-trade-type version-to-version diffs, flags overlapping or gapped `[effective_from, effective_to]` windows, and verifies `canonical_columns` across versions. Exits 0 on a clean tree, 1 on any problem (reports to stderr).

Consumers that hive-scan the dataset should use a `**/*.parquet` glob because `_dataset_schema.json` lives at the dataset root per CLAUDE.md's path.
