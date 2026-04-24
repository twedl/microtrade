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

# Ops submodule (`microtrade.ops`)

The `ops` submodule is the cron-driven planner + runner that drives
`microtrade.pipeline.run` from disk state. Previously lived in a
sibling `tp` repo; merged in so the library and its sole consumer
share one version, one CI, one test run.

## Role

Runs as a Kubernetes `CronJob`. Each run:

1. Generates microtrade spec YAMLs from workbook files (stage 1).
2. Ingests raw zipped data files into Hive-partitioned Parquet (stage 2).
3. Records state to local disk so subsequent runs skip already-done work.

All the heavy lifting (parsing workbooks, reading raw files, writing
parquet) is done by the rest of `microtrade`. The ops submodule is
planning, dispatch, and state tracking.

**Raw drop semantics (microtrade's model):** each raw file is a YTD
snapshot for its `(trade_type, year)`. A single `..._202406N.TXT.zip`
covers Jan–Jun of 2024; microtrade partitions rows internally via
each sheet's `routing_column` (a row-level Date). One raw file
therefore produces many month partitions, and the latest snapshot
per `(trade_type, year)` wins at ingest time. The unit of
reprocessing in the ops layer is `(trade_type, year)`, not a single
month.

## Stack (ops-layer specifics)

- `loguru` for logging (standard across microtrade — not gated behind
  an extra).
- Hand-rolled `dataclass` + `yaml.safe_load` + `MT_`-prefixed env
  overrides for the ops `config.yaml` (no pydantic, no
  pydantic-settings).
- `microtrade` is called as a library via module-level functions, not
  through an adapter class.
- No orchestrator — k8s `CronJob` is the scheduler. No Airflow,
  Prefect, Dagster, Datadog, or any long-running server. No network
  services. State lives on disk only.

## Config

Two separate YAMLs, two roles:

- **`config.yaml` + env vars** — *where*: paths, directories. Loaded
  by `microtrade.ops.settings.load_settings`. Changes per environment.
- **`microtrade.yaml`** — *how*: microtrade's domain config
  (workbooks, sheets, filename patterns, column casts/parses/renames).
  Read directly by microtrade via `microtrade.config.load_config`.
  The ops layer reuses the same parser to drive `match_raw`; no
  shadow parser.

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
processed_remote_dir: remote destination for processed Parquet output
manifests_remote_dir: remote shared root for spec + raw manifests
encoding: FWF text codec, default "utf-8" (optional; use cp1252 /
  latin-1 for Statistics Canada bilingual drops with non-ASCII
  bytes like 0xC9)
```

## State tracking

All state lives on disk as JSON files (one per tracked item). No
SQLite, no JSONL append log, no database. Rationale: simplest
possible model, easy to inspect with `cat`/`jq`, no concurrency
concerns for a single-pod cronjob.

Write atomically: write to `path.tmp`, then `os.replace(tmp, path)`
(see `microtrade.ops.manifest.write_manifest`).

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
  `filename_pattern` — `month` is the snapshot month, not a
  partition key)
- `processed_at` (ISO-8601 UTC)

Rows written, per-partition paths, and per-row quality issues are
owned by microtrade's own manifest under
`processed_dir/_manifests/<trade_type>/`. The ops layer does not
duplicate them.

## Planning (dirty-check) logic

### Stage 1 — spec generation

A workbook is dirty if:
- no manifest exists, OR
- `workbook_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash, OR
- any path in the manifest's `specs_written` is missing on disk.

Files in `workbooks_dir` not named in `microtrade.yaml`'s `workbooks`
mapping are logged as a warning and skipped — config is the source of
truth for what counts as a workbook, so stray files (e.g. raw zips
that ended up in `workbooks_dir` because it shares a path with
`raw_dir`) don't crash `import_spec`.

### Stage 2 — year ingest

A raw file is dirty if:
- no manifest exists, OR
- `raw_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash, OR
- `processed_dir/<trade_type>/year=<year>/` doesn't exist or is empty.

The output-exists check catches reconfigured `processed_dir` paths and
manually-deleted output, where raw manifests would otherwise claim
"done" while the parquet is gone.

If any raw file mapping to a given `(trade_type, year)` is dirty, the
whole year is dirty for that trade type. Grouping key:
`(trade_type, year)`. Rationale: microtrade reprocesses at year
granularity (one YTD snapshot drives Jan..snapshot-month); there is
no "single month" unit to reprocess.

## Year reprocessing model

Output layout is hive-partitioned:
`<trade_type>/year=YYYY/month=MM/part-N.parquet`

When `(trade_type=T, year=Y)` is dirty:

1. Call `microtrade.pipeline.run(PipelineConfig(input_dir=raw_dir,
   output_dir=processed_dir, spec_dir=specs_dir, trade_types=(T,),
   year=Y, ytd=False))` via `microtrade.ops.runner.ingest_year`.
2. Microtrade handles discovery, latest-snapshot-per-year selection,
   partition atomicity (`.tmp` + rename), and delete-before-rewrite
   internally. The ops layer does not touch `processed_dir` directly.
3. On success, write raw manifests for every raw file that maps to
   `(T, Y)`. On failure (non-zero `failed_count` in `RunSummary`),
   skip manifest updates so the year replans next run.

Self-healing on partial failure: failed years have no manifest
updates and replan automatically.

## Matching raw files to partitions

`microtrade.yaml` declares workbooks, each with sheets, each with a
`filename_pattern` (regex with named groups `year`, `month`, and
optional `flag`) and an `effective_from`/`effective_to` date window.

`microtrade.ops.planner.match_raw(filename, ProjectConfig)` iterates
workbooks and sheets, tests the filename regex, extracts
`year`/`month`/`flag`, and checks the date falls within the workbook's
effective window. First match wins. Date windows do not overlap
(enforced by `microtrade validate-specs`), so first-match is
deterministic.

A raw file with no matching sheet is logged as a warning and skipped.

## Calling microtrade

No adapter class. Two module-level functions in
`microtrade.ops.runner`:

```python
def import_spec(workbook: Path, microtrade_yaml: Path,
                specs_out: Path) -> list[Path]: ...
def ingest_year(trade_type: str, year: int, raw_dir: Path,
                specs_dir: Path, out_dir: Path) -> RunSummary: ...
```

`import_spec` wraps `microtrade.excel_spec.read_workbook` +
`schema.save_spec`. `ingest_year` wraps `microtrade.pipeline.run`
with a single `(trade_type, year)` scope. Tests replace either via
`monkeypatch.setattr("microtrade.ops.runner.import_spec", fake)` —
no adapter subclassing, no dependency-injection plumbing at the call
site.

## Transport seam

`microtrade.ops.transport` exports real (not stub) hook functions
that frame the ordering contract in `run()`:

```
pull_manifests -> mirror_upstream_raw -> pull_workbooks -> stage 1
  -> stage 2 (per year: pull_raws -> ingest -> push -> cleanup)
  -> push_manifests
```

`pull_manifests` runs before any planning so shared dirty-check state
from other operators is honoured; without it, a pod that doesn't have
the previous run's PV treats everything as dirty.

`mirror_upstream_raw` runs next because upstream drops get deleted
periodically — mirroring is how we keep the archive whole. Then
`pull_workbooks` stages every `.xls`/`.xlsx` into `workbooks_dir`
upfront (they're small, shared across years, and needed by stage 1).

**Stage 2 is a per-year pull-ingest-push-cleanup loop, not a bulk
copy.** For each dirty `(trade_type, year)`:

1. `pull_raws_for_year` stages ONLY that year's zips from
   `raw_remote_dir/current` into `raw_dir` (filtered via
   `match_raw`).
2. `ingest_year` runs microtrade against `raw_dir`.
3. `push_processed` publishes that year's parquet to
   `processed_remote_dir`.
4. `cleanup_local_year` deletes both local raws and local parquet
   for that year. Peak local disk is one year's worth of data, not
   the whole archive.

Failure semantics:

- **Ingest failure**: isolated to that year. Local raws for the
  failed year get deleted (re-pullable next run); later years
  continue; run returns non-zero.
- **Push failure**: fail-fast. Keep local parquet so the retry
  doesn't re-ingest. Abort the stage 2 loop (continuing would
  accumulate unpushed parquet across years, defeating the cycle).
- `push_manifests` still runs at the end regardless of either
  failure so partial progress reaches the remote.

`plan_stage2` iterates `raw_remote_dir/current` (the permanent
archive), not `raw_dir`, because local `raw_dir` is ephemeral under
the cleanup loop. It short-circuits the hash check with an mtime
probe: if the remote file's mtime hasn't advanced past the
manifest's `processed_at`, trust the manifest and skip the hash
(avoids re-hashing multi-GB zips on every run). The
output-exists check on a clean manifest runs against
`processed_remote_dir` — published parquet is the source of truth
for "year is done", not the ephemeral local copy.

**Path routing is baked into the library; only the per-file transfer
primitive is DI.** `run()` accepts a single `copy_file` kwarg, a
`Callable[[Path, Path], None]` that publishes one file at `dst`.
The default is a thin `shutil.copy2` wrapper that writes to
`dst.tmp` and `os.replace`'s it into place (crash-safe on local
disk / mounted PV). The target deployment has no `rsync`, no
`aws s3 sync`, no `kubectl cp -r`, no bulk-tree primitive at all —
what's available is some way to move one file at a time (per-file
`kubectl cp`, S3 `put_object`, etc.), which slots in as the
`copy_file` kwarg. `microtrade.ops.transport.sync_tree` owns the
tree walk, the skip-if-unchanged check, and the `target.parent`
mkdir; every hook calls it and threads `copy_file` through. Skip
rule is rsync's `--update` semantic: same size AND target mtime ≥
source mtime (truncated to whole seconds — sub-second mtime isn't
portable across every filesystem). This tolerates `copy_file`s that
don't preserve mtime (e.g. `mc cp` without `--preserve`) — after a
fresh copy the target's mtime is "now", still ≥ source, so the next
run skips. The hole is an upstream rollback with identical size and
an older mtime, which this doesn't detect.

Contract: `copy_file` must publish `dst` atomically — readers must
never see a half-written file at `dst`. Atomicity is the
`copy_file`'s concern, not the library's: deployments whose remote
is reached via a flaky network mount (where `os.replace` itself
drops with `ConnectionAbortedError`) or via an object store with
already-atomic `put_object` are free to write `dst` directly and
skip the tmp+rename dance. Path layout comes from `Settings` (and
thus from `config.yaml`) — `upstream_raw_dir`, `raw_remote_dir`,
`processed_remote_dir`, `manifests_remote_dir`. Change where things
land by editing config, not by overriding hooks.

## Pipeline entry point

`microtrade ops run --config config.yaml` (see
`microtrade.ops.runner.main`) does:

1. Loads `config.yaml` via `load_settings`.
2. Runs `pull_manifests` to fetch shared dirty-check state.
3. Runs `mirror_upstream_raw`, then `pull_workbooks`.
4. Plans stage 1 (dirty workbooks). Runs stage 1 if any.
5. Loads `microtrade.yaml` via `microtrade.config.load_config`.
6. Plans stage 2 (dirty `(trade_type, year)` pairs against the
   remote archive). For each dirty year in sorted order, runs the
   pull-ingest-push-cleanup cycle.
7. Runs `push_manifests` to publish updated manifests.
8. On ingest failure: log with `logger.exception`, delete local
   raws for the year, continue with other years. On push failure:
   log, keep local parquet, abort stage 2 early. Failed years have
   no raw-manifest updates, so they replan next run.

## Kubernetes deployment notes

- Single `CronJob`. One pod per run. No concurrency within a run.
- All state dirs must live on a PersistentVolume that persists across
  pod restarts.
- `microtrade.yaml` and `config.yaml` ship in the image or mount as a
  ConfigMap.
- Set `concurrencyPolicy: Forbid` on the CronJob so two runs can't
  race on the same state directory.
- Pod exit code: 0 on clean completion (including "nothing to do"),
  non-zero if any year failed. The CronJob's failure metrics then
  reflect real failures.

## Ops module layout

```
src/microtrade/ops/
  __init__.py
  hashing.py        # hash_file()
  manifest.py       # SpecManifest, RawManifest, read_manifest, write_manifest
  settings.py       # Settings dataclass + load_settings(yaml_path)
  planner.py        # match_raw, plan_stage1, plan_stage2, YearKey
  runner.py         # import_spec, ingest_year, run, main
  transport.py      # mirror/pull/push stubs + ordering contract
tests/ops/
  conftest.py       # `tree` fixture (settings + tmp dirs + microtrade.yaml)
  test_hashing.py
  test_manifest.py
  test_settings.py
  test_match_raw.py
  test_planner.py
  test_runner.py             # FakeAdapter-style monkeypatching
  test_runner_integration.py # real microtrade run against examples/
```

## Things the ops layer explicitly does NOT do

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
- `cli.py` — Typer app: `ingest`, `import-spec`, `validate-specs`, `inspect`, `version`, plus the `ops` subcommand group (`microtrade ops run`).
- `ops/` — cron-driven planner/runner on top of the library. See the "Ops submodule" section above for layout and invariants.

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
