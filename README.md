# microtrade

Turn monthly drops of zipped fixed-width (FWF) trade microdata into
Hive-partitioned Parquet datasets, one per trade type.

`microtrade` streams each raw zip directly from its archive (no extraction,
bounded memory), slices columns according to a versioned YAML spec, and writes
`year=YYYY/month=MM/part-0.parquet` atomically under a per-type dataset root.
Monthly runs reprocess all months YTD of the current year; prior years are
frozen.

Raw filenames don't have to follow any fixed convention — each committed spec
carries its own `filename_pattern` regex, so workbooks from different upstream
generations (`SHEET002_202404N.TXT.zip`, `XYZ12345_Im202404.zip`, etc.) can
coexist as long as each pattern captures `year` and `month` (and optionally
`flag`) from the filename.

Three trade types are supported, each with its own distinct schema:

- `imports`
- `exports_us`
- `exports_nonus`

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) for environment and dependency management

## Install

```sh
uv sync
```

This resolves and installs runtime + dev dependencies into `.venv/` based on
`pyproject.toml` and `uv.lock`.

## Usage

### Write a project config (`microtrade.yaml`)

The authoritative schema lives in an Excel workbook, but the **wiring**
between workbooks, trade types, period windows, and raw filenames lives in
a project config. `microtrade import-spec` reads it to produce each YAML
spec; the ingest pipeline never touches this file.

```yaml
# microtrade.yaml
workbooks:
  XYZ12345_Record_Layout.xls:
    workbook_id: XYZ12345         # optional; defaults to filename prefix
    effective_from: 2020-01
    effective_to: 2023-12         # optional; absent = open-ended
    sheets:
      Imports:
        trade_type: imports       # optional; defaults to positional (sheet index -> TRADE_TYPES)
        filename_pattern: '^XYZ12345_Im(?P<year>\d{4})(?P<month>\d{2})\.zip$'
        routing_column: year_month  # per-row Date column; used to partition output
        cast:
          year_month: Date          # workbook ships as Char; promote to Date
        parse:
          year_month: yyyymm_to_date

  Schedule_Record_Layout_2024.xlsx:
    effective_from: 2024-01
    sheets:
      Imports:
        trade_type: imports
        filename_pattern: '^IMP_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
        routing_column: period
        # Upstream renamed `business_number` -> `business_number_9_digit`;
        # keep the stable name in the combined dataset.
        rename:
          business_number_9_digit: business_number
      ExportsUS:
        trade_type: exports_us
        filename_pattern: '^EXUS_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
        routing_column: period
      ExportsNonUS:
        trade_type: exports_nonus
        filename_pattern: '^EXNONUS_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
        routing_column: period
```

Required regex groups: `year` (4 digits) and `month` (2 digits). Optional:
`flag` — when upstream publishes both `N` and `C` copies of the same period,
`N` wins at discovery time.

`routing_column` (defaults to `period`) names the per-row Date column that
the pipeline uses to bucket rows into `year=YYYY/month=MM` partitions. It
must resolve to a Date column in the final spec — either because the
workbook already declares it that way, or because `cast` + `parse` promote
it. Upstream schemas name this column differently (`period`, `year_month`,
`ref_month`, …) so the field is per-sheet rather than a hardcoded
convention.

Match the value against the *logical* (post-rename) column name. If you
rename `foo_physical → bar_logical`, set `routing_column: bar_logical`.
Columns with no rename match on their physical name.

`rename` (optional, per sheet) maps a workbook's *physical* column name
(the Description cell) to the *logical* name that shows up in the combined
dataset. Each Spec stores both, ingest slices FWF bytes under
`physical_name` and emits the parquet column under `logical_name` so
consumers see one stable column even as upstream drifts.

`cast` (optional, per sheet) overrides the workbook's declared dtype per
column. Upstream FWF specs routinely label numeric or date fields as
`Char`; use `cast` to promote them at import time, e.g.
`cast: {value_usd: Int64, year_month: Date}`. Allowed targets: `Utf8`,
`Int64`, `Float64`, `Date`.

`parse` (optional, per sheet) overrides the parser for a Date column.
The default is `yyyymmdd_to_date`; set
`parse: {year_month: yyyymm_to_date}` for `YYYYMM` fields. Only
meaningful for columns whose final dtype is Date.

`computed` (optional, per sheet) builds new parquet columns from other
columns at ingest time. Keyed by the output column's name; values
declare a named operation (`kind`) and its `sources`. First available
operation:

- `concat_to_date`: `sources: [period_date_col, day_col]` → Date
  column combining a YYYYMM `Date` and a day-of-month (`Int64` or
  `Utf8` — `'02'` parses the same as `2`) into a full YYYYMMDD date.
  Row-level failures (e.g. Feb 30, unparseable day) go to the quality
  log like any other parse error.
- `concat_text`: `sources: [col_a, col_b, ...]` → Utf8 column that
  joins N Utf8 sources with `separator` (default `" "`), skipping
  null or blank/whitespace-only sources and collapsing every run of
  whitespace (including tabs/newlines) in the joined result to a
  single space. A row with every source blank emits null. Override
  the separator with `separator: "-"` (or any string).

```yaml
computed:
  full_name:
    kind: concat_text
    sources: [first_name, last_name]
    separator: " "          # optional; default is a single space
```

```yaml
computed:
  entry_date:
    kind: concat_to_date
    sources: [period, day_of_month]
```

`drop` (optional, per sheet) omits named columns from the parquet
output. Runs after cast/rename/computed, so a dropped column can still
feed a computed column and disappear afterward:

```yaml
drop: [day_of_month]   # used by `entry_date`, then dropped from the output
```

A worked example lives at [`examples/microtrade.yaml`](examples/microtrade.yaml),
paired with `examples/microdata-layout.xls`.

### Import the schema workbook (once per schema version)

Each sheet's field table is autodetected by looking for a row containing
`Position`, `Description`, `Length`, and `Type`; rows with `Description =
Blank` are FWF padding and are skipped.

Convert a workbook to versioned YAML specs:

```sh
uv run microtrade import-spec XYZ12345_Record_Layout.xls
```

The importer looks up the workbook in `microtrade.yaml` (override location
with `--config PATH`) and writes one YAML per trade type under
`src/microtrade/specs/<trade_type>/v<effective_from>.yaml`. Each file is a
self-contained runtime contract — review and commit. Re-run with `--force`
to replace an existing version. When a new workbook lands, add a second
entry to the config with its own period window; the pipeline picks the
appropriate spec per period automatically, and a column-level diff against
the previous version is printed.

### Ingest raw monthly zips

```sh
uv run microtrade ingest \
    --input  /path/to/raw_zips   \
    --output /path/to/datasets
```

Discovery walks every committed spec's `filename_pattern` and routes each
file to the spec that matches; files that match nothing are silently
ignored, and files that match more than one spec raise (ambiguous
config — tighten the regexes). `microtrade.yaml` is *not* consulted at
ingest time; everything the pipeline needs is already baked into the YAML
specs.

Defaults: year-to-date of the current calendar year, all three trade types,
zstd-compressed Parquet. Common flags:

| Flag | Default | Purpose |
| --- | --- | --- |
| `--type imports` | all | Repeat for multiple; limits processing |
| `--year 2024` | unset | Process a single year (disables YTD logic) |
| `--month 4` | unset | Combine with `--year` for one-shot re-ingest |
| `--all` | off | Process every year present under `--input` |
| `--chunk-rows 250000` | 250000 | Rows per Parquet row group / memory batch |
| `--compression zstd` | zstd | Parquet compression codec |
| `--encoding cp1252` | cp1252 | Text encoding of the inner FWF (pass `--encoding utf-8` for UTF-8 data) |

Per-partition outcomes are logged as JSON lines under
`<output>/_manifests/<trade_type>/<run_id>.jsonl`, and a one-line summary is
printed at the end. The exit code is non-zero if any partition failed; other
partitions in the same run still complete.

## Output layout

```
output/
  imports/
    year=2024/month=01/part-0.parquet
    year=2024/month=02/part-0.parquet
    ...
  exports_us/
    year=2024/month=01/part-0.parquet
    ...
  exports_nonus/
    ...
  _manifests/
    imports/<run_id>.jsonl
    exports_us/<run_id>.jsonl
    exports_nonus/<run_id>.jsonl
```

Partition columns (`year`, `month`) are encoded in the directory path only,
not duplicated inside each Parquet file. Read with any Hive-aware scanner:

```python
import polars as pl

df = pl.scan_parquet("output/imports", hive_partitioning=True).collect()
```

Or with DuckDB:

```sql
SELECT * FROM read_parquet('output/imports/**/*.parquet', hive_partitioning=1);
```

## Ops: cron-driven runs

`microtrade ops run` is a thin planner on top of `microtrade ingest` for
unattended environments (k8s `CronJob`, typically). It hashes workbooks +
raw zips, compares them against per-file JSON manifests on disk, and
re-runs only the parts that actually changed. Use it when you want the
"every N minutes, do the minimum work" loop; use `microtrade ingest`
directly for one-shot manual reprocessing.

Two stages per run:

1. **Stage 1 — import-spec.** For each workbook under `workbooks_dir`
   whose content hash (or the paired `microtrade.yaml` hash) differs
   from its last manifest, re-run `microtrade import-spec` into
   `specs_dir`.
2. **Stage 2 — ingest.** Group raw zips under `raw_dir` by
   `(trade_type, year)` via each sheet's `filename_pattern`. If any raw
   in a year is dirty (new, changed, or the paired `microtrade.yaml`
   changed), re-run `microtrade.pipeline.run` for that whole year (since
   each raw is a YTD snapshot, the year is the reprocessing unit — not
   the month).

Paths + directories live in a separate `config.yaml` loaded by
`microtrade.ops.settings` (env overrides: `MT_RAW_DIR=/data/raw`, etc.):

```yaml
# config.yaml
microtrade_yaml:    /app/microtrade.yaml
workbooks_dir:      /data/workbooks
raw_dir:            /data/raw
specs_dir:          /data/specs
processed_dir:      /data/processed
spec_manifests_dir: /data/manifests/specs
raw_manifests_dir:  /data/manifests/raw
upstream_raw_dir:   /mnt/upstream/raw   # where the provider drops files
raw_remote_dir:     /mnt/remote/raw     # our durable archive
```

Run:

```sh
uv run microtrade ops run --config config.yaml
```

Exit code is 0 on clean completion (including "nothing to do") and
non-zero if any year or workbook failed; the failed items simply have
no manifest update, so the next cronjob run replans them automatically.
`loguru` handles logging (no custom sinks).

The manifests under `spec_manifests_dir` / `raw_manifests_dir` are the
only stateful artifact the ops layer owns. Wire the `pull_manifests`
and `push_manifests` transport hooks (below) to a shared backend and
multiple operators — different pods, different clusters, different
people — converge on the same "what's already clean" view without
needing to share the rest of the PV.

A `transport` seam wraps the ordering contract:

```
pull_manifests → mirror → pull → stage 1 → stage 2 (push per year) → push_manifests
```

`pull_manifests` fetches the shared dirty-check state *before* planning
so a pod that doesn't have the previous run's PV still skips already-
done work. `push_manifests` publishes the updated state at the end of
the run (even if some years failed — partial progress is worth
sharing). The default backends in `microtrade.ops.transport` are
`pass`-body stubs; your deployment supplies real ones as kwargs to
`run()`:

```python
from microtrade.ops.runner import run
from microtrade.ops.settings import load_settings
from my_app.transport import pull_manifests, mirror, pull, push, push_manifests

sys.exit(run(
    load_settings(Path("config.yaml")),
    pull_manifests_fn=pull_manifests,
    mirror=mirror, pull=pull, push=push,
    push_manifests_fn=push_manifests,
))
```

Point your k8s CronJob's `command` at this wrapper instead of
`microtrade ops run` when you need custom transport. See
`examples/ops_demo.py` for a runnable walkthrough.

See `CLAUDE.md` for the full list of invariants (dirty-check logic,
manifest schemas, k8s deployment guidance, what the ops layer
explicitly does *not* do).

## Architecture

```
config.load_config(yaml)       -> ProjectConfig (import-spec only)
excel_spec.read_workbook       -> Spec per sheet, with filename_pattern baked in
discover.scan(input_dir)       -> list[RawInput] (by matching each committed pattern)
schema.resolve(specs, period)  -> Spec whose [effective_from, effective_to] contains period
ingest.iter_record_batches     -> pyarrow.RecordBatch stream (bounded memory)
write.PartitionWriter          -> year=/month=/part-0.parquet.tmp, atomic rename
pipeline.run                   -> orchestrates the above + JSONL manifest
ops.planner + ops.runner       -> cron-driven dirty-check + dispatch on top of pipeline.run
```

Key invariants:

- Excel + `microtrade.yaml` are the upstream source of truth; committed YAML
  under `src/microtrade/specs/` is the runtime contract. The pipeline never
  reads Excel, and consults the project config only via `import-spec`.
- Each partition write is idempotent: re-running YTD cleanly replaces the
  current year's partitions, leaving prior years untouched.
- The zip is decompressed on the fly via `zipfile.ZipFile.open()`; the raw FWF
  is never extracted to disk and never fully materialized in memory.
- Per-partition failures are recorded in the manifest but do not abort the run
  - one bad month will not block the rest.
- `validate-specs` flags overlapping or gapped `[effective_from, effective_to]`
  windows so silent ambiguities in `schema.resolve` don't reach production.

## Development

```sh
uv run pytest                                     # full suite with coverage
uv run pytest tests/test_pipeline.py::test_name   # single test
uv run ruff format                                # auto-format
uv run ruff check                                 # lint
uv run mypy src                                   # strict type check
uv run pre-commit run --all-files                 # all pre-commit hooks
```

Tests build synthetic Excel workbooks, YAML specs, and FWF zips on the fly in
`tests/_helpers.py` rather than checking in binary fixtures, so the exercised
code paths match the real production workflow end-to-end.

## Status

The pipeline is feature-complete: scaffolding, project config, Excel → YAML,
discover + ingest + write, the orchestrated CLI subcommands (`ingest`,
`import-spec`, `inspect`, `validate-specs`), and the cron-driven ops layer
(`microtrade ops run`) are all landed and covered.
Reference YAML specs ship under `src/microtrade/specs/` but predate the
`filename_pattern` field (tracked in issue #16) — replace them by writing a
`microtrade.yaml` and running `microtrade import-spec` against the real
schema workbook. A new workbook goes into the config as a second entry with
its own `effective_from`/`effective_to`; the pipeline picks the right spec
per period automatically. Run `microtrade validate-specs` after importing to
catch dtype conflicts and window overlaps/gaps between versions.

## License

MIT (see `LICENSE`).
