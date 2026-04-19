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

  Schedule_Record_Layout_2024.xlsx:
    effective_from: 2024-01
    sheets:
      Imports:
        trade_type: imports
        filename_pattern: '^IMP_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
      ExportsUS:
        trade_type: exports_us
        filename_pattern: '^EXUS_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
      ExportsNonUS:
        trade_type: exports_nonus
        filename_pattern: '^EXNONUS_(?P<year>\d{4})(?P<month>\d{2})(?P<flag>[NC])\.TXT\.zip$'
```

Required regex groups: `year` (4 digits) and `month` (2 digits). Optional:
`flag` — when upstream publishes both `N` and `C` copies of the same period,
`N` wins at discovery time.

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
| `--encoding utf-8` | utf-8 | Text encoding of the inner FWF |

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

## Architecture

```
config.load_config(yaml)       -> ProjectConfig (import-spec only)
excel_spec.read_workbook       -> Spec per sheet, with filename_pattern baked in
discover.scan(input_dir)       -> list[RawInput] (by matching each committed pattern)
schema.resolve(specs, period)  -> Spec whose [effective_from, effective_to] contains period
ingest.iter_record_batches     -> pyarrow.RecordBatch stream (bounded memory)
write.PartitionWriter          -> year=/month=/part-0.parquet.tmp, atomic rename
pipeline.run                   -> orchestrates the above + JSONL manifest
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
discover + ingest + write, and the orchestrated CLI subcommands (`ingest`,
`import-spec`, `inspect`, `validate-specs`) are all landed and covered.
Reference YAML specs ship under `src/microtrade/specs/` but predate the
`filename_pattern` field (tracked in issue #16) — replace them by writing a
`microtrade.yaml` and running `microtrade import-spec` against the real
schema workbook. A new workbook goes into the config as a second entry with
its own `effective_from`/`effective_to`; the pipeline picks the right spec
per period automatically. Run `microtrade validate-specs` after importing to
catch dtype conflicts and window overlaps/gaps between versions.

## License

MIT (see `LICENSE`).
