# microtrade

Turn monthly drops of zipped fixed-width (FWF) trade microdata into
Hive-partitioned Parquet datasets, one per trade type.

`microtrade` streams each raw `<trade_type>_<YYYYMM>.zip` directly from its zip
archive (no extraction, bounded memory), slices columns according to a
versioned YAML spec, and writes `year=YYYY/month=MM/part-0.parquet` atomically
under a per-type dataset root. Monthly runs reprocess all months YTD of the
current year; prior years are frozen.

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

### Import the schema workbook (once per schema version)

The authoritative schema lives in an Excel workbook. Sheets are mapped
**positionally**: the first sheet becomes `imports`, the second `exports_us`,
the third `exports_nonus` (sheet names are ignored). Each sheet's field table
is autodetected by looking for a row containing `Position`, `Description`,
`Length`, and `Type`; rows with `Description = Blank` are FWF padding and are
skipped. See `examples/microdata-layout.xls` for a reference workbook with the
expected shape.

Convert the workbook to versioned YAML specs under `src/microtrade/specs/`:

```sh
uv run microtrade import-spec examples/microdata-layout.xls \
    --effective-from 2020-01
```

The resulting YAML files are the runtime contract — review and commit them.
Re-run with `--force` to replace an existing version. When a workbook changes,
run again with a later `--effective-from`; the pipeline picks the appropriate
spec per period automatically, and a column-level diff against the previous
version is printed.

### Ingest raw monthly zips

```sh
uv run microtrade ingest \
    --input  /path/to/raw_zips   \
    --output /path/to/datasets
```

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
discover.scan(input_dir)       -> list[RawInput(trade_type, year, month, path)]
schema.resolve(specs, period)  -> Spec effective for that period
ingest.iter_record_batches     -> pyarrow.RecordBatch stream (bounded memory)
write.PartitionWriter          -> year=/month=/part-0.parquet.tmp, atomic rename
pipeline.run                   -> orchestrates the above + JSONL manifest
```

Key invariants:

- Excel is the upstream source of truth; committed YAML under
  `src/microtrade/specs/` is the runtime contract. The pipeline never reads
  Excel at runtime.
- Each partition write is idempotent: re-running YTD cleanly replaces the
  current year's partitions, leaving prior years untouched.
- The zip is decompressed on the fly via `zipfile.ZipFile.open()`; the raw FWF
  is never extracted to disk and never fully materialized in memory.
- Per-partition failures are recorded in the manifest but do not abort the run
  - one bad month will not block the rest.

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

The pipeline is feature-complete: scaffolding, Excel → YAML, discover + ingest
+ write, and the orchestrated CLI `microtrade ingest` are all landed and
covered, alongside `microtrade inspect` for dumping the resolved spec and
first rows of a raw zip or FWF file. The `validate-specs` subcommand is still
a stub. Reference YAML specs generated from `examples/microdata-layout.xls`
ship under `src/microtrade/specs/`; replace them by running `microtrade
import-spec` against the real schema workbook (typically with a later
`--effective-from`, which preserves the historical layouts).

## License

MIT (see `LICENSE`).
