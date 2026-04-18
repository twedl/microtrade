# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-18

First public release. Published to PyPI as `microtrade-fwf`; the import path
and CLI command stay `microtrade`.

### Added

- Streaming FWF ingest pipeline (`microtrade ingest`) that reads each
  `<trade_type>_<YYYYMM>.zip` directly from its archive, slices columns
  against a versioned spec, and writes `year=YYYY/month=MM/part-0.parquet`
  atomically under a per-type dataset root. Per-partition JSONL manifest
  under `<output>/_manifests/<trade_type>/<run_id>.jsonl`.
- Excel → YAML spec converter (`microtrade import-spec`) for the real
  upstream workbook layout: positional sheet→trade_type mapping,
  autodetected `Position | Description | Length | Type` header, skipped
  `Blank` filler rows, `Char`/`Num` dtype tokens.
- Per-sheet preamble sanity check that refuses to accept a mis-ordered
  workbook (catches swapped imports/exports sheets before they land in
  YAML).
- `Date` dtype (pyarrow `date32`) with `yyyymmdd_to_date` /
  `yyyymm_to_date` parsers, resolved per column at stream start.
- Row-level quality-issue log: parse failures (bad numeric, blank
  non-nullable, bad date) go to
  `<output>/_quality_issues/<trade_type>/<run_id>.jsonl` and the row is
  skipped; the surrounding partition still writes. Capped per partition
  (`--max-quality-issues`, default 10,000) and guarded by a skip-rate
  abort (`--max-skip-rate`, default 0.5) so pathological inputs can't
  balloon the log or silently land a mostly-broken partition.
- Canonical `_dataset_schema.json` refreshed at
  `<output>/<trade_type>/_dataset_schema.json` after every run — union
  of all committed specs for that trade type, latest dtype wins,
  nullability widens.
- `microtrade validate-specs` — lints every `v*.yaml` under a spec dir,
  verifies filename versions match `effective_from`, prints a
  per-trade-type `diff_specs` changelog, and checks `canonical_columns`
  to surface cross-version dtype conflicts. Summary reports scan scope
  (`OK (N trade types, M specs)`).
- `microtrade inspect` — dumps the resolved spec and first rows of a raw
  zip (or plain FWF via `--type` / `--period`), with per-column
  `[start..end] dtype 'value'` annotation; `--raw` prints full lines.
- Reference YAML specs generated from `examples/microdata-layout.xls`
  ship under `src/microtrade/specs/`, so `microtrade ingest` works out
  of the box against any real data that matches the 2020-01 layout.

### Notes

- Consumers that hive-scan the dataset should use a `**/*.parquet` glob
  because `_dataset_schema.json` lives at the dataset root (see the
  README's "Output layout" section).
- All four CLI subcommands (`ingest`, `import-spec`, `validate-specs`,
  `inspect`) ship implemented; the package is fully typed (`py.typed`
  marker included in the wheel).

[unreleased]: https://github.com/twedl/microtrade/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/twedl/microtrade/releases/tag/v0.1.0
