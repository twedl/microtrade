# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-04-19

### Changed

- **Breaking:** default `--spec-dir` / `--out` paths are now `./specs`
  (cwd-relative) instead of the installed package's directory. This
  prevents `import-spec` from trying to write into
  `site-packages/microtrade/specs/` and `validate-specs` from silently
  reading the bundled examples, which closes [issue #16].
- Stopped shipping `src/microtrade/specs/` inside the wheel and sdist.
  The directory remains in-repo as test fixtures / starting examples;
  installed users produce their own specs via `microtrade import-spec`.

[issue #16]: https://github.com/twedl/microtrade/issues/16

## [0.1.1] - 2026-04-19

Moves discovery configuration into a user-supplied project YAML so raw
filenames no longer have to follow a fixed `<trade_type>_<YYYYMM>.zip`
convention.

### Added

- Project config (`microtrade.yaml`, `--config` override) declaring, per
  workbook: `effective_from`, optional `effective_to`, `workbook_id`, and
  a per-sheet `filename_pattern` regex. `microtrade import-spec PATH.xlsx`
  reads this file instead of taking `--effective-from` / `--workbook-id`
  flags.
- `Spec.effective_to` (inclusive upper bound, `None` = open-ended).
  `schema.resolve` now picks the spec whose `[effective_from,
  effective_to]` window contains the target period. `validate-specs`
  flags overlapping and gapped windows, and reports a per-spec window in
  its summary.
- `SpecSource.filename_pattern` — each committed spec embeds its own
  regex (named groups `year`, `month`, optional `flag`). `discover`
  iterates every committed spec's pattern to route files; ambiguous
  matches (one file matching multiple specs) raise.
- `SpecSource.workbook_id` — stable identifier baked in from the config
  (defaults to the workbook filename prefix).
- N/C flag preference: when upstream publishes both a `N` and `C` copy
  of the same `(trade_type, year, month)`, the `N` file wins.
- Shared helpers `schema.validate_filename_pattern`,
  `validate_period_window`, `next_period`, and `window_problems` so
  config and discover don't re-implement the same checks.

### Changed

- **Breaking:** `microtrade import-spec` CLI drops `--effective-from`
  and `--workbook-id`; reads them from the project config.
- **Breaking:** `excel_spec.read_workbook(path, effective_from, *,
  workbook_id=...)` → `read_workbook(path, WorkbookConfig)`. Sheets are
  looked up by name (with an optional positional fallback) instead of
  by sheet index; the positional/title-hint sanity check is gone —
  users declare trade types explicitly in the config.
- `discover.parse_filename` / `scan` take pattern entries (or a
  `spec_dir` to load them from) instead of matching a hardcoded
  `<trade_type>_<YYYYMM>.zip` regex.
- `pipeline.run` loads each trade type's specs once per run instead of
  re-parsing YAML per partition.

### Fixed

- `load_config` replaces a TOCTOU `is_file()` pre-check with a direct
  `read_text()` that maps filesystem errors to `ConfigError`.
- `excel_spec.read_workbook` raises `SpecError` (not `IndexError`) when
  a config lists more sheets than positional trade-type slots without
  declaring an explicit `trade_type`.

### Notes

- The reference YAML specs under `src/microtrade/specs/` predate this
  release and carry no `filename_pattern`; they load but won't route
  any files. Tracked in issue #16.

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

[unreleased]: https://github.com/twedl/microtrade/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/twedl/microtrade/releases/tag/v0.1.2
[0.1.1]: https://github.com/twedl/microtrade/releases/tag/v0.1.1
[0.1.0]: https://github.com/twedl/microtrade/releases/tag/v0.1.0
