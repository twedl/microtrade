# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.12] - 2026-04-24

### Fixed

- `plan_stage1` now skips files in `workbooks_dir` that aren't named
  in `microtrade.yaml`'s `workbooks` mapping (logged as a warning
  instead of crashing `import_spec` with
  `ConfigError: workbook ... is not listed`). This surfaced when
  `workbooks_dir` and `raw_dir` pointed at the same path and raw
  zips landed alongside workbooks.
- `plan_stage1` now re-marks a workbook dirty if any path in its
  manifest's `specs_written` is missing on disk. Previously,
  reconfiguring `specs_dir` (or deleting specs manually) would leave
  the manifest claiming "done at hash H" while stage 2's discovery
  found no specs to route with, producing a silent no-op run.
- `plan_stage2` now re-marks a `(trade_type, year)` dirty if
  `processed_dir/<trade_type>/year=<year>/` doesn't exist or is
  empty. Previously, reconfiguring `processed_dir` (or deleting
  processed output manually) would leave raw manifests claiming
  "done" while the parquet output was gone.

## [0.2.11] - 2026-04-24

### Changed

- `sync_tree`'s skip check is now rsync's `--update` rule: sizes
  match **and** the target's mtime is at least as new as the source's
  (previously: sizes match **and** mtimes are exactly equal to the
  second). Tolerates `copy_file` implementations that don't preserve
  mtime — after a fresh copy the target's mtime is "now", still ≥
  source, so the next run skips instead of pointlessly re-copying.
  Known hole: an upstream rollback with identical size and an older
  mtime won't be detected; upstream YTD snapshots in this project
  are monotonic, so this is safe in practice. `copy_file`
  implementations are no longer required to preserve mtime — the
  explicit contract is now atomicity only.

## [0.2.10] - 2026-04-24

### Breaking

- `copy_file` contract now owns atomicity. `sync_tree` calls
  `copy_file(src, target)` directly instead of writing to a
  library-owned `target.tmp` and then `os.replace`'ing. Callers whose
  `copy_file` is a thin copy (e.g. `shutil.copy2`) need to do their
  own tmp+rename to stay crash-safe — the default `_shutil_copy2`
  wrapper does this. Callers whose `copy_file` is already atomic (S3
  `put_object`, object-store SDK write) or whose remote is reached
  via a network mount where `os.replace` itself drops with
  `ConnectionAbortedError` can now publish `target` directly and skip
  the tmp+rename dance.

### Fixed

- `sync_tree` no longer fails with `ConnectionAbortedError` on remote
  filesystems whose `os.replace` is flaky. Previously the library
  forced a tmp+rename on the destination filesystem even when the
  caller's `copy_file` already provided durability; now the walk just
  hands `copy_file` the final `target` path.

## [0.2.9] - 2026-04-23

### Breaking

- `microtrade.ops.runner.run` no longer accepts `pull_manifests_fn=`,
  `mirror=`, `pull=`, `push=`, or `push_manifests_fn=`. Path routing
  is now owned by the library; `microtrade.ops.transport` ships real
  implementations of all five hooks (no more `pass`-body stubs) that
  call `sync_tree` under a single injected per-file primitive.
- `Settings` requires two new fields in `config.yaml`:
  `processed_remote_dir` (remote destination for processed Parquet)
  and `manifests_remote_dir` (shared dirty-check state). Add both
  to your `config.yaml` before upgrading.
- `pull_raw` now always splits the upstream drop by extension:
  `*.zip` → `raw_dir`, `*.xls` / `*.xlsx` → `workbooks_dir`.
  Previously the split was a demo-level concern in `ops_demo.py`.

### Added

- `microtrade.ops.runner.run` accepts a single `copy_file` kwarg —
  a `Callable[[Path, Path], None]` that moves one file from src to
  dst. Threaded through every transport hook and through
  `sync_tree`. Default is a thin `shutil.copy2` wrapper. Swap in a
  `kubectl cp` / S3 `put_object` / etc. wrapper if the default
  can't reach your remote. Contract: must preserve mtime, otherwise
  skip-if-unchanged misfires and every file re-copies next run.

### Changed

- `examples/ops_demo.py` dropped its five hook bodies — running the
  demo is now `run(settings)`.

## [0.2.8] - 2026-04-23

### Added

- `microtrade.ops.transport.sync_tree(src, dst, patterns=None)` — a
  pure-Python, stdlib-only `rsync -a`-like helper: skips files whose
  size and mtime match the destination, writes atomically via
  `target.tmp` + `os.replace` so concurrent readers never see a
  half-copied file, and supports optional glob-pattern filtering on
  the relative path. Use it directly from your transport
  implementations when rsync / aws s3 sync / kubectl cp aren't
  available on the box.

### Changed

- `examples/ops_demo.py` now uses `sync_tree` for all five transport
  hooks instead of shelling out to `rsync`, so the walkthrough runs
  on any machine out of the box. The `pull` hook also demonstrates
  splitting a mixed upstream drop (workbooks + raw zips in one
  directory) into `workbooks_dir` and `raw_dir` by extension.

## [0.2.7] - 2026-04-23

### Added

- `pull_manifests` and `push_manifests` hooks on the
  `microtrade.ops.transport` seam so multiple operators can share
  dirty-check state via whatever remote their other transport
  functions already target (S3, NFS, etc.). `pull_manifests` runs at
  the start of `run()` before any planning; `push_manifests` runs at
  the end, regardless of per-stage failures (partial successes are
  still worth sharing). Supplied as new kwargs on `run()`:
  `pull_manifests_fn=` and `push_manifests_fn=`.
- The `examples/ops_demo.py` walkthrough wires all five transport
  hooks via local rsync so you can see the manifest-sync round-trip
  without an S3 bucket.

## [0.2.6] - 2026-04-23

### Changed

- `microtrade.ops.runner.run` now accepts `mirror=`, `pull=`, and
  `push=` keyword arguments so production callers can supply
  environment-specific transport functions without monkeypatching
  module globals. Defaults still resolve to the stubs in
  `microtrade.ops.transport` at call time, so existing
  `monkeypatch.setattr(...)` test hooks keep working. Added
  `examples/ops_demo.py` as a runnable template.

## [0.2.5] - 2026-04-23

### Added

- `microtrade.ops` submodule + `microtrade ops run` CLI subcommand: a
  cron-driven planner on top of `microtrade.pipeline.run` that hashes
  workbooks and raw zips against per-file JSON manifests and only
  reprocesses the `(trade_type, year)` pairs that actually changed.
  Ships with a `config.yaml` loader (+ `MT_`-prefixed env overrides), a
  `match_raw` helper that reuses microtrade's existing `ProjectConfig`
  parser, and a `transport` seam holding the
  mirror → pull → stage1 → stage2 ordering contract. See the README
  "Ops: cron-driven runs" section and CLAUDE.md for the full invariants.
- `loguru` added as a runtime dependency; used by the ops layer and
  available for logging anywhere else in the package.

## [0.2.4] - 2026-04-22

### Added

- `concat_text` computed-column kind: joins N Utf8 sources with a
  configurable `separator` (default single space), skipping null or
  blank/whitespace-only sources and collapsing every run of whitespace
  in the joined result to a single space. A row whose sources are all
  blank emits null.

## [0.2.3] - 2026-04-22

### Changed

- `concat_to_date` now accepts a `Utf8` day source as well as `Int64`.
  A string like `'02'` is stripped and parsed to an int before being
  combined with the YYYYMM Date source; blank strings yield a null row
  and unparseable strings go to the quality log. Previously the day
  source had to be `Int64`, forcing an otherwise-unnecessary `cast` on
  specs whose upstream declares the day field as `Char`.

## [0.2.2] - 2026-04-22

### Changed

- **Breaking:** The per-row routing column is now per-spec instead of a
  hardcoded `"period"`. Every sheet in `microtrade.yaml` (and every
  committed YAML spec) now carries a `routing_column` field naming the
  Date-typed column used to bucket rows into `year=/month=/` partitions.
  Upstream schemas call this column different things (`period`,
  `year_month`, `ref_month`, …) - point the field at whichever column
  your data actually uses. The value defaults to `"period"` when
  omitted, so existing configs whose date column is literally named
  `period` keep working. `validate_spec` now rejects a spec whose
  `routing_column` is missing, non-Date, or listed in `dropped_columns`.

## [0.2.1] - 2026-04-22

### Changed

- **Breaking:** `--max-quality-issues` now short-circuits the ingest when
  the cap is exceeded instead of silently dropping further log entries
  and continuing to parse. The JSONL log is still bounded by the cap
  (writes stop at the boundary), but the partition is now marked
  failed. Set `--max-quality-issues 0` to preserve the old "never
  abort" behavior.

### Fixed

- `--max-skip-rate` now short-circuits as soon as the per-row skip
  ratio crosses the threshold instead of parsing the whole file first.
  A mostly-bad 23M-row file used to spend ~750s parsing end-to-end only
  to fail at commit time; it now aborts within a few thousand rows.
  Implementation: `iter_record_batches` takes a `max_skip_rate` kwarg
  and checks every row-skip inside `_stream_lines`.

## [0.2.0] - 2026-04-22

### Changed

- **Breaking (data model):** raw zips are now treated as YTD-cumulative
  snapshots. A filename's month is a *snapshot marker* — the file
  contains rows for every month from January through that month. Rows
  route to destination partitions based on their in-row `period` column,
  not the filename. Practical effects:
  - Discovery keeps only the highest-month snapshot per `(trade_type,
    year)`; earlier snapshots are strict subsets and skipped.
  - A single input file now writes *multiple* per-month parquet
    partitions (`MultiPartitionWriter`), one per `(year, month)` in the
    rows.
  - Rows whose `period` year doesn't match the snapshot's year, whose
    period-month exceeds the snapshot-month, or whose `period` is null
    route to the quality-issue log and are skipped.
  - The manifest now writes one JSONL line per *output partition*
    (not per input file). Each line records the source snapshot via
    `snapshot_month` on `PartitionResult`.
  - Flag priority inverted from `N > C > None` to `None > N > C`: an
    unflagged file is treated as the authoritative base snapshot.
- **Breaking (YAML):** `Spec.dropped_columns` can no longer include
  `period`; the column is required to route rows.
- **Breaking (API):** `_process_one` returns `list[PartitionResult]`
  instead of a single result. `PartitionResult` gains `snapshot_month`.

### Added

- `write.MultiPartitionWriter` — context manager that lazily spawns one
  `PartitionWriter` per `(year, month)` as rows arrive, atomic-renames
  all children on success, deletes all `.tmp` files on exception.

### Added (carried forward from Unreleased)

- tqdm progress bar over the partition loop in `microtrade ingest`,
  togglable with `--progress/--no-progress` (on by default). Programmatic
  callers (`PipelineConfig.show_progress=False` by default) stay silent
  so library usage and tests aren't affected.

## [0.1.7] - 2026-04-22

### Added

- `Spec.computed_columns` / `SheetConfig.computed` — columns built from
  other columns at ingest time (no FWF slice). Real parquet columns in
  the output; show up in `build_arrow_schema`, `canonical_columns`, and
  `diff_specs`. First named operation: `concat_to_date` merges a YYYYMM
  Date column with a DD Int column into a YYYYMMDD Date.
- `schema.ComputedColumn` dataclass, `schema.COMPUTED_KINDS` registry.
- Row-level computation failures (e.g. Feb 30) route through the
  existing quality-issue log instead of aborting the partition.
- `Spec.dropped_columns` / `SheetConfig.drop` — omit named columns from
  the parquet output. FWF slicing still runs so a computed column can
  reference a dropped source before it disappears. Validated against
  real column names; rejects a drop list that would empty the schema.

## [0.1.6] - 2026-04-21

### Added

- `SheetConfig.cast` — per-sheet `{physical_name: dtype}` override
  applied at `import-spec` time. Upstream FWF specs frequently call
  numeric/date columns `Char`; `cast` promotes them to one of the
  canonical dtypes (`Utf8`, `Int64`, `Float64`, `Date`) in the emitted
  YAML. Stale entries raise with a difflib suggestion.
- `SheetConfig.parse` — per-sheet `{physical_name: parser_name}`
  override for Date columns. The default is `yyyymmdd_to_date`; set
  `yyyymm_to_date` (or future parsers) for non-standard date formats.
  Raises if the override targets a non-Date column after cast.
- `schema.DATE_PARSERS` constant — the canonical list of Date parser
  names, consulted by the config layer so typos surface at load time
  rather than at ingest.

## [0.1.5] - 2026-04-21

### Added

- `Column.logical_name` — stable name for a column across workbook
  versions. `effective_name` returns `logical_name or physical_name`.
  `canonical_columns` and `diff_specs` key on `effective_name`, and
  `ingest.build_arrow_schema` emits parquet columns under the logical
  name so the combined dataset sees a single column even when upstream
  renames the underlying FWF column.
- `SheetConfig.rename` (in `microtrade.yaml`) — a physical-to-logical
  mapping applied at import time. `microtrade import-spec` stamps
  `logical_name` on matching columns, raises `SpecError` if a rename
  entry refers to a column the workbook doesn't declare (with
  `difflib` suggestions for likely typos), and rejects duplicate
  logical names per sheet.
- `examples/microtrade.yaml` paired with `examples/microdata-layout.xls`,
  showing both the baseline flow and the `rename:` feature. README now
  points at it.

### Changed

- **Breaking (YAML format):** `Column.name` renamed to
  `Column.physical_name` throughout. The spec YAMLs now use
  `physical_name:` instead of `name:` for each column. Regenerate with
  `microtrade import-spec` or hand-rename the field in any committed
  specs.
- `_opt_str` helper in `schema.py` collapses four copies of the
  optional-string-round-trip idiom (`workbook_id`, `filename_pattern`,
  `logical_name`, `effective_to`).
- `SheetConfig.rename` is now truly immutable once constructed
  (`MappingProxyType`), matching the surrounding `frozen=True` semantics.

## [0.1.4] - 2026-04-20

### Changed

- Ingest treats `record_length` as an upper bound rather than requiring
  strict equality. Each record must cover every real data column
  (`Spec.min_record_length`) and must not exceed `record_length`;
  trailing-filler bytes declared by the workbook's sentinel row are now
  optional in the data. This lets a single spec ingest datasets that do
  and do not include the workbook's optional trailing bytes, which was
  breaking exports_us and exports_nonus after 0.1.3. Error messages
  split into "record truncated" (too short to cover all columns) and
  "longer than declared record_length" (unexpected extra bytes).

### Added

- `Spec.min_record_length` property — the rightmost real-column byte.
  Shared between `validate_spec`'s "record_length ≥ rightmost column"
  guard and ingest's per-line truncation check.

## [0.1.3] - 2026-04-19

### Added

- `microtrade import-spec` now takes a variadic list of workbook paths,
  so shell globs work naturally (`microtrade import-spec raw/*.xls`).
  Each workbook must have a matching entry in the project config.
  Per-workbook failures are reported at the end; the command exits 1
  if any workbook failed to import.

### Changed

- **Breaking:** `--encoding` default is now `cp1252` (Windows-1252) for
  both `microtrade ingest` and `microtrade inspect`. Most government
  trade drops originate on Windows and ship in cp1252; users whose
  upstream is UTF-8 should pass `--encoding utf-8` explicitly.

### Fixed

- `microtrade inspect`'s decode-error message now points at the
  `--encoding` flag so the fix isn't buried in `--help`.
- `import-spec` now honors trailing "sentinel" rows in the record-layout
  sheet that declare a `Position` without a `Length`. The parser used to
  silently drop them, which truncated `record_length` by the trailing
  filler bytes the sentinel was meant to cover; ingest then failed with
  `expected record_length N, got N+1` on the real data. The row's
  `Position` is now treated as the last byte of the record.

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

[unreleased]: https://github.com/twedl/microtrade/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/twedl/microtrade/releases/tag/v0.2.0
[0.1.7]: https://github.com/twedl/microtrade/releases/tag/v0.1.7
[0.1.6]: https://github.com/twedl/microtrade/releases/tag/v0.1.6
[0.1.5]: https://github.com/twedl/microtrade/releases/tag/v0.1.5
[0.1.4]: https://github.com/twedl/microtrade/releases/tag/v0.1.4
[0.1.3]: https://github.com/twedl/microtrade/releases/tag/v0.1.3
[0.1.2]: https://github.com/twedl/microtrade/releases/tag/v0.1.2
[0.1.1]: https://github.com/twedl/microtrade/releases/tag/v0.1.1
[0.1.0]: https://github.com/twedl/microtrade/releases/tag/v0.1.0
