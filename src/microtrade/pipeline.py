"""Orchestrate discover -> schema.resolve -> ingest -> write for a single run.

A run processes zero or more `(trade_type, year, month)` partitions selected by
`PipelineConfig`; the default policy is YTD of the current year for all trade
types. Each partition is written atomically and logged as one JSONL line under
`<output_dir>/_manifests/<trade_type>/<run_id>.jsonl` (kept outside the dataset
root so `pl.scan_parquet(output/<trade_type>, hive_partitioning=True)` can
walk the tree without tripping on non-parquet files).

A partition-level failure is recorded in the manifest and the run continues
with the remaining inputs, so one corrupt month does not block the others.
Row-level parse failures (bad numeric, blank non-nullable, bad date) are
appended to `<output_dir>/_quality_issues/<trade_type>/<run_id>.jsonl` and the
row is skipped; the surrounding partition still writes successfully.

At the end of a run each processed trade type also gets its canonical
`_dataset_schema.json` refreshed at `<output_dir>/<trade_type>/_dataset_schema.json`,
capturing the union of every committed spec's columns for that trade type.
Callers decide what to do with failures via the returned `RunSummary`.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, TextIO

import pyarrow as pa
import pyarrow.compute as pc
from tqdm.auto import tqdm

from microtrade import discover
from microtrade.discover import RawInput
from microtrade.ingest import (
    DEFAULT_CHUNK_ROWS,
    IngestError,
    QualityIssue,
    build_arrow_schema,
    iter_record_batches,
)
from microtrade.schema import (
    TRADE_TYPES,
    CanonicalColumn,
    Spec,
    SpecError,
    canonical_columns,
    file_sha256,
    load_all,
    now_iso,
    resolve,
)
from microtrade.write import MultiPartitionWriter, WriteError

Status = Literal["ok", "failed"]
STATUS_OK: Status = "ok"
STATUS_FAILED: Status = "failed"

# Microsecond resolution so back-to-back reruns get distinct manifest files.
RUN_ID_FORMAT = "%Y-%m-%dT%H-%M-%S-%fZ"


DEFAULT_MAX_QUALITY_ISSUES: int = 10_000
DEFAULT_MAX_SKIP_RATE: float = 0.5


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    output_dir: Path
    spec_dir: Path
    trade_types: tuple[str, ...] = TRADE_TYPES
    ytd: bool = True
    current_year: int | None = None
    year: int | None = None
    month: int | None = None
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    compression: str = "zstd"
    encoding: str = "utf-8"
    # Cap per-partition quality-issue JSONL records so a pathological file
    # can't balloon the log to input-file size. 0 disables the cap.
    max_quality_issues: int = DEFAULT_MAX_QUALITY_ISSUES
    # Abort the partition (and delete its temp parquet) if the fraction of
    # rows that failed to parse exceeds this threshold. 1.0 disables the
    # abort - row-level failures just accumulate.
    max_skip_rate: float = DEFAULT_MAX_SKIP_RATE
    # Show a tqdm progress bar over the partition loop. Disabled by default
    # for programmatic callers (tests, library use); the CLI flips it on.
    show_progress: bool = False


@dataclass(frozen=True)
class PartitionResult:
    trade_type: str
    year: int
    month: int
    input_path: str
    input_sha256: str
    spec_version: str | None
    rows_written: int
    rows_skipped: int
    # `rows_skipped_logged` is <= `rows_skipped`; they diverge only when the
    # `max_quality_issues` cap truncates the JSONL mid-run.
    rows_skipped_logged: int
    duration_seconds: float
    output_path: str
    status: Status
    # The snapshot month captured from the source filename. Always equals
    # `year` combined with `snapshot_month` for the file this partition
    # came from. Routing: a single YTD snapshot (e.g. YYYY-06) writes
    # multiple PartitionResults, one per destination month (Jan..Jun).
    snapshot_month: int = 0
    error: str | None = None


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    started_at: str
    finished_at: str
    results: tuple[PartitionResult, ...] = field(default_factory=tuple)

    @property
    def total_rows(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def total_skipped(self) -> int:
        return sum(r.rows_skipped for r in self.results)

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.status == STATUS_OK)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == STATUS_FAILED)


def run(config: PipelineConfig) -> RunSummary:
    """Execute the pipeline and return a summary with per-partition results."""
    run_id = _make_run_id()
    started_at = now_iso()
    specs_by_type = {t: load_all(config.spec_dir, t) for t in config.trade_types}
    raw_inputs = _select_inputs(config, specs_by_type)

    results: list[PartitionResult] = []
    manifest_paths: dict[str, Path] = {}
    trade_types_seen: set[str] = set()

    progress = tqdm(
        raw_inputs,
        desc="ingesting",
        unit="partition",
        disable=not config.show_progress,
    )
    for raw in progress:
        progress.set_postfix_str(f"{raw.trade_type} {raw.period}", refresh=False)
        partition_results = _process_one(raw, config, specs_by_type, run_id=run_id)
        results.extend(partition_results)
        trade_types_seen.add(raw.trade_type)

        manifest_path = manifest_paths.setdefault(
            raw.trade_type, _manifest_path(config.output_dir, raw.trade_type, run_id)
        )
        for result in partition_results:
            _append_manifest(manifest_path, result, run_id=run_id, started_at=started_at)

    # Refresh the canonical dataset schema for every trade type we touched
    # (or was configured), so consumers of `output/<type>/_dataset_schema.json`
    # always see the union of committed specs after a run.
    for trade_type in sorted(set(config.trade_types) | trade_types_seen):
        _write_dataset_schema(config.output_dir, trade_type, specs_by_type.get(trade_type, []))

    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=now_iso(),
        results=tuple(results),
    )


def _select_inputs(config: PipelineConfig, specs_by_type: dict[str, list[Spec]]) -> list[RawInput]:
    if not config.input_dir.is_dir():
        raise FileNotFoundError(f"input_dir does not exist: {config.input_dir}")
    patterns = [
        entry for specs in specs_by_type.values() for entry in discover.patterns_for_specs(specs)
    ]
    candidates = discover.scan(
        config.input_dir,
        patterns=patterns,
        trade_types=config.trade_types,
        year=config.year,
        month=config.month,
    )
    if config.ytd and config.year is None:
        current_year = (
            config.current_year if config.current_year is not None else datetime.now(tz=UTC).year
        )
        candidates = discover.ytd_filter(candidates, current_year=current_year)
    return _latest_snapshot_per_year(candidates)


def _latest_snapshot_per_year(candidates: list[RawInput]) -> list[RawInput]:
    """Files are YTD snapshots: a YYYY-12 file supersedes YYYY-11 and earlier
    for the same year. Keep only the file with the highest `month` per
    `(trade_type, year)`; the rest are strict subsets."""
    latest: dict[tuple[str, int], RawInput] = {}
    for raw in candidates:
        key = (raw.trade_type, raw.year)
        current = latest.get(key)
        if current is None or raw.month > current.month:
            latest[key] = raw
    return sorted(latest.values(), key=lambda r: (r.trade_type, r.year, r.month))


def _process_one(
    raw: RawInput,
    config: PipelineConfig,
    specs_by_type: dict[str, list[Spec]],
    *,
    run_id: str,
) -> list[PartitionResult]:
    """Ingest one YTD snapshot file into per-month partitions.

    Returns one `PartitionResult` per written `(year, month)` partition on
    success, or a single failure result if the snapshot could not be
    ingested. The snapshot's filename month appears as
    `PartitionResult.snapshot_month` on every result so downstream
    consumers can trace each partition back to its source.
    """
    start = time.perf_counter()
    input_sha = _sha256_or_empty(raw.path)

    try:
        spec = _resolve_spec(raw, specs_by_type, config.spec_dir)
    except (SpecError, FileNotFoundError) as exc:
        return [
            _failure_result(raw, start, input_sha, spec_version=None, output_path="", error=exc)
        ]

    quality_path = _quality_issues_path(config.output_dir, raw.trade_type, run_id)
    multi = MultiPartitionWriter(
        dataset_root=config.output_dir,
        trade_type=raw.trade_type,
        arrow_schema=build_arrow_schema(spec),
        compression=config.compression,
    )
    with _QualityIssueWriter(
        path=quality_path, run_id=run_id, raw=raw, limit=config.max_quality_issues
    ) as issue_sink:
        try:
            with multi as w:
                for batch in iter_record_batches(
                    raw,
                    spec,
                    chunk_rows=config.chunk_rows,
                    encoding=config.encoding,
                    on_quality_issue=issue_sink,
                ):
                    filtered = _route_rows(batch, raw, issue_sink)
                    if filtered.num_rows > 0:
                        w.write_batch(filtered)
                _check_skip_rate(w.rows_written, issue_sink.count, config.max_skip_rate)
                partition_rows = dict(w.partition_rows)
                final_paths = dict(w.final_paths)
        except (IngestError, WriteError, OSError, ValueError) as exc:
            return [
                _failure_result(
                    raw,
                    start,
                    input_sha,
                    spec_version=spec.version,
                    output_path="",
                    error=exc,
                    rows_skipped=issue_sink.count,
                    rows_skipped_logged=issue_sink.count_logged,
                )
            ]

        duration = time.perf_counter() - start
        rows_skipped = issue_sink.count
        rows_skipped_logged = issue_sink.count_logged
        if not partition_rows:
            return [
                PartitionResult(
                    trade_type=raw.trade_type,
                    year=raw.year,
                    month=raw.month,
                    input_path=str(raw.path),
                    input_sha256=input_sha,
                    spec_version=spec.version,
                    rows_written=0,
                    rows_skipped=rows_skipped,
                    rows_skipped_logged=rows_skipped_logged,
                    duration_seconds=duration,
                    output_path="",
                    status=STATUS_OK,
                    snapshot_month=raw.month,
                )
            ]

        return [
            PartitionResult(
                trade_type=raw.trade_type,
                year=year,
                month=month,
                input_path=str(raw.path),
                input_sha256=input_sha,
                spec_version=spec.version,
                rows_written=rows,
                # Row-level skipped counts are tracked at the file level; we
                # attribute the whole count to each partition line so a
                # consumer summing the manifest never undercounts. Duration
                # is similarly per-file.
                rows_skipped=rows_skipped,
                rows_skipped_logged=rows_skipped_logged,
                duration_seconds=duration,
                output_path=str(final_paths[(year, month)]),
                status=STATUS_OK,
                snapshot_month=raw.month,
            )
            for (year, month), rows in sorted(partition_rows.items())
        ]


def _route_rows(
    batch: pa.RecordBatch, raw: RawInput, issue_sink: _QualityIssueWriter
) -> pa.RecordBatch:
    """Filter out rows whose `period` doesn't fit the snapshot's year window.

    A YTD snapshot for (year, snapshot_month) claims rows in that year with
    period-month <= snapshot_month. Rows outside that range or with null
    period go to the quality log and are excluded from the batch.
    """
    period = batch.column("period")
    null_mask = pc.is_null(period).to_pylist()
    years = pc.year(period).to_pylist()
    months = pc.month(period).to_pylist()

    keep: list[bool] = []
    for i in range(batch.num_rows):
        if null_mask[i]:
            issue_sink(
                QualityIssue(
                    file=raw.path.name,
                    line_no=i + 1,
                    column="period",
                    error="period is null; row cannot be routed to a partition",
                    raw_line="",
                )
            )
            keep.append(False)
            continue
        row_year = int(years[i])
        row_month = int(months[i])
        if row_year != raw.year or row_month > raw.month:
            issue_sink(
                QualityIssue(
                    file=raw.path.name,
                    line_no=i + 1,
                    column="period",
                    error=(
                        f"row period {row_year}-{row_month:02d} is outside the "
                        f"snapshot window ({raw.year}-01..{raw.year}-{raw.month:02d})"
                    ),
                    raw_line="",
                )
            )
            keep.append(False)
            continue
        keep.append(True)
    return batch.filter(pa.array(keep))


def _check_skip_rate(rows_written: int, rows_skipped: int, threshold: float) -> None:
    """Raise IngestError if the partition's bad-row fraction exceeds threshold.

    Called inside the PartitionWriter `with` block so the temp parquet gets
    cleaned up. Treats `threshold >= 1.0` as "never abort" and empty inputs
    as implicitly fine (no rows = no ratio).
    """
    total = rows_written + rows_skipped
    if threshold >= 1.0 or total == 0:
        return
    rate = rows_skipped / total
    if rate > threshold:
        raise IngestError(
            f"{rows_skipped}/{total} rows failed parsing ({rate:.1%}); "
            f"exceeds max_skip_rate {threshold:.1%}"
        )


def _failure_result(
    raw: RawInput,
    start: float,
    input_sha: str,
    *,
    spec_version: str | None,
    output_path: str,
    error: BaseException,
    rows_skipped: int = 0,
    rows_skipped_logged: int = 0,
) -> PartitionResult:
    return PartitionResult(
        trade_type=raw.trade_type,
        year=raw.year,
        month=raw.month,
        input_path=str(raw.path),
        input_sha256=input_sha,
        spec_version=spec_version,
        rows_written=0,
        rows_skipped=rows_skipped,
        rows_skipped_logged=rows_skipped_logged,
        duration_seconds=time.perf_counter() - start,
        output_path=output_path,
        status=STATUS_FAILED,
        error=f"{type(error).__name__}: {error}",
    )


def _resolve_spec(raw: RawInput, specs_by_type: dict[str, list[Spec]], spec_dir: Path) -> Spec:
    specs = specs_by_type.get(raw.trade_type, [])
    if not specs:
        raise SpecError(f"no specs found for trade_type {raw.trade_type!r} under {spec_dir}")
    return resolve(specs, raw.period)


def _manifest_path(output_dir: Path, trade_type: str, run_id: str) -> Path:
    path = output_dir / "_manifests" / trade_type / f"{run_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _quality_issues_path(output_dir: Path, trade_type: str, run_id: str) -> Path:
    return output_dir / "_quality_issues" / trade_type / f"{run_id}.jsonl"


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")


def _append_manifest(
    manifest_path: Path, result: PartitionResult, *, run_id: str, started_at: str
) -> None:
    _append_jsonl(
        manifest_path,
        {
            "run_id": run_id,
            "started_at": started_at,
            "logged_at": now_iso(),
            "trade_type": result.trade_type,
            "year": result.year,
            "month": result.month,
            "input_path": result.input_path,
            "input_sha256": result.input_sha256,
            "spec_version": result.spec_version,
            "rows_written": result.rows_written,
            "rows_skipped": result.rows_skipped,
            "rows_skipped_logged": result.rows_skipped_logged,
            "duration_seconds": round(result.duration_seconds, 4),
            "output_path": result.output_path,
            "status": result.status,
            "error": result.error,
        },
    )


def _write_dataset_schema(output_dir: Path, trade_type: str, specs: list[Spec]) -> None:
    if not specs:
        return
    try:
        cols: tuple[CanonicalColumn, ...] = canonical_columns(specs)
    except SpecError:
        return
    target = output_dir / trade_type / "_dataset_schema.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "trade_type": trade_type,
        "generated_at": now_iso(),
        "spec_versions": [s.effective_from for s in specs],
        "columns": [asdict(c) for c in cols],
    }
    target.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


class _QualityIssueWriter:
    """Context-managed JSONL sink for per-row parse failures.

    Holds one open file handle across the partition so high-volume error
    streams don't pay open/close syscalls per row; creates the parent
    directory on first write. `.count` tracks every issue seen (used for
    the manifest's `rows_skipped` and the pipeline's skip-rate abort
    check), while `.count_logged` tracks only those written to disk -
    the two diverge once `limit` is reached so pathological partitions
    can't balloon the log to input-file size.
    """

    def __init__(self, *, path: Path, run_id: str, raw: RawInput, limit: int = 0) -> None:
        self._path = path
        self._run_id = run_id
        self._raw = raw
        self._limit = limit
        self._file: TextIO | None = None
        self.count: int = 0
        self.count_logged: int = 0

    def __enter__(self) -> _QualityIssueWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def __call__(self, issue: QualityIssue) -> None:
        self.count += 1
        if self._limit > 0 and self.count_logged >= self._limit:
            return
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("a", encoding="utf-8")
        record = {
            "run_id": self._run_id,
            "trade_type": self._raw.trade_type,
            "year": self._raw.year,
            "month": self._raw.month,
            "input_path": str(self._raw.path),
            "file": issue.file,
            "line_no": issue.line_no,
            "column": issue.column,
            "error": issue.error,
            "raw_line": issue.raw_line,
            "logged_at": now_iso(),
        }
        self._file.write(json.dumps(record, sort_keys=True) + "\n")
        self.count_logged += 1


def _sha256_or_empty(path: Path) -> str:
    try:
        return file_sha256(path)
    except OSError:
        return ""


def _make_run_id() -> str:
    return datetime.now(tz=UTC).strftime(RUN_ID_FORMAT)
