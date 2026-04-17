"""Orchestrate discover -> schema.resolve -> ingest -> write for a single run.

A run processes zero or more `(trade_type, year, month)` partitions selected by
`PipelineConfig`; the default policy is YTD of the current year for all trade
types. Each partition is written atomically and logged as one JSONL line under
`<output_dir>/_manifests/<trade_type>/<run_id>.jsonl` (kept outside the dataset
root so `pl.scan_parquet(output/<trade_type>, hive_partitioning=True)` can
walk the tree without tripping on non-parquet files).

A partition-level failure is recorded in the manifest and the run continues
with the remaining inputs, so one corrupt month does not block the others.
Callers decide what to do with failures via the returned `RunSummary`.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from microtrade import discover
from microtrade.discover import RawInput
from microtrade.ingest import DEFAULT_CHUNK_ROWS, build_arrow_schema, iter_record_batches
from microtrade.schema import TRADE_TYPES, Spec, SpecError, file_sha256, load_all, resolve
from microtrade.write import PartitionWriter

# Microsecond resolution so back-to-back reruns get distinct manifest files.
RUN_ID_FORMAT = "%Y-%m-%dT%H-%M-%S-%fZ"


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


@dataclass(frozen=True)
class PartitionResult:
    trade_type: str
    year: int
    month: int
    input_path: str
    input_sha256: str
    spec_version: str | None
    rows_written: int
    duration_seconds: float
    output_path: str
    status: str  # "ok" | "failed"
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
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.status == "ok")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")


def run(config: PipelineConfig) -> RunSummary:
    """Execute the pipeline and return a summary with per-partition results."""
    run_id = _make_run_id()
    started_at = _now_iso()
    raw_inputs = _select_inputs(config)

    results: list[PartitionResult] = []
    manifest_paths: dict[str, Path] = {}

    for raw in raw_inputs:
        result = _process_one(raw, config, run_id=run_id, started_at=started_at)
        results.append(result)

        manifest_path = manifest_paths.setdefault(
            raw.trade_type, _manifest_path(config.output_dir, raw.trade_type, run_id)
        )
        _append_manifest(manifest_path, result, run_id=run_id, started_at=started_at)

    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=_now_iso(),
        results=tuple(results),
    )


def _select_inputs(config: PipelineConfig) -> list[RawInput]:
    if not config.input_dir.is_dir():
        raise FileNotFoundError(f"input_dir does not exist: {config.input_dir}")
    candidates = discover.scan(
        config.input_dir,
        trade_types=config.trade_types,
        year=config.year,
        month=config.month,
    )
    if config.ytd and config.year is None:
        current_year = config.current_year or datetime.now(tz=UTC).year
        candidates = discover.ytd_filter(candidates, current_year=current_year)
    return candidates


def _process_one(
    raw: RawInput,
    config: PipelineConfig,
    *,
    run_id: str,
    started_at: str,
) -> PartitionResult:
    start = time.perf_counter()
    try:
        spec = _resolve_spec(raw, config.spec_dir)
    except (SpecError, FileNotFoundError) as exc:
        return PartitionResult(
            trade_type=raw.trade_type,
            year=raw.year,
            month=raw.month,
            input_path=str(raw.path),
            input_sha256=_sha256_or_empty(raw.path),
            spec_version=None,
            rows_written=0,
            duration_seconds=time.perf_counter() - start,
            output_path="",
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
        )

    input_sha = _sha256_or_empty(raw.path)
    arrow_schema = build_arrow_schema(spec)
    writer = PartitionWriter(
        dataset_root=config.output_dir,
        trade_type=raw.trade_type,
        year=raw.year,
        month=raw.month,
        arrow_schema=arrow_schema,
        compression=config.compression,
    )

    try:
        with writer as w:
            for batch in iter_record_batches(
                raw,
                spec,
                chunk_rows=config.chunk_rows,
                encoding=config.encoding,
            ):
                w.write_batch(batch)
            rows_written = w.rows_written
    except Exception as exc:
        return PartitionResult(
            trade_type=raw.trade_type,
            year=raw.year,
            month=raw.month,
            input_path=str(raw.path),
            input_sha256=input_sha,
            spec_version=spec.version,
            rows_written=0,
            duration_seconds=time.perf_counter() - start,
            output_path=str(writer.final_path),
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
        )

    return PartitionResult(
        trade_type=raw.trade_type,
        year=raw.year,
        month=raw.month,
        input_path=str(raw.path),
        input_sha256=input_sha,
        spec_version=spec.version,
        rows_written=rows_written,
        duration_seconds=time.perf_counter() - start,
        output_path=str(writer.final_path),
        status="ok",
    )


def _resolve_spec(raw: RawInput, spec_dir: Path) -> Spec:
    specs = load_all(spec_dir, raw.trade_type)
    if not specs:
        raise SpecError(f"no specs found for trade_type {raw.trade_type!r} under {spec_dir}")
    return resolve(specs, raw.period)


def _manifest_path(output_dir: Path, trade_type: str, run_id: str) -> Path:
    path = output_dir / "_manifests" / trade_type / f"{run_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_manifest(
    manifest_path: Path, result: PartitionResult, *, run_id: str, started_at: str
) -> None:
    record = {
        "run_id": run_id,
        "started_at": started_at,
        "logged_at": _now_iso(),
        "trade_type": result.trade_type,
        "year": result.year,
        "month": result.month,
        "input_path": result.input_path,
        "input_sha256": result.input_sha256,
        "spec_version": result.spec_version,
        "rows_written": result.rows_written,
        "duration_seconds": round(result.duration_seconds, 4),
        "output_path": result.output_path,
        "status": result.status,
        "error": result.error,
    }
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")


def _sha256_or_empty(path: Path) -> str:
    try:
        return file_sha256(path)
    except OSError:
        return ""


def _make_run_id() -> str:
    return datetime.now(tz=UTC).strftime(RUN_ID_FORMAT)


def _now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()
