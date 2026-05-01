"""Cron-driven orchestration over microtrade's ingest pipeline.

A single ``run(settings)`` call drives the ordering:

``pull_manifests -> mirror_upstream_raw -> pull_workbooks -> stage 1
-> stage 2 (per year: pull_raws -> ingest -> push -> cleanup)
-> push_manifests``.

Stage 1 regenerates spec YAMLs for dirty workbooks. Stage 2 processes
each dirty ``(trade_type, year)`` in a pull-ingest-push-cleanup loop so
local disk peak is one year's worth of data, not the whole archive.

Failure semantics: fail-fast on any stage 2 failure. Systemic errors
(encoding mismatch, missing spec, schema drift) hit every year
identically — there's no point ingesting 29 more years to learn
the same thing. On any failure ``_run_stage2`` returns non-zero and
leaves remaining dirty years for the next run, which replans them.

- Pull / ingest failure: delete local raws (safe — next run
  re-pulls), abort stage 2.
- Push failure: keep local parquet so the next run's retry doesn't
  re-ingest, abort stage 2.
- ``push_manifests`` always runs at the end so partial progress (any
  years that completed before the failure) reaches the remote.

``import_spec`` and ``ingest_year`` are module-level functions that the
test suite replaces via ``monkeypatch.setattr`` (see
``tests/ops/test_runner.py``) — no adapter class, no DI ceremony at the
call site.

Transport is environment-specific. ``run()`` accepts a single
``copy_file`` kwarg — the per-file transfer primitive — which threads
through every hook in ``microtrade.ops.transport``. The library owns
path routing and tree-walk / atomic-publish; the caller supplies only
"how to move one file".
"""

from __future__ import annotations

import contextlib
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from microtrade import config as mt_config
from microtrade import excel_spec, schema
from microtrade.config import ProjectConfig
from microtrade.ops.manifest import (
    RawManifest,
    SpecManifest,
    write_manifest,
)
from microtrade.ops.planner import YearKey, match_raw, plan_stage1, plan_stage2
from microtrade.ops.settings import Settings, load_settings
from microtrade.ops.transport import (
    CopyFn,
    _shutil_copy2,
    cleanup_local_raws,
    cleanup_local_year,
    mirror_upstream_raw,
    pull_manifests,
    pull_raws_for_year,
    pull_workbooks,
    push_manifests,
    push_processed,
)
from microtrade.pipeline import PipelineConfig, RunSummary
from microtrade.pipeline import run as _mt_run
from microtrade.schema import file_sha256


def import_spec(workbook: Path, microtrade_yaml: Path, specs_out: Path) -> list[Path]:
    """Default stage 1 impl: Excel -> YAML specs via microtrade's library API.

    Tests replace this via ``monkeypatch.setattr("microtrade.ops.runner.import_spec", ...)``.
    """
    project_config = mt_config.load_config(microtrade_yaml)
    workbook_config = project_config.get_workbook(workbook)
    specs = excel_spec.read_workbook(workbook, workbook_config)
    effective_from = workbook_config.effective_from
    written: list[Path] = []
    for trade_type, spec in specs.items():
        target = specs_out / trade_type / f"v{effective_from}.yaml"
        target.parent.mkdir(parents=True, exist_ok=True)
        schema.save_spec(spec, target)
        written.append(target)
    return written


def ingest_year(
    trade_type: str,
    year: int,
    raw_dir: Path,
    specs_dir: Path,
    out_dir: Path,
    *,
    encoding: str = "utf-8",
) -> RunSummary:
    """Default stage 2 impl: one ``(trade_type, year)`` through microtrade.pipeline.run.

    Tests replace this via ``monkeypatch.setattr("microtrade.ops.runner.ingest_year", ...)``.
    """
    cfg = PipelineConfig(
        input_dir=raw_dir,
        output_dir=out_dir,
        spec_dir=specs_dir,
        trade_types=(trade_type,),
        year=year,
        ytd=False,
        show_progress=False,
        encoding=encoding,
    )
    return _mt_run(cfg)


def _year_output_dir(settings: Settings, key: YearKey) -> Path:
    return settings.processed_dir / key.trade_type / f"year={key.year}"


def _run_stage1(settings: Settings, cfg: ProjectConfig, mt_hash: str) -> int:
    dirty = plan_stage1(settings, cfg, microtrade_hash=mt_hash)
    if not dirty:
        logger.info("stage 1: nothing to do")
        return 0
    logger.info("stage 1: {} workbook(s) to process", len(dirty))
    failures = 0
    for wb in dirty:
        try:
            specs = import_spec(wb, settings.microtrade_yaml, settings.specs_dir)
            manifest = SpecManifest(
                workbook_name=wb.name,
                workbook_hash=file_sha256(wb),
                microtrade_hash=mt_hash,
                specs_written=specs,
                processed_at=datetime.now(tz=UTC),
            )
            write_manifest(settings.spec_manifests_dir, wb.name, manifest)
        except Exception:
            logger.exception("stage 1 failed for workbook {}", wb.name)
            failures += 1
    return failures


def _write_raw_manifests(
    settings: Settings, cfg: ProjectConfig, raws: list[Path], mt_hash: str
) -> None:
    now = datetime.now(tz=UTC)
    for raw in raws:
        m = match_raw(raw.name, cfg)
        assert m is not None
        manifest = RawManifest(
            raw_name=raw.name,
            raw_hash=file_sha256(raw),
            microtrade_hash=mt_hash,
            trade_type=m.trade_type,
            year=m.year,
            month=m.month,
            flag=m.flag or "",
            processed_at=now,
        )
        write_manifest(settings.raw_manifests_dir, raw.name, manifest)


def _bytes_sum(paths: list[Path]) -> int:
    total = 0
    for p in paths:
        with contextlib.suppress(OSError):
            total += p.stat().st_size
    return total


def _run_stage2(
    settings: Settings,
    cfg: ProjectConfig,
    mt_hash: str,
    copy_file: CopyFn,
    *,
    only_keys: frozenset[YearKey] | None = None,
) -> int:
    dirty = plan_stage2(settings, cfg, microtrade_hash=mt_hash)
    if only_keys is not None:
        dropped = [k for k in dirty if k not in only_keys]
        dirty = {k: v for k, v in dirty.items() if k in only_keys}
        if dropped:
            logger.info(
                "stage 2: only_keys filter dropped {} dirty year(s): {}",
                len(dropped),
                sorted(dropped),
            )
        missing = [k for k in only_keys if k not in dirty]
        if missing:
            logger.warning(
                "stage 2: only_keys requested {} not currently dirty (skipped); "
                "remove their raw_manifest .json to force reprocess",
                sorted(missing),
            )
    if not dirty:
        logger.info("stage 2: nothing to do")
        return 0
    total_years = len(dirty)
    logger.info("stage 2: {} (trade_type, year) to process", total_years)
    stage_start = time.perf_counter()
    rows_total = 0

    for idx, key in enumerate(sorted(dirty), start=1):
        remote_raws = dirty[key]
        size_mb = _bytes_sum(remote_raws) / (1024 * 1024)
        logger.info(
            "year {}/{}: {} year={} ({} raw(s), {:.1f} MiB)",
            idx,
            total_years,
            key.trade_type,
            key.year,
            len(remote_raws),
            size_mb,
        )

        t0 = time.perf_counter()
        try:
            local_raws = pull_raws_for_year(settings, remote_raws, copy_file=copy_file)
        except Exception:
            logger.exception("pull_raws_for_year failed for {}; aborting stage 2", key)
            return 1
        logger.info("  pulled in {:.1f}s", time.perf_counter() - t0)

        t0 = time.perf_counter()
        try:
            summary = ingest_year(
                trade_type=key.trade_type,
                year=key.year,
                raw_dir=settings.raw_dir,
                specs_dir=settings.specs_dir,
                out_dir=settings.processed_dir,
                encoding=settings.encoding,
            )
            if summary.failed_count > 0:
                raise RuntimeError(
                    f"microtrade reported {summary.failed_count} partition failure(s)"
                )
        except Exception:
            logger.exception("stage 2 ingest failed for {}; aborting stage 2", key)
            # Safe to delete local raws — next run re-pulls from remote.
            cleanup_local_raws(settings)
            return 1
        rows_total += summary.total_rows
        logger.info(
            "  ingested in {:.1f}s: {} partition(s), {:,} rows, {:,} skipped",
            time.perf_counter() - t0,
            summary.ok_count,
            summary.total_rows,
            summary.total_skipped,
        )

        t0 = time.perf_counter()
        try:
            push_processed(settings, [_year_output_dir(settings, key)], copy_file=copy_file)
        except Exception:
            # Keep local parquet so the retry doesn't re-ingest.
            logger.exception(
                "stage 2 push failed for {}; keeping local parquet, aborting stage 2", key
            )
            return 1
        logger.info("  pushed in {:.1f}s", time.perf_counter() - t0)

        _write_raw_manifests(settings, cfg, local_raws, mt_hash)
        cleanup_local_year(settings, key)

    logger.info(
        "stage 2 done: {} year(s), {:,} rows in {:.1f}s",
        total_years,
        rows_total,
        time.perf_counter() - stage_start,
    )
    return 0


def run(
    settings: Settings,
    *,
    copy_file: CopyFn = _shutil_copy2,
    only_keys: Iterable[YearKey] | None = None,
) -> int:
    """Drive one ops cycle.

    Ordering:
    ``pull_manifests -> mirror -> pull_workbooks -> stage1 ->
    stage2(per year: pull -> ingest -> push -> cleanup) -> push_manifests``.

    ``pull_manifests`` runs *before* the dirty-check so shared state
    from other operators is honoured. ``pull_workbooks`` runs once
    upfront since workbooks are small and stage 1 needs them all.
    ``push_manifests`` runs *after* both stages regardless of
    failures so partial progress is shared.

    ``copy_file`` is the single DI seam for environment-specific
    transport: a ``Callable[[Path, Path], None]`` that moves one file
    from ``src`` to ``dst``. The default is a thin ``shutil.copy2``
    wrapper (local disk / mounted PV). Swap in a ``kubectl cp`` /
    S3 ``put_object`` / etc. wrapper if the default can't reach your
    remote.

    ``only_keys`` restricts stage 2 to the listed ``(trade_type,
    year)`` pairs (intersected with the dirty plan). Use this to
    debug a single failing year without re-processing the others.
    Years requested that aren't dirty are skipped with a warning —
    delete their entry under ``raw_manifests_dir`` first if you
    want to force reprocess.

    If ``settings.log_file`` is set, a loguru file sink is added for
    the duration of this run alongside the default stderr sink. The
    sink is removed on return so repeated ``run()`` calls don't leak
    handles.
    """
    sink_id: int | None = None
    if settings.log_file:
        sink_id = logger.add(
            settings.log_file,
            rotation="10 MB",
            retention=10,
            enqueue=True,
            backtrace=False,
        )
    try:
        pull_manifests(settings, copy_file=copy_file)
        mirror_upstream_raw(settings, copy_file=copy_file)
        pull_workbooks(settings, copy_file=copy_file)

        mt_hash = file_sha256(settings.microtrade_yaml)
        cfg = mt_config.load_config(settings.microtrade_yaml)
        stage1_failures = _run_stage1(settings, cfg, mt_hash)
        only_set = frozenset(only_keys) if only_keys is not None else None
        stage2_failures = _run_stage2(settings, cfg, mt_hash, copy_file, only_keys=only_set)

        push_manifests(settings, copy_file=copy_file)

        total = stage1_failures + stage2_failures
        if total:
            logger.error("run completed with {} failure(s)", total)
            return 1
        logger.info("run completed cleanly")
        return 0
    finally:
        if sink_id is not None:
            logger.remove(sink_id)


def main(config_path: Path = Path("config.yaml")) -> None:
    sys.exit(run(load_settings(config_path)))
