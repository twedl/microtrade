"""Cron-driven orchestration over microtrade's ingest pipeline.

A single ``run(settings)`` call drives the ordering:
``mirror_upstream_raw -> pull_raw -> stage 1 -> stage 2 (push_processed per
year)``. Stage 1 regenerates spec YAMLs for dirty workbooks; stage 2
ingests each dirty ``(trade_type, year)`` and only records its raw
manifests on success, so a failed year self-heals on the next run.

``import_spec`` and ``ingest_year`` are module-level functions that the
test suite replaces via ``monkeypatch.setattr`` (see
``tests/ops/test_runner.py``) — no adapter class, no dependency
injection ceremony at the call site.

Transport (``mirror_upstream_raw`` / ``pull_raw`` / ``push_processed``)
is different: it's environment-specific, not test-only. ``run()``
accepts ``mirror=`` / ``pull=`` / ``push=`` keyword overrides so
production entrypoints can supply their own implementations without
monkeypatching module globals. Defaults point at the stubs in
``microtrade.ops.transport``.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
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
from microtrade.ops.transport import mirror_upstream_raw, pull_raw, push_processed
from microtrade.pipeline import PipelineConfig, RunSummary
from microtrade.pipeline import run as _mt_run
from microtrade.schema import file_sha256

TransportFn = Callable[[Settings], None]
PushFn = Callable[[Settings, list[Path]], None]


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
    trade_type: str, year: int, raw_dir: Path, specs_dir: Path, out_dir: Path
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
    )
    return _mt_run(cfg)


def _year_output_dir(settings: Settings, key: YearKey) -> Path:
    return settings.processed_dir / key.trade_type / f"year={key.year}"


def _run_stage1(settings: Settings, mt_hash: str) -> int:
    dirty = plan_stage1(settings, microtrade_hash=mt_hash)
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


def _run_stage2(settings: Settings, cfg: ProjectConfig, mt_hash: str, push: PushFn) -> int:
    dirty = plan_stage2(settings, cfg, microtrade_hash=mt_hash)
    if not dirty:
        logger.info("stage 2: nothing to do")
        return 0
    logger.info("stage 2: {} (trade_type, year) to process", len(dirty))
    failures = 0
    for key, raws in dirty.items():
        try:
            summary = ingest_year(
                trade_type=key.trade_type,
                year=key.year,
                raw_dir=settings.raw_dir,
                specs_dir=settings.specs_dir,
                out_dir=settings.processed_dir,
            )
            if summary.failed_count > 0:
                raise RuntimeError(
                    f"microtrade reported {summary.failed_count} partition failure(s)"
                )

            push(settings, [_year_output_dir(settings, key)])

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
        except Exception:
            logger.exception("stage 2 failed for {}", key)
            failures += 1
    return failures


def run(
    settings: Settings,
    *,
    mirror: TransportFn | None = None,
    pull: TransportFn | None = None,
    push: PushFn | None = None,
) -> int:
    """Drive one ops cycle.

    ``mirror`` / ``pull`` / ``push`` default to the stubs in
    ``microtrade.ops.transport``. Production callers supply their own:

        from microtrade.ops.runner import run
        from my_app.transport import mirror, pull, push
        sys.exit(run(settings, mirror=mirror, pull=pull, push=push))

    Defaults are resolved at call time (not at function-def time), so
    ``monkeypatch.setattr("microtrade.ops.runner.mirror_upstream_raw",
    ...)`` still works in tests that don't override via kwargs.
    """
    mirror_fn = mirror if mirror is not None else mirror_upstream_raw
    pull_fn = pull if pull is not None else pull_raw
    push_fn = push if push is not None else push_processed

    mirror_fn(settings)
    pull_fn(settings)

    mt_hash = file_sha256(settings.microtrade_yaml)
    stage1_failures = _run_stage1(settings, mt_hash)

    cfg = mt_config.load_config(settings.microtrade_yaml)
    stage2_failures = _run_stage2(settings, cfg, mt_hash, push_fn)

    total = stage1_failures + stage2_failures
    if total:
        logger.error("run completed with {} failure(s)", total)
        return 1
    logger.info("run completed cleanly")
    return 0


def main(config_path: Path = Path("config.yaml")) -> None:
    sys.exit(run(load_settings(config_path)))
