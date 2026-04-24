"""Dirty-check planning for the ops layer.

Stage 1 plans which workbooks need `microtrade import-spec` re-run.
Stage 2 plans which `(trade_type, year)` pairs need `microtrade
pipeline.run` re-run. Both compare current file hashes against the
last recorded manifest (see `microtrade.ops.manifest`).

Matches raw filenames to workbook/sheet entries in the project config
(``microtrade.yaml``) using each sheet's ``filename_pattern`` and the
workbook's ``[effective_from, effective_to]`` window. First match wins;
windows are guaranteed non-overlapping by the project config, so
first-match is deterministic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from microtrade.config import ProjectConfig
from microtrade.ops.manifest import (
    RawManifest,
    SpecManifest,
    read_manifest,
)
from microtrade.ops.settings import Settings
from microtrade.schema import file_sha256


@dataclass(frozen=True)
class Match:
    """Which workbook/sheet a raw filename belongs to, plus extracted groups."""

    workbook_id: str
    sheet_name: str
    trade_type: str
    year: str
    month: str
    flag: str | None = None


@dataclass(frozen=True, order=True)
class YearKey:
    """Reprocessing unit: one trade type, one calendar year.

    ``order=True`` gives tuple-order sorting on ``(trade_type, year)``,
    which the runner relies on to process years deterministically.
    """

    trade_type: str
    year: int


def match_raw(filename: str, cfg: ProjectConfig) -> Match | None:
    for wb_name, wb in cfg.workbooks.items():
        for sheet_name, sheet in wb.sheets.items():
            if sheet.trade_type is None:
                continue
            m = re.match(sheet.filename_pattern, filename)
            if m is None:
                continue
            gd = m.groupdict()
            ym = f"{gd['year']}-{gd['month']}"
            if ym < wb.effective_from:
                continue
            if wb.effective_to is not None and ym > wb.effective_to:
                continue
            return Match(
                workbook_id=wb.workbook_id or wb_name,
                sheet_name=sheet_name,
                trade_type=sheet.trade_type,
                year=gd["year"],
                month=gd["month"],
                flag=gd.get("flag"),
            )
    return None


def plan_stage1(
    settings: Settings,
    cfg: ProjectConfig,
    *,
    microtrade_hash: str | None = None,
) -> list[Path]:
    """Return workbooks listed in ``cfg`` that need spec regeneration.

    Files in ``workbooks_dir`` not named in ``cfg.workbooks`` are
    ignored — config is the source of truth for what counts as a
    workbook. This keeps stray files (e.g. raw zips accidentally
    pulled into ``workbooks_dir``, or unknown xlsx) from crashing
    stage 1 when ``import_spec`` looks them up.

    A workbook is also dirty if any path in the manifest's
    ``specs_written`` no longer exists on disk. Reason: renaming
    ``specs_dir`` (or deleting specs manually) would otherwise leave
    the manifest claiming "done at hash H" while stage 2's
    ``discover.scan`` finds no specs to match against, producing a
    silent no-op run.
    """
    mt_hash = microtrade_hash or file_sha256(settings.microtrade_yaml)
    dirty: list[Path] = []
    for wb in sorted(settings.workbooks_dir.iterdir()):
        if not wb.is_file():
            continue
        if wb.name not in cfg.workbooks:
            logger.warning("workbook not in microtrade.yaml, skipping: {}", wb.name)
            continue
        manifest = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
        if (
            manifest is None
            or manifest.microtrade_hash != mt_hash
            or manifest.workbook_hash != file_sha256(wb)
            or not all(p.exists() for p in manifest.specs_written)
        ):
            dirty.append(wb)
    return dirty


def _year_output_present(settings: Settings, key: YearKey) -> bool:
    """True if the remote processed year dir exists and is non-empty.

    Checks ``processed_remote_dir/<trade_type>/year=<year>/``. The
    per-year loop in ``_run_stage2`` deletes the local processed dir
    after each push succeeds, so local is an unreliable source of
    truth for "is this year done." Remote is where published output
    lives and where this check belongs.
    """
    year_dir = settings.processed_remote_dir / key.trade_type / f"year={key.year}"
    return year_dir.is_dir() and any(year_dir.iterdir())


def plan_stage2(
    settings: Settings,
    cfg: ProjectConfig,
    *,
    microtrade_hash: str | None = None,
) -> dict[YearKey, list[Path]]:
    """Plan which ``(trade_type, year)`` pairs need re-ingest.

    Iterates ``raw_remote_dir/current`` — the permanent archive — not
    the ephemeral local ``raw_dir``. In the per-year model local
    ``raw_dir`` is cleaned up after each year, so the remote archive
    is the only reliable source of truth for "what raws exist."

    Short-circuits the hash check: if the raw's remote mtime hasn't
    advanced past the manifest's ``processed_at``, trust the manifest.
    This avoids re-hashing multi-GB zips on every run. Only falls back
    to a full hash compare when mtime suggests the file changed.
    """
    mt_hash = microtrade_hash or file_sha256(settings.microtrade_yaml)
    source_dir = settings.raw_remote_dir / "current"

    years: dict[YearKey, list[Path]] = {}
    dirty_keys: set[YearKey] = set()

    if not source_dir.exists():
        return {}

    for raw in sorted(source_dir.iterdir()):
        if not raw.is_file() or raw.suffix != ".zip":
            continue
        m = match_raw(raw.name, cfg)
        if m is None:
            logger.warning("no matching sheet for raw file: {}", raw.name)
            continue
        key = YearKey(m.trade_type, int(m.year))
        years.setdefault(key, []).append(raw)

        # Once a year is known dirty, every raw in it will be reprocessed;
        # re-checking further raws in that year is wasted I/O.
        if key in dirty_keys:
            continue

        manifest = read_manifest(settings.raw_manifests_dir, raw.name, RawManifest)
        if manifest is None or manifest.microtrade_hash != mt_hash:
            dirty_keys.add(key)
            continue
        if not _year_output_present(settings, key):
            dirty_keys.add(key)
            continue
        # Cheap mtime short-circuit: only hash when the remote file
        # mtime has advanced past when we processed it.
        if int(raw.stat().st_mtime) > int(manifest.processed_at.timestamp()) and (
            file_sha256(raw) != manifest.raw_hash
        ):
            dirty_keys.add(key)

    return {k: years[k] for k in dirty_keys}
