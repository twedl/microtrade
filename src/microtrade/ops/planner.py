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
from microtrade.ops.hashing import hash_file
from microtrade.ops.manifest import (
    RawManifest,
    SpecManifest,
    read_manifest,
)
from microtrade.ops.settings import Settings


@dataclass(frozen=True)
class Match:
    """Which workbook/sheet a raw filename belongs to, plus extracted groups."""

    workbook_id: str
    sheet_name: str
    trade_type: str
    year: str
    month: str
    flag: str | None = None


@dataclass(frozen=True)
class YearKey:
    """Reprocessing unit: one trade type, one calendar year."""

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


def plan_stage1(settings: Settings) -> list[Path]:
    microtrade_hash = hash_file(settings.microtrade_yaml)
    dirty: list[Path] = []
    for wb in sorted(settings.workbooks_dir.iterdir()):
        if not wb.is_file():
            continue
        manifest = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
        if manifest is None:
            dirty.append(wb)
            continue
        if manifest.microtrade_hash != microtrade_hash:
            dirty.append(wb)
            continue
        if manifest.workbook_hash != hash_file(wb):
            dirty.append(wb)
            continue
    return dirty


def plan_stage2(settings: Settings, cfg: ProjectConfig) -> dict[YearKey, list[Path]]:
    microtrade_hash = hash_file(settings.microtrade_yaml)

    years: dict[YearKey, list[Path]] = {}
    dirty_keys: set[YearKey] = set()

    for raw in sorted(settings.raw_dir.iterdir()):
        if not raw.is_file():
            continue
        m = match_raw(raw.name, cfg)
        if m is None:
            logger.warning("no matching sheet for raw file: {}", raw.name)
            continue
        key = YearKey(m.trade_type, int(m.year))
        years.setdefault(key, []).append(raw)

        # Once a year is known dirty, every raw in it will be reprocessed;
        # hashing further raws in that year is wasted I/O.
        if key in dirty_keys:
            continue

        manifest = read_manifest(settings.raw_manifests_dir, raw.name, RawManifest)
        if (
            manifest is None
            or manifest.microtrade_hash != microtrade_hash
            or manifest.raw_hash != hash_file(raw)
        ):
            dirty_keys.add(key)

    return {k: years[k] for k in dirty_keys}
