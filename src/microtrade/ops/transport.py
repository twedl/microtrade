"""Data-movement seam between local pod disk and the durable remote store.

The hooks are ordered around a single ``run()``:

1. ``pull_manifests`` — fetch shared manifest dirs from the remote onto
   local disk so the dirty-check sees work already done elsewhere.
2. ``mirror_upstream_raw`` — copy new/changed files from ``upstream_raw_dir``
   into our archive ``raw_remote_dir`` (upstream deletes periodically).
3. ``pull_workbooks`` — stage ``.xls``/``.xlsx`` workbooks from
   ``raw_remote_dir/current`` onto ``workbooks_dir``. Workbooks are
   small and shared across years, so they live locally for the run's
   duration.
4. ``pull_raws_for_year`` — stage ONLY the raw zips matching one
   ``(trade_type, year)`` onto ``raw_dir``. Called inside stage 2's
   per-year loop so local disk only ever holds one year's raw zips at
   a time, not the full archive.
5. ``push_processed`` — push a written ``(trade_type, year)`` output dir
   to ``processed_remote_dir``, called per successful year.
6. ``cleanup_local_year`` — remove the pulled raw zips and the
   processed output for one ``(trade_type, year)`` after a successful
   push. Keeps peak local disk bounded by one year's worth of data.
7. ``push_manifests`` — publish updated manifest dirs back to the remote
   so the next run (anywhere) sees the new state.

Each hook is a real implementation here — not a stub. Callers don't
override the hooks themselves; instead they inject a single per-file
``copy_file`` primitive via ``run(..., copy_file=...)`` which threads
all the way through ``sync_tree`` / ``_copy_if_newer``. The library
owns the tree walk and the skip-if-unchanged check; ``copy_file``
owns atomicity.

``copy_file`` defaults to a thin ``shutil.copy2`` wrapper that writes
to ``target.tmp`` and ``os.replace``'s it into place (crash-safe on
local disk / mounted PV). Deployments whose remote is reached via a
flaky network mount where ``os.replace`` itself drops — or an object
store where ``put_object`` is already atomic — supply their own
``copy_file`` that publishes ``dst`` directly and skip the
tmp+rename dance. Contract: ``copy_file`` must publish ``dst``
atomically (no half-written file visible to readers). The skip check
is rsync's ``--update`` rule (size match + target mtime ≥ source
mtime), so ``copy_file`` is not required to preserve mtime — a fresh
copy leaves ``dst`` with "now" as its mtime, still ≥ source, which
the next run reads as up-to-date.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path

from microtrade.ops.planner import YearKey
from microtrade.ops.settings import Settings

CopyFn = Callable[[Path, Path], None]


def _shutil_copy2(src: Path, dst: Path) -> None:
    """Default ``CopyFn``: atomic ``shutil.copy2`` via ``.tmp`` + ``os.replace``.

    Writes to a sibling ``dst.tmp`` and renames into place so an
    interrupted copy never leaves a partially-written ``dst`` visible
    to readers.
    """
    tmp = dst.with_name(dst.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _copy_if_newer(src: Path, dst: Path, copy_file: CopyFn) -> None:
    """Copy ``src`` -> ``dst`` unless ``dst`` is already up to date.

    Skip rule: sizes match AND the target's mtime is at least as new
    as the source's (rsync ``--update`` semantic, truncated to whole
    seconds). Makes ``dst``'s parent if needed.
    """
    if dst.exists():
        s, t = src.stat(), dst.stat()
        if s.st_size == t.st_size and int(t.st_mtime) >= int(s.st_mtime):
            return
    dst.parent.mkdir(parents=True, exist_ok=True)
    copy_file(src, dst)


def sync_tree(
    src: Path,
    dst: Path,
    *,
    patterns: Iterable[str] | None = None,
    copy_file: CopyFn = _shutil_copy2,
) -> None:
    """Copy ``src`` -> ``dst`` recursively, skipping files already up to date.

    Missing source is a no-op (a fresh deployment has nothing to mirror).
    ``patterns`` filters by :py:meth:`pathlib.PurePath.match` against the
    relative path; ``None`` copies every file.
    """
    if not src.exists():
        return
    for p in src.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(src)
        if patterns is not None and not any(rel.match(pat) for pat in patterns):
            continue
        _copy_if_newer(p, dst / rel, copy_file)


def pull_manifests(settings: Settings, *, copy_file: CopyFn = _shutil_copy2) -> None:
    """Fetch ``manifests_remote_dir/{specs,raw}`` onto local manifest dirs."""
    sync_tree(
        settings.manifests_remote_dir / "specs",
        settings.spec_manifests_dir,
        copy_file=copy_file,
    )
    sync_tree(
        settings.manifests_remote_dir / "raw",
        settings.raw_manifests_dir,
        copy_file=copy_file,
    )


def mirror_upstream_raw(settings: Settings, *, copy_file: CopyFn = _shutil_copy2) -> None:
    """Copy ``upstream_raw_dir`` -> ``raw_remote_dir/current``."""
    sync_tree(
        settings.upstream_raw_dir,
        settings.raw_remote_dir / "current",
        copy_file=copy_file,
    )


def pull_workbooks(settings: Settings, *, copy_file: CopyFn = _shutil_copy2) -> None:
    """Stage ``.xls``/``.xlsx`` workbooks from ``raw_remote_dir/current``.

    Workbooks drive stage 1 (spec generation). They're small, shared
    across every year, and don't grow with the archive — so they live
    on local disk for the run's duration.
    """
    sync_tree(
        settings.raw_remote_dir / "current",
        settings.workbooks_dir,
        patterns=["*.xls", "*.xlsx"],
        copy_file=copy_file,
    )


def pull_raws_for_year(
    settings: Settings,
    remote_raws: list[Path],
    *,
    copy_file: CopyFn = _shutil_copy2,
) -> list[Path]:
    """Stage ``remote_raws`` onto ``raw_dir`` and return local paths.

    ``remote_raws`` comes from ``plan_stage2`` (which already iterated
    the archive and filtered by ``(trade_type, year)``), so this hook
    does not re-scan the remote dir. Returns one local path per
    remote path, in the same order — skipped-existing files count.
    """
    local: list[Path] = []
    for src in remote_raws:
        dst = settings.raw_dir / src.name
        _copy_if_newer(src, dst, copy_file)
        local.append(dst)
    return local


def push_processed(
    settings: Settings,
    paths: list[Path],
    *,
    copy_file: CopyFn = _shutil_copy2,
) -> None:
    """Push each local ``processed_dir`` subtree into ``processed_remote_dir``."""
    for p in paths:
        sync_tree(
            p,
            settings.processed_remote_dir / p.relative_to(settings.processed_dir),
            copy_file=copy_file,
        )


def cleanup_local_raws(settings: Settings) -> None:
    """Remove every ``.zip`` from ``raw_dir``.

    In the per-year cycle ``raw_dir`` only ever holds the current
    year's zips (pull_raws_for_year pulls this year, cleanup runs
    after ingest or on the ingest-failure path before the next
    year's pull), so this clears exactly the right set without
    needing to know which year is current.
    """
    if not settings.raw_dir.exists():
        return
    for p in settings.raw_dir.iterdir():
        if p.is_file() and p.suffix == ".zip":
            p.unlink()


def cleanup_local_year(settings: Settings, key: YearKey) -> None:
    """Remove local raws AND the local processed year dir for ``key``.

    Called after a successful push — the parquet is safely on the
    remote, no reason to keep the local copy. On push-failure paths
    the caller uses :func:`cleanup_local_raws` instead to keep the
    parquet for retry.
    """
    cleanup_local_raws(settings)
    year_dir = settings.processed_dir / key.trade_type / f"year={key.year}"
    if year_dir.exists():
        shutil.rmtree(year_dir)


def push_manifests(settings: Settings, *, copy_file: CopyFn = _shutil_copy2) -> None:
    """Publish local manifest dirs into ``manifests_remote_dir/{specs,raw}``."""
    sync_tree(
        settings.spec_manifests_dir,
        settings.manifests_remote_dir / "specs",
        copy_file=copy_file,
    )
    sync_tree(
        settings.raw_manifests_dir,
        settings.manifests_remote_dir / "raw",
        copy_file=copy_file,
    )
