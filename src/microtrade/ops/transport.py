"""Data-movement seam between local pod disk and the durable remote store.

The five hooks are ordered around a single ``run()``:

1. ``pull_manifests`` — fetch shared manifest dirs from the remote onto
   local disk so the dirty-check sees work already done elsewhere.
2. ``mirror_upstream_raw`` — copy new/changed files from ``upstream_raw_dir``
   into our archive ``raw_remote_dir`` (upstream deletes periodically).
3. ``pull_raw`` — stage ``raw_remote_dir/current/`` onto pod-local
   ``raw_dir`` (zips) and ``workbooks_dir`` (``.xls`` / ``.xlsx``).
4. ``push_processed`` — push a written ``(trade_type, year)`` output dir to
   ``processed_remote_dir``, called per successful year.
5. ``push_manifests`` — publish updated manifest dirs back to the remote so
   the next run (anywhere) sees the new state.

Each hook is a real implementation here — not a stub. Callers don't
override the hooks themselves; instead they inject a single per-file
``copy_file`` primitive via ``run(..., copy_file=...)`` which threads
all the way through ``sync_tree``. The library owns the tree walk and
the skip-if-unchanged check; ``copy_file`` owns atomicity.

``copy_file`` defaults to a thin ``shutil.copy2`` wrapper that writes
to ``target.tmp`` and ``os.replace``'s it into place (crash-safe on
local disk / mounted PV). Deployments whose remote is reached via a
flaky network mount where ``os.replace`` itself drops — or an object
store where ``put_object`` is already atomic — supply their own
``copy_file`` that publishes ``dst`` directly and skip the
tmp+rename dance. Contract: ``copy_file`` must publish ``dst``
atomically (no half-written file visible to readers). ``sync_tree``'s
skip check is rsync's ``--update`` rule (size match + target mtime ≥
source mtime), so ``copy_file`` is not required to preserve mtime —
a fresh copy leaves ``dst`` with "now" as its mtime, still ≥ source,
which the next run reads as up-to-date.
"""

import os
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path

from microtrade.ops.settings import Settings

CopyFn = Callable[[Path, Path], None]


def _shutil_copy2(src: Path, dst: Path) -> None:
    """Default ``CopyFn``: atomic ``shutil.copy2`` via ``.tmp`` + ``os.replace``.

    Writes to a sibling ``dst.tmp`` and renames into place so an
    interrupted copy never leaves a partially-written ``dst`` visible
    to readers. ``shutil.copy2`` preserves mtime, which the skip check
    in :func:`sync_tree` relies on.
    """
    tmp = dst.with_name(dst.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def sync_tree(
    src: Path,
    dst: Path,
    *,
    patterns: Iterable[str] | None = None,
    copy_file: CopyFn = _shutil_copy2,
) -> None:
    """Copy ``src`` -> ``dst`` recursively, skipping files already up to date.

    ``sync_tree`` owns the tree walk, the skip-if-unchanged check, and
    the ``target.parent`` mkdir.

    Skip rule: sizes match and the target's mtime is at least as new as
    the source's (rsync ``--update`` semantic). Works even when
    ``copy_file`` doesn't preserve mtime — after a copy the target's
    mtime is "now", still ≥ source, so the next run skips. The hole is
    an upstream rollback with identical size and an older mtime, which
    this doesn't detect.

    ``copy_file(src_file, dst_file)`` must publish ``dst_file``
    atomically — no half-written file visible to readers. The default
    wraps ``shutil.copy2`` in a ``.tmp`` + ``os.replace``, which is
    right for local disk / mounted PV.

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
        target = dst / rel
        if target.exists():
            s, t = p.stat(), target.stat()
            # Truncate to whole seconds: sub-second mtime isn't preserved
            # across every filesystem (FAT, some network FS). Skip when
            # sizes match and the target is at least as new as the source
            # (rsync --update semantic). Tolerates ``copy_file``s that
            # don't preserve mtime — after a fresh copy the target's
            # mtime is "now", still >= source, so we skip next run.
            if s.st_size == t.st_size and int(t.st_mtime) >= int(s.st_mtime):
                continue
        target.parent.mkdir(parents=True, exist_ok=True)
        copy_file(p, target)


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


def pull_raw(settings: Settings, *, copy_file: CopyFn = _shutil_copy2) -> None:
    """Stage ``raw_remote_dir/current`` onto ``raw_dir`` / ``workbooks_dir``.

    The upstream drop mixes workbooks and raw zips. Split by extension
    at pull time: ``*.zip`` -> ``raw_dir`` (stage 2 input),
    ``*.xls`` / ``*.xlsx`` -> ``workbooks_dir`` (stage 1 input).
    """
    src = settings.raw_remote_dir / "current"
    sync_tree(src, settings.raw_dir, patterns=["*.zip"], copy_file=copy_file)
    sync_tree(
        src,
        settings.workbooks_dir,
        patterns=["*.xls", "*.xlsx"],
        copy_file=copy_file,
    )


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
