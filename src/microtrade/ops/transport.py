"""Data-movement seam between local pod disk and the durable remote store.

The five functions are ordered around a single ``run()``:

1. ``pull_manifests`` — fetch shared manifest dirs from the remote onto
   local disk so the dirty-check sees work already done elsewhere.
2. ``mirror_upstream_raw`` — copy new/changed files from ``upstream_raw_dir``
   into our archive ``raw_remote_dir`` (upstream deletes periodically).
3. ``pull_raw`` — stage ``raw_remote_dir/current/`` onto pod-local ``raw_dir``.
4. ``push_processed`` — push a written ``(trade_type, year)`` output dir to
   the remote processed store, called per successful year.
5. ``push_manifests`` — publish updated manifest dirs back to the remote so
   the next run (anywhere) sees the new state.

Manifest hooks are what let multiple operators share a dirty-check
across pods and hosts without all seeing "everything is dirty" on their
first run. The raw/processed hooks handle bulk data movement.

Stubs today so the contract and call-site ordering are locked in; the
backend (rsync, s3, mounted PV, ``kubectl cp``) slots in here without
disturbing the rest of the pipeline.
"""

import os
import shutil
from collections.abc import Iterable
from pathlib import Path

from microtrade.ops.settings import Settings


def sync_tree(src: Path, dst: Path, patterns: Iterable[str] | None = None) -> None:
    """Copy ``src`` -> ``dst`` recursively, skipping files already up to date.

    Pure-Python ``rsync -a``-ish semantics: copy a file when it is missing
    at the destination or when its size / mtime differ. Preserves mtimes
    via ``shutil.copy2`` so subsequent runs skip unchanged files, and writes
    via ``target.tmp`` + ``os.replace`` so concurrent readers never see a
    half-copied file.

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
            # across every filesystem (FAT, some network FS).
            if s.st_size == t.st_size and int(s.st_mtime) == int(t.st_mtime):
                continue
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        shutil.copy2(p, tmp)
        os.replace(tmp, target)


def pull_manifests(settings: Settings) -> None:
    """Fetch spec_manifests_dir and raw_manifests_dir from the remote store.

    Called once at the start of `run()`, before any planning. Missing
    remote state should result in a no-op so a fresh deployment still
    runs cleanly (every manifest will look absent → everything dirty).
    """
    pass


def mirror_upstream_raw(settings: Settings) -> None:
    """Copy new/changed files from upstream_raw_dir into raw_remote_dir."""
    pass


def pull_raw(settings: Settings) -> None:
    """Sync raw_remote_dir/current/ -> local raw_dir."""
    pass


def push_processed(settings: Settings, paths: list[Path]) -> None:
    """Push given local paths to the remote processed store."""
    pass


def push_manifests(settings: Settings) -> None:
    """Publish spec_manifests_dir and raw_manifests_dir to the remote store.

    Called once at the end of `run()`, after both stages complete. Runs
    even when some partitions failed - partial progress is still worth
    sharing (a failed year simply has no manifest update to publish).
    """
    pass
