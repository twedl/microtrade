"""Data-movement seam between local pod disk and the durable remote store.

The three functions are ordered around a single ``run()``:

1. ``mirror_upstream_raw`` — copy new/changed files from ``upstream_raw_dir``
   into our archive ``raw_remote_dir`` (upstream deletes periodically).
2. ``pull_raw`` — stage ``raw_remote_dir/current/`` onto pod-local ``raw_dir``.
3. ``push_processed`` — push a written ``(trade_type, year)`` output dir to
   the remote processed store, called per successful year.

Stubs today so the contract and call-site ordering are locked in; the
backend (rsync, s3, mounted PV, ``kubectl cp``) slots in here without
disturbing the rest of the pipeline.
"""

from pathlib import Path

from microtrade.ops.settings import Settings


def mirror_upstream_raw(settings: Settings) -> None:
    """Copy new/changed files from upstream_raw_dir into raw_remote_dir."""
    pass


def pull_raw(settings: Settings) -> None:
    """Sync raw_remote_dir/current/ -> local raw_dir."""
    pass


def push_processed(settings: Settings, paths: list[Path]) -> None:
    """Push given local paths to the remote processed store."""
    pass
