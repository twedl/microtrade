#!/usr/bin/env python3
"""Walkthrough of `microtrade.ops` against your project's real config.

Run from a project directory that contains `config.yaml` and the
`microtrade.yaml` it points at:

    uv run python path/to/ops_demo.py

Demonstrates three things:

  1. Loading the ops Settings from a cwd-relative ``config.yaml``.
  2. Calling ``microtrade.ops.runner.run`` directly — same API the CLI
     (``microtrade ops run``) uses under the hood.
  3. Supplying environment-specific transport functions
     (``mirror`` / ``pull`` / ``push``) as kwargs to ``run()``. The
     defaults are the ``pass``-body stubs in
     ``microtrade.ops.transport``; overriding them is how you plug in
     rsync, s3, mounted-PV, ``kubectl cp``, etc. without touching the
     library.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from microtrade.ops.runner import run
from microtrade.ops.settings import Settings, load_settings

# --- 1. Wire up transport -------------------------------------------------
#
# Replace the bodies with whatever your deployment needs. The contract:
#
#   mirror(settings)           # run once, before pull. Copy new/changed
#                              # files from upstream_raw_dir into
#                              # raw_remote_dir (upstream deletes drops
#                              # periodically, so verify by content hash
#                              # rather than mtime).
#
#   pull(settings)             # run once, before stage 2. Sync
#                              # raw_remote_dir/current/ -> raw_dir so
#                              # stage 2 has inputs on pod-local disk.
#
#   push(settings, year_dirs)  # run after each successful year. Push
#                              # the listed processed year directories
#                              # to the remote processed store.
#
# The example below uses ``rsync -a`` locally so you can see real I/O
# happen without needing an S3 bucket. Nothing here is
# microtrade-specific — swap in your own transport.


def mirror(settings: Settings) -> None:
    _rsync(settings.upstream_raw_dir, settings.raw_remote_dir / "current")


def pull(settings: Settings) -> None:
    _rsync(settings.raw_remote_dir / "current", settings.raw_dir)


def push(settings: Settings, paths: list[Path]) -> None:
    remote = settings.raw_remote_dir.parent / "processed"
    remote.mkdir(parents=True, exist_ok=True)
    for p in paths:
        _rsync(p, remote / p.relative_to(settings.processed_dir))


def _rsync(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.mkdir(parents=True, exist_ok=True)
    subprocess.run(["rsync", "-a", f"{src}/", f"{dst}/"], check=True)


# --- 2. Entry point -------------------------------------------------------


def main() -> None:
    cwd = Path.cwd()
    config_path = cwd / "config.yaml"
    if not config_path.is_file():
        print(f"error: no config.yaml in {cwd}", file=sys.stderr)
        sys.exit(2)

    settings = load_settings(config_path)
    _summarize(settings)

    print("\n=== run #1 ===")
    rc1 = _run_with_transport(settings, mirror, pull, push)

    print("\n=== run #2 (expect 'stage 1/2: nothing to do' if inputs didn't change) ===")
    rc2 = _run_with_transport(settings, mirror, pull, push)

    sys.exit(rc1 or rc2)


def _run_with_transport(
    settings: Settings,
    mirror_fn: Callable[[Settings], None],
    pull_fn: Callable[[Settings], None],
    push_fn: Callable[[Settings, list[Path]], None],
) -> int:
    rc = run(settings, mirror=mirror_fn, pull=pull_fn, push=push_fn)
    print(f"exit code: {rc}")
    return rc


def _summarize(settings: Settings) -> None:
    def count(p: Path) -> int:
        return sum(1 for _ in p.iterdir()) if p.is_dir() else 0

    print(f"microtrade.yaml: {settings.microtrade_yaml}")
    print(f"workbooks_dir:   {settings.workbooks_dir}  ({count(settings.workbooks_dir)} file(s))")
    print(f"raw_dir:         {settings.raw_dir}  ({count(settings.raw_dir)} file(s))")
    print(f"specs_dir:       {settings.specs_dir}")
    print(f"processed_dir:   {settings.processed_dir}")
    print(f"spec manifests:  {count(settings.spec_manifests_dir)}")
    print(f"raw manifests:   {count(settings.raw_manifests_dir)}")


if __name__ == "__main__":
    main()
