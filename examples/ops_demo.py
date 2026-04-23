#!/usr/bin/env python3
"""Walkthrough of `microtrade.ops` against your project's real config.

Run from a project directory that contains `config.yaml` and the
`microtrade.yaml` it points at:

    uv run python path/to/ops_demo.py

Demonstrates three things:

  1. Loading the ops Settings from a cwd-relative ``config.yaml``.
  2. Calling ``microtrade.ops.runner.run`` directly — same API the CLI
     (``microtrade ops run``) uses under the hood.
  3. Supplying all five environment-specific transport functions as
     kwargs to ``run()`` — ``pull_manifests`` / ``mirror`` / ``pull`` /
     ``push`` / ``push_manifests``. The defaults are the ``pass``-body
     stubs in ``microtrade.ops.transport``; overriding them is how you
     plug in rsync, s3, mounted-PV, ``kubectl cp``, etc. without
     touching the library. The manifest hooks let multiple operators
     share dirty-check state so nobody sees "everything dirty" just
     because they don't have the other's PV.

This demo uses ``microtrade.ops.transport.sync_tree`` (pure-Python,
stdlib-only, skips unchanged files by size+mtime) for every hook, so
it works on any machine without rsync installed. Real deployments
typically swap in rsync, ``aws s3 sync``, ``kubectl cp``, etc.
"""

from __future__ import annotations

import sys
from pathlib import Path

from microtrade.ops.runner import run
from microtrade.ops.settings import Settings, load_settings
from microtrade.ops.transport import sync_tree

# Backends for the five transport hooks. Uses microtrade.ops.transport.sync_tree
# (pure-Python, stdlib-only, rsync-like skip-if-unchanged) so the demo runs on
# any box. Swap sync_tree for rsync / aws s3 sync / kubectl cp / etc. in your
# real deployment.
#
# Manifests live under their own remote root so two operators running from
# different machines converge on the same dirty-check state.


def pull_manifests(settings: Settings) -> None:
    remote = _manifests_remote_root(settings)
    sync_tree(remote / "specs", settings.spec_manifests_dir)
    sync_tree(remote / "raw", settings.raw_manifests_dir)


def mirror(settings: Settings) -> None:
    sync_tree(settings.upstream_raw_dir, settings.raw_remote_dir / "current")


def pull(settings: Settings) -> None:
    # Upstream drops workbooks (.xls/.xlsx) and raw zips (.zip) into one
    # directory. Split by extension on the way down so stage 1 reads
    # workbooks_dir and stage 2 reads raw_dir without stepping on each other.
    src = settings.raw_remote_dir / "current"
    sync_tree(src, settings.raw_dir, patterns=["*.zip"])
    sync_tree(src, settings.workbooks_dir, patterns=["*.xls", "*.xlsx"])


def push(settings: Settings, paths: list[Path]) -> None:
    remote = settings.raw_remote_dir.parent / "processed"
    for p in paths:
        sync_tree(p, remote / p.relative_to(settings.processed_dir))


def push_manifests(settings: Settings) -> None:
    remote = _manifests_remote_root(settings)
    sync_tree(settings.spec_manifests_dir, remote / "specs")
    sync_tree(settings.raw_manifests_dir, remote / "raw")


def _manifests_remote_root(settings: Settings) -> Path:
    return settings.raw_remote_dir.parent / "manifests"


# --- 2. Entry point -------------------------------------------------------


def main() -> None:
    cwd = Path.cwd()
    config_path = cwd / "config.yaml"
    if not config_path.is_file():
        print(f"error: no config.yaml in {cwd}", file=sys.stderr)
        sys.exit(2)

    settings = load_settings(config_path)
    _summarize(settings)

    transport_kwargs = {
        "pull_manifests_fn": pull_manifests,
        "mirror": mirror,
        "pull": pull,
        "push": push,
        "push_manifests_fn": push_manifests,
    }

    print("\n=== run #1 ===")
    rc1 = run(settings, **transport_kwargs)
    print(f"exit code: {rc1}")

    print("\n=== run #2 (expect 'stage 1/2: nothing to do' if inputs didn't change) ===")
    rc2 = run(settings, **transport_kwargs)
    print(f"exit code: {rc2}")

    sys.exit(rc1 or rc2)


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
