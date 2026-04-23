#!/usr/bin/env python3
"""Walkthrough of `microtrade.ops` against your project's real config.

Run from a project directory that contains `config.yaml` and the
`microtrade.yaml` it points at:

    uv run python path/to/ops_demo.py

Demonstrates:

  1. Loading the ops Settings from a cwd-relative ``config.yaml``.
  2. Calling ``microtrade.ops.runner.run`` directly — same API the CLI
     (``microtrade ops run``) uses under the hood.
  3. (Optional) overriding the per-file transfer primitive via the
     ``copy_file`` kwarg on ``run()``. The default is a thin
     ``shutil.copy2`` wrapper (local disk / mounted PV). Deployments
     that need something different (``kubectl cp``, S3 ``put_object``,
     …) plug it in here. This demo sticks with the default, so it
     works on any machine without rsync / aws cli / kubectl installed.

The library owns all path routing: ``mirror_upstream_raw`` /
``pull_raw`` / ``push_processed`` / ``pull_manifests`` /
``push_manifests`` are real implementations in
``microtrade.ops.transport``, not stubs. Your ``config.yaml`` picks
the paths; ``copy_file`` picks the primitive.
"""

from __future__ import annotations

import sys
from pathlib import Path

from microtrade.ops.runner import run
from microtrade.ops.settings import Settings, load_settings


def main() -> None:
    cwd = Path.cwd()
    config_path = cwd / "config.yaml"
    if not config_path.is_file():
        print(f"error: no config.yaml in {cwd}", file=sys.stderr)
        sys.exit(2)

    settings = load_settings(config_path)
    _summarize(settings)

    print("\n=== run #1 ===")
    rc1 = run(settings)
    print(f"exit code: {rc1}")

    print("\n=== run #2 (expect 'stage 1/2: nothing to do' if inputs didn't change) ===")
    rc2 = run(settings)
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
