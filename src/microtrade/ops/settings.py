"""Ops settings: paths + dirs loaded from a YAML file with ``MT_`` env overrides.

Hand-rolled to avoid pulling pydantic into the dependency tree. Schema
is still declared as a dataclass so missing keys surface at load time
rather than first use.
"""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class SettingsError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    microtrade_yaml: Path
    workbooks_dir: Path
    raw_dir: Path
    specs_dir: Path
    processed_dir: Path
    spec_manifests_dir: Path
    raw_manifests_dir: Path
    upstream_raw_dir: Path
    raw_remote_dir: Path
    processed_remote_dir: Path
    manifests_remote_dir: Path
    # Encoding for reading raw fixed-width files. Statistics Canada
    # drops are often Windows-1252 / Latin-1 (e.g. "É" = 0xC9), so
    # surface this as config. Matches ``PipelineConfig.encoding``.
    encoding: str = "utf-8"
    # Optional path to a loguru sink file. When set, ``run()`` adds a
    # file sink alongside the default stderr sink so runs leave an
    # on-disk trail without the caller having to configure loguru.
    log_file: str | None = None


def load_settings(yaml_path: Path) -> Settings:
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    data: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}

    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(Settings):
        env_value = os.environ.get(f"MT_{f.name.upper()}")
        value = env_value if env_value is not None else data.get(f.name)
        has_default = (
            f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING
        )
        if value is None:
            if has_default:
                continue
            raise SettingsError(f"missing required field {f.name!r} in {yaml_path}")
        # Required fields are Path-typed; optional ones keep their raw value.
        kwargs[f.name] = value if has_default else Path(value)
    return Settings(**kwargs)
