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


def load_settings(yaml_path: Path) -> Settings:
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    data: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}

    kwargs: dict[str, Path] = {}
    for f in dataclasses.fields(Settings):
        env_value = os.environ.get(f"MT_{f.name.upper()}")
        value = env_value if env_value is not None else data.get(f.name)
        if value is None:
            raise SettingsError(f"missing required field {f.name!r} in {yaml_path}")
        kwargs[f.name] = Path(value)
    return Settings(**kwargs)
