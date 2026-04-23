"""Per-file JSON manifests for ops state tracking.

One JSON per workbook (stage 1) or raw zip (stage 2). Read with
``read_manifest``, write atomically with ``write_manifest`` (tmp-then-
rename so a crash mid-write can't corrupt an existing manifest).
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class SpecManifest:
    workbook_name: str
    workbook_hash: str
    microtrade_hash: str
    specs_written: list[Path]
    processed_at: datetime


@dataclass
class RawManifest:
    raw_name: str
    raw_hash: str
    microtrade_hash: str
    trade_type: str
    year: str
    month: str
    flag: str
    processed_at: datetime


def _manifest_path(directory: Path, key: str) -> Path:
    return directory / f"{key}.json"


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"cannot serialize {type(obj).__name__}")


def read_manifest[M: (SpecManifest, RawManifest)](
    directory: Path, key: str, model: type[M]
) -> M | None:
    p = _manifest_path(directory, key)
    try:
        data = json.loads(p.read_text())
    except FileNotFoundError:
        return None
    data["processed_at"] = datetime.fromisoformat(data["processed_at"])
    if model is SpecManifest:
        data["specs_written"] = [Path(s) for s in data["specs_written"]]
    return model(**data)


def write_manifest(directory: Path, key: str, manifest: SpecManifest | RawManifest) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    p = _manifest_path(directory, key)
    tmp = directory / f"{p.name}.tmp"
    tmp.write_text(json.dumps(asdict(manifest), default=_json_default, indent=2))
    os.replace(tmp, p)
