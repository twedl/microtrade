"""Project configuration for `microtrade import-spec`.

The project config is a single YAML file (conventionally `microtrade.yaml`
at the working directory root) that tells the importer, for each Excel
workbook it might see:

- the workbook's stable identifier,
- the period window the workbook's layout is active for,
- per-sheet filename patterns for raw data drops,
- optional explicit trade-type overrides when the positional mapping
  (sheet index -> TRADE_TYPES) doesn't fit.

The CLI reads this file once per `import-spec` run, looks up the workbook
by its filename basename, and passes the resulting `WorkbookConfig` into
`excel_spec.read_workbook`. At `microtrade ingest` time the config is not
consulted - the per-sheet patterns and period windows are baked into the
emitted YAML specs so discovery stays self-contained.

Example:

```yaml
workbooks:
  XYZ12345_Record_Layout.xls:
    workbook_id: XYZ12345
    effective_from: 2020-01
    effective_to: 2023-12        # optional; absent = open-ended
    sheets:
      Imports:
        trade_type: imports      # optional; defaults to positional
        filename_pattern: '^XYZ12345_Im(?P<year>\\d{4})(?P<month>\\d{2})\\.zip$'
```
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from microtrade.schema import (
    TRADE_TYPES,
    validate_filename_pattern,
    validate_period_window,
)

DEFAULT_CONFIG_PATH = Path("microtrade.yaml")


class ConfigError(ValueError):
    """Raised when the project config is malformed or references unknown values."""


@dataclass(frozen=True)
class SheetConfig:
    """Per-sheet rules within one workbook."""

    filename_pattern: str
    trade_type: str | None = None  # None -> positional mapping

    def __post_init__(self) -> None:
        validate_filename_pattern(self.filename_pattern, error_cls=ConfigError)
        if self.trade_type is not None and self.trade_type not in TRADE_TYPES:
            raise ConfigError(
                f"sheets.<name>.trade_type {self.trade_type!r} is not a known trade type; "
                f"allowed: {list(TRADE_TYPES)}"
            )


@dataclass(frozen=True)
class WorkbookConfig:
    """Config for one Excel workbook. All of its sheets share period window + workbook_id."""

    effective_from: str
    sheets: Mapping[str, SheetConfig]
    workbook_id: str | None = None
    effective_to: str | None = None

    def __post_init__(self) -> None:
        validate_period_window(self.effective_from, self.effective_to, error_cls=ConfigError)
        if not self.sheets:
            raise ConfigError("workbook config must declare at least one sheet")


@dataclass(frozen=True)
class ProjectConfig:
    """Full contents of `microtrade.yaml`, keyed by workbook filename basename."""

    workbooks: Mapping[str, WorkbookConfig]

    def get_workbook(self, workbook_path: Path) -> WorkbookConfig:
        """Look up a workbook's config by its filename basename."""
        name = workbook_path.name
        if name not in self.workbooks:
            known = sorted(self.workbooks)
            raise ConfigError(
                f"workbook {name!r} is not listed in the project config; known workbooks: {known}"
            )
        return self.workbooks[name]


def load_config(path: Path) -> ProjectConfig:
    """Parse `path` as a project config file. Raises `ConfigError` on any problem."""
    try:
        text = path.read_text(encoding="utf-8")
    except (FileNotFoundError, IsADirectoryError, PermissionError) as exc:
        raise ConfigError(f"cannot read project config {path}: {exc}") from exc
    raw = yaml.safe_load(text)
    if raw is None:
        raise ConfigError(f"{path}: empty file")
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: expected a mapping at top level, got {type(raw).__name__}")

    workbooks_raw = raw.get("workbooks")
    if not isinstance(workbooks_raw, dict) or not workbooks_raw:
        raise ConfigError(f"{path}: missing or empty 'workbooks' mapping")

    workbooks: dict[str, WorkbookConfig] = {}
    for name, entry in workbooks_raw.items():
        if not isinstance(entry, dict):
            raise ConfigError(f"{path}: workbook {name!r} must be a mapping")
        workbooks[str(name)] = _workbook_from_dict(str(name), entry)
    return ProjectConfig(workbooks=workbooks)


def _workbook_from_dict(name: str, data: dict[str, Any]) -> WorkbookConfig:
    try:
        effective_from = str(data["effective_from"])
    except KeyError as exc:
        raise ConfigError(f"workbook {name!r}: missing 'effective_from'") from exc

    sheets_raw = data.get("sheets")
    if not isinstance(sheets_raw, dict) or not sheets_raw:
        raise ConfigError(f"workbook {name!r}: missing or empty 'sheets' mapping")

    sheets: dict[str, SheetConfig] = {}
    for sheet_name, sheet_entry in sheets_raw.items():
        if not isinstance(sheet_entry, dict):
            raise ConfigError(f"workbook {name!r}: sheet {sheet_name!r} must be a mapping")
        sheets[str(sheet_name)] = _sheet_from_dict(name, str(sheet_name), sheet_entry)

    return WorkbookConfig(
        effective_from=effective_from,
        effective_to=(str(data["effective_to"]) if data.get("effective_to") else None),
        workbook_id=(str(data["workbook_id"]) if data.get("workbook_id") else None),
        sheets=sheets,
    )


def _sheet_from_dict(workbook_name: str, sheet_name: str, data: dict[str, Any]) -> SheetConfig:
    try:
        pattern = str(data["filename_pattern"])
    except KeyError as exc:
        raise ConfigError(
            f"workbook {workbook_name!r} sheet {sheet_name!r}: missing 'filename_pattern'"
        ) from exc
    trade_type = data.get("trade_type")
    return SheetConfig(
        filename_pattern=pattern,
        trade_type=str(trade_type) if trade_type is not None else None,
    )
