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

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from microtrade.schema import (
    CANONICAL_DTYPES,
    COMPUTED_KINDS,
    DATE_PARSERS,
    TRADE_TYPES,
    ComputedColumn,
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
    # Name of the column that carries a YYYYMM (or YYYYMMDD) date value
    # identifying which month each row belongs to. Ingest uses it to
    # route rows to (year, month) partitions. The column does not have
    # to be literally named "period" - set this to whatever the sheet
    # actually calls it (e.g. "year_month"). The referenced column must
    # end up Date-typed in the spec (via workbook dtype or `cast` here).
    #
    # Matched against the *logical* (post-rename) column name. If you
    # rename `foo_physical` -> `bar_logical`, set `routing_column:
    # bar_logical`. Columns with no rename match on their physical name
    # since `effective_name` falls back to that.
    routing_column: str = "period"
    trade_type: str | None = None  # None -> positional mapping
    # Physical-to-logical column renames applied at import time. Each entry
    # `physical: logical` stamps `logical_name=logical` on the Column whose
    # Description cell (physical_name) is `physical`. Use this when upstream
    # renames a column across workbook versions - the combined dataset sees
    # one stable logical column even as the physical name drifts.
    rename: Mapping[str, str] = field(default_factory=dict)
    # Override the workbook's declared dtype for specific columns. Upstream
    # FWF specs often call numeric columns `Char` (they're just character
    # fields); `cast` promotes them to Int64/Float64/Date at import time.
    # Keys are physical_name, values must be in CANONICAL_DTYPES.
    cast: Mapping[str, str] = field(default_factory=dict)
    # Override the parse-function name for specific columns (e.g. use
    # `yyyymm_to_date` instead of the Date default `yyyymmdd_to_date`).
    # Keys are physical_name, values must be in DATE_PARSERS.
    parse: Mapping[str, str] = field(default_factory=dict)
    # Columns computed from other columns at ingest time (e.g. concat a
    # YYYYMM Date column and a DD Int column into a YYYYMMDD Date). Keyed
    # by output column name; values describe the operation.
    computed: tuple[ComputedColumn, ...] = ()
    # Column names to omit from the final parquet output. Applied after
    # renames/casts/computed, so a dropped column can still feed a
    # `computed` entry before disappearing. Names match `effective_name`
    # for FWF columns, `name` for computed columns.
    drop: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_filename_pattern(self.filename_pattern, error_cls=ConfigError)
        if self.trade_type is not None and self.trade_type not in TRADE_TYPES:
            raise ConfigError(
                f"sheets.<name>.trade_type {self.trade_type!r} is not a known trade type; "
                f"allowed: {list(TRADE_TYPES)}"
            )
        counts = Counter(self.rename.values())
        dupes = sorted(name for name, n in counts.items() if n > 1)
        if dupes:
            raise ConfigError(
                f"rename produces duplicate logical_name(s) {dupes}; each "
                f"logical name may be used only once per sheet"
            )
        bad_cast = sorted(v for v in self.cast.values() if v not in CANONICAL_DTYPES)
        if bad_cast:
            raise ConfigError(
                f"cast targets {bad_cast} are not canonical dtypes; "
                f"allowed: {sorted(CANONICAL_DTYPES)}"
            )
        bad_parse = sorted(v for v in self.parse.values() if v not in DATE_PARSERS)
        if bad_parse:
            raise ConfigError(
                f"parse values {bad_parse} are unknown; allowed: {sorted(DATE_PARSERS)}"
            )
        bad_kinds = sorted({c.kind for c in self.computed if c.kind not in COMPUTED_KINDS})
        if bad_kinds:
            raise ConfigError(
                f"computed kinds {bad_kinds} are unknown; allowed: {sorted(COMPUTED_KINDS)}"
            )
        comp_counts = Counter(c.name for c in self.computed)
        dup_computed = sorted(name for name, n in comp_counts.items() if n > 1)
        if dup_computed:
            raise ConfigError(
                f"computed column name(s) {dup_computed} appear more than once per sheet"
            )
        drop_dupes = sorted({n for n, c in Counter(self.drop).items() if c > 1})
        if drop_dupes:
            raise ConfigError(f"drop lists duplicate name(s): {drop_dupes}")
        object.__setattr__(self, "rename", MappingProxyType(dict(self.rename)))
        object.__setattr__(self, "cast", MappingProxyType(dict(self.cast)))
        object.__setattr__(self, "parse", MappingProxyType(dict(self.parse)))


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
    rename = _str_mapping(data, "rename", workbook_name=workbook_name, sheet_name=sheet_name)
    cast = _str_mapping(data, "cast", workbook_name=workbook_name, sheet_name=sheet_name)
    parse = _str_mapping(data, "parse", workbook_name=workbook_name, sheet_name=sheet_name)
    computed = _computed_columns(data, workbook_name=workbook_name, sheet_name=sheet_name)
    drop_raw = data.get("drop") or []
    if not isinstance(drop_raw, list) or not all(isinstance(s, str) for s in drop_raw):
        raise ConfigError(
            f"workbook {workbook_name!r} sheet {sheet_name!r}: 'drop' must be a list of strings"
        )
    routing_column = data.get("routing_column")
    return SheetConfig(
        filename_pattern=pattern,
        routing_column=str(routing_column) if routing_column is not None else "period",
        trade_type=str(trade_type) if trade_type is not None else None,
        rename=rename,
        cast=cast,
        parse=parse,
        computed=computed,
        drop=tuple(drop_raw),
    )


def _computed_columns(
    data: dict[str, Any], *, workbook_name: str, sheet_name: str
) -> tuple[ComputedColumn, ...]:
    raw = data.get("computed") or {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"workbook {workbook_name!r} sheet {sheet_name!r}: 'computed' must be a mapping, "
            f"got {type(raw).__name__}"
        )
    out: list[ComputedColumn] = []
    for name, entry in raw.items():
        if not isinstance(entry, dict):
            raise ConfigError(
                f"workbook {workbook_name!r} sheet {sheet_name!r}: computed column "
                f"{name!r} must be a mapping with kind/sources/dtype keys"
            )
        try:
            kind = str(entry["kind"])
            sources_raw = entry["sources"]
        except KeyError as exc:
            raise ConfigError(
                f"workbook {workbook_name!r} sheet {sheet_name!r}: computed column "
                f"{name!r} missing required key {exc.args[0]!r}"
            ) from exc
        if not isinstance(sources_raw, list) or not all(isinstance(s, str) for s in sources_raw):
            raise ConfigError(
                f"workbook {workbook_name!r} sheet {sheet_name!r}: computed column "
                f"{name!r} 'sources' must be a list of column names"
            )
        # Dtype defaults to the kind's canonical output.
        kind_default_dtype = {"concat_to_date": "Date", "concat_text": "Utf8"}
        default_dtype = kind_default_dtype.get(kind)
        dtype = (
            str(entry.get("dtype", default_dtype)) if entry.get("dtype") or default_dtype else ""
        )
        if not dtype:
            raise ConfigError(
                f"workbook {workbook_name!r} sheet {sheet_name!r}: computed column "
                f"{name!r} has no dtype and kind {kind!r} has no default"
            )
        nullable = bool(entry.get("nullable", True))
        separator = str(entry.get("separator", " "))
        out.append(
            ComputedColumn(
                name=str(name),
                dtype=dtype,
                kind=kind,
                sources=tuple(sources_raw),
                nullable=nullable,
                separator=separator,
            )
        )
    return tuple(out)


def _str_mapping(
    data: dict[str, Any], key: str, *, workbook_name: str, sheet_name: str
) -> dict[str, str]:
    raw = data.get(key) or {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"workbook {workbook_name!r} sheet {sheet_name!r}: {key!r} must be a mapping, "
            f"got {type(raw).__name__}"
        )
    return {str(k): str(v) for k, v in raw.items()}
