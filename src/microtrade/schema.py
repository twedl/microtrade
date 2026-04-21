"""Versioned spec model and YAML I/O for microtrade.

A `Spec` describes one trade type's FWF layout for a range of periods starting
at `effective_from`. Specs live under `src/microtrade/specs/<trade_type>/` and
are generated from the upstream Excel workbook by `microtrade import-spec`.
"""

from __future__ import annotations

import hashlib
import itertools
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

TRADE_TYPES: tuple[str, ...] = ("imports", "exports_us", "exports_nonus")

CANONICAL_DTYPES: frozenset[str] = frozenset({"Utf8", "Int64", "Float64", "Date"})

# Named groups that a Spec's `source.filename_pattern` may expose. `year`/`month`
# are required so discovery can route files to partitions; `flag` is optional and
# used purely for N/C dedup preference.
PATTERN_REQUIRED_GROUPS: tuple[str, ...] = ("year", "month")
PATTERN_OPTIONAL_GROUPS: tuple[str, ...] = ("flag",)

_PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")


@dataclass(frozen=True)
class Column:
    physical_name: str
    start: int
    length: int
    dtype: str
    nullable: bool = True
    parse: str | None = None
    description: str | None = None
    # Stable name across workbook versions; `canonical_columns` merges on
    # this, so a rename upstream doesn't fork the dataset.
    logical_name: str | None = None

    @property
    def end(self) -> int:
        return self.start + self.length - 1

    @property
    def effective_name(self) -> str:
        """Stable name across spec versions; falls back to `physical_name`."""
        return self.logical_name if self.logical_name is not None else self.physical_name


@dataclass(frozen=True)
class SpecSource:
    workbook: str
    sha256: str
    sheet: str
    imported_at: str
    # Stable identifier for the workbook (e.g. "XYZ12345" extracted from
    # "XYZ12345_Record_Layout.xls"). Provenance only; discovery matches on
    # `filename_pattern` instead.
    workbook_id: str | None = None
    # Regex (Python-syntax) that raw data filenames for this spec must match.
    # Must expose named groups `year` and `month` (4 and 2 digits); may
    # optionally expose `flag` for dedup preference when upstream publishes
    # multiple copies per period. Baked in by `import-spec` from the project
    # config's `sheets.<name>.filename_pattern`.
    filename_pattern: str | None = None


@dataclass(frozen=True)
class Spec:
    trade_type: str
    version: str
    effective_from: str
    record_length: int
    columns: tuple[Column, ...]
    source: SpecSource | None = None
    derived: tuple[tuple[str, str], ...] = ()
    partition_by: tuple[str, ...] = ("year", "month")
    # Inclusive upper bound on the period range this spec applies to (YYYY-MM).
    # None means open-ended (this spec is still current).
    effective_to: str | None = None

    @property
    def ordered_columns(self) -> tuple[Column, ...]:
        """Columns sorted by start position (the FWF read order)."""
        return tuple(sorted(self.columns, key=lambda c: c.start))

    @property
    def min_record_length(self) -> int:
        """Rightmost real-column byte - the shortest record that still covers
        every declared column. `record_length` is an upper bound and may exceed
        this to allow trailing filler bytes that the data does not always ship.
        """
        return max((c.end for c in self.columns), default=0)


class SpecError(ValueError):
    """Raised when a spec is structurally invalid."""


def validate_period(period: str) -> None:
    if not _PERIOD_RE.match(period):
        raise SpecError(f"period must be 'YYYY-MM', got {period!r}")


def validate_period_window(
    effective_from: str, effective_to: str | None, *, error_cls: type[Exception] = SpecError
) -> None:
    """Shared check: effective_to must be a valid period >= effective_from.

    Raised with `error_cls` so callers in different layers (schema vs. config)
    can map the invariant onto their own exception type without duplicating
    the condition or the message.
    """
    validate_period(effective_from)
    if effective_to is None:
        return
    try:
        validate_period(effective_to)
    except SpecError as exc:
        if error_cls is SpecError:
            raise
        raise error_cls(str(exc)) from exc
    if effective_to < effective_from:
        raise error_cls(f"effective_to {effective_to!r} precedes effective_from {effective_from!r}")


def next_period(period: str) -> str:
    """Return the YYYY-MM that immediately follows `period`."""
    validate_period(period)
    year = int(period[:4])
    month = int(period[5:7]) + 1
    if month > 12:
        month = 1
        year += 1
    return f"{year:04d}-{month:02d}"


def validate_filename_pattern(
    pattern: str, *, error_cls: type[Exception] = SpecError
) -> re.Pattern[str]:
    """Compile `pattern` and verify its named groups match the discovery contract.

    Returns the compiled `re.Pattern` so callers don't have to recompile. Raised
    errors use `error_cls` so config-layer callers can surface `ConfigError`
    while the spec-layer default stays `SpecError`.
    """
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise error_cls(f"invalid filename_pattern {pattern!r}: {exc}") from exc
    names = set(compiled.groupindex)
    missing = [g for g in PATTERN_REQUIRED_GROUPS if g not in names]
    if missing:
        raise error_cls(
            f"filename_pattern {pattern!r} is missing required named group(s): {missing}. "
            f"Required: {list(PATTERN_REQUIRED_GROUPS)}; "
            f"optional: {list(PATTERN_OPTIONAL_GROUPS)}."
        )
    unknown = names - set(PATTERN_REQUIRED_GROUPS) - set(PATTERN_OPTIONAL_GROUPS)
    if unknown:
        raise error_cls(
            f"filename_pattern {pattern!r} has unknown named group(s): {sorted(unknown)}. "
            f"Only {list(PATTERN_REQUIRED_GROUPS + PATTERN_OPTIONAL_GROUPS)} are recognized."
        )
    return compiled


def validate_spec(spec: Spec) -> None:
    if spec.trade_type not in TRADE_TYPES:
        raise SpecError(f"unknown trade_type {spec.trade_type!r}")
    validate_period_window(spec.effective_from, spec.effective_to)
    if spec.source is not None and spec.source.filename_pattern is not None:
        validate_filename_pattern(spec.source.filename_pattern)
    if not spec.columns:
        raise SpecError("spec has no columns")

    seen_physical: set[str] = set()
    seen_logical: set[str] = set()
    prev_end = 0
    for col in sorted(spec.columns, key=lambda c: c.start):
        if col.physical_name in seen_physical:
            raise SpecError(f"duplicate column physical_name {col.physical_name!r}")
        seen_physical.add(col.physical_name)
        if col.effective_name in seen_logical:
            raise SpecError(
                f"duplicate column logical_name {col.effective_name!r} "
                f"(physical_name {col.physical_name!r})"
            )
        seen_logical.add(col.effective_name)
        if col.dtype not in CANONICAL_DTYPES:
            raise SpecError(
                f"column {col.physical_name!r} has non-canonical dtype {col.dtype!r}; "
                f"allowed: {sorted(CANONICAL_DTYPES)}"
            )
        if col.start <= prev_end:
            raise SpecError(
                f"column {col.physical_name!r} overlaps previous (start={col.start}, "
                f"previous end={prev_end})"
            )
        if col.length <= 0:
            raise SpecError(f"column {col.physical_name!r} has non-positive length {col.length}")
        prev_end = col.end

    if spec.record_length < spec.min_record_length:
        raise SpecError(
            f"record_length {spec.record_length} is shorter than rightmost column "
            f"end {spec.min_record_length}"
        )


def _opt_str(data: Mapping[str, Any], key: str) -> str | None:
    """Read an optional string from a dict, returning None when absent or empty.

    Used for round-tripping nullable text fields through YAML where the
    serializer only emits the key when the value is non-None, so a missing
    key and an explicit None are indistinguishable on read-back.
    """
    value = data.get(key)
    return str(value) if value else None


def _column_to_dict(col: Column) -> dict[str, Any]:
    out: dict[str, Any] = {
        "physical_name": col.physical_name,
        "start": col.start,
        "length": col.length,
        "dtype": col.dtype,
        "nullable": col.nullable,
    }
    if col.logical_name is not None:
        out["logical_name"] = col.logical_name
    if col.parse is not None:
        out["parse"] = col.parse
    if col.description is not None:
        out["description"] = col.description
    return out


def _column_from_dict(data: dict[str, Any]) -> Column:
    return Column(
        physical_name=str(data["physical_name"]),
        start=int(data["start"]),
        length=int(data["length"]),
        dtype=str(data["dtype"]),
        nullable=bool(data.get("nullable", True)),
        parse=data.get("parse"),
        description=data.get("description"),
        logical_name=_opt_str(data, "logical_name"),
    )


def spec_to_dict(spec: Spec) -> dict[str, Any]:
    out: dict[str, Any] = {
        "trade_type": spec.trade_type,
        "version": spec.version,
        "effective_from": spec.effective_from,
    }
    if spec.effective_to is not None:
        out["effective_to"] = spec.effective_to
    out["record_length"] = spec.record_length
    if spec.source is not None:
        source: dict[str, Any] = {
            "workbook": spec.source.workbook,
            "sha256": spec.source.sha256,
            "sheet": spec.source.sheet,
            "imported_at": spec.source.imported_at,
        }
        if spec.source.workbook_id is not None:
            source["workbook_id"] = spec.source.workbook_id
        if spec.source.filename_pattern is not None:
            source["filename_pattern"] = spec.source.filename_pattern
        out["source"] = source
    out["columns"] = [_column_to_dict(c) for c in spec.columns]
    if spec.derived:
        out["derived"] = [{name: expr} for name, expr in spec.derived]
    out["partition_by"] = list(spec.partition_by)
    return out


def spec_from_dict(data: dict[str, Any]) -> Spec:
    source_data = data.get("source")
    source = (
        SpecSource(
            workbook=str(source_data["workbook"]),
            sha256=str(source_data["sha256"]),
            sheet=str(source_data["sheet"]),
            imported_at=str(source_data["imported_at"]),
            workbook_id=_opt_str(source_data, "workbook_id"),
            filename_pattern=_opt_str(source_data, "filename_pattern"),
        )
        if source_data is not None
        else None
    )
    derived_raw = data.get("derived") or []
    derived: tuple[tuple[str, str], ...] = tuple(
        (name, expr) for item in derived_raw for name, expr in item.items()
    )
    spec = Spec(
        trade_type=str(data["trade_type"]),
        version=str(data["version"]),
        effective_from=str(data["effective_from"]),
        record_length=int(data["record_length"]),
        columns=tuple(_column_from_dict(c) for c in data["columns"]),
        source=source,
        derived=derived,
        partition_by=tuple(data.get("partition_by", ("year", "month"))),
        effective_to=_opt_str(data, "effective_to"),
    )
    validate_spec(spec)
    return spec


def save_spec(spec: Spec, path: Path) -> None:
    validate_spec(spec)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(spec_to_dict(spec), sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )


def load_spec(path: Path) -> Spec:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SpecError(f"{path}: expected a mapping at top level")
    return spec_from_dict(data)


def load_all(spec_dir: Path, trade_type: str) -> list[Spec]:
    dir_path = spec_dir / trade_type
    if not dir_path.is_dir():
        return []
    specs = [load_spec(p) for p in sorted(dir_path.glob("v*.yaml"))]
    return sorted(specs, key=lambda s: s.effective_from)


def window_problems(trade_type: str, specs: list[Spec]) -> list[str]:
    """Flag overlapping or gapped [effective_from, effective_to] windows.

    Specs must be pre-sorted by `effective_from`. Overlap is fatal (resolve
    would be ambiguous); gaps produce "no spec applies" at resolve time and
    are surfaced here so callers see them during validation instead.
    """
    problems: list[str] = []
    for prev, curr in itertools.pairwise(specs):
        if prev.effective_to is None:
            problems.append(
                f"{trade_type}: v{prev.effective_from} is open-ended "
                f"(effective_to=None) but v{curr.effective_from} follows it; "
                f"set effective_to on the earlier spec."
            )
            continue
        if prev.effective_to >= curr.effective_from:
            problems.append(
                f"{trade_type}: v{prev.effective_from} window ends at "
                f"{prev.effective_to!r}, overlapping v{curr.effective_from}."
            )
        elif next_period(prev.effective_to) != curr.effective_from:
            problems.append(
                f"{trade_type}: gap between v{prev.effective_from} "
                f"(ends {prev.effective_to}) and v{curr.effective_from}; no spec "
                f"applies to the intervening months."
            )
    return problems


def resolve(specs: list[Spec], period: str) -> Spec:
    """Return the spec whose [effective_from, effective_to] window contains `period`.

    Windows are inclusive and `effective_to=None` is open-ended. If more
    than one spec's window covers the period, the one with the latest
    `effective_from` wins; `validate-specs` should catch overlapping
    windows before they reach here.
    """
    validate_period(period)
    applicable = [
        s
        for s in specs
        if s.effective_from <= period and (s.effective_to is None or period <= s.effective_to)
    ]
    if not applicable:
        earliest = specs[0].effective_from if specs else "none"
        raise SpecError(f"no spec applies to period {period!r}; earliest available is {earliest}")
    return max(applicable, key=lambda s: s.effective_from)


def now_iso() -> str:
    """UTC timestamp truncated to seconds, ISO-formatted. Shared by import-spec and pipeline."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True)
class CanonicalColumn:
    """Logical column definition for a dataset's union schema across spec versions."""

    name: str
    dtype: str
    nullable: bool


def canonical_columns(specs: list[Spec]) -> tuple[CanonicalColumn, ...]:
    """Compute the union of columns across a trade type's committed specs.

    Columns merge on `effective_name` so a physical rename declared via
    `logical_name` resolves to a single canonical column. Columns are
    ordered by first appearance. When a column's dtype changes between
    versions, `SpecError` is raised (widening we don't auto-resolve);
    nullability widens (once nullable, always nullable in the canonical view).
    """
    ordered_names: list[str] = []
    seen: dict[str, CanonicalColumn] = {}
    for spec in sorted(specs, key=lambda s: s.effective_from):
        for col in spec.ordered_columns:
            name = col.effective_name
            existing = seen.get(name)
            if existing is None:
                ordered_names.append(name)
                seen[name] = CanonicalColumn(name=name, dtype=col.dtype, nullable=col.nullable)
                continue
            if existing.dtype != col.dtype:
                raise SpecError(
                    f"column {name!r} changes dtype across spec versions: "
                    f"{existing.dtype!r} -> {col.dtype!r}"
                )
            seen[name] = CanonicalColumn(
                name=name,
                dtype=col.dtype,
                nullable=existing.nullable or col.nullable,
            )
    return tuple(seen[n] for n in ordered_names)


@dataclass(frozen=True)
class SpecDiff:
    added: tuple[Column, ...] = field(default_factory=tuple)
    removed: tuple[Column, ...] = field(default_factory=tuple)
    changed: tuple[tuple[Column, Column], ...] = field(default_factory=tuple)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.changed)


def diff_specs(previous: Spec, current: Spec) -> SpecDiff:
    """Diff two specs by `effective_name` so a physical rename (same
    `logical_name`) appears as a change, not an add+remove."""
    prev_by_name = {c.effective_name: c for c in previous.columns}
    curr_by_name = {c.effective_name: c for c in current.columns}

    added = tuple(c for name, c in curr_by_name.items() if name not in prev_by_name)
    removed = tuple(c for name, c in prev_by_name.items() if name not in curr_by_name)
    changed = tuple(
        (prev_by_name[name], curr_by_name[name])
        for name in prev_by_name.keys() & curr_by_name.keys()
        if prev_by_name[name] != curr_by_name[name]
    )
    return SpecDiff(added=added, removed=removed, changed=changed)
