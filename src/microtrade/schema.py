"""Versioned spec model and YAML I/O for microtrade.

A `Spec` describes one trade type's FWF layout for a range of periods starting
at `effective_from`. Specs live under `src/microtrade/specs/<trade_type>/` and
are generated from the upstream Excel workbook by `microtrade import-spec`.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

TRADE_TYPES: tuple[str, ...] = ("imports", "exports_us", "exports_nonus")

CANONICAL_DTYPES: frozenset[str] = frozenset({"Utf8", "Int64", "Float64", "Date"})

_PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")


@dataclass(frozen=True)
class Column:
    name: str
    start: int
    length: int
    dtype: str
    nullable: bool = True
    parse: str | None = None
    description: str | None = None

    @property
    def end(self) -> int:
        return self.start + self.length - 1


@dataclass(frozen=True)
class SpecSource:
    workbook: str
    sha256: str
    sheet: str
    imported_at: str


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

    @property
    def ordered_columns(self) -> tuple[Column, ...]:
        """Columns sorted by start position (the FWF read order)."""
        return tuple(sorted(self.columns, key=lambda c: c.start))


class SpecError(ValueError):
    """Raised when a spec is structurally invalid."""


def validate_period(period: str) -> None:
    if not _PERIOD_RE.match(period):
        raise SpecError(f"period must be 'YYYY-MM', got {period!r}")


def validate_spec(spec: Spec) -> None:
    if spec.trade_type not in TRADE_TYPES:
        raise SpecError(f"unknown trade_type {spec.trade_type!r}")
    validate_period(spec.effective_from)
    if not spec.columns:
        raise SpecError("spec has no columns")

    seen_names: set[str] = set()
    prev_end = 0
    for col in sorted(spec.columns, key=lambda c: c.start):
        if col.name in seen_names:
            raise SpecError(f"duplicate column name {col.name!r}")
        seen_names.add(col.name)
        if col.dtype not in CANONICAL_DTYPES:
            raise SpecError(
                f"column {col.name!r} has non-canonical dtype {col.dtype!r}; "
                f"allowed: {sorted(CANONICAL_DTYPES)}"
            )
        if col.start <= prev_end:
            raise SpecError(
                f"column {col.name!r} overlaps previous (start={col.start}, "
                f"previous end={prev_end})"
            )
        if col.length <= 0:
            raise SpecError(f"column {col.name!r} has non-positive length {col.length}")
        prev_end = col.end

    expected_record_length = max(c.end for c in spec.columns)
    if spec.record_length < expected_record_length:
        raise SpecError(
            f"record_length {spec.record_length} is shorter than rightmost column "
            f"end {expected_record_length}"
        )


def _column_to_dict(col: Column) -> dict[str, Any]:
    out: dict[str, Any] = {
        "name": col.name,
        "start": col.start,
        "length": col.length,
        "dtype": col.dtype,
        "nullable": col.nullable,
    }
    if col.parse is not None:
        out["parse"] = col.parse
    if col.description is not None:
        out["description"] = col.description
    return out


def _column_from_dict(data: dict[str, Any]) -> Column:
    return Column(
        name=str(data["name"]),
        start=int(data["start"]),
        length=int(data["length"]),
        dtype=str(data["dtype"]),
        nullable=bool(data.get("nullable", True)),
        parse=data.get("parse"),
        description=data.get("description"),
    )


def spec_to_dict(spec: Spec) -> dict[str, Any]:
    out: dict[str, Any] = {
        "trade_type": spec.trade_type,
        "version": spec.version,
        "effective_from": spec.effective_from,
        "record_length": spec.record_length,
    }
    if spec.source is not None:
        out["source"] = {
            "workbook": spec.source.workbook,
            "sha256": spec.source.sha256,
            "sheet": spec.source.sheet,
            "imported_at": spec.source.imported_at,
        }
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


def resolve(specs: list[Spec], period: str) -> Spec:
    """Return the spec whose `effective_from` is the latest value <= `period`."""
    validate_period(period)
    applicable = [s for s in specs if s.effective_from <= period]
    if not applicable:
        raise SpecError(
            f"no spec applies to period {period!r}; earliest available is "
            f"{specs[0].effective_from if specs else 'none'}"
        )
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

    Columns are ordered by first appearance. When a column's dtype or
    nullability changes between versions, the latest spec wins; the dtype
    must stay in `CANONICAL_DTYPES`. Raises `SpecError` if two specs
    disagree on a column's dtype (a widening change we don't auto-resolve).
    """
    ordered_names: list[str] = []
    seen: dict[str, CanonicalColumn] = {}
    for spec in sorted(specs, key=lambda s: s.effective_from):
        for col in spec.ordered_columns:
            existing = seen.get(col.name)
            if existing is None:
                ordered_names.append(col.name)
                seen[col.name] = CanonicalColumn(
                    name=col.name, dtype=col.dtype, nullable=col.nullable
                )
                continue
            if existing.dtype != col.dtype:
                raise SpecError(
                    f"column {col.name!r} changes dtype across spec versions: "
                    f"{existing.dtype!r} -> {col.dtype!r}"
                )
            # Widen nullability: once nullable, always nullable in the canonical view.
            seen[col.name] = CanonicalColumn(
                name=col.name,
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
    prev_by_name = {c.name: c for c in previous.columns}
    curr_by_name = {c.name: c for c in current.columns}

    added = tuple(c for name, c in curr_by_name.items() if name not in prev_by_name)
    removed = tuple(c for name, c in prev_by_name.items() if name not in curr_by_name)
    changed = tuple(
        (prev_by_name[name], curr_by_name[name])
        for name in prev_by_name.keys() & curr_by_name.keys()
        if prev_by_name[name] != curr_by_name[name]
    )
    return SpecDiff(added=added, removed=removed, changed=changed)
