"""Scan an input directory for raw trade zips and parse their period/type.

Each committed Spec carries its own `source.filename_pattern` - a Python
regex with named groups `year`, `month`, and optional `flag`. Discovery
walks every committed spec, compiles its pattern, and matches files
against the full set. A file that matches exactly one spec's pattern
becomes a `RawInput`; files that match nothing are silently ignored;
files that match multiple specs raise `DiscoverError` because the
upstream configuration is ambiguous.

When the same `(trade_type, year, month)` appears with more than one
`flag`, `N` wins over `C`; any other flag value, or absence of a flag,
comes last.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from microtrade.schema import TRADE_TYPES, Spec, load_all, validate_filename_pattern

# None (unflagged) wins over N wins over C when dedup'ing a (trade_type, year, month).
_FLAG_PRIORITY: Mapping[str, int] = {"N": 1, "C": 2}


class DiscoverError(ValueError):
    """Raised for filenames that parse as a known trade type but have invalid fields."""


@dataclass(frozen=True)
class RawInput:
    trade_type: str
    year: int
    month: int
    path: Path
    flag: str | None = None

    @property
    def period(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class PatternEntry:
    """One compiled (filename_pattern, trade_type) pair derived from a Spec."""

    trade_type: str
    pattern: re.Pattern[str]
    source_label: str  # "<trade_type>/v<effective_from>" - used in error messages


def patterns_for_specs(specs: Iterable[Spec]) -> list[PatternEntry]:
    """Compile the `filename_pattern` of each spec that declares one.

    Specs whose `source` is missing or whose `filename_pattern` is None are
    silently skipped - they can still be resolved by `schema.resolve` but
    no raw files will be routed to them. Raises `DiscoverError` on a
    malformed regex so misconfiguration surfaces up front.
    """
    return [entry for spec in specs if (entry := _entry_for(spec)) is not None]


def load_patterns(spec_dir: Path) -> list[PatternEntry]:
    """Convenience: load every committed spec under `spec_dir` and compile its pattern."""
    return patterns_for_specs(
        spec for trade_type in TRADE_TYPES for spec in load_all(spec_dir, trade_type)
    )


def _entry_for(spec: Spec) -> PatternEntry | None:
    if spec.source is None or spec.source.filename_pattern is None:
        return None
    compiled = validate_filename_pattern(spec.source.filename_pattern, error_cls=DiscoverError)
    return PatternEntry(
        trade_type=spec.trade_type,
        pattern=compiled,
        source_label=f"{spec.trade_type}/v{spec.effective_from}",
    )


def parse_filename(path: Path, patterns: Iterable[PatternEntry]) -> RawInput | None:
    """Match `path` against every pattern; return a RawInput if exactly one hits."""
    hits: list[tuple[PatternEntry, re.Match[str]]] = []
    for entry in patterns:
        match = entry.pattern.match(path.name)
        if match is not None:
            hits.append((entry, match))
    if not hits:
        return None
    if len(hits) > 1:
        labels = sorted(h[0].source_label for h in hits)
        raise DiscoverError(
            f"{path.name}: matches multiple spec filename_patterns: {labels}. "
            f"Tighten the regexes so each file routes to one spec."
        )
    entry, match = hits[0]
    groups = match.groupdict()
    year = int(groups["year"])
    month = int(groups["month"])
    if not 1 <= month <= 12:
        raise DiscoverError(f"{path.name}: month {month} out of range 1-12")
    return RawInput(
        trade_type=entry.trade_type,
        year=year,
        month=month,
        path=path,
        flag=groups.get("flag"),
    )


def scan(
    input_dir: Path,
    *,
    spec_dir: Path | None = None,
    patterns: list[PatternEntry] | None = None,
    trade_types: Iterable[str] | None = None,
    year: int | None = None,
    month: int | None = None,
) -> list[RawInput]:
    """List `(trade_type, year, month, path)` tuples for zips under `input_dir`.

    Pass either `spec_dir` (to load and compile patterns here) or `patterns`
    (already compiled, e.g. when the caller shares them across multiple
    scans). Results are sorted by (trade_type, year, month). Non-matching
    files and files whose spec has no `filename_pattern` are silently
    ignored; `N`-flagged files beat `C` for the same partition.
    """
    if (spec_dir is None) == (patterns is None):
        raise DiscoverError("scan requires exactly one of `spec_dir` or `patterns`")
    if not input_dir.is_dir():
        raise DiscoverError(f"input dir does not exist or is not a directory: {input_dir}")

    wanted_types = set(trade_types) if trade_types is not None else None
    if wanted_types is not None:
        unknown = wanted_types - set(TRADE_TYPES)
        if unknown:
            raise DiscoverError(f"unknown trade_types requested: {sorted(unknown)}")

    if patterns is None:
        assert spec_dir is not None
        patterns = load_patterns(spec_dir)

    candidates: list[RawInput] = []
    for entry in input_dir.iterdir():
        if not entry.is_file():
            continue
        parsed = parse_filename(entry, patterns)
        if parsed is None:
            continue
        if wanted_types is not None and parsed.trade_type not in wanted_types:
            continue
        if year is not None and parsed.year != year:
            continue
        if month is not None and parsed.month != month:
            continue
        candidates.append(parsed)

    return _dedup_by_flag(candidates)


def _dedup_by_flag(candidates: list[RawInput]) -> list[RawInput]:
    """Keep the highest-priority flag per (trade_type, year, month)."""
    chosen: dict[tuple[str, int, int], RawInput] = {}
    for raw in candidates:
        key = (raw.trade_type, raw.year, raw.month)
        current = chosen.get(key)
        if current is None or _flag_rank(raw.flag) < _flag_rank(current.flag):
            chosen[key] = raw
    return sorted(chosen.values(), key=lambda r: (r.trade_type, r.year, r.month))


def _flag_rank(flag: str | None) -> int:
    if flag is None:
        return 0
    return _FLAG_PRIORITY.get(flag, len(_FLAG_PRIORITY) + 1)


def ytd_filter(raw_inputs: Iterable[RawInput], *, current_year: int) -> list[RawInput]:
    """Keep only inputs whose year matches `current_year`. Prior years are frozen."""
    return [r for r in raw_inputs if r.year == current_year]


def latest_snapshot_per_year(candidates: Iterable[RawInput]) -> list[RawInput]:
    """Pick the file with the highest `month` per `(trade_type, year)`.

    Files are YTD snapshots, so a YYYY-12 file supersedes YYYY-11 and earlier;
    the pipeline only ever needs the latest snapshot per year.
    """
    latest: dict[tuple[str, int], RawInput] = {}
    for raw in candidates:
        key = (raw.trade_type, raw.year)
        current = latest.get(key)
        if current is None or raw.month > current.month:
            latest[key] = raw
    return sorted(latest.values(), key=lambda r: (r.trade_type, r.year, r.month))
