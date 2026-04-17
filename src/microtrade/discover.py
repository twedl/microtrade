"""Scan an input directory for raw trade zips and parse their period/type.

Filenames are expected to follow `<trade_type>_<YYYY><MM>.zip`, e.g.
`imports_202404.zip`. Files that don't match are ignored silently so that
sidecar artifacts (README.txt, checksums, etc.) don't trip the scanner.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from microtrade.schema import TRADE_TYPES

_FILENAME_RE = re.compile(
    rf"^(?P<trade_type>{'|'.join(TRADE_TYPES)})_(?P<year>\d{{4}})(?P<month>\d{{2}})\.zip$"
)


class DiscoverError(ValueError):
    """Raised for filenames that parse as a known trade type but have invalid fields."""


@dataclass(frozen=True)
class RawInput:
    trade_type: str
    year: int
    month: int
    path: Path

    @property
    def period(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def parse_filename(path: Path) -> RawInput | None:
    """Return a RawInput if the filename matches the expected pattern, else None."""
    match = _FILENAME_RE.match(path.name)
    if match is None:
        return None
    month = int(match["month"])
    if not 1 <= month <= 12:
        raise DiscoverError(f"{path.name}: month {month} out of range 1-12")
    return RawInput(
        trade_type=match["trade_type"],
        year=int(match["year"]),
        month=month,
        path=path,
    )


def scan(
    input_dir: Path,
    *,
    trade_types: Iterable[str] | None = None,
    year: int | None = None,
    month: int | None = None,
) -> list[RawInput]:
    """List `(trade_type, year, month, path)` tuples for zips under `input_dir`.

    Results are sorted by (trade_type, year, month). Non-matching files are ignored.
    """
    if not input_dir.is_dir():
        raise DiscoverError(f"input dir does not exist or is not a directory: {input_dir}")

    wanted_types = set(trade_types) if trade_types is not None else None
    if wanted_types is not None:
        unknown = wanted_types - set(TRADE_TYPES)
        if unknown:
            raise DiscoverError(f"unknown trade_types requested: {sorted(unknown)}")

    out: list[RawInput] = []
    for entry in input_dir.iterdir():
        if not entry.is_file():
            continue
        parsed = parse_filename(entry)
        if parsed is None:
            continue
        if wanted_types is not None and parsed.trade_type not in wanted_types:
            continue
        if year is not None and parsed.year != year:
            continue
        if month is not None and parsed.month != month:
            continue
        out.append(parsed)

    return sorted(out, key=lambda r: (r.trade_type, r.year, r.month))


def ytd_filter(raw_inputs: Iterable[RawInput], *, current_year: int) -> list[RawInput]:
    """Keep only inputs whose year matches `current_year`. Prior years are frozen."""
    return [r for r in raw_inputs if r.year == current_year]
