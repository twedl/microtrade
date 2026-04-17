"""Tests for the input-directory scanner."""

from __future__ import annotations

from pathlib import Path

import pytest

from microtrade.discover import DiscoverError, RawInput, parse_filename, scan, ytd_filter


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def test_parse_filename_accepts_all_three_trade_types(tmp_path: Path) -> None:
    for trade_type in ("imports", "exports_us", "exports_nonus"):
        parsed = parse_filename(tmp_path / f"{trade_type}_202404.zip")
        assert parsed is not None
        assert parsed.trade_type == trade_type
        assert parsed.year == 2024
        assert parsed.month == 4
        assert parsed.period == "2024-04"


def test_parse_filename_returns_none_for_non_matching(tmp_path: Path) -> None:
    assert parse_filename(tmp_path / "README.txt") is None
    assert parse_filename(tmp_path / "imports_2024.zip") is None
    assert parse_filename(tmp_path / "imports_202404.gz") is None
    assert parse_filename(tmp_path / "IMPORTS_202404.zip") is None  # case-sensitive


def test_parse_filename_rejects_out_of_range_month(tmp_path: Path) -> None:
    with pytest.raises(DiscoverError, match="month 13 out of range"):
        parse_filename(tmp_path / "imports_202413.zip")


def test_scan_finds_and_sorts_inputs(tmp_path: Path) -> None:
    _touch(tmp_path / "imports_202402.zip")
    _touch(tmp_path / "imports_202401.zip")
    _touch(tmp_path / "exports_us_202401.zip")
    _touch(tmp_path / "README.txt")
    _touch(tmp_path / "subdir" / "imports_202403.zip")  # not recursive

    found = scan(tmp_path)
    assert [(r.trade_type, r.year, r.month) for r in found] == [
        ("exports_us", 2024, 1),
        ("imports", 2024, 1),
        ("imports", 2024, 2),
    ]


def test_scan_filters_by_trade_type_year_month(tmp_path: Path) -> None:
    _touch(tmp_path / "imports_202401.zip")
    _touch(tmp_path / "imports_202402.zip")
    _touch(tmp_path / "imports_202501.zip")
    _touch(tmp_path / "exports_us_202401.zip")

    imports_2024 = scan(tmp_path, trade_types=["imports"], year=2024)
    assert [(r.year, r.month) for r in imports_2024] == [(2024, 1), (2024, 2)]

    jan_only = scan(tmp_path, month=1)
    assert {(r.trade_type, r.year) for r in jan_only} == {
        ("imports", 2024),
        ("imports", 2025),
        ("exports_us", 2024),
    }


def test_scan_rejects_unknown_trade_type(tmp_path: Path) -> None:
    with pytest.raises(DiscoverError, match="unknown trade_types"):
        scan(tmp_path, trade_types=["not_a_type"])


def test_scan_rejects_missing_dir(tmp_path: Path) -> None:
    with pytest.raises(DiscoverError, match="does not exist"):
        scan(tmp_path / "nope")


def test_ytd_filter_keeps_current_year(tmp_path: Path) -> None:
    inputs = [
        RawInput("imports", 2023, 12, tmp_path / "x"),
        RawInput("imports", 2024, 1, tmp_path / "x"),
        RawInput("imports", 2024, 6, tmp_path / "x"),
    ]
    assert [(r.year, r.month) for r in ytd_filter(inputs, current_year=2024)] == [
        (2024, 1),
        (2024, 6),
    ]
