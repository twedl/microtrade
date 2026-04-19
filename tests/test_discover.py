"""Tests for the input-directory scanner."""

from __future__ import annotations

from pathlib import Path

import pytest

from microtrade import excel_spec, schema
from microtrade.config import load_config
from microtrade.discover import (
    DiscoverError,
    PatternEntry,
    RawInput,
    load_patterns,
    parse_filename,
    scan,
    ytd_filter,
)
from tests._helpers import (
    SHEET_TITLES,
    build_project_config,
    build_workbook,
    default_filename_pattern,
    input_filename,
)


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def _patterns_for(*trade_types: str) -> list[PatternEntry]:
    """Compile the default synthetic patterns for the named trade types."""
    import re

    return [
        PatternEntry(
            trade_type=tt,
            pattern=re.compile(default_filename_pattern(SHEET_TITLES[tt])),
            source_label=f"{tt}/v2020-01",
        )
        for tt in trade_types
    ]


@pytest.fixture
def spec_dir(tmp_path: Path) -> Path:
    """A spec_dir whose specs were generated from a synthetic workbook + config."""
    workbook = build_workbook(tmp_path / "wb.xlsx")
    config_path = build_project_config(tmp_path / "microtrade.yaml", workbook, "2020-01")
    project = load_config(config_path)
    specs = excel_spec.read_workbook(workbook, project.get_workbook(workbook))
    spec_root = tmp_path / "specs"
    for trade_type, spec in specs.items():
        schema.save_spec(spec, spec_root / trade_type / "v2020-01.yaml")
    return spec_root


def test_load_patterns_one_per_spec(spec_dir: Path) -> None:
    entries = load_patterns(spec_dir)
    assert {e.trade_type for e in entries} == set(SHEET_TITLES)
    for entry in entries:
        assert entry.pattern.groupindex.keys() >= {"year", "month"}


def test_parse_filename_routes_by_pattern(tmp_path: Path) -> None:
    patterns = _patterns_for("imports", "exports_us", "exports_nonus")
    for trade_type, sheet_title in SHEET_TITLES.items():
        parsed = parse_filename(tmp_path / input_filename(sheet_title, 2024, 4), patterns)
        assert parsed is not None
        assert parsed.trade_type == trade_type
        assert parsed.year == 2024
        assert parsed.month == 4
        assert parsed.period == "2024-04"
        assert parsed.flag == "N"


def test_parse_filename_returns_none_for_non_matching(tmp_path: Path) -> None:
    patterns = _patterns_for("imports")
    imports_sheet = SHEET_TITLES["imports"]
    assert parse_filename(tmp_path / "README.txt", patterns) is None
    assert parse_filename(tmp_path / f"{imports_sheet}_2024N.TXT.zip", patterns) is None
    assert parse_filename(tmp_path / f"{imports_sheet}_202404.TXT.zip", patterns) is None
    # Different sheet prefix -> no pattern in the current list matches.
    assert parse_filename(tmp_path / "OTHER_202404N.TXT.zip", patterns) is None


def test_parse_filename_rejects_out_of_range_month(tmp_path: Path) -> None:
    patterns = _patterns_for("imports")
    imports_sheet = SHEET_TITLES["imports"]
    with pytest.raises(DiscoverError, match="month 13 out of range"):
        parse_filename(tmp_path / f"{imports_sheet}_202413N.TXT.zip", patterns)


def test_parse_filename_rejects_ambiguous_match(tmp_path: Path) -> None:
    """If two spec patterns both match the same file, the config is ambiguous."""
    import re

    loose = PatternEntry(
        trade_type="imports",
        pattern=re.compile(r"^.+_(?P<year>\d{4})(?P<month>\d{2})N\.TXT\.zip$"),
        source_label="imports/v2020-01",
    )
    strict = PatternEntry(
        trade_type="exports_us",
        pattern=re.compile(r"^foo_(?P<year>\d{4})(?P<month>\d{2})N\.TXT\.zip$"),
        source_label="exports_us/v2020-01",
    )
    with pytest.raises(DiscoverError, match="matches multiple spec filename_patterns"):
        parse_filename(tmp_path / "foo_202404N.TXT.zip", [loose, strict])


def test_scan_finds_and_sorts_inputs(tmp_path: Path, spec_dir: Path) -> None:
    input_dir = tmp_path / "input"
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 2))
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 1))
    _touch(input_dir / input_filename(SHEET_TITLES["exports_us"], 2024, 1))
    _touch(input_dir / "README.txt")
    # Not recursive.
    _touch(input_dir / "subdir" / input_filename(SHEET_TITLES["imports"], 2024, 3))

    found = scan(input_dir, spec_dir=spec_dir)
    assert [(r.trade_type, r.year, r.month) for r in found] == [
        ("exports_us", 2024, 1),
        ("imports", 2024, 1),
        ("imports", 2024, 2),
    ]


def test_scan_prefers_n_over_c(tmp_path: Path, spec_dir: Path) -> None:
    input_dir = tmp_path / "input"
    n_file = _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 4, flag="N"))
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 4, flag="C"))
    c_only = _touch(input_dir / input_filename(SHEET_TITLES["exports_us"], 2024, 4, flag="C"))

    found = scan(input_dir, spec_dir=spec_dir)
    by_key = {(r.trade_type, r.year, r.month): r for r in found}
    assert by_key[("imports", 2024, 4)].path == n_file
    assert by_key[("imports", 2024, 4)].flag == "N"
    assert by_key[("exports_us", 2024, 4)].path == c_only
    assert by_key[("exports_us", 2024, 4)].flag == "C"


def test_scan_filters_by_trade_type_year_month(tmp_path: Path, spec_dir: Path) -> None:
    input_dir = tmp_path / "input"
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 1))
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2024, 2))
    _touch(input_dir / input_filename(SHEET_TITLES["imports"], 2025, 1))
    _touch(input_dir / input_filename(SHEET_TITLES["exports_us"], 2024, 1))

    imports_2024 = scan(input_dir, spec_dir=spec_dir, trade_types=["imports"], year=2024)
    assert [(r.year, r.month) for r in imports_2024] == [(2024, 1), (2024, 2)]

    jan_only = scan(input_dir, spec_dir=spec_dir, month=1)
    assert {(r.trade_type, r.year) for r in jan_only} == {
        ("imports", 2024),
        ("imports", 2025),
        ("exports_us", 2024),
    }


def test_scan_rejects_unknown_trade_type(tmp_path: Path, spec_dir: Path) -> None:
    with pytest.raises(DiscoverError, match="unknown trade_types"):
        scan(tmp_path, spec_dir=spec_dir, trade_types=["not_a_type"])


def test_scan_rejects_missing_dir(tmp_path: Path, spec_dir: Path) -> None:
    with pytest.raises(DiscoverError, match="does not exist"):
        scan(tmp_path / "nope", spec_dir=spec_dir)


def test_load_patterns_skips_specs_without_filename_pattern(tmp_path: Path) -> None:
    """A spec whose source omits filename_pattern still loads via `schema.resolve`
    but contributes no PatternEntry - its raw files simply won't be discovered."""
    workbook = build_workbook(tmp_path / "wb.xlsx")
    config_path = build_project_config(tmp_path / "microtrade.yaml", workbook, "2020-01")
    project = load_config(config_path)
    specs = excel_spec.read_workbook(workbook, project.get_workbook(workbook))
    spec_root = tmp_path / "specs"

    imports = specs["imports"]
    assert imports.source is not None
    stripped = schema.Spec(
        trade_type=imports.trade_type,
        version=imports.version,
        effective_from=imports.effective_from,
        effective_to=imports.effective_to,
        record_length=imports.record_length,
        columns=imports.columns,
        source=schema.SpecSource(
            workbook=imports.source.workbook,
            sha256=imports.source.sha256,
            sheet=imports.source.sheet,
            imported_at=imports.source.imported_at,
            workbook_id=imports.source.workbook_id,
            filename_pattern=None,
        ),
        derived=imports.derived,
        partition_by=imports.partition_by,
    )
    schema.save_spec(stripped, spec_root / "imports" / "v2020-01.yaml")
    schema.save_spec(specs["exports_us"], spec_root / "exports_us" / "v2020-01.yaml")

    entries = load_patterns(spec_root)
    assert [e.trade_type for e in entries] == ["exports_us"]


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
