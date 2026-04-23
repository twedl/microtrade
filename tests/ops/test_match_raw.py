from pathlib import Path

import pytest

from microtrade.config import load_config
from microtrade.ops.planner import match_raw

FIXTURE_YAML = """
workbooks:
  microdata-layout.xls:
    workbook_id: MICRODATA2020
    effective_from: 2020-01
    effective_to: 2023-12
    sheets:
      S1:
        trade_type: imports
        filename_pattern: '^S1_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
        routing_column: year_month
      S2:
        trade_type: exports_us
        filename_pattern: '^S2_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
  microdata-layout-2024.xls:
    workbook_id: MICRODATA2024
    effective_from: 2024-01
    sheets:
      S1:
        trade_type: imports
        filename_pattern: '^S1_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
"""


@pytest.fixture
def cfg(tmp_path: Path):
    p = tmp_path / "microtrade.yaml"
    p.write_text(FIXTURE_YAML)
    return load_config(p)


def test_happy_match(cfg):
    m = match_raw("S1_202001N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2020"
    assert m.sheet_name == "S1"
    assert m.trade_type == "imports"
    assert m.year == "2020"
    assert m.month == "01"
    assert m.flag == "N"


def test_no_match(cfg):
    assert match_raw("random_file.txt", cfg) is None
    assert match_raw("S1_202001X.TXT.zip", cfg) is None


def test_first_match_wins_across_workbooks(cfg):
    m = match_raw("S1_202006N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2020"


def test_date_outside_2020_falls_through_to_2024(cfg):
    m = match_raw("S1_202503N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2024"


def test_date_outside_all_windows_no_match(cfg):
    assert match_raw("S1_201912N.TXT.zip", cfg) is None


def test_open_ended_effective_to(cfg):
    m = match_raw("S1_209912N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2024"


def test_window_boundaries_inclusive(cfg):
    m1 = match_raw("S1_202001N.TXT.zip", cfg)
    m2 = match_raw("S1_202312N.TXT.zip", cfg)
    m3 = match_raw("S1_202401N.TXT.zip", cfg)
    assert m1 is not None and m1.workbook_id == "MICRODATA2020"
    assert m2 is not None and m2.workbook_id == "MICRODATA2020"
    assert m3 is not None and m3.workbook_id == "MICRODATA2024"


def test_sheet_only_in_2020_does_not_leak(cfg):
    assert match_raw("S2_202503N.TXT.zip", cfg) is None
