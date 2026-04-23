from pathlib import Path

import pytest

from microtrade.ops.settings import load_settings

MICROTRADE_YAML = """
workbooks:
  wb2020.xls:
    workbook_id: WB2020
    effective_from: 2020-01
    effective_to: 2023-12
    sheets:
      S1:
        trade_type: imports
        filename_pattern: '^S1_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
      S2:
        trade_type: exports_us
        filename_pattern: '^S2_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
"""


@pytest.fixture
def tree(tmp_path: Path):
    for sub in (
        "workbooks",
        "raw",
        "specs",
        "processed",
        "manifests/specs",
        "manifests/raw",
    ):
        (tmp_path / sub).mkdir(parents=True)

    mt_yaml = tmp_path / "microtrade.yaml"
    mt_yaml.write_text(MICROTRADE_YAML)

    cfg_yaml = tmp_path / "config.yaml"
    cfg_yaml.write_text(
        f"""
microtrade_yaml: {mt_yaml}
workbooks_dir: {tmp_path / "workbooks"}
raw_dir: {tmp_path / "raw"}
specs_dir: {tmp_path / "specs"}
processed_dir: {tmp_path / "processed"}
spec_manifests_dir: {tmp_path / "manifests" / "specs"}
raw_manifests_dir: {tmp_path / "manifests" / "raw"}
upstream_raw_dir: {tmp_path / "upstream"}
raw_remote_dir: {tmp_path / "remote"}
"""
    )
    return load_settings(cfg_yaml), tmp_path
