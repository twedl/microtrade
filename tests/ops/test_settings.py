from pathlib import Path

import pytest

from microtrade.ops.settings import SettingsError, load_settings

FIXTURE_YAML = """
microtrade_yaml: /app/microtrade.yaml
workbooks_dir: /data/workbooks
raw_dir: /data/raw
specs_dir: /data/specs
processed_dir: /data/processed
spec_manifests_dir: /data/manifests/specs
raw_manifests_dir: /data/manifests/raw
upstream_raw_dir: /mnt/upstream/raw
raw_remote_dir: /mnt/remote/raw
processed_remote_dir: /mnt/remote/processed
manifests_remote_dir: /mnt/remote/manifests
"""


def test_load_settings_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(FIXTURE_YAML)
    cfg = load_settings(yaml_path)
    assert cfg.microtrade_yaml == Path("/app/microtrade.yaml")
    assert cfg.raw_dir == Path("/data/raw")
    assert cfg.upstream_raw_dir == Path("/mnt/upstream/raw")
    assert cfg.raw_remote_dir == Path("/mnt/remote/raw")


def test_env_overrides_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(FIXTURE_YAML)
    monkeypatch.setenv("MT_RAW_DIR", "/override/raw")
    cfg = load_settings(yaml_path)
    assert cfg.raw_dir == Path("/override/raw")
    assert cfg.processed_dir == Path("/data/processed")


def test_missing_field_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("raw_dir: /data/raw\n")
    with pytest.raises(SettingsError, match="missing required field"):
        load_settings(yaml_path)
