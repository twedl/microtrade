"""End-to-end ops run against microtrade's real library API.

Uses the example workbook + microtrade.yaml shipped under
``examples/``. After the tp merge these live in-repo so the tests
always exercise the real thing; no skip condition.
"""

import shutil
from pathlib import Path

import pytest

from microtrade.ops.manifest import SpecManifest, read_manifest
from microtrade.ops.runner import import_spec, ingest_year, run
from microtrade.ops.settings import load_settings

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MICROTRADE_EXAMPLES = REPO_ROOT / "examples"
EXAMPLE_WORKBOOK = MICROTRADE_EXAMPLES / "microdata-layout.xls"
EXAMPLE_YAML = MICROTRADE_EXAMPLES / "microtrade.yaml"

pytestmark = pytest.mark.skipif(
    not (EXAMPLE_WORKBOOK.exists() and EXAMPLE_YAML.exists()),
    reason="microtrade example files not available",
)


@pytest.fixture
def real_tree(tmp_path: Path):
    for sub in (
        "workbooks",
        "raw",
        "specs",
        "processed",
        "manifests/specs",
        "manifests/raw",
    ):
        (tmp_path / sub).mkdir(parents=True)

    shutil.copy(EXAMPLE_WORKBOOK, tmp_path / "workbooks" / EXAMPLE_WORKBOOK.name)
    mt_yaml = tmp_path / "microtrade.yaml"
    shutil.copy(EXAMPLE_YAML, mt_yaml)

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


def test_import_spec_against_real_workbook(real_tree):
    settings, _root = real_tree
    wb = next(settings.workbooks_dir.iterdir())
    specs = import_spec(wb, settings.microtrade_yaml, settings.specs_dir)

    assert {p.parent.name for p in specs} == {
        "imports",
        "exports_us",
        "exports_nonus",
    }
    for p in specs:
        assert p.exists()
        assert p.name.startswith("v2020-01")


def test_full_run_stage1_only(real_tree):
    """Stage 1 succeeds, stage 2 has no raw files -> no-op. Exits 0."""
    settings, _root = real_tree
    assert run(settings) == 0

    wb = next(settings.workbooks_dir.iterdir())
    m = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    assert m is not None
    assert m.workbook_name == wb.name
    assert len(m.specs_written) == 3
    for spec_path in m.specs_written:
        assert spec_path.exists()


def test_rerun_is_idempotent(real_tree):
    settings, _root = real_tree

    assert run(settings) == 0
    wb = next(settings.workbooks_dir.iterdir())
    m1 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)

    assert run(settings) == 0
    m2 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    assert m1 == m2


def test_ingest_year_against_empty_raw_dir(real_tree):
    """Direct call into the default ingest_year with no raw files -> empty
    RunSummary with no failures."""
    settings, _root = real_tree
    wb = next(settings.workbooks_dir.iterdir())
    import_spec(wb, settings.microtrade_yaml, settings.specs_dir)

    summary = ingest_year(
        trade_type="imports",
        year=2020,
        raw_dir=settings.raw_dir,
        specs_dir=settings.specs_dir,
        out_dir=settings.processed_dir,
    )
    assert summary.failed_count == 0
    assert summary.ok_count == 0


def test_microtrade_yaml_change_retriggers_import(real_tree):
    settings, _root = real_tree

    assert run(settings) == 0
    wb = next(settings.workbooks_dir.iterdir())
    m1 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)

    settings.microtrade_yaml.write_text(
        settings.microtrade_yaml.read_text() + "\n# bumped\n"
    )
    assert run(settings) == 0
    m2 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    assert m1 is not None and m2 is not None
    assert m2.microtrade_hash != m1.microtrade_hash
