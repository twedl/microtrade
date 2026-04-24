from datetime import UTC, datetime
from pathlib import Path

from microtrade.config import load_config
from microtrade.ops.manifest import RawManifest, SpecManifest, write_manifest
from microtrade.ops.planner import YearKey, plan_stage1, plan_stage2
from microtrade.schema import file_sha256


def _load_cfg(tree):
    settings, _root = tree
    return load_config(settings.microtrade_yaml)


def _write_workbook(tree, name: str, content: bytes = b"wb") -> Path:
    _settings, root = tree
    p = root / "workbooks" / name
    p.write_bytes(content)
    return p


def _write_raw(tree, name: str, content: bytes = b"raw") -> Path:
    _settings, root = tree
    p = root / "raw" / name
    p.write_bytes(content)
    return p


def _mark_workbook_clean(tree, wb: Path) -> None:
    settings, _root = tree
    m = SpecManifest(
        workbook_name=wb.name,
        workbook_hash=file_sha256(wb),
        microtrade_hash=file_sha256(settings.microtrade_yaml),
        specs_written=[],
        processed_at=datetime.now(tz=UTC),
    )
    write_manifest(settings.spec_manifests_dir, wb.name, m)


def _mark_raw_clean(tree, raw: Path, *, trade_type: str, year: str, month: str, flag: str) -> None:
    settings, _root = tree
    m = RawManifest(
        raw_name=raw.name,
        raw_hash=file_sha256(raw),
        microtrade_hash=file_sha256(settings.microtrade_yaml),
        trade_type=trade_type,
        year=year,
        month=month,
        flag=flag,
        processed_at=datetime.now(tz=UTC),
    )
    write_manifest(settings.raw_manifests_dir, raw.name, m)
    # Match the post-ingest filesystem shape so plan_stage2's output-exists
    # check treats this raw as cleanly processed.
    year_dir = settings.processed_dir / trade_type / f"year={year}" / f"month={month}"
    year_dir.mkdir(parents=True, exist_ok=True)
    (year_dir / "part-0.parquet").write_bytes(b"stub")


def test_stage1_no_manifests_all_dirty(tree):
    settings, _root = tree
    wb_a = _write_workbook(tree, "wb2020.xls")
    wb_b = _write_workbook(tree, "wb2024.xls")
    assert plan_stage1(settings, _load_cfg(tree)) == [wb_a, wb_b]


def test_stage1_all_clean_returns_empty(tree):
    settings, _root = tree
    wb_a = _write_workbook(tree, "wb2020.xls")
    _mark_workbook_clean(tree, wb_a)
    assert plan_stage1(settings, _load_cfg(tree)) == []


def test_stage1_workbook_content_changed(tree):
    settings, _root = tree
    wb_a = _write_workbook(tree, "wb2020.xls", b"original")
    _mark_workbook_clean(tree, wb_a)
    wb_a.write_bytes(b"changed")
    assert plan_stage1(settings, _load_cfg(tree)) == [wb_a]


def test_stage1_microtrade_yaml_changed_marks_all_dirty(tree):
    settings, _root = tree
    wb_a = _write_workbook(tree, "wb2020.xls")
    wb_b = _write_workbook(tree, "wb2024.xls")
    _mark_workbook_clean(tree, wb_a)
    _mark_workbook_clean(tree, wb_b)
    # Reload cfg after mutating the yaml so the dirty check sees the new config.
    settings.microtrade_yaml.write_text(settings.microtrade_yaml.read_text() + "\n# bumped\n")
    assert set(plan_stage1(settings, _load_cfg(tree))) == {wb_a, wb_b}


def test_stage1_dirty_when_spec_output_missing(tree):
    """Clean manifest but a recorded spec file is gone -> replan.

    Guards against reconfiguring ``specs_dir`` (or manually deleting
    specs) after a prior successful run. Without this, the manifest
    says "done at hash H" while stage 2 finds no specs to route with.
    """
    settings, _root = tree
    wb = _write_workbook(tree, "wb2020.xls")
    spec_path = settings.specs_dir / "imports" / "v2020-01.yaml"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text("stub\n")
    m = SpecManifest(
        workbook_name=wb.name,
        workbook_hash=file_sha256(wb),
        microtrade_hash=file_sha256(settings.microtrade_yaml),
        specs_written=[spec_path],
        processed_at=datetime.now(tz=UTC),
    )
    write_manifest(settings.spec_manifests_dir, wb.name, m)

    assert plan_stage1(settings, _load_cfg(tree)) == []
    spec_path.unlink()
    assert plan_stage1(settings, _load_cfg(tree)) == [wb]


def test_stage1_unknown_workbook_skipped(tree):
    """Files in workbooks_dir that aren't named in the project config are ignored.

    Guards against the case where ``workbooks_dir`` and ``raw_dir``
    point at the same directory and raw zips end up alongside real
    workbooks — previously this crashed ``import_spec`` on the zip.
    """
    settings, _root = tree
    wb = _write_workbook(tree, "wb2020.xls")
    _write_workbook(tree, "CRO0176390_Im201812.zip", b"not a workbook")
    _write_workbook(tree, "unknown.xlsx", b"not in config")
    assert plan_stage1(settings, _load_cfg(tree)) == [wb]


def test_stage2_no_raw_files_empty(tree):
    settings, _root = tree
    assert plan_stage2(settings, _load_cfg(tree)) == {}


def test_stage2_unmatched_file_skipped(tree):
    _write_raw(tree, "junk.txt")
    settings, _root = tree
    assert plan_stage2(settings, _load_cfg(tree)) == {}


def test_stage2_groups_months_of_same_year(tree):
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    b = _write_raw(tree, "S1_202002N.TXT.zip")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert set(plan.keys()) == {YearKey("imports", 2020)}
    assert set(plan[YearKey("imports", 2020)]) == {a, b}


def test_stage2_separates_by_trade_type_and_year(tree):
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    b = _write_raw(tree, "S1_202101N.TXT.zip")
    c = _write_raw(tree, "S2_202001N.TXT.zip")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert set(plan.keys()) == {
        YearKey("imports", 2020),
        YearKey("imports", 2021),
        YearKey("exports_us", 2020),
    }
    assert plan[YearKey("imports", 2020)] == [a]
    assert plan[YearKey("imports", 2021)] == [b]
    assert plan[YearKey("exports_us", 2020)] == [c]


def test_stage2_all_clean_returns_empty(tree):
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    _mark_raw_clean(tree, a, trade_type="imports", year="2020", month="01", flag="N")
    assert plan_stage2(settings, _load_cfg(tree)) == {}


def test_stage2_dirty_year_includes_clean_siblings(tree):
    settings, _root = tree
    clean = _write_raw(tree, "S1_202001N.TXT.zip", b"clean")
    dirty = _write_raw(tree, "S1_202002N.TXT.zip", b"dirty")
    _mark_raw_clean(tree, clean, trade_type="imports", year="2020", month="01", flag="N")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert set(plan.keys()) == {YearKey("imports", 2020)}
    assert set(plan[YearKey("imports", 2020)]) == {clean, dirty}


def test_stage2_raw_content_changed(tree):
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip", b"v1")
    _mark_raw_clean(tree, a, trade_type="imports", year="2020", month="01", flag="N")
    a.write_bytes(b"v2")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert plan == {YearKey("imports", 2020): [a]}


def test_stage2_microtrade_change_dirties_all_years(tree):
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    b = _write_raw(tree, "S2_202001N.TXT.zip")
    _mark_raw_clean(tree, a, trade_type="imports", year="2020", month="01", flag="N")
    _mark_raw_clean(tree, b, trade_type="exports_us", year="2020", month="01", flag="N")
    settings.microtrade_yaml.write_text(settings.microtrade_yaml.read_text() + "\n# bump\n")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert set(plan.keys()) == {
        YearKey("imports", 2020),
        YearKey("exports_us", 2020),
    }


def test_stage2_dirty_when_year_output_missing(tree):
    """Clean manifest but no processed year dir -> replan.

    Guards against reconfiguring ``processed_dir`` (or manually
    deleting output) after a successful run. Without this, manifests
    would say "done at hash H" while the parquet output is absent.
    """
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    _mark_raw_clean(tree, a, trade_type="imports", year="2020", month="01", flag="N")

    assert plan_stage2(settings, _load_cfg(tree)) == {}

    # Simulate a reconfigured processed_dir / deleted output.
    import shutil

    shutil.rmtree(settings.processed_dir / "imports" / "year=2020")
    assert plan_stage2(settings, _load_cfg(tree)) == {YearKey("imports", 2020): [a]}


def test_stage2_dirty_when_year_output_empty(tree):
    """An empty year dir is treated the same as a missing one."""
    settings, _root = tree
    a = _write_raw(tree, "S1_202001N.TXT.zip")
    _mark_raw_clean(tree, a, trade_type="imports", year="2020", month="01", flag="N")

    # Wipe month= subdirs so the year dir is empty.
    import shutil

    for child in (settings.processed_dir / "imports" / "year=2020").iterdir():
        shutil.rmtree(child)
    assert plan_stage2(settings, _load_cfg(tree)) == {YearKey("imports", 2020): [a]}


def test_stage2_clean_year_not_dirtied_by_other_year(tree):
    settings, _root = tree
    clean_a = _write_raw(tree, "S1_202001N.TXT.zip")
    _mark_raw_clean(tree, clean_a, trade_type="imports", year="2020", month="01", flag="N")
    dirty_b = _write_raw(tree, "S1_202101N.TXT.zip")
    plan = plan_stage2(settings, _load_cfg(tree))
    assert plan == {YearKey("imports", 2021): [dirty_b]}
