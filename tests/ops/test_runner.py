import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

from microtrade.ops.manifest import RawManifest, SpecManifest, read_manifest
from microtrade.ops.runner import run


@dataclass
class FakeSummary:
    failed_count: int = 0
    ok_count: int = 0


class FakeAdapter:
    def __init__(
        self,
        *,
        ingest_fail_for: set[tuple[str, int]] | None = None,
        import_fail_for: set[str] | None = None,
        summary_failed_count: int = 0,
    ) -> None:
        self.import_calls: list[tuple[Path, Path, Path]] = []
        self.ingest_calls: list[tuple[str, int, Path, Path, Path]] = []
        self.ingest_fail_for = ingest_fail_for or set()
        self.import_fail_for = import_fail_for or set()
        self.summary_failed_count = summary_failed_count

    def import_spec(self, workbook: Path, microtrade_yaml: Path, specs_out: Path) -> list[Path]:
        self.import_calls.append((workbook, microtrade_yaml, specs_out))
        if workbook.name in self.import_fail_for:
            raise RuntimeError(f"import boom: {workbook.name}")
        out = specs_out / f"{workbook.stem}.spec.yaml"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("stub\n")
        return [out]

    def ingest_year(
        self,
        trade_type: str,
        year: int,
        raw_dir: Path,
        specs_dir: Path,
        out_dir: Path,
    ) -> FakeSummary:
        self.ingest_calls.append((trade_type, year, raw_dir, specs_dir, out_dir))
        if (trade_type, year) in self.ingest_fail_for:
            raise RuntimeError(f"ingest boom: {(trade_type, year)}")
        year_dir = out_dir / trade_type / f"year={year}"
        (year_dir / "month=01").mkdir(parents=True, exist_ok=True)
        (year_dir / "month=01" / "part-0.parquet").write_text("stub")
        return FakeSummary(failed_count=self.summary_failed_count)


@pytest.fixture
def install_adapter(monkeypatch: pytest.MonkeyPatch):
    def _install(adapter: FakeAdapter) -> FakeAdapter:
        monkeypatch.setattr("microtrade.ops.runner.import_spec", adapter.import_spec)
        monkeypatch.setattr("microtrade.ops.runner.ingest_year", adapter.ingest_year)
        return adapter

    return _install


def test_empty_run_exits_clean(tree, install_adapter):
    settings, _root = tree
    adapter = install_adapter(FakeAdapter())
    assert run(settings) == 0
    assert adapter.import_calls == []
    assert adapter.ingest_calls == []


def test_happy_path(tree, install_adapter):
    settings, root = tree
    wb = root / "workbooks" / "wb2020.xls"
    wb.write_bytes(b"workbook")
    raw_a = root / "raw" / "S1_202001N.TXT.zip"
    raw_a.write_bytes(b"raw-a")
    raw_b = root / "raw" / "S2_202003N.TXT.zip"
    raw_b.write_bytes(b"raw-b")

    adapter = install_adapter(FakeAdapter())
    assert run(settings) == 0

    assert len(adapter.import_calls) == 1
    called_wb, called_yaml, called_specs_out = adapter.import_calls[0]
    assert called_wb == wb
    assert called_yaml == settings.microtrade_yaml
    assert called_specs_out == settings.specs_dir

    assert len(adapter.ingest_calls) == 2
    keys = {(c[0], c[1]) for c in adapter.ingest_calls}
    assert keys == {("imports", 2020), ("exports_us", 2020)}
    for _tt, _y, _rd, _sd, out_dir in adapter.ingest_calls:
        assert out_dir == settings.processed_dir

    spec_m = read_manifest(settings.spec_manifests_dir, "wb2020.xls", SpecManifest)
    assert spec_m is not None

    rm_a = read_manifest(settings.raw_manifests_dir, raw_a.name, RawManifest)
    assert rm_a is not None
    assert rm_a.trade_type == "imports"
    assert rm_a.year == "2020"
    assert rm_a.month == "01"

    # push_processed mirrored the per-year output dirs into processed_remote_dir.
    assert (settings.processed_remote_dir / "imports" / "year=2020").is_dir()
    assert (settings.processed_remote_dir / "exports_us" / "year=2020").is_dir()
    # push_manifests mirrored the manifest dirs into manifests_remote_dir.
    assert (settings.manifests_remote_dir / "specs" / "wb2020.xls.json").is_file()
    assert (settings.manifests_remote_dir / "raw" / f"{raw_a.name}.json").is_file()


def test_rerun_is_noop(tree, install_adapter):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    a1 = install_adapter(FakeAdapter())
    assert run(settings) == 0
    assert len(a1.import_calls) == 1
    assert len(a1.ingest_calls) == 1

    a2 = install_adapter(FakeAdapter())
    assert run(settings) == 0
    assert a2.import_calls == []
    assert a2.ingest_calls == []


def test_year_failure_isolated_nonzero_exit(tree, install_adapter):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    good = root / "raw" / "S1_202001N.TXT.zip"
    bad = root / "raw" / "S2_202003N.TXT.zip"
    good.write_bytes(b"g")
    bad.write_bytes(b"b")

    install_adapter(FakeAdapter(ingest_fail_for={("exports_us", 2020)}))
    assert run(settings) == 1

    assert read_manifest(settings.raw_manifests_dir, good.name, RawManifest) is not None
    assert read_manifest(settings.raw_manifests_dir, bad.name, RawManifest) is None
    # Good year published; bad year did not.
    assert (settings.processed_remote_dir / "imports" / "year=2020").is_dir()
    assert not (settings.processed_remote_dir / "exports_us" / "year=2020").exists()


def test_stage1_failure_isolated_nonzero_exit(tree, install_adapter):
    settings, root = tree
    good_wb = root / "workbooks" / "good.xls"
    bad_wb = root / "workbooks" / "bad.xls"
    good_wb.write_bytes(b"g")
    bad_wb.write_bytes(b"b")

    install_adapter(FakeAdapter(import_fail_for={bad_wb.name}))
    assert run(settings) == 1

    assert read_manifest(settings.spec_manifests_dir, good_wb.name, SpecManifest) is not None
    assert read_manifest(settings.spec_manifests_dir, bad_wb.name, SpecManifest) is None


def test_microtrade_failed_count_triggers_year_failure(tree, install_adapter):
    settings, root = tree
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    install_adapter(FakeAdapter(summary_failed_count=1))
    assert run(settings) == 1
    assert read_manifest(settings.raw_manifests_dir, "S1_202001N.TXT.zip", RawManifest) is None


def test_default_adapter_raises(tree):
    """Without fakes installed, the real import_spec fails on a stub workbook."""
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    assert run(settings) == 1


def test_copy_file_is_used_by_every_hook(tree, install_adapter):
    """A custom copy_file is threaded through pull_manifests, mirror_upstream_raw,
    pull_raw, push_processed, and push_manifests."""
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    # Populate the remote so pull_manifests, mirror, and pull all have work.
    upstream_zip = settings.upstream_raw_dir / "S1_202001N.TXT.zip"
    upstream_zip.parent.mkdir(parents=True, exist_ok=True)
    upstream_zip.write_bytes(b"upstream-z")
    seeded_manifest = settings.manifests_remote_dir / "raw" / "seed.json"
    seeded_manifest.parent.mkdir(parents=True, exist_ok=True)
    seeded_manifest.write_text("{}")

    install_adapter(FakeAdapter())

    calls: list[tuple[Path, Path]] = []

    def spy(src: Path, dst: Path) -> None:
        calls.append((src, dst))
        shutil.copy2(src, dst)

    assert run(settings, copy_file=spy) == 0

    # Every copy target is a .tmp sibling (sync_tree's atomicity discipline).
    assert calls, "expected copy_file to be invoked at least once"
    assert all(d.name.endswith(".tmp") for _, d in calls)

    # Every hook that has work to do went through the spy. Checked by
    # destinations the copy targets land under.
    dsts = [d for _, d in calls]
    assert any(settings.raw_remote_dir / "current" in d.parents for d in dsts)  # mirror
    assert any(settings.raw_dir in d.parents for d in dsts)  # pull_raw (zip split)
    assert any(settings.processed_remote_dir in d.parents for d in dsts)  # push_processed
    assert any(settings.manifests_remote_dir in d.parents for d in dsts)  # push_manifests
    assert any(settings.raw_manifests_dir in d.parents for d in dsts)  # pull_manifests
