import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

from microtrade.ops.manifest import RawManifest, SpecManifest, read_manifest
from microtrade.ops.runner import run


def _drop_raw(settings, name: str, content: bytes = b"raw") -> Path:
    """Drop a raw zip into the remote archive (plan_stage2's source of truth)."""
    current = settings.raw_remote_dir / "current"
    current.mkdir(parents=True, exist_ok=True)
    p = current / name
    p.write_bytes(content)
    return p


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
    # Raws live in the remote archive — the per-year loop pulls each
    # year into raw_dir at ingest time and cleans up afterwards.
    raw_a = _drop_raw(settings, "S1_202001N.TXT.zip", b"raw-a")
    raw_b = _drop_raw(settings, "S2_202003N.TXT.zip", b"raw-b")

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

    # cleanup_local_year ran for each successful year — local raw_dir
    # and local processed output are empty, so peak disk stays bounded.
    assert not any(settings.raw_dir.iterdir())
    assert not (settings.processed_dir / "imports" / "year=2020").exists()
    assert not (settings.processed_dir / "exports_us" / "year=2020").exists()
    # Remote archive is permanent — raws still there for future reference.
    assert raw_a.exists()
    assert raw_b.exists()


def test_rerun_is_noop(tree, install_adapter):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    _drop_raw(settings, "S1_202001N.TXT.zip")

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
    good = _drop_raw(settings, "S1_202001N.TXT.zip", b"g")
    bad = _drop_raw(settings, "S2_202003N.TXT.zip", b"b")

    install_adapter(FakeAdapter(ingest_fail_for={("exports_us", 2020)}))
    assert run(settings) == 1

    assert read_manifest(settings.raw_manifests_dir, good.name, RawManifest) is not None
    assert read_manifest(settings.raw_manifests_dir, bad.name, RawManifest) is None
    # Good year published; bad year did not.
    assert (settings.processed_remote_dir / "imports" / "year=2020").is_dir()
    assert not (settings.processed_remote_dir / "exports_us" / "year=2020").exists()
    # Local raw_dir cleaned for both years — good via the success path,
    # bad via the ingest-failure path (raws_only=True).
    assert not any(settings.raw_dir.iterdir())


def test_stage1_failure_isolated_nonzero_exit(tree, install_adapter):
    settings, root = tree
    good_wb = root / "workbooks" / "wb2020.xls"
    bad_wb = root / "workbooks" / "wb2024.xls"
    good_wb.write_bytes(b"g")
    bad_wb.write_bytes(b"b")

    install_adapter(FakeAdapter(import_fail_for={bad_wb.name}))
    assert run(settings) == 1

    assert read_manifest(settings.spec_manifests_dir, good_wb.name, SpecManifest) is not None
    assert read_manifest(settings.spec_manifests_dir, bad_wb.name, SpecManifest) is None


def test_microtrade_failed_count_triggers_year_failure(tree, install_adapter):
    settings, _root = tree
    _drop_raw(settings, "S1_202001N.TXT.zip")

    install_adapter(FakeAdapter(summary_failed_count=1))
    assert run(settings) == 1
    assert read_manifest(settings.raw_manifests_dir, "S1_202001N.TXT.zip", RawManifest) is None


def test_default_adapter_raises(tree):
    """Without fakes installed, the real import_spec fails on a stub workbook."""
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    assert run(settings) == 1


def test_copy_file_is_used_by_every_hook(tree, install_adapter):
    """A custom copy_file threads through every hook that has work to do:
    pull_manifests, mirror_upstream_raw, pull_workbooks, pull_raws_for_year,
    push_processed, push_manifests."""
    settings, _root = tree

    # Populate upstream with a zip and an xlsx so mirror + pull_workbooks
    # both have something to copy; raw_dir starts empty so the per-year
    # pull has to pull the zip from the raw_remote_dir.
    upstream_zip = settings.upstream_raw_dir / "S1_202001N.TXT.zip"
    upstream_wb = settings.upstream_raw_dir / "wb2020.xls"
    upstream_zip.parent.mkdir(parents=True, exist_ok=True)
    upstream_zip.write_bytes(b"upstream-z")
    upstream_wb.write_bytes(b"upstream-wb")

    seeded_manifest = settings.manifests_remote_dir / "raw" / "seed.json"
    seeded_manifest.parent.mkdir(parents=True, exist_ok=True)
    seeded_manifest.write_text("{}")

    install_adapter(FakeAdapter())

    calls: list[tuple[Path, Path]] = []

    def spy(src: Path, dst: Path) -> None:
        calls.append((src, dst))
        shutil.copy2(src, dst)

    assert run(settings, copy_file=spy) == 0

    # copy_file receives the final target path; atomicity is the copy_file's job.
    assert calls, "expected copy_file to be invoked at least once"
    assert not any(d.name.endswith(".tmp") for _, d in calls)

    dsts = [d for _, d in calls]
    assert any(settings.raw_remote_dir / "current" in d.parents for d in dsts)  # mirror
    assert any(settings.workbooks_dir in d.parents for d in dsts)  # pull_workbooks
    assert any(settings.raw_dir in d.parents for d in dsts)  # pull_raws_for_year
    assert any(settings.processed_remote_dir in d.parents for d in dsts)  # push_processed
    assert any(settings.manifests_remote_dir in d.parents for d in dsts)  # push_manifests
    assert any(settings.raw_manifests_dir in d.parents for d in dsts)  # pull_manifests


def test_push_failure_aborts_stage_2_retains_local_parquet(
    tree, install_adapter, monkeypatch
):
    """Fail-fast on push: stop processing later years, keep local parquet.

    Rationale: if push fails, continuing to other years would accumulate
    unpushed parquet on local disk and defeat the point of the per-year
    cycle. Next run will pick up from the retained parquet.
    """
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    a = _drop_raw(settings, "S1_202001N.TXT.zip", b"a")
    b = _drop_raw(settings, "S2_202003N.TXT.zip", b"b")

    install_adapter(FakeAdapter())

    push_attempts: list = []

    def failing_push(settings_, paths, *, copy_file):
        push_attempts.append(list(paths))
        raise RuntimeError("push boom")

    monkeypatch.setattr("microtrade.ops.runner.push_processed", failing_push)

    assert run(settings) == 1

    # Fail-fast: push attempted exactly once (first dirty year), no later years.
    assert len(push_attempts) == 1
    # Local parquet retained for the attempted year so retry doesn't re-ingest.
    # Years are processed sorted by (trade_type, year); exports_us sorts before imports.
    assert (settings.processed_dir / "exports_us" / "year=2020").is_dir()
    # The later year was never ingested.
    assert not (settings.processed_dir / "imports" / "year=2020").exists()
    # No raw manifests written (push for the attempted year failed; the later
    # year never ran).
    assert read_manifest(settings.raw_manifests_dir, a.name, RawManifest) is None
    assert read_manifest(settings.raw_manifests_dir, b.name, RawManifest) is None


def test_per_year_pull_only_pulls_that_years_zips(tree, install_adapter, monkeypatch):
    """pull_raws_for_year filters by (trade_type, year) so local disk
    only ever holds one year's zips, not the whole archive."""
    settings, _root = tree

    # Remote archive has zips for two different years. Only the dirty
    # year's zips should land in raw_dir when its turn comes.
    current = settings.raw_remote_dir / "current"
    current.mkdir(parents=True, exist_ok=True)
    (current / "S1_202001N.TXT.zip").write_bytes(b"y2020")
    (current / "S1_202101N.TXT.zip").write_bytes(b"y2021")

    seen_raw_dir_contents: list[set[str]] = []

    adapter = install_adapter(FakeAdapter())
    original_ingest = adapter.ingest_year

    def snooping_ingest(trade_type, year, raw_dir, specs_dir, out_dir):
        seen_raw_dir_contents.append({p.name for p in raw_dir.iterdir()})
        return original_ingest(trade_type, year, raw_dir, specs_dir, out_dir)

    monkeypatch.setattr("microtrade.ops.runner.ingest_year", snooping_ingest)

    assert run(settings) == 0

    # Two years processed. At ingest time, raw_dir held ONLY the current
    # year's zip — the cleanup from the prior year plus the filter in
    # pull_raws_for_year ensure no cross-year accumulation.
    assert len(seen_raw_dir_contents) == 2
    for contents in seen_raw_dir_contents:
        assert len(contents) == 1
