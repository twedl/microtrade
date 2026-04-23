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


@pytest.fixture
def transport_spy(monkeypatch: pytest.MonkeyPatch):
    calls: dict[str, list] = {"mirror": [], "pull": [], "push": []}
    monkeypatch.setattr(
        "microtrade.ops.runner.mirror_upstream_raw",
        lambda s: calls["mirror"].append(s),
    )
    monkeypatch.setattr("microtrade.ops.runner.pull_raw", lambda s: calls["pull"].append(s))
    monkeypatch.setattr(
        "microtrade.ops.runner.push_processed",
        lambda s, dirs: calls["push"].append(list(dirs)),
    )
    return calls


def test_empty_run_exits_clean(tree, transport_spy, install_adapter):
    settings, _root = tree
    adapter = install_adapter(FakeAdapter())
    assert run(settings) == 0
    assert adapter.import_calls == []
    assert adapter.ingest_calls == []
    assert len(transport_spy["mirror"]) == 1
    assert len(transport_spy["pull"]) == 1
    assert transport_spy["push"] == []


def test_happy_path(tree, transport_spy, install_adapter):
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

    assert len(transport_spy["mirror"]) == 1
    assert len(transport_spy["pull"]) == 1
    assert len(transport_spy["push"]) == 2


def test_rerun_is_noop(tree, transport_spy, install_adapter):
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


def test_year_failure_isolated_nonzero_exit(tree, transport_spy, install_adapter):
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
    assert len(transport_spy["push"]) == 1


def test_stage1_failure_isolated_nonzero_exit(tree, transport_spy, install_adapter):
    settings, root = tree
    good_wb = root / "workbooks" / "good.xls"
    bad_wb = root / "workbooks" / "bad.xls"
    good_wb.write_bytes(b"g")
    bad_wb.write_bytes(b"b")

    install_adapter(FakeAdapter(import_fail_for={bad_wb.name}))
    assert run(settings) == 1

    assert read_manifest(settings.spec_manifests_dir, good_wb.name, SpecManifest) is not None
    assert read_manifest(settings.spec_manifests_dir, bad_wb.name, SpecManifest) is None


def test_microtrade_failed_count_triggers_year_failure(tree, transport_spy, install_adapter):
    settings, root = tree
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    install_adapter(FakeAdapter(summary_failed_count=1))
    assert run(settings) == 1
    assert read_manifest(settings.raw_manifests_dir, "S1_202001N.TXT.zip", RawManifest) is None


def test_default_adapter_raises(tree, transport_spy):
    """Without fakes installed, the real import_spec fails on a stub workbook."""
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    assert run(settings) == 1


def test_transport_kwargs_override_defaults(tree, install_adapter):
    """Passing transport fns directly bypasses the module-level stubs,
    and the full ordering (pull_manifests before stage 1, push_manifests
    after stage 2) is honoured."""
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    install_adapter(FakeAdapter())
    order: list[str] = []
    calls: dict[str, list] = {
        "pull_manifests": [],
        "mirror": [],
        "pull": [],
        "push": [],
        "push_manifests": [],
    }

    def record(name: str):
        def fn(*args):
            calls[name].append(args)
            order.append(name)

        return fn

    assert (
        run(
            settings,
            pull_manifests_fn=record("pull_manifests"),
            mirror=record("mirror"),
            pull=record("pull"),
            push=record("push"),
            push_manifests_fn=record("push_manifests"),
        )
        == 0
    )
    assert len(calls["pull_manifests"]) == 1
    assert len(calls["mirror"]) == 1
    assert len(calls["pull"]) == 1
    assert len(calls["push"]) == 1  # one dirty year
    assert len(calls["push_manifests"]) == 1
    # Ordering: pull_manifests first, push_manifests last.
    assert order[0] == "pull_manifests"
    assert order[-1] == "push_manifests"
