from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from microtrade.ops.manifest import (
    RawManifest,
    SpecManifest,
    read_manifest,
    write_manifest,
)


def _spec(name: str = "microdata-layout.xls") -> SpecManifest:
    return SpecManifest(
        workbook_name=name,
        workbook_hash="aaa",
        microtrade_hash="bbb",
        specs_written=[Path("/specs/a.yaml"), Path("/specs/b.yaml")],
        processed_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
    )


def _raw(name: str = "SHEETNAME001_202001N.TXT.zip") -> RawManifest:
    return RawManifest(
        raw_name=name,
        raw_hash="rawhash",
        microtrade_hash="mthash",
        trade_type="imports",
        year="2020",
        month="01",
        flag="N",
        processed_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
    )


def test_read_missing_returns_none(tmp_path: Path) -> None:
    assert read_manifest(tmp_path, "nope", SpecManifest) is None


def test_spec_roundtrip(tmp_path: Path) -> None:
    m = _spec()
    write_manifest(tmp_path, m.workbook_name, m)
    got = read_manifest(tmp_path, m.workbook_name, SpecManifest)
    assert got == m


def test_raw_roundtrip(tmp_path: Path) -> None:
    m = _raw()
    write_manifest(tmp_path, m.raw_name, m)
    got = read_manifest(tmp_path, m.raw_name, RawManifest)
    assert got == m


def test_independent_keys(tmp_path: Path) -> None:
    a = _spec("a.xls")
    b = _spec("b.xls")
    write_manifest(tmp_path, a.workbook_name, a)
    write_manifest(tmp_path, b.workbook_name, b)
    assert read_manifest(tmp_path, "a.xls", SpecManifest) == a
    assert read_manifest(tmp_path, "b.xls", SpecManifest) == b


def test_overwrite_replaces(tmp_path: Path) -> None:
    a = _spec()
    write_manifest(tmp_path, a.workbook_name, a)
    b = replace(a, workbook_hash="different")
    write_manifest(tmp_path, a.workbook_name, b)
    assert read_manifest(tmp_path, a.workbook_name, SpecManifest) == b


def test_creates_directory(tmp_path: Path) -> None:
    nested = tmp_path / "deeply" / "nested"
    m = _spec()
    write_manifest(nested, m.workbook_name, m)
    assert read_manifest(nested, m.workbook_name, SpecManifest) == m


def test_failed_replace_leaves_target_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a = _spec()
    write_manifest(tmp_path, a.workbook_name, a)

    def boom(src: object, dst: object) -> None:
        raise OSError("simulated crash")

    monkeypatch.setattr("microtrade.ops.manifest.os.replace", boom)
    b = replace(a, workbook_hash="new")
    with pytest.raises(OSError):
        write_manifest(tmp_path, a.workbook_name, b)

    monkeypatch.undo()
    assert read_manifest(tmp_path, a.workbook_name, SpecManifest) == a


def test_processed_at_is_iso8601(tmp_path: Path) -> None:
    m = _spec()
    write_manifest(tmp_path, m.workbook_name, m)
    raw = (tmp_path / f"{m.workbook_name}.json").read_text()
    assert "2026-04-22T12:00:00" in raw
