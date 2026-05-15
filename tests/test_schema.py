"""Unit tests for microtrade.schema."""

from __future__ import annotations

from pathlib import Path

import pytest

from microtrade.schema import (
    Column,
    EncodingOverride,
    Spec,
    SpecError,
    SpecSource,
    canonical_columns,
    diff_specs,
    load_all,
    load_spec,
    resolve,
    save_spec,
    validate_period,
    validate_spec,
)


def _cols() -> tuple[Column, ...]:
    return (
        Column(
            physical_name="period",
            start=1,
            length=6,
            dtype="Date",
            nullable=False,
            parse="yyyymm_to_date",
        ),
        Column(physical_name="hs_code", start=7, length=10, dtype="Utf8", nullable=False),
        Column(physical_name="value_usd", start=17, length=15, dtype="Int64", nullable=False),
    )


def _spec(effective_from: str = "2024-01", **overrides: object) -> Spec:
    base: dict[str, object] = {
        "trade_type": "imports",
        "version": effective_from,
        "effective_from": effective_from,
        "record_length": 31,
        "columns": _cols(),
    }
    base.update(overrides)
    return Spec(**base)  # type: ignore[arg-type]


def test_validate_spec_accepts_valid() -> None:
    validate_spec(_spec())


def test_validate_spec_rejects_overlap() -> None:
    cols = (
        Column(physical_name="a", start=1, length=5, dtype="Utf8"),
        Column(physical_name="b", start=4, length=5, dtype="Utf8"),
    )
    with pytest.raises(SpecError, match="overlaps"):
        validate_spec(_spec(columns=cols, record_length=10))


def test_validate_spec_rejects_duplicate_names() -> None:
    cols = (
        Column(physical_name="a", start=1, length=5, dtype="Utf8"),
        Column(physical_name="a", start=6, length=5, dtype="Utf8"),
    )
    with pytest.raises(SpecError, match="duplicate column"):
        validate_spec(_spec(columns=cols, record_length=10))


def test_validate_spec_rejects_bad_dtype() -> None:
    cols = (Column(physical_name="a", start=1, length=5, dtype="Decimal"),)
    with pytest.raises(SpecError, match="non-canonical dtype"):
        validate_spec(_spec(columns=cols, record_length=5))


def test_validate_spec_rejects_short_record_length() -> None:
    with pytest.raises(SpecError, match="shorter than rightmost"):
        validate_spec(_spec(record_length=10))


def test_validate_period_rejects_garbage() -> None:
    with pytest.raises(SpecError):
        validate_period("2024/01")


def test_yaml_roundtrip(tmp_path: Path) -> None:
    spec = _spec()
    path = tmp_path / "v2024-01.yaml"
    save_spec(spec, path)
    loaded = load_spec(path)
    assert loaded == spec


def test_yaml_roundtrip_with_coerce_invalid_to_null(tmp_path: Path) -> None:
    """The new flag round-trips through YAML so a spec written with
    coerce_invalid_to_null=True survives load_spec round-tripping."""
    cols = (
        Column(physical_name="period", start=1, length=6, dtype="Date", parse="yyyymm_to_date"),
        Column(physical_name="hs_code", start=7, length=10, dtype="Utf8", nullable=False),
        Column(physical_name="value_usd", start=17, length=15, dtype="Int64", nullable=False),
        Column(
            physical_name="entry_date",
            start=32,
            length=8,
            dtype="Date",
            nullable=True,
            parse="yyyymmdd_to_date",
            coerce_invalid_to_null=True,
        ),
    )
    spec = _spec(columns=cols, record_length=39)
    path = tmp_path / "v2024-01.yaml"
    save_spec(spec, path)
    loaded = load_spec(path)
    assert loaded == spec
    assert loaded.columns[3].coerce_invalid_to_null is True
    # Other columns default to False — emitted only when True.
    assert loaded.columns[0].coerce_invalid_to_null is False


def test_yaml_roundtrip_with_encoding(tmp_path: Path) -> None:
    """`encoding` is optional, emitted only when set, and round-trips through YAML."""
    spec_no_enc = _spec()
    path = tmp_path / "v2024-01.yaml"
    save_spec(spec_no_enc, path)
    assert "encoding" not in path.read_text()
    assert load_spec(path).encoding is None

    spec_enc = _spec(encoding="cp850")
    save_spec(spec_enc, path)
    assert load_spec(path).encoding == "cp850"


def test_yaml_roundtrip_with_encoding_overrides(tmp_path: Path) -> None:
    """encoding_overrides round-trip; emitted only when non-empty; key absent when empty."""
    spec = _spec(
        effective_from="2010-01",
        effective_to="2020-12",
        encoding="cp1252",
        encoding_overrides=(
            EncodingOverride(effective_from="2010-01", effective_to="2013-12", encoding="cp850"),
        ),
    )
    path = tmp_path / "v2010-01.yaml"
    save_spec(spec, path)
    text = path.read_text()
    assert "encoding_overrides:" in text
    loaded = load_spec(path)
    assert loaded == spec

    # No overrides → key omitted entirely.
    bare = _spec(effective_from="2010-01", effective_to="2020-12")
    save_spec(bare, path)
    assert "encoding_overrides" not in path.read_text()
    assert load_spec(path).encoding_overrides == ()


def test_encoding_for_picks_override_window_then_spec_default() -> None:
    """encoding_for(): override hit beats spec.encoding; out-of-range falls back."""
    spec = _spec(
        effective_from="2010-01",
        effective_to="2020-12",
        encoding="cp1252",
        encoding_overrides=(
            EncodingOverride(effective_from="2010-01", effective_to="2013-12", encoding="cp850"),
            EncodingOverride(effective_from="2018-06", effective_to=None, encoding="utf-8"),
        ),
    )
    assert spec.encoding_for("2010-01") == "cp850"
    assert spec.encoding_for("2013-12") == "cp850"
    assert spec.encoding_for("2014-01") == "cp1252"  # gap → spec default
    assert spec.encoding_for("2018-05") == "cp1252"
    assert spec.encoding_for("2018-06") == "utf-8"  # open-ended override
    assert spec.encoding_for("2025-01") == "utf-8"


def test_encoding_for_returns_none_without_default_or_match() -> None:
    spec = _spec(
        effective_from="2010-01",
        encoding_overrides=(
            EncodingOverride(effective_from="2010-01", effective_to="2010-12", encoding="cp850"),
        ),
    )
    assert spec.encoding_for("2010-06") == "cp850"
    assert spec.encoding_for("2011-01") is None  # no spec default + no match


def test_validate_spec_rejects_override_outside_spec_window() -> None:
    with pytest.raises(SpecError, match="precedes spec effective_from"):
        validate_spec(
            _spec(
                effective_from="2015-01",
                encoding_overrides=(
                    EncodingOverride(
                        effective_from="2010-01", effective_to="2014-12", encoding="cp850"
                    ),
                ),
            )
        )
    with pytest.raises(SpecError, match="exceeds spec effective_to"):
        validate_spec(
            _spec(
                effective_from="2010-01",
                effective_to="2015-12",
                encoding_overrides=(
                    EncodingOverride(
                        effective_from="2010-01", effective_to="2016-12", encoding="cp850"
                    ),
                ),
            )
        )


def test_validate_spec_rejects_overlapping_overrides() -> None:
    with pytest.raises(SpecError, match="encoding_overrides overlap"):
        validate_spec(
            _spec(
                effective_from="2010-01",
                effective_to="2020-12",
                encoding_overrides=(
                    EncodingOverride(
                        effective_from="2010-01", effective_to="2014-12", encoding="cp850"
                    ),
                    EncodingOverride(
                        effective_from="2014-06", effective_to="2017-12", encoding="utf-8"
                    ),
                ),
            )
        )


def test_validate_spec_rejects_open_ended_override_in_closed_spec() -> None:
    with pytest.raises(SpecError, match="open-ended"):
        validate_spec(
            _spec(
                effective_from="2010-01",
                effective_to="2015-12",
                encoding_overrides=(
                    EncodingOverride(effective_from="2010-01", effective_to=None, encoding="cp850"),
                ),
            )
        )


def test_yaml_roundtrip_with_source_and_derived(tmp_path: Path) -> None:
    spec = _spec(
        source=SpecSource(
            workbook="wb.xlsx", sha256="abc", sheet="imports", imported_at="2026-01-01T00:00:00"
        ),
        derived=(("year", "year(period)"), ("month", "month(period)")),
    )
    path = tmp_path / "v2024-01.yaml"
    save_spec(spec, path)
    assert load_spec(path) == spec


def test_resolve_picks_latest_applicable() -> None:
    a = _spec(effective_from="2020-01")
    b = _spec(effective_from="2023-07")
    c = _spec(effective_from="2025-01")
    assert resolve([a, b, c], "2024-06") == b
    assert resolve([a, b, c], "2020-01") == a
    assert resolve([a, b, c], "2025-01") == c
    assert resolve([a, b, c], "2030-12") == c


def test_resolve_errors_before_any_spec() -> None:
    a = _spec(effective_from="2024-01")
    with pytest.raises(SpecError, match="no spec applies"):
        resolve([a], "2023-12")


def test_load_all_returns_empty_when_missing(tmp_path: Path) -> None:
    assert load_all(tmp_path, "imports") == []


def test_load_all_sorts_by_effective_from(tmp_path: Path) -> None:
    spec_dir = tmp_path / "imports"
    save_spec(_spec(effective_from="2023-07"), spec_dir / "v2023-07.yaml")
    save_spec(_spec(effective_from="2020-01"), spec_dir / "v2020-01.yaml")
    save_spec(_spec(effective_from="2025-01"), spec_dir / "v2025-01.yaml")

    specs = load_all(tmp_path, "imports")
    assert [s.effective_from for s in specs] == ["2020-01", "2023-07", "2025-01"]


def test_diff_specs_detects_added_removed_changed() -> None:
    prev = _spec()
    new_cols = (
        Column(
            physical_name="period",
            start=1,
            length=6,
            dtype="Date",
            nullable=False,
            parse="yyyymm_to_date",
        ),
        Column(physical_name="hs_code", start=7, length=10, dtype="Utf8", nullable=False),
        Column(
            physical_name="value_usd", start=17, length=15, dtype="Float64", nullable=False
        ),  # dtype change
        Column(physical_name="qty_kg", start=32, length=10, dtype="Int64"),  # added
    )
    curr = _spec(effective_from="2025-01", columns=new_cols, record_length=41)
    diff = diff_specs(prev, curr)
    assert {c.effective_name for c in diff.added} == {"qty_kg"}
    assert diff.removed == ()
    assert {old.effective_name for old, _ in diff.changed} == {"value_usd"}
    assert not diff.is_empty


def test_diff_specs_empty_when_identical() -> None:
    assert diff_specs(_spec(), _spec()).is_empty


def test_canonical_columns_unions_across_versions() -> None:
    cols_v1 = (
        Column(physical_name="a", start=1, length=5, dtype="Utf8", nullable=False),
        Column(physical_name="b", start=6, length=5, dtype="Int64", nullable=False),
    )
    cols_v2 = (
        Column(physical_name="a", start=1, length=5, dtype="Utf8", nullable=True),  # widened
        Column(physical_name="b", start=6, length=5, dtype="Int64", nullable=False),
        Column(physical_name="c", start=11, length=5, dtype="Float64", nullable=True),  # added
    )
    v1 = _spec(effective_from="2020-01", columns=cols_v1, record_length=10)
    v2 = _spec(effective_from="2024-01", columns=cols_v2, record_length=15)

    canonical = canonical_columns([v1, v2])
    assert [c.name for c in canonical] == ["a", "b", "c"]
    by_name = {c.name: c for c in canonical}
    assert by_name["a"].nullable is True  # widened once seen nullable
    assert by_name["c"].dtype == "Float64"


def test_canonical_columns_rejects_dtype_conflict() -> None:
    cols_v1 = (Column(physical_name="a", start=1, length=5, dtype="Utf8"),)
    cols_v2 = (Column(physical_name="a", start=1, length=5, dtype="Int64"),)
    v1 = _spec(effective_from="2020-01", columns=cols_v1, record_length=5)
    v2 = _spec(effective_from="2024-01", columns=cols_v2, record_length=5)
    with pytest.raises(SpecError, match="changes dtype"):
        canonical_columns([v1, v2])
