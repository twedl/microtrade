from __future__ import annotations

import os
from pathlib import Path

from microtrade.ops.transport import sync_tree


def _write(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def test_sync_tree_copies_everything_to_empty_dst(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "a.txt", "hello")
    _write(src / "sub/b.txt", "world")

    sync_tree(src, dst)

    assert (dst / "a.txt").read_text() == "hello"
    assert (dst / "sub/b.txt").read_text() == "world"


def test_sync_tree_missing_src_is_noop(tmp_path: Path) -> None:
    sync_tree(tmp_path / "missing", tmp_path / "dst")
    assert not (tmp_path / "dst").exists()


def test_sync_tree_skips_unchanged_and_overwrites_modified(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "same.txt", "s")
    _write(src / "changed.txt", "v1")
    sync_tree(src, dst)

    # Pin identical mtime on src/dst so we can detect whether the skip fires.
    os.utime(dst / "same.txt", (1_700_000_000, 1_700_000_000))
    os.utime(src / "same.txt", (1_700_000_000, 1_700_000_000))
    (src / "changed.txt").write_text("v2-longer")

    sync_tree(src, dst)

    assert int((dst / "same.txt").stat().st_mtime) == 1_700_000_000
    assert (dst / "changed.txt").read_text() == "v2-longer"


def test_sync_tree_uses_injected_copy_file(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "a.txt", "hello")
    _write(src / "sub/b.txt", "world")

    calls: list[tuple[Path, Path]] = []

    def fake_copy(s: Path, d: Path) -> None:
        calls.append((s, d))
        d.write_text(s.read_text())
        # Preserve mtime so subsequent runs can skip.
        st = s.stat()
        os.utime(d, (st.st_atime, st.st_mtime))

    sync_tree(src, dst, copy_file=fake_copy)

    # Two files copied, each to the final target path. Atomicity is
    # now the copy_file's job — sync_tree no longer forces a .tmp sibling.
    assert len(calls) == 2
    assert not any(d.name.endswith(".tmp") for _, d in calls)
    assert (dst / "a.txt").read_text() == "hello"
    assert (dst / "sub/b.txt").read_text() == "world"


def test_sync_tree_pattern_filters(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(src / "keep.zip", "z")
    _write(src / "drop.txt", "t")
    _write(src / "nested/also.zip", "zz")

    sync_tree(src, dst, patterns=["*.zip"])

    assert (dst / "keep.zip").exists()
    assert (dst / "nested/also.zip").exists()
    assert not (dst / "drop.txt").exists()
