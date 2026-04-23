from pathlib import Path

from microtrade.ops.hashing import hash_file


def test_hash_known_content(tmp_path: Path) -> None:
    p = tmp_path / "f.txt"
    p.write_bytes(b"hello")
    assert (
        hash_file(p)
        == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )


def test_hash_streams_larger_than_chunk(tmp_path: Path) -> None:
    p = tmp_path / "big.bin"
    p.write_bytes(b"x" * (2 * (1 << 20) + 5))
    h = hash_file(p)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
