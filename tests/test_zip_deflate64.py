"""Verify the stdlib ``zipfile`` patch for Deflate64 (compress_type=9).

The patch lives in ``microtrade._zip_deflate64`` and is applied on
import. ``microtrade.ingest`` imports it for side effect, so any
real ingest path goes through the patched dispatch.

The committed fixture ``tests/fixtures/deflate64_sample.zip`` was
produced with ``7z a -tzip -mm=Deflate64`` and contains a single
member ``payload.txt`` that is 5000 ``A`` bytes + newline + 5000
``B`` bytes (10001 bytes uncompressed, ~55 compressed).
"""

from __future__ import annotations

import zipfile
from pathlib import Path

# Side-effect import: applies the patch.
import microtrade._zip_deflate64  # noqa: F401

FIXTURE = Path(__file__).parent / "fixtures" / "deflate64_sample.zip"


def test_fixture_is_actually_deflate64() -> None:
    """Sanity: confirm the fixture really is compress_type=9, so this
    test is exercising the patch and not the stock zlib path."""
    with zipfile.ZipFile(FIXTURE) as zf:
        (member,) = zf.infolist()
        assert member.compress_type == 9


def test_open_member_decompresses_full_payload() -> None:
    with zipfile.ZipFile(FIXTURE) as zf:
        (member,) = zf.infolist()
        with zf.open(member) as f:
            data = f.read()
    assert len(data) == 10001
    assert data.count(b"A") == 5000
    assert data.count(b"B") == 5000
    assert data.startswith(b"A")
    assert data.endswith(b"B")


def test_open_member_chunked_read() -> None:
    """Patched ``_read1`` must drive ``ZipExtFile`` correctly across
    multiple ``read(n)`` calls."""
    with zipfile.ZipFile(FIXTURE) as zf:
        (member,) = zf.infolist()
        with zf.open(member) as f:
            chunks: list[bytes] = []
            while True:
                chunk = f.read(2048)
                if not chunk:
                    break
                chunks.append(chunk)
    data = b"".join(chunks)
    assert len(data) == 10001
    assert data.count(b"A") == 5000
    assert data.count(b"B") == 5000
