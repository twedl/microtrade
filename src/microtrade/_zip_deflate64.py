"""Patch stdlib :mod:`zipfile` to read Deflate64 (compress_type=9).

Some upstream raw zips ship the data file compressed with Deflate64
(method=9), a 7-Zip / WinZip extension to deflate. CPython's stdlib
``zipfile`` only supports stored, deflate, bzip2 and lzma — it raises
``NotImplementedError("That compression method is not supported")``
on method 9.

This module installs the minimum patches needed to decompress
Deflate64 streams using the third-party :mod:`inflate64` package
(which ships cp313 manylinux wheels). Importing this module once is
enough; the patches mutate ``zipfile`` globals.

Tested on Python 3.13. The relevant ``zipfile`` surface — namely
``_check_compression``, ``_get_decompressor``, and
``ZipExtFile._read1`` — has been stable for years, but track CPython
changes here if a future stdlib version refactors the dispatch.

We deliberately do not vendor or bundle a Deflate64 *encoder*;
microtrade only consumes upstream zips, never produces them.
"""

from __future__ import annotations

import zipfile
from binascii import crc32
from typing import Any, cast

import inflate64

ZIP_DEFLATED64 = 9


class _Deflate64Decompressor:
    """Adapter around :class:`inflate64.Inflater` shaped like ``zlib.decompressobj``.

    ``zipfile.ZipExtFile._read1`` expects a decompressor with
    ``decompress(data, max_length)``, ``eof``, ``unused_data``,
    ``unconsumed_tail`` and ``flush()``. ``inflate64.Inflater`` only
    exposes ``inflate(data)`` and ``eof``. We bridge the rest with
    ``b""`` for unused-data / unconsumed-tail (the patched ``_read1``
    branch never relies on incremental tail handling) and a no-op
    ``flush``.
    """

    def __init__(self) -> None:
        self._inflater = inflate64.Inflater()
        self.unused_data = b""
        self.unconsumed_tail = b""

    @property
    def eof(self) -> bool:
        return bool(self._inflater.eof)

    def decompress(self, data: bytes, max_length: int = 0) -> bytes:
        # ``max_length`` is advisory in zipfile's loop (it re-reads if it
        # gets less than asked). ``inflate64.Inflater.inflate`` is
        # incremental and returns whatever it can produce from ``data``.
        return cast(bytes, self._inflater.inflate(data))

    def flush(self) -> bytes:
        return b""


def _patch() -> None:
    """Apply patches to :mod:`zipfile`. Idempotent.

    All access to private :mod:`zipfile` attributes goes through
    ``getattr`` / ``setattr`` so mypy's strict mode doesn't flag the
    intentional internals reach. The contract here *is* the private
    surface — if CPython refactors it, this file is what breaks.
    """
    zf: Any = zipfile
    if getattr(zf, "_microtrade_deflate64_patched", False):
        return

    orig_check = zf._check_compression
    orig_get = zf._get_decompressor
    orig_read1 = zf.ZipExtFile._read1

    def _check(compress_type: int) -> None:
        if compress_type == ZIP_DEFLATED64:
            return
        orig_check(compress_type)

    def _get(compress_type: int) -> Any:
        if compress_type == ZIP_DEFLATED64:
            return _Deflate64Decompressor()
        return orig_get(compress_type)

    def _read1_patched(self: Any, n: int) -> bytes:
        if self._compress_type != ZIP_DEFLATED64:
            return cast(bytes, orig_read1(self, n))
        # Routed branch for compress_type=9. Mirrors the LZMA branch
        # in CPython's zipfile (no unconsumed_tail handling needed —
        # our adapter consumes input chunk-by-chunk with no leftover).
        if self._eof or n <= 0:
            return b""
        data = self._read2(n)
        n = max(n, self.MIN_READ_SIZE)
        decompressor = self._decompressor
        data = decompressor.decompress(data, n)
        self._eof = decompressor.eof or self._compress_left <= 0
        data = data[: self._left]
        self._left -= len(data)
        self._running_crc = crc32(data, self._running_crc)
        return cast(bytes, data)

    zf._check_compression = _check
    zf._get_decompressor = _get
    zf.ZipExtFile._read1 = _read1_patched
    zf.compressor_names[ZIP_DEFLATED64] = "deflate64"
    zf._microtrade_deflate64_patched = True


_patch()
