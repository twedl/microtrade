"""Shared pytest fixtures.

Everything here is thin glue over `tests/_helpers.py` so individual test modules
can drive their own variations if they need to.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._helpers import build_workbook


@pytest.fixture
def schema_workbook(tmp_path: Path) -> Path:
    """A freshly built synthetic schema workbook (one sheet per trade type)."""
    return build_workbook(tmp_path / "schema_workbook.xlsx")
