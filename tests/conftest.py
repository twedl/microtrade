"""Shared pytest fixtures.

Everything here is thin glue over `tests/_helpers.py` so individual test modules
can drive their own variations if they need to.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from microtrade.config import WorkbookConfig, load_config
from tests._helpers import build_project_config, build_workbook


@pytest.fixture
def schema_workbook(tmp_path: Path) -> Path:
    """A freshly built synthetic schema workbook (one sheet per trade type)."""
    return build_workbook(tmp_path / "schema_workbook.xlsx")


@pytest.fixture
def project_config_path(tmp_path: Path, schema_workbook: Path) -> Path:
    """A microtrade.yaml sitting next to `schema_workbook` with default patterns."""
    return build_project_config(tmp_path / "microtrade.yaml", schema_workbook, "2020-01")


@pytest.fixture
def workbook_config(project_config_path: Path, schema_workbook: Path) -> WorkbookConfig:
    """Loaded WorkbookConfig for the synthetic workbook."""
    return load_config(project_config_path).get_workbook(schema_workbook)
