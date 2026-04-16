"""Smoke tests verifying the package imports and the CLI entry point resolves."""

from __future__ import annotations

from typer.testing import CliRunner

import microtrade
from microtrade.cli import app


def test_package_exposes_version() -> None:
    assert isinstance(microtrade.__version__, str)
    assert microtrade.__version__


def test_cli_version_command() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert microtrade.__version__ in result.stdout
