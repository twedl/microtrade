"""Typer CLI entry point.

Phase 1 wires the CLI surface with stubs. Real implementations land in later
phases (see the project plan: ingest, import-spec, validate-specs, inspect).
"""

from __future__ import annotations

from pathlib import Path

import typer

from microtrade import __version__

app = typer.Typer(
    name="microtrade",
    help="Turn zipped FWF trade microdata into Hive-partitioned Parquet.",
    no_args_is_help=True,
)


@app.command()
def version() -> None:
    """Print the installed microtrade version."""
    typer.echo(__version__)


@app.command()
def ingest(
    input_dir: Path = typer.Option(..., "--input", exists=True, file_okay=False),
    output_dir: Path = typer.Option(..., "--output", file_okay=False),
    ytd: bool = typer.Option(True, "--ytd/--all"),
) -> None:
    """Process zipped FWF inputs into Hive-partitioned Parquet (stub)."""
    _not_implemented("ingest", input_dir=input_dir, output_dir=output_dir, ytd=ytd)


@app.command("import-spec")
def import_spec(
    workbook: Path = typer.Argument(..., exists=True, dir_okay=False),
    effective_from: str = typer.Option(..., "--effective-from"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    """Convert a schema Excel workbook into versioned YAML specs (stub)."""
    _not_implemented(
        "import-spec", workbook=workbook, effective_from=effective_from, force=force
    )


@app.command("validate-specs")
def validate_specs(
    spec_dir: Path = typer.Option(None, "--spec-dir", exists=True, file_okay=False),
) -> None:
    """Lint YAML specs and print a changelog across versions (stub)."""
    _not_implemented("validate-specs", spec_dir=spec_dir)


@app.command()
def inspect(path: Path = typer.Argument(..., exists=True, dir_okay=False)) -> None:
    """Dump first rows and resolved spec for a raw zip (stub)."""
    _not_implemented("inspect", path=path)


def _not_implemented(name: str, **kwargs: object) -> None:
    typer.echo(f"'{name}' is not implemented yet. Args: {kwargs}", err=True)
    raise typer.Exit(code=2)
