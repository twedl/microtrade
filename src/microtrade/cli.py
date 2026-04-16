"""Typer CLI entry point.

Phase 2 wires `import-spec`; `ingest`, `validate-specs`, and `inspect` remain
stubs until later phases.
"""

from __future__ import annotations

from pathlib import Path

import typer

from microtrade import __version__, excel_spec, schema

app = typer.Typer(
    name="microtrade",
    help="Turn zipped FWF trade microdata into Hive-partitioned Parquet.",
    no_args_is_help=True,
)

DEFAULT_SPEC_DIR = Path(__file__).parent / "specs"


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
    effective_from: str = typer.Option(
        ..., "--effective-from", help="YYYY-MM period when this spec becomes active."
    ),
    out: Path = typer.Option(
        DEFAULT_SPEC_DIR, "--out", help="Directory to write per-trade-type YAML specs into."
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing YAML at the target path."
    ),
) -> None:
    """Convert a schema Excel workbook into versioned YAML specs."""
    specs = excel_spec.read_workbook(workbook, effective_from)
    for trade_type, spec in specs.items():
        target = out / trade_type / f"v{effective_from}.yaml"
        if target.exists() and not force:
            typer.echo(f"{target} already exists; pass --force to overwrite.", err=True)
            raise typer.Exit(code=1)

        previous = _latest_previous(out, trade_type, effective_from)
        schema.save_spec(spec, target)
        typer.echo(f"wrote {target} ({len(spec.columns)} columns)")
        if previous is not None:
            diff = schema.diff_specs(previous, spec)
            _print_diff(previous, spec, diff)


@app.command("validate-specs")
def validate_specs(
    spec_dir: Path = typer.Option(DEFAULT_SPEC_DIR, "--spec-dir", exists=True, file_okay=False),
) -> None:
    """Lint YAML specs and print a changelog across versions (stub)."""
    _not_implemented("validate-specs", spec_dir=spec_dir)


@app.command()
def inspect(path: Path = typer.Argument(..., exists=True, dir_okay=False)) -> None:
    """Dump first rows and resolved spec for a raw zip (stub)."""
    _not_implemented("inspect", path=path)


def _latest_previous(spec_dir: Path, trade_type: str, effective_from: str) -> schema.Spec | None:
    existing = schema.load_all(spec_dir, trade_type)
    earlier = [s for s in existing if s.effective_from < effective_from]
    return max(earlier, key=lambda s: s.effective_from) if earlier else None


def _print_diff(previous: schema.Spec, current: schema.Spec, diff: schema.SpecDiff) -> None:
    if diff.is_empty:
        typer.echo(f"  diff vs v{previous.effective_from}: no column changes")
        return
    typer.echo(f"  diff vs v{previous.effective_from}:")
    for col in diff.added:
        typer.echo(f"    + {col.name} ({col.dtype}, start={col.start}, length={col.length})")
    for col in diff.removed:
        typer.echo(f"    - {col.name} ({col.dtype})")
    for old, new in diff.changed:
        typer.echo(f"    ~ {old.name}: {old.dtype}/{old.length} -> {new.dtype}/{new.length}")


def _not_implemented(name: str, **kwargs: object) -> None:
    typer.echo(f"'{name}' is not implemented yet. Args: {kwargs}", err=True)
    raise typer.Exit(code=2)
