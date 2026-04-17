"""Typer CLI entry point.

Commands:
    version        - print the installed microtrade version
    ingest         - process zipped FWF inputs into Hive-partitioned Parquet
    import-spec    - convert an Excel schema workbook into versioned YAML specs
    validate-specs - lint YAML specs and print a changelog across versions (stub)
    inspect        - dump first rows and resolved spec for one zip (stub)
"""

from __future__ import annotations

from pathlib import Path

import typer

from microtrade import __version__, excel_spec, pipeline, schema
from microtrade.ingest import DEFAULT_CHUNK_ROWS

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
    input_dir: Path = typer.Option(
        ..., "--input", exists=True, file_okay=False, help="Directory of raw zips."
    ),
    output_dir: Path = typer.Option(
        ..., "--output", file_okay=False, help="Root of the per-trade-type Parquet datasets."
    ),
    spec_dir: Path = typer.Option(
        DEFAULT_SPEC_DIR,
        "--spec-dir",
        exists=True,
        file_okay=False,
        help="Directory containing <trade_type>/v<effective_from>.yaml specs.",
    ),
    trade_types: list[str] = typer.Option(
        [],
        "--type",
        help="Limit to these trade types (may be repeated). Default: all three.",
    ),
    year: int | None = typer.Option(None, "--year", help="Process only this calendar year."),
    month: int | None = typer.Option(None, "--month", help="Process only this month (1-12)."),
    ytd: bool = typer.Option(
        True,
        "--ytd/--all",
        help="YTD (current year only) vs. all years in the input dir. Ignored when --year is set.",
    ),
    current_year: int | None = typer.Option(
        None,
        "--current-year",
        help="Override 'today' year for YTD selection (primarily for testing).",
        hidden=True,
    ),
    chunk_rows: int = typer.Option(
        DEFAULT_CHUNK_ROWS,
        "--chunk-rows",
        help="Rows per streaming RecordBatch / Parquet row group.",
    ),
    compression: str = typer.Option("zstd", "--compression"),
    encoding: str = typer.Option("utf-8", "--encoding"),
) -> None:
    """Process zipped FWF inputs into per-type Hive-partitioned Parquet datasets."""
    wanted_types = tuple(trade_types) if trade_types else schema.TRADE_TYPES
    unknown = [t for t in wanted_types if t not in schema.TRADE_TYPES]
    if unknown:
        typer.echo(f"unknown trade types: {unknown}", err=True)
        raise typer.Exit(code=2)

    config = pipeline.PipelineConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        trade_types=wanted_types,
        ytd=ytd,
        current_year=current_year,
        year=year,
        month=month,
        chunk_rows=chunk_rows,
        compression=compression,
        encoding=encoding,
    )

    summary = pipeline.run(config)
    _print_summary(summary)
    if summary.failed_count > 0:
        raise typer.Exit(code=1)


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
            _print_diff(previous, diff)


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


def _print_diff(previous: schema.Spec, diff: schema.SpecDiff) -> None:
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


def _print_summary(summary: pipeline.RunSummary) -> None:
    typer.echo(
        f"run {summary.run_id}: {summary.ok_count} ok, {summary.failed_count} failed, "
        f"{summary.total_rows} rows"
    )
    for r in summary.results:
        status = r.status.upper()
        typer.echo(
            f"  [{status}] {r.trade_type} {r.year:04d}-{r.month:02d} "
            f"rows={r.rows_written} ({r.duration_seconds:.2f}s) -> {r.output_path}"
            + (f"  error={r.error}" if r.error else "")
        )


def _not_implemented(name: str, **kwargs: object) -> None:
    typer.echo(f"'{name}' is not implemented yet. Args: {kwargs}", err=True)
    raise typer.Exit(code=2)
