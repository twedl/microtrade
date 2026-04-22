"""Typer CLI entry point.

Commands:
    version        - print the installed microtrade version
    ingest         - process zipped FWF inputs into Hive-partitioned Parquet
    import-spec    - convert an Excel schema workbook into versioned YAML specs
    validate-specs - lint YAML specs and print a changelog across versions
    inspect        - dump the resolved spec and first rows of a raw file
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import IO

import typer

from microtrade import __version__, config, discover, excel_spec, pipeline, schema
from microtrade.ingest import DEFAULT_CHUNK_ROWS

app = typer.Typer(
    name="microtrade",
    help="Turn zipped FWF trade microdata into Hive-partitioned Parquet.",
    no_args_is_help=True,
)

DEFAULT_SPEC_DIR = Path("specs")


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
    encoding: str = typer.Option(
        "cp1252",
        "--encoding",
        help="Text encoding of the inner FWF. Defaults to cp1252 (Windows-1252), "
        "the codec used by most government trade drops; pass --encoding utf-8 "
        "if your upstream ships UTF-8.",
    ),
    max_quality_issues: int = typer.Option(
        pipeline.DEFAULT_MAX_QUALITY_ISSUES,
        "--max-quality-issues",
        help="Abort the ingest if row-level quality issues exceed this cap "
        "(0 = never abort). The JSONL log is also capped at this value so it "
        "can't balloon past the cap.",
    ),
    max_skip_rate: float = typer.Option(
        pipeline.DEFAULT_MAX_SKIP_RATE,
        "--max-skip-rate",
        min=0.0,
        max=1.0,
        help="Abort a partition if this fraction of rows fails to parse (1.0 = never abort).",
    ),
    progress: bool = typer.Option(
        True,
        "--progress/--no-progress",
        help="Show a tqdm progress bar over the partitions.",
    ),
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
        max_quality_issues=max_quality_issues,
        max_skip_rate=max_skip_rate,
        show_progress=progress,
    )

    summary = pipeline.run(config)
    _print_summary(summary)
    if summary.failed_count > 0:
        raise typer.Exit(code=1)


@app.command("import-spec")
def import_spec(
    workbooks: list[Path] = typer.Argument(..., exists=True, dir_okay=False),
    config_path: Path = typer.Option(
        config.DEFAULT_CONFIG_PATH,
        "--config",
        help="Path to the project config (YAML) listing these workbooks.",
    ),
    out: Path = typer.Option(
        DEFAULT_SPEC_DIR, "--out", help="Directory to write per-trade-type YAML specs into."
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing YAML at the target path."
    ),
) -> None:
    """Convert one or more schema Excel workbooks into versioned YAML specs.

    Accepts a list of workbook paths so shell globs work naturally
    (`microtrade import-spec raw/*.xls`). Each workbook must have a
    matching entry in the project config (default `microtrade.yaml`)
    that supplies its `effective_from` / `effective_to` window,
    `workbook_id`, and per-sheet `filename_pattern`. Per-workbook
    failures are reported at the end; the command exits 1 if any
    workbook failed to import.
    """
    try:
        project_config = config.load_config(config_path)
    except config.ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc

    failures: list[tuple[Path, str]] = []
    for workbook in workbooks:
        try:
            _import_one_workbook(workbook, project_config, out=out, force=force)
        except (config.ConfigError, schema.SpecError) as exc:
            typer.echo(f"{workbook.name}: {exc}", err=True)
            failures.append((workbook, f"{type(exc).__name__}: {exc}"))

    if failures:
        typer.echo("", err=True)
        typer.echo(f"FAIL: {len(failures)} of {len(workbooks)} workbook(s) failed", err=True)
        raise typer.Exit(code=1)


def _import_one_workbook(
    workbook: Path, project_config: config.ProjectConfig, *, out: Path, force: bool
) -> None:
    workbook_config = project_config.get_workbook(workbook)
    specs = excel_spec.read_workbook(workbook, workbook_config)
    effective_from = workbook_config.effective_from
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
    """Lint YAML specs and print a per-trade-type changelog across versions.

    Exits 0 when every spec loads cleanly, filenames match `effective_from`,
    and `canonical_columns` succeeds per trade type. Exits 1 on any problem
    (a pointer to the offending file is printed to stderr).
    """
    problems: list[str] = []
    any_specs = False
    trade_types_scanned = 0
    total_specs = 0

    for trade_type in schema.TRADE_TYPES:
        trade_dir = spec_dir / trade_type
        yaml_files = sorted(trade_dir.glob("v*.yaml")) if trade_dir.is_dir() else []
        if not yaml_files:
            typer.echo(f"{trade_type}: no specs")
            continue
        any_specs = True

        specs: list[schema.Spec] = []
        for yaml_path in yaml_files:
            expected_version = yaml_path.stem.removeprefix("v")
            try:
                spec = schema.load_spec(yaml_path)
            except schema.SpecError as exc:
                problems.append(f"{yaml_path}: {exc}")
                continue
            if spec.effective_from != expected_version:
                problems.append(
                    f"{yaml_path}: filename version {expected_version!r} does not match "
                    f"effective_from {spec.effective_from!r}"
                )
                continue
            specs.append(spec)

        if not specs:
            continue
        trade_types_scanned += 1
        total_specs += len(specs)

        specs.sort(key=lambda s: s.effective_from)
        typer.echo(f"{trade_type}:")
        for i, spec in enumerate(specs):
            window = (
                f"{spec.effective_from}..{spec.effective_to}"
                if spec.effective_to is not None
                else f"{spec.effective_from}..(open)"
            )
            typer.echo(
                f"  v{spec.effective_from}  "
                f"({len(spec.columns)} columns, record_length={spec.record_length}, "
                f"window={window})"
            )
            if i > 0:
                _print_diff(specs[i - 1], schema.diff_specs(specs[i - 1], spec))

        problems.extend(schema.window_problems(trade_type, specs))

        try:
            schema.canonical_columns(specs)
        except schema.SpecError as exc:
            problems.append(f"{trade_type}: canonical-schema conflict: {exc}")

    if not any_specs:
        typer.echo(f"no specs found under {spec_dir}", err=True)
        raise typer.Exit(code=1)

    if problems:
        typer.echo("", err=True)
        typer.echo(f"FAIL: {len(problems)} problem(s)", err=True)
        for p in problems:
            typer.echo(f"  - {p}", err=True)
        raise typer.Exit(code=1)

    tt_label = "trade type" if trade_types_scanned == 1 else "trade types"
    spec_label = "spec" if total_specs == 1 else "specs"
    typer.echo("")
    typer.echo(f"OK ({trade_types_scanned} {tt_label}, {total_specs} {spec_label})")


@app.command()
def inspect(
    path: Path = typer.Argument(..., exists=True, dir_okay=False),
    spec_dir: Path = typer.Option(
        DEFAULT_SPEC_DIR,
        "--spec-dir",
        exists=True,
        file_okay=False,
        help="Directory containing <trade_type>/v<effective_from>.yaml specs.",
    ),
    trade_type: str | None = typer.Option(
        None,
        "--type",
        help="Trade type override; defaults to parse from filename. "
        "Required if the filename is not `<SHEET>_<YYYYMM><N|C>.TXT.zip`.",
    ),
    period: str | None = typer.Option(
        None,
        "--period",
        help="Period (YYYY-MM) override; defaults to parse from filename. "
        "Required if the filename is not `<SHEET>_<YYYYMM><N|C>.TXT.zip`.",
    ),
    rows: int = typer.Option(5, "--rows", "-n", help="Number of data rows to show (0 = none)."),
    raw: bool = typer.Option(
        False, "--raw", help="Print full lines without per-column annotation."
    ),
    encoding: str = typer.Option(
        "cp1252",
        "--encoding",
        help="Text encoding of the inner FWF. Defaults to cp1252 (Windows-1252); "
        "pass --encoding utf-8 for UTF-8 data.",
    ),
) -> None:
    """Dump the resolved spec and first rows of a raw trade file.

    Accepts either a `<trade_type>_<YYYYMM>.zip` (filename drives spec
    resolution) or a plain FWF file (pass `--type` and `--period`).
    """
    resolved_type, resolved_period = _resolve_inspect_target(path, trade_type, period, spec_dir)

    specs = schema.load_all(spec_dir, resolved_type)
    if not specs:
        typer.echo(f"no specs found for trade_type {resolved_type!r} under {spec_dir}", err=True)
        raise typer.Exit(code=2)
    try:
        spec = schema.resolve(specs, resolved_period)
    except schema.SpecError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"file:   {path.name}  ({resolved_type} {resolved_period})")
    typer.echo(
        f"spec:   v{spec.version}  record_length={spec.record_length}  columns={len(spec.columns)}"
    )
    if rows <= 0:
        return
    typer.echo("")
    for line_no, line in _iter_inspect_lines(path, encoding=encoding, limit=rows):
        _print_inspect_row(line, spec, line_no, annotated=not raw)


def _resolve_inspect_target(
    path: Path, trade_type: str | None, period: str | None, spec_dir: Path
) -> tuple[str, str]:
    patterns = discover.load_patterns(spec_dir)
    parsed = discover.parse_filename(path, patterns)
    resolved_type = trade_type or (parsed.trade_type if parsed is not None else None)
    resolved_period = period or (parsed.period if parsed is not None else None)
    if resolved_type is None or resolved_period is None:
        typer.echo(
            f"{path.name}: filename does not match either supported pattern "
            f"(or its sheet/workbook_id has no spec); pass --type and --period "
            f"to inspect anyway.",
            err=True,
        )
        raise typer.Exit(code=2)
    if resolved_type not in schema.TRADE_TYPES:
        typer.echo(f"unknown trade_type {resolved_type!r}", err=True)
        raise typer.Exit(code=2)
    try:
        schema.validate_period(resolved_period)
    except schema.SpecError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    return resolved_type, resolved_period


def _iter_inspect_lines(path: Path, *, encoding: str, limit: int) -> Iterator[tuple[int, str]]:
    # Content-sniff rather than trusting the suffix so `.fwf` zips and
    # extension-less files both route correctly.
    is_zip = zipfile.is_zipfile(path)
    try:
        if is_zip:
            with zipfile.ZipFile(path) as zf:
                members = [m for m in zf.infolist() if not m.is_dir()]
                if len(members) != 1:
                    typer.echo(
                        f"{path.name}: expected exactly one inner file, found "
                        f"{[m.filename for m in members]}",
                        err=True,
                    )
                    raise typer.Exit(code=2)
                with zf.open(members[0]) as binstream:
                    yield from _first_lines(binstream, encoding=encoding, limit=limit)
            return
        with path.open("rb") as binstream:
            yield from _first_lines(binstream, encoding=encoding, limit=limit)
    except zipfile.BadZipFile as exc:
        typer.echo(f"{path.name}: malformed zip: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    except UnicodeDecodeError as exc:
        typer.echo(
            f"{path.name}: cannot decode as {encoding} at byte {exc.start}: {exc.reason}. "
            f"Pass --encoding to try a different codec (e.g. --encoding latin-1 or "
            f"--encoding utf-8).",
            err=True,
        )
        raise typer.Exit(code=2) from exc


def _first_lines(binstream: IO[bytes], *, encoding: str, limit: int) -> Iterator[tuple[int, str]]:
    text = io.TextIOWrapper(binstream, encoding=encoding, newline="")
    for line_no, raw_line in enumerate(text, start=1):
        if line_no > limit:
            return
        yield line_no, raw_line.rstrip("\n").rstrip("\r")


def _print_inspect_row(line: str, spec: schema.Spec, line_no: int, *, annotated: bool) -> None:
    typer.echo(f"--- line {line_no} (length={len(line)}) ---")
    if not annotated:
        typer.echo(line)
        return
    for col in spec.ordered_columns:
        chunk = line[col.start - 1 : col.start - 1 + col.length]
        label = (
            col.physical_name
            if col.logical_name is None
            else f"{col.physical_name} -> {col.logical_name}"
        )
        typer.echo(f"  {label:<36} [{col.start:>4}..{col.end:>4}] {col.dtype:<7} {chunk!r}")


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
        typer.echo(
            f"    + {col.effective_name} ({col.dtype}, start={col.start}, length={col.length})"
        )
    for col in diff.removed:
        typer.echo(f"    - {col.effective_name} ({col.dtype})")
    for old, new in diff.changed:
        label = (
            f"{old.effective_name} (physical {old.physical_name} -> {new.physical_name})"
            if old.physical_name != new.physical_name
            else old.effective_name
        )
        typer.echo(f"    ~ {label}: {old.dtype}/{old.length} -> {new.dtype}/{new.length}")


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
