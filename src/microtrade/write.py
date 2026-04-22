"""Streaming, crash-safe, idempotent Parquet writer for one Hive partition.

`PartitionWriter` opens a `pyarrow.parquet.ParquetWriter` against
`year=YYYY/month=MM/part-0.parquet.tmp`, accepts one
`pyarrow.RecordBatch` at a time (each written as a Parquet row group), and on a
clean close atomically renames the temp file to `part-0.parquet`, replacing any
prior file at that path. On exception the temp file is removed so a failed run
leaves nothing behind.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Self

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


class WriteError(RuntimeError):
    """Raised when a partition write fails in a way the caller should surface."""


class PartitionWriter:
    """Write one `(trade_type, year, month)` Parquet partition atomically."""

    def __init__(
        self,
        dataset_root: Path,
        trade_type: str,
        year: int,
        month: int,
        arrow_schema: pa.Schema,
        *,
        compression: str = "zstd",
        part: int = 0,
    ) -> None:
        self.dataset_root = dataset_root
        self.trade_type = trade_type
        self.year = year
        self.month = month
        self.arrow_schema = arrow_schema
        self.compression = compression
        self.part = part

        self.partition_dir = dataset_root / trade_type / f"year={year:04d}" / f"month={month:02d}"
        self.final_path = self.partition_dir / f"part-{part}.parquet"
        self.tmp_path = self.partition_dir / f"part-{part}.parquet.tmp"

        self._writer: pq.ParquetWriter | None = None
        self._rows_written: int = 0

    @property
    def rows_written(self) -> int:
        return self._rows_written

    def __enter__(self) -> Self:
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_path.unlink(missing_ok=True)
        self._writer = pq.ParquetWriter(
            self.tmp_path, self.arrow_schema, compression=self.compression
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        writer = self._writer
        self._writer = None
        try:
            if writer is not None:
                writer.close()
        except Exception:
            self.tmp_path.unlink(missing_ok=True)
            raise

        if exc_type is not None:
            self.tmp_path.unlink(missing_ok=True)
            return

        self.tmp_path.replace(self.final_path)

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if self._writer is None:
            raise WriteError(
                f"PartitionWriter for {self.partition_dir} not entered; use `with` block"
            )
        if batch.schema != self.arrow_schema:
            raise WriteError(
                f"batch schema does not match partition schema for {self.partition_dir}"
            )
        if batch.num_rows == 0:
            return
        self._writer.write_batch(batch)
        self._rows_written += batch.num_rows


class MultiPartitionWriter:
    """Route rows to per-`(year, month)` Parquet partitions based on the
    `period` column of each incoming batch.

    A single YTD-snapshot input file typically spans multiple months; each
    row's `period` Date column picks the destination partition. Child
    PartitionWriters are opened lazily on first row for each partition,
    and all commit (atomic rename) together on successful exit or roll
    back (delete .tmp) together on exception.
    """

    def __init__(
        self,
        *,
        dataset_root: Path,
        trade_type: str,
        arrow_schema: pa.Schema,
        compression: str = "zstd",
    ) -> None:
        self.dataset_root = dataset_root
        self.trade_type = trade_type
        self.arrow_schema = arrow_schema
        self.compression = compression
        self._writers: dict[tuple[int, int], PartitionWriter] = {}
        self._active = False
        if "period" not in arrow_schema.names:
            raise WriteError(
                "MultiPartitionWriter requires a 'period' column in the arrow schema "
                "(used to route rows to per-month partitions)"
            )

    def __enter__(self) -> Self:
        self._active = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._active = False
        first_error: BaseException | None = None
        for writer in self._writers.values():
            try:
                writer.__exit__(exc_type, exc_val, exc_tb)
            except Exception as close_err:
                if first_error is None:
                    first_error = close_err
        if first_error is not None and exc_type is None:
            raise first_error

    @property
    def partition_rows(self) -> dict[tuple[int, int], int]:
        """Rows written per (year, month) partition."""
        return {key: w.rows_written for key, w in self._writers.items()}

    @property
    def rows_written(self) -> int:
        return sum(w.rows_written for w in self._writers.values())

    @property
    def final_paths(self) -> dict[tuple[int, int], Path]:
        return {key: w.final_path for key, w in self._writers.items()}

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if not self._active:
            raise WriteError("MultiPartitionWriter not entered; use `with` block")
        if batch.num_rows == 0:
            return

        period = batch.column("period")
        years = pc.year(period).to_pylist()
        months = pc.month(period).to_pylist()
        groups: dict[tuple[int, int], list[int]] = {}
        for i, (y, m) in enumerate(zip(years, months, strict=True)):
            if y is None or m is None:
                continue
            groups.setdefault((int(y), int(m)), []).append(i)

        for key, indices in groups.items():
            sub = batch.take(pa.array(indices))
            self._writer_for(key).write_batch(sub)

    def _writer_for(self, key: tuple[int, int]) -> PartitionWriter:
        existing = self._writers.get(key)
        if existing is not None:
            return existing
        year, month = key
        writer = PartitionWriter(
            dataset_root=self.dataset_root,
            trade_type=self.trade_type,
            year=year,
            month=month,
            arrow_schema=self.arrow_schema,
            compression=self.compression,
        )
        writer.__enter__()
        self._writers[key] = writer
        return writer
