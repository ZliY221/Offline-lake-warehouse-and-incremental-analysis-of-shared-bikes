"""Exact local Parquet partition replacement for controlled backfills."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any


def replace_date_partition(
    dataframe: Any,
    output_path: Path,
    partition_column: str,
    partition_value: date,
) -> None:
    """Replace one date partition while preserving every sibling partition."""
    root = Path(output_path).resolve()
    existing_partition_dirs = (
        [path for path in root.glob(f"{partition_column}=*") if path.is_dir()]
        if root.is_dir()
        else []
    )
    if root.is_dir() and not existing_partition_dirs:
        parquet_files = list(root.glob("*.parquet"))
        if parquet_files:
            if dataframe.sparkSession.read.parquet(str(root)).count() != 0:
                raise ValueError(
                    f"Cannot incrementally replace a non-partitioned dataset: {root}"
                )
            for parquet_file in parquet_files:
                parquet_file.unlink()
            for marker in (root / "_SUCCESS", root / "._SUCCESS.crc"):
                if marker.is_file():
                    marker.unlink()
            for checksum in root.glob(".part-*.crc"):
                checksum.unlink()
    partition_path = root / f"{partition_column}={partition_value.isoformat()}"
    dataframe.drop(partition_column).write.mode("overwrite").parquet(str(partition_path))
