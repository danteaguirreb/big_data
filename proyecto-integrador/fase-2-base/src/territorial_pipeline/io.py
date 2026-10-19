"""Utilidades de escritura y evidencia."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def safe_write_parquet(
    df: DataFrame,
    path: Path,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
    target_partitions: int | tuple[int, list[str]] | None = None,
    max_records_per_file: int = 250_000,
) -> None:
    """Write Parquet through a temporary directory, then atomically promote it.

    This avoids leaving half-written output in Colab/local filesystem failures.
    Massive fact tables should use repartition, never coalesce(1).
    """

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if target_partitions:
        if isinstance(target_partitions, tuple):
            n_partitions, columns = target_partitions
            df = df.repartition(n_partitions, *columns)
        else:
            df = df.repartition(target_partitions)

    tmp_path = Path(str(path) + "_tmp")
    if tmp_path.exists():
        shutil.rmtree(tmp_path, ignore_errors=True)
    if path.exists() and mode == "overwrite":
        shutil.rmtree(path, ignore_errors=True)

    writer = (
        df.write.mode(mode)
        .option("compression", "snappy")
        .option("maxRecordsPerFile", str(max_records_per_file))
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(str(tmp_path))

    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    try:
        os.rename(tmp_path, path)
    except OSError:
        shutil.move(str(tmp_path), str(path))


def write_parquet(
    df: DataFrame,
    path: Path,
    partition_by: list[str] | None = None,
    target_partitions: int | tuple[int, list[str]] | None = None,
) -> None:
    safe_write_parquet(df, path, partition_cols=partition_by, target_partitions=target_partitions)


def write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv(df: DataFrame, path: Path) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(path))
