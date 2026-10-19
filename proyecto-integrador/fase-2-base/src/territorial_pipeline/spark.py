"""Creacion de SparkSession local/Colab con parametros documentados."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def create_spark(
    app_name: str,
    shuffle_partitions: int,
    tmp_root: str | Path | None = None,
    warehouse_dir: str | Path | None = None,
    driver_memory: str | None = None,
) -> SparkSession:
    """Build a SparkSession tuned for Colab High-RAM/local execution.

    Driver memory is intentionally parameterized because Colab runtimes can vary.
    For High-RAM use 20g when available; use 12g/16g if session creation fails.
    """

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory or "20g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.driver.maxResultSize", "2g")
    )

    if tmp_root is not None:
        Path(tmp_root).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(tmp_root))
    if warehouse_dir is not None:
        Path(warehouse_dir).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.sql.warehouse.dir", str(warehouse_dir))

    return builder.getOrCreate()
