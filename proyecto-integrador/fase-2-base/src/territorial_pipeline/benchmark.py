"""Experimento de optimizacion base vs Parquet particionado."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .io import safe_write_parquet


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def benchmark_eventos(spark: SparkSession, eventos: DataFrame, base_path: Path) -> list[dict[str, object]]:
    bench = base_path / "_benchmark"
    unpartitioned = bench / "eventos_unpartitioned"
    partitioned = bench / "eventos_partitioned"
    _reset(unpartitioned)
    _reset(partitioned)

    metrics: list[dict[str, object]] = []

    total_registros = eventos.count()
    selected_partition = (
        eventos.groupBy("anio", "mes", "zona_gestion", "tipo_evento")
        .agg(F.count("*").alias("registros"))
        .orderBy(F.desc("registros"))
        .first()
    )
    filtro = {
        "anio": int(selected_partition["anio"]),
        "mes": int(selected_partition["mes"]),
        "zona_gestion": selected_partition["zona_gestion"],
        "tipo_evento": selected_partition["tipo_evento"],
    }

    start = time.perf_counter()
    safe_write_parquet(eventos, unpartitioned, target_partitions=48)
    write_base = time.perf_counter() - start

    start = time.perf_counter()
    base_count = (
        spark.read.parquet(str(unpartitioned))
        .filter(
            (F.col("anio") == filtro["anio"])
            & (F.col("mes") == filtro["mes"])
            & (F.col("zona_gestion") == filtro["zona_gestion"])
            & (F.col("tipo_evento") == filtro["tipo_evento"])
        )
        .groupBy("zona_gestion", "tipo_evento")
        .agg(F.count("*").alias("total_eventos"))
        .count()
    )
    read_base = time.perf_counter() - start

    metrics.append(
        {
            "experimento": "base",
            "dataset": "eventos_normalizados",
            "registros_procesados": total_registros,
            "tiempo_segundos": round(read_base, 3),
            "optimizacion_aplicada": "Parquet sin particionamiento; filtro selectivo sin pruning fisico",
            "mejora_porcentual": 0.0,
            "observaciones": (
                f"Filtro {filtro}; grupos agregados: {base_count}; escritura base {write_base:.3f}s; "
                f"lectura/agregacion {read_base:.3f}s"
            ),
        }
    )

    start = time.perf_counter()
    safe_write_parquet(
        eventos,
        partitioned,
        partition_cols=["anio", "mes", "zona_gestion", "tipo_evento"],
        target_partitions=(96, ["anio", "mes", "zona_gestion", "tipo_evento"]),
    )
    write_opt = time.perf_counter() - start

    start = time.perf_counter()
    opt_count = (
        spark.read.parquet(str(partitioned))
        .filter(
            (F.col("anio") == filtro["anio"])
            & (F.col("mes") == filtro["mes"])
            & (F.col("zona_gestion") == filtro["zona_gestion"])
            & (F.col("tipo_evento") == filtro["tipo_evento"])
        )
        .groupBy("zona_gestion", "tipo_evento")
        .agg(F.count("*").alias("total_eventos"))
        .count()
    )
    read_opt = time.perf_counter() - start
    improvement = ((read_base - read_opt) / read_base * 100) if read_base else 0.0

    metrics.append(
        {
            "experimento": "optimizado",
            "dataset": "eventos_normalizados",
            "registros_procesados": total_registros,
            "tiempo_segundos": round(read_opt, 3),
            "optimizacion_aplicada": "Particionado por anio, mes, zona_gestion y tipo_evento; filtro selectivo",
            "mejora_porcentual": round(improvement, 2),
            "observaciones": (
                f"Filtro {filtro}; grupos agregados: {opt_count}; escritura particionada {write_opt:.3f}s; "
                f"lectura/agregacion {read_opt:.3f}s"
            ),
        }
    )
    return metrics
