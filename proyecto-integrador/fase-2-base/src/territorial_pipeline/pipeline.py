"""Orquestacion del pipeline reproducible."""

from __future__ import annotations

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

from pyspark.sql import functions as F

from .benchmark import benchmark_eventos
from .config import build_paths, get_profile
from .io import ensure_dirs, write_csv, write_json, write_parquet
from .spark import create_spark
from .synthetic import build_raw_tables
from .transforms import build_gold, build_silver


T = TypeVar("T")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resource_snapshot(root: Path) -> dict[str, object]:
    try:
        import psutil

        ram_total_gb = round(psutil.virtual_memory().total / 1024**3, 2)
        ram_free_gb = round(psutil.virtual_memory().available / 1024**3, 2)
    except Exception:
        ram_total_gb = None
        ram_free_gb = None

    disk = shutil.disk_usage(str(root))
    return {
        "ram_total_gb": ram_total_gb,
        "ram_free_gb": ram_free_gb,
        "disk_total_gb": round(disk.total / 1024**3, 2),
        "disk_free_gb": round(disk.free / 1024**3, 2),
    }


def timed_step(step_name: str, fn: Callable[[], T], execution_steps: list[dict[str, object]]) -> T:
    start = time.time()
    try:
        result = fn()
        execution_steps.append(
            {
                "step": step_name,
                "status": "success",
                "seconds": round(time.time() - start, 3),
                "error": None,
            }
        )
        return result
    except Exception as exc:
        execution_steps.append(
            {
                "step": step_name,
                "status": "failed",
                "seconds": round(time.time() - start, 3),
                "error": repr(exc),
            }
        )
        raise


def _bronze_write_plan(name: str, scale_mode: str) -> dict[str, object]:
    if name == "eventos_raw":
        return {"target_partitions": (96 if scale_mode == "full" else 24, ["zona_gestion", "tipo_evento"])}
    if name == "logs_raw":
        return {"target_partitions": 48 if scale_mode == "full" else 12}
    if name in {"clima_raw", "cuadrillas_raw", "satisfaccion_raw", "fotografias_metadata_raw"}:
        return {"target_partitions": 1}
    return {"target_partitions": 8}


def run_pipeline(
    scale_mode: str = "small",
    root: str | Path = ".",
    tmp_root: str | Path | None = None,
    warehouse_dir: str | Path | None = None,
    checkpoint_dir: str | Path | None = None,
    driver_memory: str | None = None,
) -> dict[str, object]:
    profile = get_profile(scale_mode)
    paths = build_paths(root)
    ensure_dirs(paths.bronze, paths.silver, paths.gold, paths.evidence)

    started_at = _now_iso()
    start_total = time.perf_counter()
    execution_steps: list[dict[str, object]] = []
    resources_start = _resource_snapshot(paths.root)
    spark = None

    summary: dict[str, object] = {
        "scale_mode": profile.name,
        "n_solicitudes_config": profile.solicitudes,
        "n_eventos_config": profile.eventos,
        "n_logs_config": profile.logs,
        "shuffle_partitions": profile.shuffle_partitions,
        "started_at": started_at,
        "execution_status": "running",
        "failed_step": None,
        "error_message": None,
        "ram_total_gb": resources_start["ram_total_gb"],
        "ram_free_gb_start": resources_start["ram_free_gb"],
        "disk_free_gb_start": resources_start["disk_free_gb"],
    }

    def persist_evidence() -> None:
        write_json(summary, paths.evidence / "run_summary.json")
        write_json({"steps": execution_steps}, paths.evidence / "execution_steps.json")

    try:
        spark = create_spark(
            f"Fase2-ElMonte-{profile.name}",
            profile.shuffle_partitions,
            tmp_root=tmp_root,
            warehouse_dir=warehouse_dir,
            driver_memory=driver_memory,
        )
        if checkpoint_dir is not None:
            Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
            spark.sparkContext.setCheckpointDir(str(checkpoint_dir))
        summary["spark_version"] = spark.version
    except Exception as exc:
        summary.update(
            {
                "execution_status": "failed",
                "failed_step": "create_spark",
                "error_message": repr(exc),
                "finished_at": _now_iso(),
                "total_seconds": round(time.perf_counter() - start_total, 3),
                "disk_free_gb_end": _resource_snapshot(paths.root)["disk_free_gb"],
            }
        )
        persist_evidence()
        raise

    start_total = time.perf_counter()

    try:
        raw = timed_step("build_raw_tables", lambda: build_raw_tables(spark, profile), execution_steps)

        bronze_counts = {}
        known_bronze_counts = {
            "solicitudes_raw": profile.solicitudes,
            "eventos_raw": profile.eventos,
            "logs_raw": profile.logs,
            "fotografias_metadata_raw": int(profile.solicitudes * profile.fotos_factor),
            "clima_raw": 2922 * 4,
            "cuadrillas_raw": 4 * 8,
            "satisfaccion_raw": int(profile.solicitudes * profile.satisfaccion_factor),
        }
        for name, df in raw.items():
            def write_bronze(name=name, df=df) -> None:
                write_parquet(df, paths.bronze / name, **_bronze_write_plan(name, profile.name))

            timed_step(f"bronze_write_{name}", write_bronze, execution_steps)
            bronze_counts[name] = known_bronze_counts.get(name, df.count())

        summary["bronze_counts"] = bronze_counts
        summary["bronze_total_rows"] = sum(bronze_counts.values())
        persist_evidence()

        silver = timed_step("build_silver", lambda: build_silver(raw), execution_steps)
        silver_counts = {}
        for name, df in silver.items():
            target_partitions = 48 if name == "eventos_normalizados" and profile.name == "full" else None
            timed_step(
                f"silver_write_{name}",
                lambda name=name, df=df, target_partitions=target_partitions: write_parquet(
                    df, paths.silver / name, target_partitions=target_partitions
                ),
                execution_steps,
            )
            silver_counts[name] = timed_step(f"silver_count_{name}", df.count, execution_steps)

        summary["silver_counts"] = silver_counts
        persist_evidence()

        gold = timed_step("build_gold", lambda: build_gold(silver), execution_steps)
        gold_counts = {}
        for name, df in gold.items():
            timed_step(f"gold_write_{name}", lambda name=name, df=df: write_parquet(df, paths.gold / name), execution_steps)
            if name in {
                "kpi_sla",
                "backlog_operacional",
                "zonas_criticas",
                "satisfaccion_ciudadana",
                "desempeno_cuadrillas",
            }:
                timed_step(f"gold_csv_{name}", lambda name=name, df=df: write_csv(df, paths.gold / f"{name}_csv"), execution_steps)
            gold_counts[name] = timed_step(f"gold_count_{name}", df.count, execution_steps)

        summary["gold_counts"] = gold_counts
        persist_evidence()

        optimization_metrics = timed_step(
            "benchmark_eventos",
            lambda: benchmark_eventos(spark, silver["eventos_normalizados"], paths.evidence),
            execution_steps,
        )
        write_json({"experimentos": optimization_metrics}, paths.evidence / "optimization_metrics.json")
        timed_step(
            "write_optimization_metrics_csv",
            lambda: spark.createDataFrame(optimization_metrics)
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(paths.evidence / "optimization_metrics_csv")),
            execution_steps,
        )

        quality_metrics = {
            "silver_null_id_solicitud": timed_step(
                "quality_null_id_solicitud",
                lambda: silver["solicitudes_enriquecidas"].filter(F.col("id_solicitud").isNull()).count(),
                execution_steps,
            ),
            "silver_hash_ciudadano_presente": "ciudadano_hash" in silver["solicitudes_enriquecidas"].columns,
            "coordenadas_fuera_rango": timed_step(
                "quality_coordenadas_fuera_rango",
                lambda: silver["solicitudes_enriquecidas"]
                .filter(~(F.col("latitud").between(-34.0, -33.0) & F.col("longitud").between(-71.5, -70.0)))
                .count(),
                execution_steps,
            ),
        }

        resources_end = _resource_snapshot(paths.root)
        summary.update(
            {
                "quality_metrics": quality_metrics,
                "execution_status": "success",
                "finished_at": _now_iso(),
                "total_seconds": round(time.perf_counter() - start_total, 3),
                "total_runtime_seconds": round(time.perf_counter() - start_total, 3),
                "disk_free_gb_end": resources_end["disk_free_gb"],
                "ram_free_gb_end": resources_end["ram_free_gb"],
                "full_scale_requirement": "En modo full, Bronze supera 10 millones de filas mediante eventos/logs/tablas derivadas.",
            }
        )
        persist_evidence()
        return summary
    except Exception as exc:
        failed_step = execution_steps[-1]["step"] if execution_steps else "unknown"
        resources_end = _resource_snapshot(paths.root)
        summary.update(
            {
                "execution_status": "failed",
                "failed_step": failed_step,
                "error_message": repr(exc),
                "finished_at": _now_iso(),
                "total_seconds": round(time.perf_counter() - start_total, 3),
                "total_runtime_seconds": round(time.perf_counter() - start_total, 3),
                "disk_free_gb_end": resources_end["disk_free_gb"],
                "ram_free_gb_end": resources_end["ram_free_gb"],
                "recommendation": (
                    "Si full falla en Colab High-RAM, ejecutar la misma version en Dataproc o Spark Serverless "
                    "con almacenamiento en Cloud Storage. No reducir la escala full para reportar exito."
                ),
            }
        )
        persist_evidence()
        raise
    finally:
        if spark is not None:
            spark.stop()
