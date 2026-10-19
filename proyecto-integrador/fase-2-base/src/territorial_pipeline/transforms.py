"""Transformaciones Silver y Gold del lakehouse medallion."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_silver(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    solicitudes = (
        raw["solicitudes_raw"]
        .dropDuplicates(["id_solicitud"])
        .filter(F.col("id_solicitud").isNotNull())
        .filter(F.col("fecha_ingreso").between("2018-01-01", "2025-12-31"))
        .filter(F.col("latitud").between(-34.0, -33.0) & F.col("longitud").between(-71.5, -70.0))
        .withColumn("anio_ingreso", F.year("fecha_ingreso"))
        .withColumn("mes_ingreso", F.month("fecha_ingreso"))
        .withColumn("dia_semana", F.dayofweek("fecha_ingreso"))
        .withColumn("hora_ingreso", (F.pmod(F.abs(F.hash("id_solicitud")), F.lit(24))).cast("int"))
        .withColumn("es_critica", F.col("prioridad_declarada") == "alta")
        .withColumn(
            "vulnerabilidad_territorial_simulada",
            F.when(F.col("zona_gestion") == "Zona 1", 0.82)
            .when(F.col("zona_gestion") == "Zona 2", 0.68)
            .when(F.col("zona_gestion") == "Zona 3", 0.57)
            .otherwise(0.49),
        )
    )

    eventos = (
        raw["eventos_raw"]
        .dropDuplicates(["id_evento"])
        .filter(F.col("id_solicitud").isNotNull())
        .withColumn("anio", F.year("timestamp_evento"))
        .withColumn("mes", F.month("timestamp_evento"))
    )
    event_window = Window.partitionBy("id_solicitud").orderBy("timestamp_evento", "id_evento")
    eventos = eventos.withColumn(
        "minutos_desde_evento_anterior",
        (
            F.unix_timestamp("timestamp_evento")
            - F.unix_timestamp(F.lag("timestamp_evento").over(event_window))
        )
        / 60,
    ).fillna({"minutos_desde_evento_anterior": 0})

    eventos_resumen = eventos.groupBy("id_solicitud").agg(
        F.max("timestamp_evento").alias("ultimo_evento_ts"),
        F.max(F.when(F.col("tipo_evento") == "cierre", 1).otherwise(0)).alias("solicitud_resuelta"),
        F.max(F.when(F.col("tipo_evento") == "reapertura", 1).otherwise(0)).alias("tiene_reapertura"),
        F.max(F.when(F.col("tipo_evento") == "deteccion_duplicidad", 1).otherwise(0)).alias(
            "posible_duplicado"
        ),
    )
    fotos_resumen = raw["fotografias_metadata_raw"].groupBy("id_solicitud").agg(
        F.count("*").alias("total_fotos"),
        F.max(F.col("validada").cast("int")).alias("tiene_fotografia"),
    )
    satisfaccion = raw["satisfaccion_raw"].dropDuplicates(["id_encuesta"])
    clima = raw["clima_raw"].dropDuplicates(["fecha", "zona_gestion"])
    cuadrillas = raw["cuadrillas_raw"].dropDuplicates(["unidad_responsable", "zona_base"])

    solicitudes_enriquecidas = (
        solicitudes.join(eventos_resumen, "id_solicitud", "left")
        .join(fotos_resumen, "id_solicitud", "left")
        .join(
            clima,
            (solicitudes["fecha_ingreso"] == clima["fecha"])
            & (solicitudes["zona_gestion"] == clima["zona_gestion"]),
            "left",
        )
        .drop(clima["fecha"])
        .drop(clima["zona_gestion"])
        .join(
            cuadrillas,
            (solicitudes["unidad_responsable"] == cuadrillas["unidad_responsable"])
            & (solicitudes["zona_gestion"] == cuadrillas["zona_base"]),
            "left",
        )
        .drop(cuadrillas["unidad_responsable"])
        .drop("zona_base")
        .withColumn(
            "tiempo_resolucion_horas",
            (
                F.unix_timestamp("ultimo_evento_ts")
                - F.unix_timestamp(F.to_timestamp("fecha_ingreso"))
            )
            / 3600,
        )
        .withColumn("cumple_sla", F.col("tiempo_resolucion_horas") <= F.col("SLA_horas"))
        .withColumn("es_vencida", ~F.col("cumple_sla"))
        .fillna(
            {
                "solicitud_resuelta": 0,
                "tiene_reapertura": 0,
                "posible_duplicado": 0,
                "total_fotos": 0,
                "tiene_fotografia": 0,
                "cumple_sla": False,
                "es_vencida": True,
                "precipitacion_mm": 0.0,
                "alerta_climatica": False,
            }
        )
        .withColumn("reincidencia_historica_simulada", F.col("es_reincidente").cast("int"))
    )

    logs = raw["logs_raw"].dropDuplicates(["id_log"])

    return {
        "solicitudes_enriquecidas": solicitudes_enriquecidas,
        "eventos_normalizados": eventos,
        "logs_normalizados": logs,
        "fotografias_metadata": raw["fotografias_metadata_raw"].dropDuplicates(["id_foto"]),
        "satisfaccion": satisfaccion,
        "clima": clima,
        "cuadrillas": cuadrillas,
    }


def build_gold(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    sol = silver["solicitudes_enriquecidas"]
    sat = silver["satisfaccion"].join(
        sol.select("id_solicitud", "zona_gestion", "categoria"), "id_solicitud", "left"
    )

    kpi_sla = sol.groupBy("zona_gestion", "categoria", "unidad_responsable").agg(
        F.count("*").alias("total_solicitudes"),
        F.sum("solicitud_resuelta").alias("solicitudes_resueltas"),
        F.sum(F.col("es_vencida").cast("int")).alias("solicitudes_vencidas"),
        (F.avg(F.col("cumple_sla").cast("double")) * 100).alias("porcentaje_cumplimiento_sla"),
        F.avg("tiempo_resolucion_horas").alias("tiempo_promedio_resolucion_horas"),
    )

    backlog_operacional = (
        sol.withColumn(
            "estado_actual",
            F.when(F.col("solicitud_resuelta") == 1, "cerrada").otherwise("pendiente"),
        )
        .filter(F.col("estado_actual") == "pendiente")
        .groupBy("zona_gestion", "unidad_responsable", "estado_actual")
        .agg(
            F.count("*").alias("total_pendientes"),
            F.avg("tiempo_resolucion_horas").alias("antiguedad_promedio_horas"),
        )
    )

    zonas_criticas = sol.groupBy("zona_gestion").agg(
        F.count("*").alias("total_solicitudes"),
        F.sum(F.col("es_critica").cast("int")).alias("total_criticas"),
        F.avg(F.col("es_reincidente").cast("double")).alias("tasa_reincidencia"),
        F.avg(F.col("tiene_reapertura").cast("double")).alias("tasa_reapertura"),
        (
            F.avg(F.col("es_critica").cast("double")) * 0.35
            + F.avg(F.col("es_reincidente").cast("double")) * 0.25
            + F.avg(F.col("tiene_reapertura").cast("double")) * 0.20
            + F.avg(F.col("es_vencida").cast("double")) * 0.20
        ).alias("indice_presion_territorial"),
    )

    satisfaccion_ciudadana = sat.groupBy("zona_gestion", "categoria").agg(
        F.avg("nota_satisfaccion").alias("nota_promedio"),
        F.count("*").alias("total_encuestas"),
        (F.avg((F.col("nota_satisfaccion") <= 3).cast("double")) * 100).alias(
            "porcentaje_baja_satisfaccion"
        ),
    )

    desempeno_cuadrillas = sol.groupBy("id_cuadrilla", "unidad_responsable", "zona_gestion").agg(
        F.count("*").alias("solicitudes_asignadas"),
        F.avg(F.col("cumple_sla").cast("double")).alias("tasa_cumplimiento_sla"),
        F.avg("tiempo_resolucion_horas").alias("tiempo_promedio_resolucion_horas"),
    )

    dataset_modelo_priorizacion = sol.select(
        "id_solicitud",
        "zona_gestion",
        "categoria",
        "canal_ingreso",
        "prioridad_declarada",
        F.col("mes_ingreso").alias("mes"),
        "dia_semana",
        "hora_ingreso",
        "precipitacion_mm",
        "alerta_climatica",
        "reincidencia_historica_simulada",
        "vulnerabilidad_territorial_simulada",
        F.when(F.col("prioridad_declarada") == "alta", "alta")
        .when(F.col("alerta_climatica") | F.col("es_reincidente"), "media")
        .otherwise("baja")
        .alias("urgencia_operativa"),
    )

    return {
        "kpi_sla": kpi_sla,
        "backlog_operacional": backlog_operacional,
        "zonas_criticas": zonas_criticas,
        "satisfaccion_ciudadana": satisfaccion_ciudadana,
        "desempeno_cuadrillas": desempeno_cuadrillas,
        "dataset_modelo_priorizacion": dataset_modelo_priorizacion,
    }
