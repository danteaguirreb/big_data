"""Generacion sintetica escalable usando expresiones Spark."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import ScaleProfile


ZONAS = ["Zona 1", "Zona 2", "Zona 3", "Zona 4"]
CATEGORIAS = [
    "Alumbrado publico",
    "Residuos domiciliarios",
    "Microbasurales",
    "Baches y caminos",
    "Arbolado urbano/rural",
    "Seguridad publica",
    "Transito",
    "Espacios publicos",
    "Emergencia climatica",
    "Fiscalizacion",
    "Animales",
    "Salud/riesgo sanitario",
]
UNIDADES = [
    "DIMAO",
    "Seguridad Publica",
    "Transito",
    "DOM",
    "DIDECO",
    "Salud Municipal",
    "Emergencia",
    "Administracion Municipal",
]
EVENTOS = [
    "creacion",
    "validacion",
    "georreferenciacion",
    "deteccion_duplicidad",
    "clasificacion",
    "derivacion",
    "asignacion_cuadrilla",
    "visita_terreno",
    "carga_fotografia",
    "cambio_estado",
    "cierre",
    "reapertura",
    "notificacion",
    "encuesta_satisfaccion",
    "auditoria",
]


def arr(values: list[str]):
    return F.array(*[F.lit(value) for value in values])


def pick(values: list[str], idx_col: F.Column) -> F.Column:
    return F.element_at(arr(values), ((idx_col % F.lit(len(values))) + F.lit(1)).cast("int"))


def solicitudes_raw(spark: SparkSession, profile: ScaleProfile) -> DataFrame:
    base = spark.range(profile.solicitudes).withColumnRenamed("id", "seq")
    return (
        base.select(
            F.concat(F.lit("SOL-"), F.lpad((F.col("seq") + 1).cast("string"), 8, "0")).alias(
                "id_solicitud"
            ),
            F.date_add(F.lit("2018-01-01").cast("date"), (F.col("seq") % 2922).cast("int")).alias(
                "fecha_ingreso"
            ),
            pick(["web", "telefono", "presencial", "app_movil"], F.col("seq")).alias("canal_ingreso"),
            pick(ZONAS, F.col("seq")).alias("zona_gestion"),
            pick(["reclamo", "denuncia", "solicitud", "emergencia"], F.col("seq") + 3).alias(
                "tipo_solicitud"
            ),
            pick(CATEGORIAS, F.col("seq") + 5).alias("categoria"),
            F.concat(F.lit("subcategoria_"), ((F.col("seq") % 7) + 1).cast("string")).alias(
                "subcategoria"
            ),
            pick(["baja", "media", "alta"], F.col("seq") + 2).alias("prioridad_declarada"),
            F.concat(F.lit("Descripcion sintetica de solicitud territorial "), F.col("seq")).alias(
                "descripcion_sintetica"
            ),
            (F.lit(-33.68) + (F.col("seq") % 1000) / F.lit(100000)).cast("double").alias("latitud"),
            (F.lit(-70.99) + (F.col("seq") % 1000) / F.lit(100000)).cast("double").alias("longitud"),
            pick(UNIDADES, F.col("seq") + 1).alias("unidad_responsable"),
            pick(["ingresada", "validada", "pendiente"], F.col("seq")).alias("estado_inicial"),
            F.sha2(F.concat(F.lit("ciudadano-"), (F.col("seq") % 17000).cast("string")), 256).alias(
                "ciudadano_hash"
            ),
            ((F.col("seq") % 9) == 0).alias("es_reincidente"),
            F.when((F.col("seq") % 12) >= 9, 24)
            .when((F.col("seq") % 12) >= 5, 72)
            .otherwise(120)
            .alias("SLA_horas"),
        )
    )


def eventos_raw(spark: SparkSession, profile: ScaleProfile) -> DataFrame:
    base = spark.range(profile.eventos).withColumnRenamed("id", "seq")
    solicitud_num = (F.col("seq") % F.lit(profile.solicitudes)) + F.lit(1)
    base_fecha = F.date_add(
        F.lit("2018-01-01").cast("date"), ((solicitud_num - F.lit(1)) % F.lit(2922)).cast("int")
    )
    ordinal_evento = F.floor(F.col("seq") / F.lit(profile.solicitudes))
    return base.select(
        F.concat(F.lit("EVT-"), F.lpad((F.col("seq") + 1).cast("string"), 12, "0")).alias("id_evento"),
        F.concat(F.lit("SOL-"), F.lpad(solicitud_num.cast("string"), 8, "0")).alias("id_solicitud"),
        F.to_timestamp(
            F.from_unixtime(
                F.unix_timestamp(F.to_timestamp(base_fecha))
                + (ordinal_evento * F.lit(3600))
                + ((F.col("seq") % F.lit(60)) * F.lit(60))
            )
        ).alias("timestamp_evento"),
        pick(EVENTOS, F.col("seq")).alias("tipo_evento"),
        pick(ZONAS, solicitud_num).alias("zona_gestion"),
        pick(UNIDADES, solicitud_num + 1).alias("unidad_responsable"),
        pick(["analista", "supervisor", "operador", "sistema", "auditor"], F.col("seq") + 3).alias(
            "usuario_rol"
        ),
        pick(["ingresada", "validada", "asignada", "en_terreno", "cerrada"], F.col("seq")).alias(
            "estado_anterior"
        ),
        pick(["validada", "asignada", "en_terreno", "cerrada", "reabierta"], F.col("seq") + 1).alias(
            "estado_nuevo"
        ),
        ((F.col("seq") % 240) + 5).cast("int").alias("duracion_minutos"),
        F.concat(F.lit("Evento operacional sintetico "), F.col("seq")).alias("observacion_sintetica"),
        F.sha2(F.concat(F.lit("evento-"), F.col("seq").cast("string")), 256).alias("hash_evento"),
    )


def logs_raw(spark: SparkSession, profile: ScaleProfile) -> DataFrame:
    base = spark.range(profile.logs).withColumnRenamed("id", "seq")
    solicitud_num = (F.col("seq") % F.lit(profile.solicitudes)) + F.lit(1)
    base_fecha = F.date_add(
        F.lit("2018-01-01").cast("date"), ((solicitud_num - F.lit(1)) % F.lit(2922)).cast("int")
    )
    ordinal_log = F.floor(F.col("seq") / F.lit(profile.solicitudes))
    return base.select(
        F.concat(F.lit("LOG-"), F.lpad((F.col("seq") + 1).cast("string"), 10, "0")).alias("id_log"),
        F.to_timestamp(
            F.from_unixtime(
                F.unix_timestamp(F.to_timestamp(base_fecha))
                + (ordinal_log * F.lit(900))
                + ((F.col("seq") % F.lit(60)) * F.lit(10))
            )
        ).alias("timestamp_log"),
        F.concat(F.lit("SOL-"), F.lpad(solicitud_num.cast("string"), 8, "0")).alias("id_solicitud"),
        pick(["insert", "update", "read", "export", "login"], F.col("seq")).alias("tipo_accion"),
        pick(["portal_ciudadano", "crm_municipal", "api_geografica", "batch_spark"], F.col("seq") + 2).alias(
            "sistema_origen"
        ),
        pick(["INFO", "WARN", "ERROR"], F.col("seq") + 4).alias("nivel_log"),
        ((F.col("seq") % 950) + 50).cast("int").alias("latencia_ms"),
        pick(["analista", "supervisor", "operador", "sistema"], F.col("seq")).alias("usuario_rol"),
        pick(["ok", "timeout", "rechazado", "reintento"], F.col("seq") + 1).alias("resultado"),
    )


def fotografias_metadata_raw(spark: SparkSession, profile: ScaleProfile) -> DataFrame:
    total = int(profile.solicitudes * profile.fotos_factor)
    base = spark.range(total).withColumnRenamed("id", "seq")
    solicitud_num = (F.col("seq") % F.lit(profile.solicitudes)) + F.lit(1)
    return base.select(
        F.concat(F.lit("FOTO-"), F.lpad((F.col("seq") + 1).cast("string"), 10, "0")).alias("id_foto"),
        F.concat(F.lit("SOL-"), F.lpad(solicitud_num.cast("string"), 8, "0")).alias("id_solicitud"),
        F.date_add(F.lit("2018-01-01").cast("date"), (F.col("seq") % 2922).cast("int")).alias("fecha_carga"),
        pick(["antes", "durante", "despues"], F.col("seq")).alias("tipo_foto"),
        pick(["jpg", "png", "webp"], F.col("seq") + 1).alias("formato"),
        ((F.col("seq") % 3000) + 120).cast("int").alias("size_kb"),
        F.concat(F.lit("gs://municipal-el-monte/fotos/"), F.col("seq"), F.lit(".jpg")).alias(
            "ruta_storage_simulada"
        ),
        F.sha2(F.concat(F.lit("foto-"), F.col("seq").cast("string")), 256).alias("hash_archivo"),
        ((F.col("seq") % 100) / 100.0).cast("double").alias("calidad_simulada"),
        ((F.col("seq") % 5) != 0).alias("validada"),
    )


def clima_raw(spark: SparkSession) -> DataFrame:
    dias = spark.range(2922).withColumnRenamed("id", "dia")
    zonas = spark.createDataFrame([(zona,) for zona in ZONAS], ["zona_gestion"])
    return dias.crossJoin(zonas).select(
        F.date_add(F.lit("2018-01-01").cast("date"), F.col("dia").cast("int")).alias("fecha"),
        F.col("zona_gestion"),
        (F.lit(12) + (F.col("dia") % 18)).cast("double").alias("temperatura_promedio"),
        F.when((F.col("dia") % 17) == 0, 35.0).otherwise((F.col("dia") % 9).cast("double")).alias(
            "precipitacion_mm"
        ),
        ((F.col("dia") % 17) == 0).alias("alerta_climatica"),
        F.when((F.col("dia") % 17) == 0, "lluvia_intensa").otherwise("normal").alias(
            "tipo_evento_climatico"
        ),
    )


def cuadrillas_raw(spark: SparkSession) -> DataFrame:
    rows = []
    seq = 1
    for zona in ZONAS:
        for unidad in UNIDADES:
            rows.append(
                (
                    f"CUAD-{seq:03d}",
                    unidad,
                    zona,
                    8 + (seq % 7),
                    ["electricidad", "aseo", "vialidad", "seguridad", "social"][seq % 5],
                    seq % 6 != 0,
                )
            )
            seq += 1
    return spark.createDataFrame(
        rows,
        ["id_cuadrilla", "unidad_responsable", "zona_base", "capacidad_diaria", "especialidad", "disponibilidad"],
    )


def satisfaccion_raw(spark: SparkSession, profile: ScaleProfile) -> DataFrame:
    total = int(profile.solicitudes * profile.satisfaccion_factor)
    base = spark.range(total).withColumnRenamed("id", "seq")
    solicitud_num = (F.col("seq") % F.lit(profile.solicitudes)) + F.lit(1)
    return base.select(
        F.concat(F.lit("ENC-"), F.lpad((F.col("seq") + 1).cast("string"), 10, "0")).alias("id_encuesta"),
        F.concat(F.lit("SOL-"), F.lpad(solicitud_num.cast("string"), 8, "0")).alias("id_solicitud"),
        F.date_add(F.lit("2018-01-03").cast("date"), (F.col("seq") % 2922).cast("int")).alias(
            "fecha_respuesta"
        ),
        ((F.col("seq") % 7) + 1).cast("int").alias("nota_satisfaccion"),
        pick(["web", "telefono", "presencial"], F.col("seq")).alias("canal_respuesta"),
        F.concat(F.lit("Comentario sintetico encuesta "), F.col("seq")).alias("comentario_sintetico"),
    )


def build_raw_tables(spark: SparkSession, profile: ScaleProfile) -> dict[str, DataFrame]:
    return {
        "solicitudes_raw": solicitudes_raw(spark, profile),
        "eventos_raw": eventos_raw(spark, profile),
        "logs_raw": logs_raw(spark, profile),
        "fotografias_metadata_raw": fotografias_metadata_raw(spark, profile),
        "clima_raw": clima_raw(spark),
        "cuadrillas_raw": cuadrillas_raw(spark),
        "satisfaccion_raw": satisfaccion_raw(spark, profile),
    }
