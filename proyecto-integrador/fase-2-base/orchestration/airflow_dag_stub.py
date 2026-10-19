"""DAG ilustrativo para evolucion futura a Cloud Composer.

No se ejecuta en esta entrega local; documenta la secuencia batch esperada.
"""

from datetime import datetime


DAG_ID = "fase2_gestion_territorial_el_monte"
SCHEDULE = "@daily"
START_DATE = datetime(2026, 1, 1)

TASKS = [
    "generar_fuentes_sinteticas",
    "ingestar_bronze_parquet",
    "transformar_silver",
    "publicar_gold",
    "calcular_metricas_optimizacion",
    "actualizar_dashboard",
]
