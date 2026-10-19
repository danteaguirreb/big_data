# Resultados

Este archivo resume como interpretar la evidencia generada por cada corrida. La evidencia ejecutable queda en:

- `evidence/run_summary.json`
- `evidence/execution_steps.json`
- `evidence/optimization_metrics.json`
- `evidence/optimization_metrics_csv`

## Validacion local small

La compatibilidad del modo de prueba rapida se valido con:

```bash
PYTHONPATH=src /opt/anaconda3/bin/python3 -m territorial_pipeline.cli \
  --scale-mode small \
  --root work/validation_small \
  --tmp-root work/validation_small/spark_tmp \
  --warehouse-dir work/validation_small/spark_warehouse \
  --checkpoint-dir work/validation_small/spark_checkpoints \
  --driver-memory 4g
```

Resultado de la validacion local:

- `execution_status`: success
- `bronze_total_rows`: 177.470
- `solicitudes_raw`: 5.000
- `eventos_raw`: 120.000
- `logs_raw`: 35.000
- `silver_null_id_solicitud`: 0
- `coordenadas_fuera_rango`: 0

Nota: la prueba local uso `small` solo para comprobar que el flujo no se rompio. No reemplaza la evidencia `full`.

## Ejecucion full

Para la entrega final usar `full`:

```bash
PYTHONPATH=src python3 -m territorial_pipeline.cli \
  --scale-mode full \
  --root /content/fase2_gestion_territorial_el_monte \
  --tmp-root /content/spark_tmp \
  --warehouse-dir /content/spark_warehouse \
  --checkpoint-dir /content/spark_checkpoints \
  --driver-memory 20g
```

El modo `full` debe mantener:

- 50.000 solicitudes.
- 10.800.000 eventos.
- 1.500.000 logs.
- Bronze sobre 10 millones de filas.

No se debe reportar `full` reduciendo filas, eliminando tablas o bajando escala. Si `full` falla en Colab High-RAM, registrar:

- `execution_status=failed`
- `failed_step`
- `error_message`
- RAM y disco disponibles al inicio y al final
- etapa exacta en `execution_steps.json`
- recomendacion tecnica: Dataproc o Spark Serverless con Cloud Storage

## Criterio de reporte

Si `execution_status=success`, se reportan conteos, tiempos y benchmark desde `evidence/`. Si `execution_status=failed`, se reporta el intento fallido de forma honesta y reproducible. Esa evidencia tambien es valida para justificar que la escala full requiere un entorno administrado.

“La evidencia cuantitativa se registra en JSON/CSV. No se adjuntan capturas Spark UI por limitaciones de exportación del entorno Colab; se conserva trazabilidad por etapa en execution_steps.json.”
