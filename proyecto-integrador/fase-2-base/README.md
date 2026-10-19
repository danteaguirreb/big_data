# Gestion Territorial Inteligente de Solicitudes Ciudadanas - Municipalidad de El Monte

Proyecto academico reproducible para la Fase 2 del Proyecto Integrador de Big Data y Cloud Computing del Magister en Data Science UDD.

## Objetivo de la Fase 2

Implementar un pipeline funcional en PySpark que demuestre en la practica la arquitectura lakehouse medallion definida en la Fase 1. La solucion genera datos sinteticos a escala, ingesta fuentes operacionales, transforma datos hacia capas Bronze, Silver y Gold, escribe Parquet, mide rendimiento y deja documentados elementos de gobierno, seguridad, linaje y FinOps.

## Arquitectura implementada

- Bronze: ingesta cruda sintetica en Parquet, conservando estructura cercana al origen.
- Silver: limpieza, deduplicacion, validaciones, enriquecimiento con clima/cuadrillas/SLA y flags operacionales.
- Gold: datasets analiticos para dashboard, BigQuery o Looker Studio.
- Evidencia: metricas de ejecucion, conteos, calidad y comparacion base versus optimizada.
- Evolucion cloud: Cloud Storage, Dataproc o Serverless Spark, BigQuery, Looker Studio, IAM, Cloud Logging y Vertex AI.

## Estructura del repositorio

```text
config/                  Parametros de perfiles de escala
docs/                    Gobierno, linaje, FinOps y dashboard
evidence/                Resultados generados por la ejecucion
notebooks/               Notebook ejecutable de la Fase 2
orchestration/           Stub de orquestacion batch
src/territorial_pipeline Codigo modular PySpark
tests/                   Pruebas basicas de configuracion
data/                    Salidas Bronze/Silver/Gold generadas localmente
```

## Requisitos tecnicos

- Python 3.10+
- PySpark 3.5.x
- Java compatible con Spark
- Memoria suficiente segun escala

Instalacion local:

```bash
python3 -m pip install -r requirements.txt
export PYTHONPATH="$PWD/src"
```

## Ejecucion local

Prueba rapida:

```bash
PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode small --root .
```

Escala media:

```bash
PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode medium --root .
```

Entrega final de escala Big Data:

```bash
PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode full --root .
```

El modo `full` genera 50.000 solicitudes, 10,8 millones de eventos y 1,5 millones de logs. Puede requerir ajustar memoria local o ejecutar en Dataproc/Serverless Spark.

## Ejecucion en Google Colab

1. Usar runtime High-RAM sin GPU.
2. Copiar o clonar el repositorio en `/content/fase2_gestion_territorial_el_monte`.
3. No escribir Parquet masivo directamente en Google Drive.
4. Instalar dependencias con `pip install -r requirements.txt` si Colab no tiene PySpark.
5. Ejecutar `notebooks/fase2_pipeline_gestion_territorial.ipynb`.
6. Dejar `SCALE_MODE="full"` para la evidencia final. `small` y `medium` son solo pruebas.
7. Copiar a Drive solo al final los ZIP de evidencia y repositorio sin `data/`.

Rutas usadas en Colab:

```text
PROJECT_ROOT=/content/fase2_gestion_territorial_el_monte
DATA_ROOT=/content/fase2_gestion_territorial_el_monte/data
EVIDENCE_ROOT=/content/fase2_gestion_territorial_el_monte/evidence
TMP_ROOT=/content/spark_tmp
WAREHOUSE_DIR=/content/spark_warehouse
CHECKPOINT_DIR=/content/spark_checkpoints
```

El notebook valida RAM y disco disponibles. Si RAM libre es menor a 20 GB o disco libre menor a 80 GB, muestra advertencia, pero no bloquea la ejecucion.

Version CLI equivalente para Colab:

```bash
PYTHONPATH=src python3 -m territorial_pipeline.cli \
  --scale-mode full \
  --root /content/fase2_gestion_territorial_el_monte \
  --tmp-root /content/spark_tmp \
  --warehouse-dir /content/spark_warehouse \
  --checkpoint-dir /content/spark_checkpoints \
  --driver-memory 20g
```

Si Colab no permite `20g`, probar `16g` o `12g`. Si `full` falla, no reducir escala para declarar exito: reportar `evidence/run_summary.json` y `evidence/execution_steps.json`, incluyendo etapa fallida y recomendacion Dataproc/Serverless Spark.

## Ejecucion en Google Colab desde Drive

Si el repositorio vive en Drive:

1. Montar Drive solo para copiar fuentes.
2. Instalar dependencias con `pip install -r requirements.txt`.
3. Copiar el repositorio a `/content/fase2_gestion_territorial_el_monte`.
4. Ejecutar desde `/content`.
5. Copiar a Drive solo los ZIP finales de evidencia.

## Datos sinteticos

El proyecto genera:

- `solicitudes_raw`
- `eventos_raw`
- `logs_raw`
- `fotografias_metadata_raw`
- `clima_raw`
- `cuadrillas_raw`
- `satisfaccion_raw`

Las zonas son Zona 1 a Zona 4. Las categorias, unidades responsables, eventos operacionales y datos sensibles hasheados siguen el enunciado de la Fase 2.

## Perfiles de escala

- `small`: 5.000 solicitudes, 120.000 eventos y 35.000 logs para prueba rapida.
- `medium`: 50.000 solicitudes, 2.000.000 eventos y 500.000 logs.
- `full`: 50.000 solicitudes, 10.800.000 eventos y 1.500.000 logs. Bronze supera 10 millones de filas.

## Resultados esperados

La ejecucion escribe:

- `data/bronze/*`
- `data/silver/*`
- `data/gold/*`
- `evidence/run_summary.json`
- `evidence/execution_steps.json`
- `evidence/optimization_metrics.json`
- `evidence/optimization_metrics_csv`

## Evidencia de optimizacion

El experimento compara:

- Base: Parquet sin particionamiento y agregacion completa.
- Optimizado: Parquet particionado por `anio`, `mes`, `zona_gestion` y `tipo_evento`, con filtro selectivo alineado a esas particiones.

La tabla final incluye `experimento`, `dataset`, `registros_procesados`, `tiempo_segundos`, `optimizacion_aplicada`, `mejora_porcentual` y `observaciones`.

## Gobierno y seguridad

La solucion usa datos sinteticos, hashes SHA-256 para identificadores ciudadanos, separacion Bronze/Silver/Gold, principio de minimo privilegio y roles simulados. Ver `docs/gobierno_seguridad.md`.

## Limitaciones

- No reemplaza una implementacion productiva con IAM real, redes privadas, KMS o monitoreo operativo completo.
- Las metricas dependen del hardware local o cluster Spark usado.
- Los precios cloud se documentan como formulas editables, no como valores fijos.
- El modo `full` no debe simularse reduciendo filas. Si Colab High-RAM no logra completar la corrida, el resultado correcto es evidencia controlada de fallo y recomendacion de ejecucion administrada.

## Evolucion futura hacia GCP

- Cloud Storage como data lake.
- Dataproc o Serverless Spark para procesamiento batch.
- BigQuery para consumo analitico.
- Looker Studio para dashboard.
- Cloud Logging/Monitoring para observabilidad.
- IAM y KMS para seguridad.
- Vertex AI para modelos de priorizacion con validacion humana.

## Declaracion de uso de IA generativa

Se utilizo IA generativa como apoyo para estructurar codigo, documentacion y notebook academico. El resultado debe ser revisado, ejecutado y validado por el equipo antes de su entrega formal.
## Fase 3 - Documentacion tecnica

Esta carpeta contiene la entrega final de documentacion tecnica del Proyecto Integrador Big Data y Cloud Computing UDD 2026 para el caso Gestion Territorial Inteligente de Solicitudes Ciudadanas - Municipalidad de El Monte.

### Evidencia usada

- Notebook ejecutado: `notebooks/fase2_pipeline_gestion_territorial.ipynb`
- Resumen de corrida: `evidence/run_summary.json`
- Trazabilidad por etapa: `evidence/execution_steps.json`
- Benchmark: `evidence/optimization_metrics.json`
- Informe Fase 3: `Informe_Fase_3_Gestion_Territorial_El_Monte_Grupo_2.docx`

### Estado de ejecucion

La evidencia adjunta corresponde a `SCALE_MODE=full` con `execution_status=success`. Se procesaron 12.429.220 filas Bronze, con 10.800.000 eventos y 1.500.000 logs. Esta corrida full cumple el umbral Big Data del enunciado al superar 10 millones de filas en Bronze. El codigo queda preparado para repetir la ejecucion en Colab High-RAM o migrarla a Dataproc/Spark Serverless para operacion administrada.

### Nota de empaquetado

El ZIP final de Fase 3 conserva codigo, notebook, README, ADRs, informe, diagrama y evidencia JSON/CSV full. Los Parquet pesados de `_benchmark/` no se duplican dentro del ZIP final para mantenerlo entregable; la evidencia pesada queda en el archivo separado `fase2_evidence_full.zip`.

### Arquitectura final

El diagrama C4 Nivel 2 se encuentra en `docs/assets/arquitectura_c4_nivel2.png`. La arquitectura mantiene capas Bronze/Silver/Gold en Parquet, procesamiento PySpark batch, evidencia de optimizacion y evolucion futura a GCP con Cloud Storage, Dataproc/Spark Serverless, BigQuery, Looker Studio, IAM, KMS y Cloud Logging.

### ADRs

Los Architecture Decision Records se encuentran en `docs/adr/`:

- `ADR-001-lakehouse-medallion.md`: ADR-001: Arquitectura lakehouse medallion
- `ADR-002-pyspark-parquet-batch.md`: ADR-002: PySpark y Parquet para procesamiento batch
- `ADR-003-ejecucion-colab-y-gcp.md`: ADR-003: Ejecucion reproducible en Colab con ruta a GCP administrado
- `ADR-004-benchmark-particionamiento.md`: ADR-004: Benchmark de particionamiento y lectura critica
- `ADR-005-gobierno-seguridad-datos.md`: ADR-005: Gobierno, seguridad y datos sinteticos

### Operacion resumida

1. Instalar dependencias: `pip install -r requirements.txt`.
2. Ejecutar prueba rapida opcional: `PYTHONPATH=src python3 -m territorial_pipeline.cli --scale-mode small --root .`.
3. Para reproducir full en Colab/GCP, usar las rutas temporales documentadas en el notebook y no escribir Parquet masivo en Google Drive.
4. Revisar `evidence/run_summary.json` y `evidence/execution_steps.json` para confirmar estado final.
5. Si full falla por recursos, no bajar escala para reportar exito; documentar etapa fallida y migrar a Dataproc/Spark Serverless.
