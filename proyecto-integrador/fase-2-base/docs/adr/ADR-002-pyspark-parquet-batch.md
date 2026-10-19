# ADR-002: PySpark y Parquet para procesamiento batch

## Estado
Aceptado

## Contexto
El enunciado exige demostrar Big Data con al menos 10 millones de filas o 5 GB en Bronze. El pipeline debe ser reproducible en Colab/GCP y no depender de pandas.

## Decision
Usar PySpark como motor distribuido y Parquet con compresion Snappy como formato de almacenamiento.

## Alternativas consideradas
- pandas: descartado porque no representa una solucion Big Data.
- CSV: util para intercambio, pero ineficiente para lectura selectiva y tipos.
- Delta/Iceberg: valiosos para produccion, pero agregan dependencias no necesarias para el alcance academico.

## Consecuencias
PySpark permite generacion distribuida con `spark.range`, transformaciones escalables y particionamiento. Parquet reduce I/O y habilita analitica posterior. Requiere controlar particiones, memoria y escritura temporal.
