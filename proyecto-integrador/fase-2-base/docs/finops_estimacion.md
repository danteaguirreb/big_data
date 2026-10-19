# Estimacion FinOps para evolucion GCP

No se fijan precios exactos porque deben consultarse en la calculadora oficial de Google Cloud al momento de la entrega. Se dejan supuestos y formulas editables.

## Supuestos editables

| Variable | Descripcion | Valor editable |
|---|---|---:|
| `gb_parquet_bronze` | GB almacenados en Bronze | 25 |
| `gb_parquet_silver` | GB almacenados en Silver | 18 |
| `gb_parquet_gold` | GB almacenados en Gold | 4 |
| `ejecuciones_mensuales` | Corridas batch mensuales | 30 |
| `duracion_job_horas` | Duracion promedio del job Spark | 0.75 |
| `nodos_dataproc` | Nodos equivalentes usados por corrida | 3 |
| `gb_leidos_bigquery_por_consulta` | GB leidos por consulta analitica | 5 |
| `consultas_mensuales` | Consultas mensuales BigQuery | 300 |
| `dashboards_mensuales` | Vistas mensuales Looker Studio | 500 |

## Formulas

Cloud Storage:

```text
costo_storage = (gb_parquet_bronze + gb_parquet_silver + gb_parquet_gold) * precio_gb_mes_storage
```

Dataproc o Serverless Spark:

```text
costo_batch = ejecuciones_mensuales * duracion_job_horas * nodos_dataproc * precio_nodo_hora
```

BigQuery:

```text
costo_bigquery = gb_leidos_bigquery_por_consulta * consultas_mensuales * precio_gb_procesado
```

Looker Studio:

```text
costo_dashboard = costo_conectores_o_licencias + costo_bigquery_derivado_de_actualizaciones
```

## Recomendaciones FinOps

- Mantener Gold pequeno y agregado para dashboard.
- Particionar Silver/Gold por fecha y zona para reducir lectura.
- Usar politicas de ciclo de vida en Bronze historico.
- Medir costo por corrida y costo por KPI publicado.
- Separar ambientes academico, prueba y produccion futura.
