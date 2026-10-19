# Proyecto Integrador: Gestión Territorial Inteligente de Solicitudes Ciudadanas

Entrega final de la cuarta iteración de la Fase 3 del curso **Big Data y Cloud Computing**, Magíster en Data Science, Universidad del Desarrollo. El proyecto diseña una arquitectura lakehouse para apoyar la gestión territorial de solicitudes ciudadanas de referencia municipal.

## Equipo

Grupo 2: Sofía Alanís, Osvaldo Neira, Jaime Sandoval y Dante Aguirre.

## Entregables finales

- [Informe final de Fase 3](entregables/fase-3-cuarta-iteracion/informe-final-fase-3.pdf)
- [Presentación final](entregables/fase-3-cuarta-iteracion/presentacion-final.pdf)

## Código y trazabilidad

La carpeta [`fase-2-base/`](fase-2-base/) conserva el paquete técnico PySpark incluido en la entrega final como base de implementación. Su README y sus rutas internas mantienen explícito que corresponde a la Fase 2; los documentos de Fase 3 recogen la documentación técnica y la evaluación final.

El pipeline genera **datos sintéticos** y desarrolla las capas Bronze, Silver y Gold, escritura Parquet, métricas de ejecución, transformaciones, gobierno, linaje y estimación FinOps. No se incorporan datos reales de ciudadanos ni salidas masivas de Spark.

## Reproducción

Consulta los requisitos y pasos de ejecución en [`fase-2-base/README.md`](fase-2-base/README.md). La configuración `full` puede requerir recursos de cómputo elevados; los resultados finales y sus limitaciones deben interpretarse según el informe de Fase 3.

## Alcance de la publicación

Este repositorio contiene una selección de código y entregables académicos finales. Omite datasets y archivos Parquet generados, archivos temporales y copias intermedias.
