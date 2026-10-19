# ADR-001: Arquitectura lakehouse medallion

## Estado
Aceptado

## Contexto
El caso municipal combina solicitudes ciudadanas, eventos operacionales, logs, clima, cuadrillas, fotografias metadata y satisfaccion. La solucion requiere separar datos crudos, datos curados y datasets de consumo analitico.

## Decision
Adoptar una arquitectura lakehouse medallion con capas Bronze, Silver y Gold, persistidas en Parquet.

## Alternativas consideradas
- Una unica tabla analitica: simple, pero poco trazable y fragil ante cambios.
- Base relacional tradicional: conocida, pero menos adecuada para datos semiestructurados y volumen operacional.
- Data lake sin capas: flexible, pero con mayor riesgo de desorden y baja gobernabilidad.

## Consecuencias
La separacion mejora trazabilidad, calidad y gobierno. El costo es mayor complejidad operacional y necesidad de documentar contratos entre capas.
