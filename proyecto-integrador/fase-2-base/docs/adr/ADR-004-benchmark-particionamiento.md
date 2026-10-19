# ADR-004: Benchmark de particionamiento y lectura critica

## Estado
Aceptado

## Contexto
La Fase 2 exige comparar una version base sin particionamiento contra una version optimizada. La evidencia full adjunta muestra que la lectura particionada fue mas lenta para el filtro selectivo medido.

## Decision
Mantener el benchmark base versus particionado por anio, mes, zona_gestion y tipo_evento, reportando el resultado negativo como evidencia tecnica.

## Alternativas consideradas
- Omitir benchmark: incumple el enunciado.
- Ajustar filtros hasta obtener mejora: riesgoso si no representa uso real.
- Reportar solo escritura: insuficiente para analizar consumo analitico.

## Consecuencias
El benchmark ensena que particionar tiene costo y que su beneficio depende de volumen, selectividad, numero de archivos y patron de consulta. En este experimento full no fue favorable; debe revalidarse con otros filtros y en GCP.
