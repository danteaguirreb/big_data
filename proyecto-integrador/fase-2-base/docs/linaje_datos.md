# Linaje de datos

| Tabla origen | Transformacion aplicada | Tabla destino | Responsable logico | Proposito | Controles de calidad |
|---|---|---|---|---|---|
| solicitudes_raw | Deduplicacion, validacion de nulos, fechas y coordenadas | solicitudes_enriquecidas | Data Engineering | Solicitudes confiables para analisis | ID no nulo, rango fecha, rango lat/lon |
| eventos_raw | Normalizacion, particion por fecha/zona/evento, calculo entre eventos | eventos_normalizados | Data Engineering | Ciclo de vida operacional | ID evento unico, timestamp valido |
| logs_raw | Deduplicacion y tipificacion basica | logs_normalizados | Plataforma | Auditoria tecnica simulada | ID log unico, nivel valido |
| fotografias_metadata_raw | Deduplicacion y agregacion por solicitud | solicitudes_enriquecidas | Operaciones municipales | Evidencia fotografica simulada | Hash archivo, flag validada |
| clima_raw | Join por fecha y zona | solicitudes_enriquecidas | Analitica territorial | Contexto climatico | Fecha/zona unica |
| cuadrillas_raw | Join por zona y unidad | solicitudes_enriquecidas | Operaciones municipales | Capacidad operativa | Unidad/zona unica |
| satisfaccion_raw | Join con solicitudes y agregacion | satisfaccion_ciudadana | Experiencia ciudadana | Medir satisfaccion | Nota entre 1 y 7 |
| solicitudes_enriquecidas | Agregaciones SLA, backlog, criticidad | kpi_sla, backlog_operacional, zonas_criticas | Analitica municipal | Indicadores de gestion | Conteos y porcentajes consistentes |
| solicitudes_enriquecidas | Seleccion de variables sin leakage | dataset_modelo_priorizacion | Ciencia de datos | Modelo auxiliar de priorizacion | Variables disponibles al ingreso |
