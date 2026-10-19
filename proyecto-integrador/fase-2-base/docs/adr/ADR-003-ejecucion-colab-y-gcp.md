# ADR-003: Ejecucion reproducible en Colab con ruta a GCP administrado

## Estado
Aceptado

## Contexto
El proyecto debia demostrar escala Big Data sin reducir artificialmente el perfil full. Despues de varios intentos, la ejecucion full pudo completarse con evidencia controlada.

## Decision
Mantener el notebook preparado para full, ejecutar el perfil completo sin reducir filas y conservar evidencia JSON/CSV de la corrida. La ruta GCP sigue recomendada para operacion repetible y gobernada.

## Alternativas consideradas
- Reducir full para hacerlo pasar: descartado por falta de honestidad tecnica.
- Forzar ejecucion local: descartado por memoria/disco limitados.
- Migrar a Dataproc inmediatamente: recomendado para produccion, pero no fue indispensable para obtener la evidencia academica full.

## Consecuencias
La evidencia full demuestra cumplimiento de escala y reproducibilidad end-to-end. La recomendacion de GCP queda como evolucion operacional, no como excusa por falta de evidencia.
