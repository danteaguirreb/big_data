# ADR-005: Gobierno, seguridad y datos sinteticos

## Estado
Aceptado

## Contexto
El caso simula informacion ciudadana municipal. Aunque los datos son sinteticos, el diseno debe considerar privacidad, roles y trazabilidad.

## Decision
Usar datos sinteticos, identificadores hasheados, linaje manual, separacion por capas y RBAC simulado por rol.

## Alternativas consideradas
- Datos reales municipales: descartados por privacidad y autorizacion.
- Sin controles de acceso: no cumple buenas practicas ni el enunciado.
- Anonimizacion posterior: menos segura que diseñar desde el origen con datos sinteticos.

## Consecuencias
La solucion es segura para entrega academica y deja una ruta clara hacia IAM, KMS, Cloud Logging y auditoria en GCP.
