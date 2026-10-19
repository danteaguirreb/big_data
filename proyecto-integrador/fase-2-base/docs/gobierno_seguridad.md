# Gobierno y seguridad

## Naturaleza de los datos

El proyecto usa datos sinteticos. No incorpora datos personales reales, fotografias reales ni direcciones exactas de vecinos.

## Datos sensibles

- `ciudadano_hash` representa un identificador ciudadano anonimizado mediante hash SHA-256.
- Las rutas de fotografias son simuladas y no apuntan a imagenes reales.
- Las coordenadas son sinteticas y solo sirven para demostrar validaciones y segmentacion territorial.

## Separacion por capas

- Bronze: conserva datos crudos sinteticos y requiere acceso tecnico restringido.
- Silver: contiene datos normalizados, validados y enriquecidos.
- Gold: contiene agregados para indicadores, dashboard y toma de decisiones.

## RBAC simulado

| Rol | Acceso |
|---|---|
| Administrador tecnico | Bronze, Silver y Gold |
| Analista de datos | Silver y Gold |
| Supervisor municipal | Gold y dashboard |
| Funcionario operativo | Solo solicitudes asignadas |
| Autoridad municipal | Indicadores agregados |

## Principios

- Minimo privilegio.
- Separacion de responsabilidades.
- Trazabilidad manual de origen y destino.
- Evitar exposicion innecesaria de Bronze.
- Publicar dashboards desde Gold agregado.

## Evolucion futura

- IAM por grupos y service accounts.
- Cifrado en reposo con Cloud KMS cuando aplique.
- Cifrado en transito con configuraciones administradas de GCP.
- Auditoria con Cloud Audit Logs.
- Monitoreo con Cloud Logging y Cloud Monitoring.
- Politicas de retencion y ciclo de vida en Cloud Storage.
