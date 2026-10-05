# Evidencia de Kestra

Capturas de la UI de Kestra 1.3.37 con las ejecuciones del flow `pset2.ingest_openalex_bronze` en la
cuenta del equipo (4-oct-2026, horas en UTC). El detalle de cada prueba está en
[../ingesta_kestra.md](../ingesta_kestra.md#evidencia-cuenta-del-equipo-4-oct-2026-warehouse-x-small).

| Captura | Qué muestra |
|---|---|
| [01_ejecuciones.png](01_ejecuciones.png) | Las ejecuciones del flow. La del ícono de calendario la disparó el trigger `daily`; las demás son manuales (carga, re-ejecución sin filas nuevas y backfill). |
| [02_backfill_gantt.png](02_backfill_gantt.png) | Backfill `2026-09-20` a `2026-09-21`: carga de works y `dbt build` en verde. Spark falló por memoria (executor de 1g, corregido en #5), el flow quedó `FAILED` y corrió el bloque `errors` (`report_failure`). |
| [03_trigger_diario.png](03_trigger_diario.png) | El trigger `Schedule` diario (06:00 UTC) habilitado, con su próxima ejecución, y los logs del bloque `errors`. |
| [04_reintentos_gantt.png](04_reintentos_gantt.png) | Prueba de reintentos: la tarea falla durante 7 min 50 s, entre intentos, y después corre `errors`. |
| [05_reintentos_logs.png](05_reintentos_logs.png) | Logs de la prueba: el intento 5 falla y `report_failure` registra que se agotaron los 5 intentos. |

La prueba de reintentos usa [prueba_reintentos.yml](prueba_reintentos.yml): la misma política de retry
del flow de ingesta (`exponential`, 30 s, factor 2, tope de 10 min, 5 intentos) contra una tabla que no
existe, para forzar la falla sin tocar datos. Los intentos empezaron a las 03:18:01, 03:18:41, 03:19:43,
03:21:46 y 03:25:47 UTC. No se despliega con `kestra-deploy` porque no está en `kestra/flows/`.
