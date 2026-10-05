# Ingesta con Kestra (Rol 1)

Flow: [`kestra/flows/01_ingest_openalex_bronze.yml`](../kestra/flows/01_ingest_openalex_bronze.yml), namespace `pset2`, id `ingest_openalex_bronze`.

```
S3 público de OpenAlex ──COPY INTO──▶ BRONZE.RAW_OPENALEX_* ──▶ dbt build (SILVER, GOLD) ──▶ Spark (OBT)
   (Parquet, sin credenciales)          (registro original en RAW)     contenedor pset2-dbt          pset2-spark-master
```

## Fuente y granularidad

| Entidad | Ruta en S3 | Granularidad | Qué se carga |
|---|---|---|---|
| `topics` | `s3://openalex/data/parquet/topics/` | Catálogo (4 516) | Completo |
| `funders` | `…/funders/` | Catálogo (~46 mil) | Completo |
| `awards` | `…/awards/` | Partición diaria `updated_date=AAAA-MM-DD`, ~5 GB en total | Completo |
| `works` | `…/works/` | Partición diaria, **~136 GB por día** | Una partición por corrida, como máximo `works_files_per_day` archivos (default 2, ~1.8 GB) |

Cada fila de Bronze es un registro de un archivo Parquet. Guarda el registro original en `RAW` (VARIANT) y además `_SOURCE_FILE`, `_SOURCE_ROW_NUMBER` y `_LOADED_AT`. Es el esquema del [contrato con dbt](../dbt/docs/CONTRATO_BRONZE.md).

## Proceso de carga

1. **`works_partition`** calcula qué partición de works toca: `partition_date`, o bien la fecha del trigger menos `works_lag_days`.
2. **`setup_stage`** y **`setup_tables`** crean, si no existen, el stage externo sobre el bucket público y las 4 tablas del contrato. Son idempotentes, así que un clon nuevo del repo corre sin pasos manuales en Snowflake, salvo `docs/snowflake_setup.sql`.
3. **`load_catalogs`** hace un `COPY INTO` de topics, funders y awards con `PATTERN = '.*[.]parquet'`. La primera corrida carga todo; las siguientes solo agregan archivos nuevos, porque Snowflake guarda qué archivos ya cargó cada tabla y no los repite (`FORCE` queda en su default, `FALSE`).
4. **`load_works`** hace un `COPY INTO` de works con `PATTERN = '.*updated_date=<fecha>/part_000[0-N][.]parquet'`.
5. **`load_summary`** registra las filas nuevas y totales por tabla.
6. **`transformations`** ejecuta `dbt build` y después el job de la OBT (`run_obt.sh`) con `docker exec` sobre los contenedores del compose. Se puede apagar con `run_transformations = false`.

`ON_ERROR = ABORT_STATEMENT`: si un archivo falla, el `COPY` completo se revierte y no deja cargas parciales.

## Frecuencia

Trigger `daily` (`Schedule`, `0 6 * * *`, 06:00 UTC), por dos razones:
- OpenAlex publica particiones diarias.
- El caso de uso (recomendar financiamiento) tolera días de latencia. Ver memo §6.

Además, OpenAlex publica su snapshot con unos 11 días de retraso (al 4-oct-2026, la última partición era la del 23-sep). Por eso works se carga con un desfase de 14 días (`works_lag_days`); con un desfase menor, la mayoría de las corridas no encontraría archivos.

## Errores y reintentos

- **Reintentos:** todas las tareas de Snowflake heredan de `pluginDefaults` un retry exponencial: 30 s, 60 s, 120 s…, con un tope de 10 min entre intentos (`maxInterval`) y 5 intentos en total. Cubre fallas transitorias de red, de Snowflake (429/503) o de un warehouse que se está reanudando. Está verificado: con una conexión inválida la tarea hace 3 intentos con espera creciente y después falla.
- **Jitter:** el retry exponencial de Kestra (1.3.37) no tiene parámetro de jitter (rechaza el campo `jitter`). El jitter que pide el diseño del equipo lo aporta el driver JDBC de Snowflake, que reintenta sus llamadas HTTP con backoff de jitter decorrelacionado (`DecorrelatedJitterBackoff`). Kestra agrega encima el backoff exponencial a nivel de tarea.
- **Bloque `errors`:** cuando una tarea agota sus intentos, deja un log de nivel ERROR con la ejecución y la partición. Ahí se puede conectar una alerta, por ejemplo un webhook de Slack.
- **Idempotencia:** reintentar o re-ejecutar no duplica datos, porque `COPY INTO` salta archivos ya cargados. Si un archivo se recargara (por ejemplo, porque OpenAlex lo reescribió), Silver descarta la copia exacta (métrica A02 del perfil de calidad).

## Backfill

- **Por rango de fechas:** en la UI, Flows → `ingest_openalex_bronze` → Triggers → `daily` → **Backfill**, y elige el rango. Kestra crea una ejecución por fecha y cada una carga su partición de works (fecha − `works_lag_days`).
- **Una partición puntual:** Execute con `partition_date = AAAA-MM-DD` (y `works_lag_days` se ignora). Sirve también para recuperar una fecha en la que la partición todavía no estaba publicada (`load_summary` muestra 0 filas nuevas en WORKS).
- **Catálogos:** no necesitan backfill: cada corrida carga todos los archivos que todavía no están en Bronze.

## Verificación rápida en Snowflake

```sql
-- Filas y archivos por tabla
SELECT 'AWARDS' t, COUNT(*) filas, COUNT(DISTINCT _SOURCE_FILE) archivos FROM PSET2_DB.BRONZE.RAW_OPENALEX_AWARDS
UNION ALL SELECT 'WORKS', COUNT(*), COUNT(DISTINCT _SOURCE_FILE) FROM PSET2_DB.BRONZE.RAW_OPENALEX_WORKS;

-- Historial de cargas de una tabla (últimos 14 días)
SELECT file_name, status, row_count, last_load_time
FROM TABLE(PSET2_DB.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'PSET2_DB.BRONZE.RAW_OPENALEX_WORKS',
  START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;
```

## Evidencia (cuenta del equipo, 4-oct-2026, warehouse X-Small)

| Prueba | Resultado |
|---|---|
| Primera corrida completa (`partition_date = 2026-09-23`, 2 archivos de works) | `SUCCESS`. Cargas: topics 4 516 filas (3 s), funders 45 661 (5 s), awards 17 139 262 (1 min 34 s), works 558 403 (1 min 18 s). Después, `dbt build` PASS=127 y OBT con 313 264 filas = `FACT_AWARD_WORKS` |
| Re-ejecución de la misma partición | 0 filas nuevas en las 4 tablas, en 15 s: `COPY INTO` no recarga archivos |
| Backfill de un día (trigger `daily`, 3-oct) | `trigger.date = 2026-10-03`. Carga la partición de works 2026-09-19: 669 102 filas nuevas en 1 min 40 s. El trigger retoma su calendario al terminar |
| Reintentos | Con una conexión inválida, la tarea hace 3 intentos con espera exponencial y después corre el bloque `errors` |
