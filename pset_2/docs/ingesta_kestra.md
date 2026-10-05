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
| `works` | `…/works/` | Partición diaria, **~136 GB por día** | Incremental por watermark: las particiones nuevas desde la última cargada, como máximo `works_max_days` días (default 7) y `works_files_per_day` archivos por día (default 2, ~1.8 GB) |

Cada fila de Bronze es un registro de un archivo Parquet. Guarda el registro original en `RAW` (VARIANT) y además `_SOURCE_FILE`, `_SOURCE_ROW_NUMBER` y `_LOADED_AT`. Es el esquema del [contrato con dbt](../dbt/docs/CONTRATO_BRONZE.md).

## Proceso de carga

1. **`setup_stage`** y **`setup_tables`** crean el stage externo sobre el bucket público y las 4 tablas del contrato. Son idempotentes, así que un clon nuevo del repo corre sin pasos manuales en Snowflake, salvo `docs/snowflake_setup.sql`.
2. **`load_catalogs`** hace un `COPY INTO` de topics, funders y awards con `PATTERN = '.*[.]parquet'`. Snowflake guarda qué archivos ya cargó cada tabla y no los repite (`FORCE` queda en su default, `FALSE`).
3. **`snapshot`** lee la fecha del último snapshot publicado en `works/manifest.json` (por ejemplo, `2026-09-23`).
4. **`works_plan`** calcula qué particiones `updated_date` faltan. Desde el día siguiente a la última partición ya cargada en Bronze (el *watermark*, sacado de `_SOURCE_FILE`) hasta la fecha del snapshot, con un tope de `works_max_days` días. Con Bronze vacío, empieza en el snapshot (`works_initial_days`).
5. **`load_works`** hace un `COPY INTO` de los primeros `works_files_per_day` archivos de cada partición planificada (`PATTERN = '.*updated_date=(d1|d2|…)/part_000[0-N][.]parquet'`). Si no hay particiones nuevas, lo registra y sigue.
6. **`load_summary`** registra las filas nuevas y totales por tabla.
7. **`transformations`** ejecuta `dbt build` y después el job de la OBT (`run_obt.sh`) con `docker exec` sobre los contenedores del compose. Se puede apagar con `run_transformations = false`.

`ON_ERROR = ABORT_STATEMENT`: si un archivo falla, el `COPY` completo se revierte y no deja cargas parciales.

## Frecuencia

Trigger `daily` (`Schedule`, `0 6 * * *`, 06:00 UTC), por dos razones:
- OpenAlex publica **snapshots completos periódicos**, no un archivo por día. Al 4-oct-2026 el último era del 23-sep, y todos sus archivos tienen esa fecha.
- El caso de uso (recomendar financiamiento) tolera días de latencia. Ver memo §6.

Los días sin snapshot nuevo, la corrida termina en segundos ("works al día"). Cuando aparece uno, el watermark carga las particiones nuevas en una o varias corridas, según el tope `works_max_days`, sin huecos ni intervención manual.

Además, el trigger usa `recoverMissedSchedules: LAST`: si Kestra estuvo apagado, recupera solo la última fecha, y el watermark se encarga de lo demás. El flow tiene `concurrency: 1`, así que una corrida manual y la del schedule nunca cargan en paralelo. **El trigger debe quedar activo en una sola instancia de Kestra:** si varios integrantes levantan el stack con el mismo `.env`, que lo desactive todo el mundo menos uno (UI → Triggers → `daily` → Disable).

## Errores y reintentos

- **Reintentos:** todas las tareas de Snowflake heredan de `pluginDefaults` un retry exponencial: 30 s, 60 s, 120 s…, con un tope de 10 min entre intentos (`maxInterval`) y 5 intentos en total. Cubre fallas transitorias de red, de Snowflake (429/503) o de un warehouse que se está reanudando. Está verificado con una conexión inválida a propósito: la tarea reintenta con espera creciente hasta agotar los intentos y después corre el bloque `errors`.
- **Jitter:** el retry exponencial de Kestra (1.3.37) no tiene parámetro de jitter (rechaza el campo `jitter`). El jitter que pide el diseño del equipo lo aporta el driver JDBC de Snowflake, que reintenta sus llamadas HTTP con backoff de jitter decorrelacionado (`DecorrelatedJitterBackoff`). Kestra agrega encima el backoff exponencial a nivel de tarea.
- **Bloque `errors`:** cuando una tarea agota sus intentos, deja un log de nivel ERROR con la ejecución. Ahí se puede conectar una alerta, por ejemplo un webhook de Slack.
- **Idempotencia:** reintentar o re-ejecutar no duplica datos, porque `COPY INTO` salta archivos ya cargados. Si un archivo se recargara (por ejemplo, porque OpenAlex lo reescribió), Silver descarta la copia exacta (métrica A02 del perfil de calidad).

## Backfill

- **Históricos de works:** Execute con `backfill_from` y `backfill_to` (particiones `updated_date`, por ejemplo `2026-08-01` → `2026-08-31`). Carga hasta `works_max_days` días por ejecución; si el rango es más largo, el log lo avisa y basta con volver a ejecutar desde el día siguiente. Un backfill no mueve el watermark hacia atrás.
- **Particiones nuevas:** no necesitan backfill. El watermark recupera solo todo lo publicado desde la última partición cargada.
- **Catálogos:** cada corrida carga los archivos que todavía no están en Bronze.
- El *Backfill* de la UI de Kestra (Triggers → `daily` → Backfill) también funciona, pero con el watermark cada fecha re-ejecuta la misma puesta al día, sin efecto adicional. Para históricos, usa el rango.


## Limitaciones conocidas

- **Recarga de catálogos:** cada snapshot nuevo de OpenAlex reescribe todos sus archivos, así que `COPY INTO` vuelve a cargar los catálogos completos (~17 M de awards por snapshot). Silver descarta las copias exactas (métrica A02), pero se paga ese cómputo una vez por snapshot.
- **Tope de archivos por partición:** se cargan `part_0000`…`part_000N` (máximo 10). Una partición real tiene hasta 160 archivos, así que works es una **muestra** de cada día y no el día completo.
- **Jitter:** el retry de Kestra no lo tiene (ver arriba).
- **Borrados de OpenAlex:** `deleted_ids.csv.gz` no se procesa (ver memo §7).

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
| Primera corrida completa (partición 2026-09-23, 2 archivos de works) | `SUCCESS`. Cargas: topics 4 516 filas (3 s), funders 45 661 (5 s), awards 17 139 262 (1 min 34 s), works 558 403 (1 min 18 s). Después, `dbt build` PASS=127 y OBT con 313 264 filas = `FACT_AWARD_WORKS` (muestra inicial de una sola partición; la OBT final, tras el backfill, tiene 1 882 074 filas) |
| Re-ejecución sobre lo ya cargado | 0 filas nuevas en las 4 tablas, en 15 s: `COPY INTO` no recarga archivos |
| Modo automático (watermark) sin snapshot nuevo | Última partición cargada = snapshot (2026-09-23): termina en "works al día", con 0 filas y en 15 s |
| Backfill `backfill_from=2026-09-20`, `backfill_to=2026-09-21` (1 archivo por día) | Plan de 2 particiones y 754 922 works nuevos en 1 min 35 s; `dbt build` incremental PASS=127 en 2 min 34 s. El paso de Spark de esta corrida falló por memoria (fila siguiente); con el executor de 2g (#5) la OBT quedó en 1 882 074 filas = `FACT_AWARD_WORKS` |
| Falla real (Spark sin memoria con executor de 1g, antes de #5) | La tarea `spark_obt` falla, el flow queda en `FAILED` y corre el bloque `errors` |
| Reintentos | Flow de prueba con la misma política de retry ([`prueba_reintentos.yml`](evidencia_kestra/prueba_reintentos.yml)) contra una tabla inexistente: 5 intentos con esperas de ~30, 60, 120 y 240 s (7 min 50 s en total) y después corre `errors` ([capturas](evidencia_kestra/)) |
| `docker compose up` desde cero | `kestra-deploy` despliega los flows, rechaza (exit 1) un flow inválido e ignora copias `main_*.yml` |
