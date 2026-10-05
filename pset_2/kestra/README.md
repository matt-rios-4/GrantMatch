# Kestra (ROL 1)
UI: http://localhost:8080 (usuario y clave en `KESTRA_USER` / `KESTRA_PASSWORD` del `.env`).
Los `.yml` de `kestra/flows/` los importa el servicio `kestra-deploy` del compose (namespace `pset2`) en cada `docker compose up`; tras editar uno: `docker compose up kestra-deploy`.
Credenciales en los flows: `{{ envs.snowflake_user }}`, `{{ envs.snowflake_password }}`, … (nunca hardcodear).

| Flow | Qué hace |
|---|---|
| `00_smoke_test_snowflake.yml` | Comprueba la conexión a Snowflake |
| `01_ingest_openalex_bronze.yml` | OpenAlex S3 → BRONZE (`COPY INTO`) → `dbt build` → OBT de Spark. Trigger diario, works incremental por watermark, retry exponencial, backfill por rango y `concurrency: 1` |

Detalle en [docs/ingesta_kestra.md](../docs/ingesta_kestra.md).

Nombra los archivos `NN_<id>.yml`. La sincronización por `micronaut.io.watch` se quitó: en Kestra 1.3 no cargaba los flows y escribía copias (`main_*.yml`, `NN_pset2_*.yml`) en esta carpeta; `.gitignore` las sigue excluyendo por si alguien la reactiva.
