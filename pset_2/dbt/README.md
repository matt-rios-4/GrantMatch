# dbt (ROL 2)
Proyecto dbt Core que construye Silver y Gold sobre Snowflake. Bronze lo carga Kestra.

```bash
docker compose exec dbt dbt debug     # verifica conexion
docker compose exec dbt dbt run
docker compose exec dbt dbt test
docker compose exec dbt dbt build --select silver+
```
Reglas: `source()`/`ref()`, materializacion `incremental` + `append`, tests `not_null`/`unique`/`relationships`.
