# dbt: Silver y Gold de "Subvenciones con Ruta" [ROL 2]

Transforma los datos crudos de OpenAlex que Kestra carga en `BRONZE` en una capa limpia (`SILVER`) y en un esquema estrella (`GOLD`) que lee Spark para construir la OBT.

## Cómo correrlo

```bash
docker compose exec dbt dbt deps                 # instala dbt_utils (una vez)
docker compose exec dbt dbt debug                # verifica la conexión a Snowflake
docker compose exec dbt dbt build                # modelos + tests, en orden de dependencias
docker compose exec dbt dbt build --full-refresh # reconstruye todo desde Bronze
```

Perfil de calidad para el memo (escanea Bronze completo; córrelo una vez después de la carga):

```bash
docker compose exec dbt dbt compile --select data_quality_profile
# pegar target/compiled/pset2/analyses/data_quality_profile.sql en un worksheet de Snowflake
```

## Requisitos de Bronze

Las cuatro tablas `BRONZE.RAW_OPENALEX_{AWARDS,WORKS,FUNDERS,TOPICS}` deben existir, con las columnas `RAW VARIANT`, `_SOURCE_FILE`, `_SOURCE_ROW_NUMBER` y `_LOADED_AT`. El detalle está en [docs/CONTRATO_BRONZE.md](docs/CONTRATO_BRONZE.md).

## Modelos

| Capa | Modelo | Grain | Materialización |
|---|---|---|---|
| silver | `slv_awards_history` | award + versión | incremental · **append** |
| silver | `slv_works_history` | work + versión | incremental · **append** |
| silver | `slv_awards` | award (versión actual) | incremental · merge |
| silver | `slv_works` | work (versión actual) | incremental · merge |
| silver | `slv_work_authorships` | work + autor | incremental · delete+insert |
| silver | `slv_work_awards` | work + award | incremental · delete+insert |
| silver | `slv_funders`, `slv_topics` | funder / topic | table (catálogos pequeños) |
| gold | `fact_awards` | **un award otorgado** | incremental · merge |
| gold | `fact_award_works` | **una publicación financiada por un award** | incremental · delete+insert |
| gold | `brg_work_author` | work + autor | incremental · delete+insert |
| gold | `dim_award`, `dim_work`, `dim_author`, `dim_institution` | una entidad | incremental · merge |
| gold | `dim_funder`, `dim_topic`, `dim_date` | una entidad / un día | table |

**Por qué no todo es append.** Append se usa donde el grain es un evento inmutable: una versión de un award o de un work. En las tablas de estado actual, append duplicaría cada registro que cambia, así que esas usan merge o delete+insert. Ambas estrategias son incrementales y solo procesan lo que llegó en la última carga (filtro `_loaded_at > max(_loaded_at)`). La justificación completa está en el encabezado de cada `.sql` y en [docs/memo_secciones_3_4.md](docs/memo_secciones_3_4.md).

## Estructura

```
dbt/
├── dbt_project.yml        # esquemas silver/gold, append por defecto, vars de fechas
├── packages.yml           # dbt_utils
├── macros/                # generate_schema_name (evita SILVER_GOLD), oa_id, clean_text, ...
├── models/bronze/         # solo source() de las tablas que carga Kestra
├── models/silver/         # limpieza + schema.yml (tests)
├── models/gold/           # star schema + schema.yml (tests)
├── tests/                 # tests singulares: grain de fact_awards, amount_usd solo en USD
└── analyses/              # data_quality_profile.sql: métricas de la Sección 3
```
