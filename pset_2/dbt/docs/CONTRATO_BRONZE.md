# Contrato de interfaces: Bronze (Kestra) → Silver (dbt)

Este archivo dice qué espera dbt de lo que carga Kestra. Si algo de aquí cambia, avisen al rol de dbt antes de hacer merge.

## Tablas que Kestra debe crear y llenar

Base `PSET2_DB`, esquema `BRONZE`. Las cuatro tablas deben existir aunque estén vacías, porque dbt las lee con `source()` y falla si una no existe.

| Tabla | Carpeta de origen en S3 | Volumen aproximado |
|---|---|---|
| `RAW_OPENALEX_AWARDS` | `s3://openalex/data/parquet/awards/` | ~17 M de awards |
| `RAW_OPENALEX_FUNDERS` | `s3://openalex/data/parquet/funders/` | ~30 mil |
| `RAW_OPENALEX_TOPICS` | `s3://openalex/data/parquet/topics/` | 4.516 |
| `RAW_OPENALEX_WORKS` | `s3://openalex/data/parquet/works/` | subconjunto de particiones (ver costo) |

Las cuatro tablas tienen las mismas columnas:

```sql
CREATE TABLE IF NOT EXISTS PSET2_DB.BRONZE.RAW_OPENALEX_AWARDS (
    RAW                 VARIANT        NOT NULL,  -- registro original completo, tal como viene en el Parquet
    _SOURCE_FILE        VARCHAR        NOT NULL,  -- METADATA$FILENAME (incluye updated_date=YYYY-MM-DD)
    _SOURCE_ROW_NUMBER  NUMBER(38,0),             -- METADATA$FILE_ROW_NUMBER
    _LOADED_AT          TIMESTAMP_LTZ  NOT NULL   -- momento de la carga
);
-- Igual para RAW_OPENALEX_FUNDERS, RAW_OPENALEX_TOPICS y RAW_OPENALEX_WORKS.
```

## Por qué una sola columna VARIANT

- **Conserva el dato original**, como pide el PSet para Bronze: nada se pierde ni se reinterpreta al cargar.
- **Aguanta cambios de esquema.** OpenAlex agrega campos seguido (la entidad `awards` es de 2025). Con columnas fijas, un campo nuevo rompería el `COPY INTO`.
- **Las listas y objetos anidados** (`authorships`, `funded_outputs`, `institution_awarded`, `abstract_inverted_index`) quedan intactos para que Silver los aplane con `LATERAL FLATTEN`.

## Ejemplo de carga

```sql
CREATE STAGE IF NOT EXISTS PSET2_DB.BRONZE.OPENALEX_STAGE
  URL = 's3://openalex/data/parquet/'          -- bucket público: no requiere credenciales
  FILE_FORMAT = (TYPE = PARQUET);

COPY INTO PSET2_DB.BRONZE.RAW_OPENALEX_AWARDS (RAW, _SOURCE_FILE, _SOURCE_ROW_NUMBER, _LOADED_AT)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @PSET2_DB.BRONZE.OPENALEX_STAGE/awards/
)
PATTERN = '.*updated_date={{ trigger.date | date("yyyy-MM-dd") }}/.*[.]parquet'
ON_ERROR = ABORT_STATEMENT;
```

- **Backfill:** cada ejecución del backfill recibe su fecha en `trigger.date` y carga solo esa partición `updated_date`.
- **Reintentos:** `COPY INTO` no vuelve a cargar un archivo que ya cargó, gracias a los metadatos de carga de Snowflake. Por eso un reintento no duplica filas. **No usen `FORCE = TRUE`.** Si alguna vez se recarga un archivo, Silver descarta la copia (métricas A02 y W02 del perfil de calidad).
- **Works:** el snapshot completo pesa cientos de GB. Para la cuenta de prueba, carguen un rango acotado de particiones y anoten cuál fue para la sección de Limitaciones.

## Qué entrega dbt a Spark (OBT)

Esquema `PSET2_DB.GOLD`:

- **Hechos:** `FACT_AWARDS`, `FACT_AWARD_WORKS`
- **Puente:** `BRG_WORK_AUTHOR`
- **Dimensiones:** `DIM_AWARD`, `DIM_FUNDER`, `DIM_TOPIC`, `DIM_INSTITUTION`, `DIM_WORK`, `DIM_AUTHOR`, `DIM_DATE`

Las llaves son los IDs cortos de OpenAlex (`G…`, `F…`, `T…`, `I…`, `W…`, `A…`). Los valores sin referencia válida van a `'UNKNOWN'`, o a `-1` en las fechas.
