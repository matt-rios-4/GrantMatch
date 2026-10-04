# Ingesta de OpenAlex a Snowflake BRONZE con Kestra

## Alcance y estado

Flujo de datos: OpenAlex S3 → Kestra → Snowflake `PSET2_DB.BRONZE` → dbt
SILVER/GOLD → Spark OBT. Este documento cubre la ingesta Bronze.

Objetos conocidos:

- Stage authors: `PSET2_DB.BRONZE.OPENALEX_PARQUET_STAGE`.
- Stage works: `PSET2_DB.BRONZE.OPENALEX_WORKS_STAGE`.
- File format: `PSET2_DB.BRONZE.OPENALEX_PARQUET_FORMAT`.
- Tabla authors: `PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS`.
- En el preflight actual (2026-10-04), authors tiene 104,067 filas de un solo
  archivo; `LIST` reporta 503 Parquet, por lo que 502 no están en
  `_SOURCE_FILE`.
- El contexto de sesión verificado fue cuenta `CE74715`, usuario
  `MAATEONICOLAS`, rol `PSET2_ROLE`, warehouse `PSET2_WH` y base `PSET2_DB`.
- El stage WORKS apunta a `s3://openalex/data/works/`; el `LIST` actual no
  devuelve objetos y `RAW_OPENALEX_WORKS` no existe.
- El resultado vivo de 104,067 filas contradice un conteo previo reportado de
  aproximadamente 7,166,142; se debe usar el resultado del preflight y revisar
  la conexión/estado anterior antes de aprobar una carga completa.

La API local de Kestra debe estar accesible y autenticada para leer los
resultados del flow `openalex_ingestion_preflight`. No ejecutar el lote si no se
ha recuperado una lista vigente de archivos, historial de carga y conteos.

## Flows

Todos los flows son manuales, están en namespace `pset2` y reciben credenciales
desde `ENV_SNOWFLAKE_*` declaradas en `docker-compose.yml`; ningún secreto va en
el repositorio.

| Flow ID | Uso |
|---|---|
| `smoke_test_snowflake` | Comprobar conectividad con Snowflake. |
| `copy_single_openalex_author_file` | Prueba controlada de un archivo authors exacto. |
| `openalex_ingestion_preflight` | Solo lectura: objetos, definición de stages, LIST, conteos, archivos cargados, COPY_HISTORY y forma de PAYLOAD. Consulta COPY_HISTORY y conteo de WORKS solo si la tabla existe. |
| `ingest_openalex_authors_batch` | COPY por lista explícita de archivos authors, máximo 1000 por ejecución. |
| `ingest_openalex_works_batch` | Crea `RAW_OPENALEX_WORKS` si falta y copia una lista explícita, máximo 1000 por ejecución. |

Los flujos de ingesta especifican `FILES`, `FORCE = FALSE` y
`ON_ERROR = 'ABORT_STATEMENT'`; preservan `METADATA$FILENAME`,
`METADATA$START_SCAN_TIME` y el ID de ejecución de Kestra. No contienen
comodines, triggers ni operaciones de borrado. `FORCE = FALSE` usa el historial
de Snowflake para omitir archivos cargados recientemente; para protegerse de
cargas antiguas o metadatos expirados, compara siempre la lista contra
`_SOURCE_FILE` antes de armar el lote. Además, cada flow valida contra su tabla
Bronze que ninguna clave ya exista en `_SOURCE_FILE`, que no haya claves
repetidas en la solicitud y que el lote tenga como máximo 1000 elementos; si
alguna comprobación falla, se detiene antes del COPY.

## Diagnóstico de solo lectura

Ejecuta `openalex_ingestion_preflight` desde la UI de Kestra en el namespace
`pset2`. Confirma que terminó en `SUCCESS` y revisa sus salidas. Las siguientes
consultas también son de solo lectura:

```sql
SHOW STAGES IN SCHEMA PSET2_DB.BRONZE;
SHOW FILE FORMATS IN SCHEMA PSET2_DB.BRONZE;
SHOW TABLES IN SCHEMA PSET2_DB.BRONZE;
DESC STAGE PSET2_DB.BRONZE.OPENALEX_PARQUET_STAGE;
DESC STAGE PSET2_DB.BRONZE.OPENALEX_WORKS_STAGE;

LIST @PSET2_DB.BRONZE.OPENALEX_PARQUET_STAGE/authors/ PATTERN = '.*[.]parquet$';
LIST @PSET2_DB.BRONZE.OPENALEX_WORKS_STAGE PATTERN = '.*[.]parquet$';
LIST @PSET2_DB.BRONZE.OPENALEX_WORKS_STAGE;

SELECT COUNT(*) AS TOTAL_FILAS,
       COUNT(DISTINCT _SOURCE_FILE) AS ARCHIVOS_DISTINTOS,
       COUNT(DISTINCT _RUN_ID) AS EJECUCIONES
FROM PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS;

SELECT _SOURCE_FILE, COUNT(*) AS FILAS,
       COUNT(DISTINCT _RUN_ID) AS EJECUCIONES,
       MIN(_LOADED_AT) AS PRIMERA_CARGA,
       MAX(_LOADED_AT) AS ULTIMA_CARGA
FROM PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS
GROUP BY _SOURCE_FILE
ORDER BY ULTIMA_CARGA DESC;

SELECT FILE_NAME, STATUS, LAST_LOAD_TIME, ROW_COUNT, ROW_PARSED,
       FIRST_ERROR_MESSAGE
FROM TABLE(PSET2_DB.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS',
  START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

SELECT TYPEOF(PAYLOAD) AS TIPO,
       ARRAY_TO_STRING(OBJECT_KEYS(PAYLOAD), ',') AS CLAVES,
       PAYLOAD:"id"::VARCHAR AS ID_EJEMPLO
FROM PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS
LIMIT 1;

SELECT COUNT(*) AS TABLE_COUNT
FROM PSET2_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'BRONZE'
  AND TABLE_NAME = 'RAW_OPENALEX_WORKS'
  AND TABLE_TYPE = 'BASE TABLE';
```

`COPY_HISTORY` de `INFORMATION_SCHEMA` cubre los últimos 14 días. La tabla
Bronze con `_SOURCE_FILE` es la referencia para cotejar archivos cargados fuera
de esa ventana. Para comparar, usa la clave relativa del stage que corresponda
a `METADATA$FILENAME`; elimina de `LIST.name` el prefijo URL del stage si está
presente y no cambies el resto de la ruta.

## Preparar el lote, sin cargar

1. Ejecuta preflight y guarda las rutas exactas Parquet reportadas por `LIST`
   para ambos stages. El stage works se debe listar antes de decidir cualquier
   clave: no asumas su prefijo ni su estructura.
2. Compara cada clave de `LIST` con `_SOURCE_FILE` de su tabla Bronze. Excluye
   toda clave ya presente; compara el archivo de prueba authors como ya cargado.
3. Cuenta los archivos restantes y las filas existentes por archivo. Prepara
   lotes de hasta 1000 claves, separadas por coma y sin espacios. No uses
   carpetas, `PATTERN` de carpeta ni rutas construidas por conjetura.
4. Antes de la ingesta completa, informa el conteo de candidatos, archivos ya
   cargados, clave y filas del archivo de prueba y cualquier diferencia entre
   `LIST`, `_SOURCE_FILE` y `COPY_HISTORY`. Espera aprobación explícita.

No volver a ejecutar un COPY sobre toda la carpeta authors. Una carga masiva
requiere autorización después de mostrar el plan y el impacto esperado.

## Ejecutar una prueba controlada

Solo después de aprobación, abre `copy_single_openalex_author_file` en namespace
`pset2` y proporciona en `s3_file_key` una única ruta authors confirmada por
`LIST`, relativa a la raíz del stage (por ejemplo,
`authors/updated_date=2026-04-08/part_0000.parquet`). El flow lista el archivo en
`FILES`, nunca usa un patrón. Revisa la salida del COPY antes de continuar.

## Ejecutar lotes completos

Solo después de aprobación explícita del plan, inicia manualmente el flow
correspondiente desde Kestra → namespace `pset2` → **Execute**:

- Authors: `ingest_openalex_authors_batch`; pega en `file_keys` hasta 1000
  claves exactas no cargadas, relativas a `OPENALEX_PARQUET_STAGE`, separadas
  por comas. Cada clave debe comenzar con `authors/`.
- Works: `ingest_openalex_works_batch`; pega en `file_keys` hasta 1000 claves
  exactas no cargadas, relativas a `OPENALEX_WORKS_STAGE`, separadas por comas.
  El prefijo se deriva de `LIST`, no se presupone.

La ejecución equivalente desde PowerShell usa la API local de Kestra. Define las
claves a partir de `LIST` y conserva las credenciales únicamente en variables
locales; no pegues claves ya cargadas. Requiere `curl.exe` y solo debe ejecutarse
después de aprobar el plan:

```powershell
$authorsFileKeys = @('authors/<clave exacta no cargada 1>.parquet', 'authors/<clave exacta no cargada 2>.parquet')
curl.exe --fail-with-body --user "$($env:KESTRA_USER):$($env:KESTRA_PASSWORD)" `
  --form "file_keys=$($authorsFileKeys -join ',')" `
  'http://localhost:8080/api/v1/main/executions/pset2/ingest_openalex_authors_batch?wait=true'

$worksFileKeys = @('<clave exacta no cargada 1>.parquet', '<clave exacta no cargada 2>.parquet')
curl.exe --fail-with-body --user "$($env:KESTRA_USER):$($env:KESTRA_PASSWORD)" `
  --form "file_keys=$($worksFileKeys -join ',')" `
  'http://localhost:8080/api/v1/main/executions/pset2/ingest_openalex_works_batch?wait=true'
```

Reemplaza todos los marcadores por rutas reales copiadas de `LIST`; no uses el
ejemplo de autores ya cargado como candidato. La respuesta debe terminar en
`SUCCESS`; conserva los resultados por archivo de la tarea COPY.

Para más de 1000 claves, divide el conjunto aprobado en lotes disjuntos. Guarda
la salida `rows_loaded`, `rows_parsed`, `status` y `file` de cada ejecución y
valida el conteo de archivos/filas al terminar cada lote. Una reejecución solo
debe usar los archivos todavía ausentes según `_SOURCE_FILE` y `COPY_HISTORY`.

## Validación posterior

```sql
SELECT COUNT(*) AS TOTAL_FILAS,
       COUNT(DISTINCT _SOURCE_FILE) AS ARCHIVOS_DISTINTOS,
       COUNT(DISTINCT _RUN_ID) AS EJECUCIONES
FROM PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS;

SELECT _RUN_ID, _SOURCE_FILE, COUNT(*) AS FILAS,
       MIN(_LOADED_AT) AS PRIMERA_CARGA,
       MAX(_LOADED_AT) AS ULTIMA_CARGA
FROM PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS
GROUP BY _RUN_ID, _SOURCE_FILE
ORDER BY ULTIMA_CARGA DESC;

SELECT COUNT(*) AS TOTAL_FILAS,
       COUNT(DISTINCT _SOURCE_FILE) AS ARCHIVOS_DISTINTOS,
       COUNT(DISTINCT _RUN_ID) AS EJECUCIONES
FROM PSET2_DB.BRONZE.RAW_OPENALEX_WORKS;
```

No se requiere modificar dbt/Spark para esta ingesta. El siguiente paso tras
validar Bronze es coordinar con el responsable de dbt la selección de modelos
SILVER sobre `RAW_OPENALEX_AUTHORS` y `RAW_OPENALEX_WORKS`.
