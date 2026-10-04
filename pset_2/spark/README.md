# Spark: One Big Table (ROL 3)

Lee el star schema de `GOLD` en Snowflake, lo aplana en una One Big Table y la vuelve a escribir en `OBT`.

| | |
|---|---|
| Job | [`jobs/build_obt.py`](jobs/build_obt.py) |
| Entrada | `GOLD.FACT_AWARD_WORKS`, `FACT_AWARDS`, `DIM_AWARD`, `DIM_FUNDER`, `DIM_INSTITUTION`, `DIM_WORK`, `DIM_TOPIC`, `DIM_DATE`, `BRG_WORK_AUTHOR`, `DIM_AUTHOR` |
| Salida | `OBT.OBT_AWARD_WORK` (la OBT) y `OBT.OBT_AWARD_WORK_VALIDATION` (una fila por corrida con los chequeos) |
| Grain | Una fila por publicación financiada por un award: `award_key + work_key` (el mismo de `FACT_AWARD_WORKS`) |

## Ejecutar

Desde `pset_2/`, con el `.env` completo:

```bash
docker compose up -d spark-master spark-worker
docker compose exec spark-master /opt/spark-apps/run_obt.sh
```

La primera corrida descarga el conector de Snowflake (~1 min). Después queda en caché, en el volumen `spark_ivy`.
El job termina con código `1` y **no escribe la OBT** si falla cualquier validación. Así un orquestador lo marca como fallido.

### Pruebas locales (sin Snowflake)

```bash
docker compose run --rm --no-deps spark-master /opt/spark/bin/spark-submit \
  --master "local[2]" /opt/spark-apps/tests/test_build_obt.py
```

Las 19 pruebas usan datos sintéticos y comprueban:
- **Grain:** se conserva aunque un work tenga varios autores y varios awards, o ninguno.
- **Valores correctos:** el funder vigente, el primer autor y los textos para el embedding (`?`, `U.S.`, vacíos).
- **Abortos:** el job se detiene si una tabla trae llaves repetidas, si una FK o un autor no encuentra su dimensión, si el hecho base viene duplicado o si la llave RSA está cifrada.

Los filtros `*_IN_SCOPE` corren en Snowflake y no se ejercitan aquí. En la corrida real, un filtro equivocado aparece como huérfano y aborta el job.

### Autenticación

- **Por defecto:** `SNOWFLAKE_USER` + `SNOWFLAKE_PASSWORD`, como el resto del equipo.
- **Llave RSA** (necesaria si la cuenta exige MFA para login con password):
  1. Genera la llave **sin passphrase** y en PKCS#8: `openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out snowflake_rsa_key.p8`. El job rechaza llaves cifradas con un mensaje claro.
  2. Pon la llave en `pset_2/secrets/snowflake_rsa_key.p8`. Esa carpeta está en `.gitignore` y solo se monta en `spark-master`, donde corre el driver.
  3. Define `SNOWFLAKE_PRIVATE_KEY_FILE=/opt/secrets/snowflake_rsa_key.p8` en `.env`.

`run_obt.sh` configura `spark.redaction.regex` y `spark.sql.redaction.options.regex` para que ni la llave ni el password aparezcan en la UI ni en los logs de Spark.

## Cómo se construye

1. **Hecho base:** `FACT_AWARD_WORKS` fija el grain y el número de filas esperado.
2. **Joins muchos a uno:** se unen `FACT_AWARDS` y las dimensiones del award (texto, funder, institución principal, tema, fecha de inicio) y las de la publicación (título, abstract, tema, fecha). `DIM_TOPIC` y `DIM_DATE` se usan dos veces, con prefijos `award_` y `work_` (*role-playing*).
3. **Autores:** como son muchos por publicación, primero se agregan a **una fila por work**. Esa fila lleva la lista ordenada de `author_keys`, el primer autor, los autores de correspondencia y los países de afiliación. Recién entonces se une a la OBT, sin multiplicar filas.
4. **Columnas para el modelo:**
   - `label = 1`: todo par de la tabla es un financiamiento real observado.
   - `award_text` y `work_text`: textos listos para vectorizar con Cortex en la siguiente etapa.

## Validaciones (el job aborta si alguna falla)

| # | Chequeo | Qué previene |
|---|---|---|
| 1 | Cada tabla que se une es única en su llave | Un join con llave repetida multiplica filas: es la causa raíz de los duplicados |
| 2 | Ningún join queda sin pareja (cada lado lleva un marcador que solo es nulo si no hubo match) | FKs sin dimensión que dejarían atributos vacíos en silencio |
| 3 | `COUNT(OBT) = COUNT(FACT_AWARD_WORKS)` y `(award_key, work_key)` único | Que el aplanado agregue o pierda observaciones |

Cada corrida, exitosa o fallida (incluidos errores de conexión o de columnas), deja una fila en `OBT.OBT_AWARD_WORK_VALIDATION`:
- columnas `status`, `base_rows`, `obt_rows`, `orphans_total`, `error` y `metrics_json`;
- un `-1` significa que ese chequeo no llegó a ejecutarse.

Si la OBT se escribió bien pero falla solo este registro, el job termina en `OK` y deja un aviso en el log.

**Consistencia:** cada tabla de `GOLD` se lee en una consulta separada. Por eso el job debe correr **después** de `dbt build` y no en paralelo, que es como lo encadena Kestra. Si dbt modificara Gold a mitad de la lectura, el chequeo de huérfanos lo detecta y el job aborta sin escribir.

## Re-ejecución

La OBT se escribe con `overwrite`. Es derivada de `GOLD`, así que reconstruirla en cada batch da el mismo resultado (idempotente). El conector escribe primero en una tabla temporal y la intercambia al final, así que nadie ve una OBT a medio escribir.

## Disparo desde Kestra

Probado con Kestra 1.3.37: la ejecución termina en `SUCCESS` y escribe la OBT. La imagen de Kestra no trae el cliente `docker`, pero el compose ya le monta `docker.sock`. La tarea levanta un contenedor efímero con el cliente de Docker y ejecuta el job dentro del master de Spark:

```yaml
  - id: spark_obt
    type: io.kestra.plugin.scripts.shell.Commands
    taskRunner:
      type: io.kestra.plugin.scripts.runner.docker.Docker
      volumes:
        - /var/run/docker.sock:/var/run/docker.sock
    containerImage: docker:27-cli
    commands:
      - docker exec pset2-spark-master /opt/spark-apps/run_obt.sh
```

Para que Kestra permita montar volúmenes, la sección de Kestra en `docker-compose.yml` necesita esto dentro de `KESTRA_CONFIGURATION`:

```yaml
          plugins:
            configurations:
              - type: io.kestra.plugin.scripts.runner.docker.Docker
                values:
                  volume-enabled: true
```

## Mensajes normales en el log

Ninguno afecta los datos:

| Mensaje | Qué es |
|---|---|
| `ERROR Inbox: Ignoring error ... NotSerializableException: StorageStatus` | Telemetría del conector de Snowflake al pedir el estado del cluster. Spark la ignora |
| `WARN StageWriter$: Load file which isn't uploaded by SC` | Aviso del conector al cargar sus archivos temporales. El `COPY` inserta exactamente las filas de la OBT (verificado: 160 176 de 160 176) |

En Kestra estas líneas salen en nivel `ERROR` porque Spark escribe en stderr. El estado real de la corrida lo dan el código de salida y `OBT.OBT_AWARD_WORK_VALIDATION`.
