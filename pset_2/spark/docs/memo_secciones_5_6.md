# Secciones 5 y 6 del memo (rol Spark)

> Cifras de las corridas del 4-oct-2026 sobre la muestra cargada en Bronze: todos los
> awards de OpenAlex (17.1 M), funders, topics y dos archivos de works de la partición
> `updated_date=2026-09-23` (558 403 publicaciones), la misma muestra del memo §3-4. Si el
> equipo carga otra muestra, actualiza los números con `OBT.OBT_AWARD_WORK_VALIDATION`.

---

## 5. Spark y OBT

**Qué construye.** El job `spark/jobs/build_obt.py`:
1. Lee el star schema de `GOLD` desde Snowflake con el conector `spark-snowflake`.
2. Lo aplana en `OBT.OBT_AWARD_WORK` (62 columnas).
3. La escribe de vuelta en Snowflake.

Corre en el cluster Spark del `docker-compose.yml` (1 master + 1 worker de 2 cores) y tarda ~7 minutos para 313 mil filas.

**Grain: una fila por publicación financiada por un award** (`award_key + work_key`). Es el mismo grain de `FACT_AWARD_WORKS` y cada fila es un ejemplo positivo del recomendador. En la misma fila quedan:
- el texto del award (título y descripción, es decir, lo que se financió);
- el texto de la publicación (título y abstract, es decir, lo que produjo el investigador).

Ese es el par que el modelo de embeddings comparará con similitud del coseno. También trae:
- el contexto de cada lado: funder, institución principal, monto en USD, duración y tema del award; tipo, año, citas y tema del work;
- `label = 1`;
- `award_text` y `work_text`, listos para `CORTEX.EMBED_TEXT_768`.

**Cómo se arma sin romper el grain.**
- **Joins muchos a uno.** Todos parten de `FACT_AWARD_WORKS`: `FACT_AWARDS` y las dimensiones del award y del work. `DIM_TOPIC` y `DIM_DATE` entran dos veces (*role-playing*), con prefijos `award_` y `work_`.
- **Autores.** Son la única relación muchos a muchos: un work tiene en promedio 10.9 autores (mediana 6, máximo 4 902). Por eso primero se agregan a **una fila por work** (lista ordenada de `author_keys`, primer autor, autores de correspondencia y países de afiliación), y recién después se unen. Unir el puente directamente habría producido **15 781 610 filas en vez de 313 264** (50 veces más).
- **Filtrado en Snowflake.** El job no copia Gold completo: filtra dentro de Snowflake a lo que participa en la OBT. Por ejemplo, lee 243 287 de los 17.1 M de awards, en vez de traer gigabytes de descripciones a Spark.

**Validación de los joins.** El job aborta y no escribe la OBT si falla cualquiera de estos chequeos:

| Chequeo | Resultado |
|---|---|
| El hecho base y cada tabla que se une son únicos en su llave (p. ej. `dim_award`: 243 287 filas = 243 287 llaves) | 12 de 12 únicos (hecho base, 9 joins, puente y `dim_author`) |
| Ningún join queda sin pareja (un marcador por join, nulo solo si no hubo match) | 0 huérfanos en los 10 joins |
| `COUNT(OBT) = COUNT(FACT_AWARD_WORKS)` | 313 264 = 313 264 |
| `(award_key, work_key)` único en la OBT | 313 264 pares distintos |

Además:
- **Registro por corrida.** Cada corrida, exitosa o fallida (incluidos errores de conexión), deja su resultado en `OBT.OBT_AWARD_WORK_VALIDATION`.
- **Pruebas sin Snowflake.** `spark/tests/test_build_obt.py` comprueba con datos sintéticos que el job **sí aborta** ante una tabla duplicada, una FK o un autor huérfano, o un hecho base duplicado (19/19 pruebas).
- **Idempotencia e incrementalidad.** Las corridas repetidas sobre el mismo Gold dieron exactamente las mismas filas. Cuando dbt incorporó un segundo lote de works, la OBT pasó de 160 176 a 313 264 filas y siguió cuadrando exacto con `FACT_AWARD_WORKS`. Se reescribe completa (`overwrite` sobre tabla temporal + swap).

**Lo que mostró la OBT sobre los datos.** Esto condiciona la siguiente etapa:
- **Texto del award casi ausente.** El 85.5 % de los pares (267 964) no tiene **ningún** texto del award: ni título ni descripción (`award_text` es NULL), y solo el 11.4 % tiene descripción. La causa es OpenAlex, no la limpieza: Silver cuadra exacto con Bronze. En cambio, el 78.0 % tiene abstract de la publicación. Por eso cada award conviene representarlo por los abstracts de las publicaciones que financió, y la OBT lo permite agrupando por `award_key`.
- **Tema del award casi ausente.** El 97.6 % de los pares tiene el tema del award en `UNKNOWN`, porque solo el 7 % de los awards trae `primary_topic`. El filtro por dominio de la fase "Verificar" debe usar el tema del **work**.
- **Monto comparable escaso.** Solo el 2.6 % tiene `amount_usd`.
- **Concentración por funder.** Los funders con más pares son NSFC (39 983), NIH (18 455) y NCI (11 285). El modelo debe controlar ese desbalance.

**Star schema u OBT.**
- **Star schema (`GOLD`)** para análisis y BI: cuánto financia cada funder por área y año, o qué instituciones reciben más. Cada métrica vive una vez en su grain, sin riesgo de sumar dos veces el monto de un award.
- **OBT** para entrenar y servir el modelo: una sola tabla ancha sin joins en el momento de la inferencia, con texto y atributos de ambos lados del par. No sirve para sumar dinero, porque el monto del award se repite en cada publicación que financió.

---

## 6. Batch vs. streaming

**La fuente ya es batch.** OpenAlex no emite eventos: publica un snapshot en S3 particionado por `updated_date`.
- **Retraso propio.** Al 4-oct-2026 la última partición disponible era la del 23-sep, así que la fuente llega con ~11 días de retraso por sí sola.
- **Deltas masivos.** Un solo día de works pesa 136.6 GB en 160 archivos, porque OpenAlex reemite registros cuando los reprocesa. Los awards, en cambio, suman ~0.2 GB diarios.
- **Consecuencia.** Ningún diseño puede entregar datos más frescos que el snapshot, y procesar 136 GB evento por evento sería mucho más caro que un `COPY INTO` por partición.

**La latencia del negocio se mide en semanas.**
- Las convocatorias de financiamiento abren con meses de anticipación y tienen plazos fijos.
- El perfil de un investigador cambia cuando publica, es decir, en meses.
- Una oficina de transferencia revisa recomendaciones con periodicidad semanal o mensual.

Una recomendación calculada con datos de hace unos días tiene el mismo valor que una de hace segundos.

**El costo favorece lotes.**
- Los embeddings de Cortex se cobran por token, y la similitud se calcula como cruce entre autores y awards. En lotes se vectoriza una sola vez lo nuevo de cada partición (incremental) y se recalculan los rankings de una vez.
- En streaming se mantendría un warehouse encendido y se perdería el *pruning* por partición que hace barato el `COPY INTO`.

Por eso el pipeline corre como batch, alineado con la llegada de particiones de OpenAlex.

**Qué tendría que cambiar para justificar streaming.**
1. **Una fuente que emita eventos.** Por ejemplo, un feed de convocatorias en tiempo real (webhooks o RSS de grants.gov o de los funders), en vez de un snapshot.
2. **Decisiones con ventana de horas.** Convocatorias de respuesta rápida, como fondos de emergencia que cierran en días, donde avisar tarde equivale a no avisar.
3. **Personalización en línea.** Que el investigador edite su perfil en una app y espere recomendaciones al instante.

En ese escenario la arquitectura cambiaría así:
- **Ingesta:** cola de eventos (Kafka o Kinesis) con Snowpipe Streaming hacia Bronze.
- **Transformación:** Dynamic Tables o dbt micro-batch en vez de corridas programadas.
- **Embeddings:** se calcularían al llegar cada convocatoria.
- **Consulta:** un índice vectorial en línea.

Aun así, el historial de OpenAlex (publicaciones y awards pasados) seguiría cargándose por lotes. Sería una arquitectura mixta: batch para el histórico y streaming solo para las convocatorias nuevas.
