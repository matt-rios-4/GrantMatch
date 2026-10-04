# Secciones 5 y 6 del memo (rol Spark)

> Cifras de la corrida de prueba del 4-oct-2026 sobre la muestra cargada en Bronze:
> todos los awards de OpenAlex (17.1 M), funders, topics y un archivo de works
> (`updated_date=2026-09-23/part_0000`, 278 720 publicaciones). Si el equipo carga otra
> muestra, actualiza los números con `OBT.OBT_AWARD_WORK_VALIDATION`.

---

## 5. Spark y OBT

**Qué construye.** El job `spark/jobs/build_obt.py`:
1. Lee el star schema de `GOLD` desde Snowflake con el conector `spark-snowflake`.
2. Lo aplana en `OBT.OBT_AWARD_WORK` (62 columnas).
3. La escribe de vuelta en Snowflake.

Corre en el cluster Spark del `docker-compose.yml` (1 master + 1 worker de 2 cores) y tarda ~5 minutos.

**Grain: una fila por publicación financiada por un award** (`award_key + work_key`). Es el mismo grain de `FACT_AWARD_WORKS` y cada fila es un ejemplo positivo del recomendador. En la misma fila quedan:
- el texto del award (título y descripción, es decir, lo que se financió);
- el texto de la publicación (título y abstract, es decir, lo que produjo el investigador).

Ese es el par que el modelo de embeddings comparará con similitud del coseno. También trae:
- el contexto de cada lado: funder, institución principal, monto en USD, duración y tema del award; tipo, año, citas y tema del work;
- `label = 1`;
- `award_text` y `work_text`, listos para `CORTEX.EMBED_TEXT_768`.

**Cómo se arma sin romper el grain.**
- **Joins muchos a uno.** Todos parten de `FACT_AWARD_WORKS`: `FACT_AWARDS` y las dimensiones del award y del work. `DIM_TOPIC` y `DIM_DATE` entran dos veces (*role-playing*), con prefijos `award_` y `work_`.
- **Autores.** Son la única relación muchos a muchos: un work tiene en promedio 10.9 autores (mediana 6, máximo 3 132). Por eso primero se agregan a **una fila por work** (lista ordenada de `author_keys`, primer autor, autores de correspondencia y países de afiliación), y recién después se unen. Unir el puente directamente habría producido **9 198 514 filas en vez de 160 176** (57 veces más).
- **Filtrado en Snowflake.** El job no copia Gold completo: filtra dentro de Snowflake a lo que participa en la OBT. Por ejemplo, lee 132 197 de los 17.1 M de awards, en vez de traer gigabytes de descripciones a Spark.

**Validación de los joins.** El job aborta y no escribe la OBT si falla cualquiera de estos chequeos:

| Chequeo | Resultado |
|---|---|
| El hecho base y cada tabla que se une son únicos en su llave (p. ej. `dim_award`: 132 197 filas = 132 197 llaves) | 8 de 8 tablas + hecho base únicos |
| Ningún join queda sin pareja (un marcador por join, nulo solo si no hubo match) | 0 huérfanos en los 9 joins |
| `COUNT(OBT) = COUNT(FACT_AWARD_WORKS)` | 160 176 = 160 176 |
| `(award_key, work_key)` único en la OBT | 160 176 pares distintos |

Además:
- **Registro por corrida.** Cada corrida deja su resultado en `OBT.OBT_AWARD_WORK_VALIDATION`.
- **Pruebas sin Snowflake.** `spark/tests/test_build_obt.py` comprueba con datos sintéticos que el job **sí aborta** ante una dimensión duplicada, una FK huérfana o un hecho base duplicado (10/10 pruebas).
- **Idempotencia.** Tres corridas seguidas produjeron exactamente las mismas 160 176 filas, todas con estado `OK`: la OBT se reescribe completa (`overwrite` sobre tabla temporal + swap).

**Lo que mostró la OBT sobre los datos.** Esto condiciona la siguiente etapa:
- **Texto del award escaso.** Solo el 11.2 % de los pares tiene descripción del award. En el snapshot completo es el 29 %, y la causa es OpenAlex, no la limpieza: Silver cuadra exacto con Bronze. En cambio, el 78.1 % tiene abstract de la publicación. Por eso conviene representar cada award también por los abstracts de las publicaciones que financió, y la OBT lo permite agrupando por `award_key`.
- **Tema del award casi ausente.** El 97.6 % de los pares tiene el tema del award en `UNKNOWN`, porque solo el 7 % de los awards trae `primary_topic`. El filtro por dominio de la fase "Verificar" debe usar el tema del **work**.
- **Monto comparable escaso.** Solo el 2.6 % tiene `amount_usd`.
- **Concentración por funder.** Los funders con más pares son NSFC (19 651), NIH (9 154) y NCI (5 939). El modelo debe controlar ese desbalance.

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
