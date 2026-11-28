# Memo, parte del Rol 1: Datos, negativos y EDA

> Borrador para el Tech Lead, que unifica el memo final. Todas las cifras salen de `notebooks/01_eda.ipynb` y de `data/processed/manifest.json`.

## Datos

La fuente es el snapshot de `OBT.OBT_AWARD_WORK` del PSet 2 (corte 2026-10-23, 261 633 filas, hash `ef2b4387...`), exportado a CSV antes de que venciera la cuenta de Snowflake. Cada fila es un par publicación–award observado en OpenAlex: un positivo. Hay 68 946 publicaciones y 203 173 awards. De ahí salen 189 642 positivos de 2017 a 2025: 160 850 de 39 474 publicaciones para train/CV (2017–2024) y 28 792 de 7 795 publicaciones para el test (2025). 2026 queda fuera por ser un año incompleto. Cada publicación va completa a un solo split, así que ninguna aparece en los dos.

La OBT no trae el texto, así que se descargó de la API de OpenAlex en lotes de 100 (abstracts de las publicaciones y descripciones de los awards, recortados a 2 000 caracteres). Se bajaron los abstracts de 36 133 publicaciones que la OBT marca con abstract y se obtuvieron 33 298; se pidieron las descripciones de 26 400 awards y quedaron 24 940 con texto. Con ellos se calcularon embeddings `all-MiniLM-L6-v2` (384 dimensiones, normalizados) de título + texto para las 47 269 publicaciones y para los awards con algún texto.

## Negativos

El problema solo tiene positivos observados, así que los negativos se generan: **4 por positivo**, entre los awards **vigentes** en el año de la publicación y que esa publicación no recibió. La mitad son al azar y la otra mitad **difíciles**: awards del mismo país que algún autor, o del mismo topic.

- **Vigencia.** Solo el 18 % de los awards trae fechas. Para el resto, la ventana se infiere de sus publicaciones observadas (de 3 años antes de la primera a 3 años después de la última), y para los que tienen fechas se usa `[inicio, fin + 3 años]`. Los positivos caen siempre dentro de la ventana. Para construir el train solo se usan publicaciones hasta 2024: nada de 2025 entra a los negativos del train.
- **Dificultad.** El 49.0 % de los negativos son difíciles (el 98 % de los pedidos se logró). Casi todos lo son por el país del funder.
- **Un problema detectado y corregido.** En una primera versión, el topic competía de igual a igual con los países como fuente de negativos difíciles. Como solo el 2.4 % de los positivos trae topic de award, el 10.3 % de los negativos lo traía: un modelo habría aprendido "award con topic = negativo". Se redujo el peso del topic a 5 % de las extracciones y quedó en 5.0 % de los negativos (el azar solo da 3.3 %).

## EDA: hallazgos que afectan el modelado

| Hallazgo | Evidencia | Consecuencia en el modelo |
|---|---|---|
| El techo de precision@3 es 0.69 | El 36.0 % de las publicaciones tiene 1 award y el 19.9 %, 2; solo el 44.1 % tiene 3 o más. Un ranking al azar da 0.20 | Los modelos se comparan entre 0.20 y 0.69, y las diferencias chicas importan |
| Unas pocas publicaciones pesan muchísimo | Mediana de 2 awards, máximo de 1 609; el 1 % de las publicaciones concentra el 14.2 % de los positivos | GroupKFold por publicación; considerar pesos por publicación |
| El texto del award casi no existe | 78.1 % de los pares con abstract de la publicación, pero 23.0 % con título de award y 17.1 % con descripción; 22.8 % de los awards con embedding | Las features de similitud valen solo en el 23 % de los pares: llevan un indicador y un valor neutro |
| `funded_outputs_count` es fuga | AUC 0.586 como puntaje único; cuenta las publicaciones del award, incluidas la de este par y las futuras | No se usa; la popularidad del award, solo con años anteriores y dentro de cada fold |
| Colas largas | `cited_by_count`: asimetría 50.7 y 9.8 % de atípicos por IQR (0.6 % por Z ≥ 3); `amount_usd`: 94.4 % vacío | Transformaciones log o por rangos; Spearman en vez de Pearson |
| Los negativos difieren de los positivos en metadatos | Con fecha de inicio: 17.8 % de los positivos contra 24.0 % de los negativos; con topic: 2.4 % contra 5.0 % | Las features que dependen de que el award tenga fecha o topic tienen señal parcialmente artificial |

## Limitaciones de los datos (para la sección 8)

- **Los positivos son solo los observados**: un award que financió una publicación sin que OpenAlex lo registre aparece como negativo.
- **La vigencia es inferida** para el 82 % de los awards, y los negativos difíciles dependen de la cobertura del país y del topic.
- **El texto del award falta en casi 8 de cada 10 pares**, lo que acota lo que pueden aprender los modelos basados en embeddings.
- **La API gratuita de OpenAlex tiene un presupuesto diario por IP**; la descarga está en caché y es reanudable, pero el snapshot de OpenAlex puede cambiar y con él los abstracts.
