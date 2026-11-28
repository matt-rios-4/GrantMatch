# Rol 1 (Datos): referencia técnica para el resto del equipo y sus asistentes

Documento de referencia de lo que entrega `src/datos.py`. Está escrito para que alguien (o una IA) que no vio el trabajo pueda usar los datos sin leer el código. Todas las cifras salen del `data/processed/manifest.json` y de `notebooks/01_eda.ipynb`.

## 1. Qué se construyó

El snapshot de `OBT.OBT_AWARD_WORK` (PSet 2) trae solo **positivos**: cada fila es un par publicación–award observado en OpenAlex. `src/datos.py` lo convierte en un problema de clasificación:

1. Lee el CSV (`data/raw/obt_award_work_full.csv`, sha256 `ef2b4387...3052`).
2. Genera **4 negativos por positivo** entre los awards vigentes ese año (mitad al azar, mitad "difíciles").
3. Descarga de OpenAlex el abstract de cada publicación y la descripción de cada award.
4. Escribe los pares en dos parquet con split temporal: **train/CV = publicaciones 2017–2024; test = 2025** (2026 queda fuera, año incompleto).
5. Calcula embeddings `all-MiniLM-L6-v2` de título + texto.
6. Escribe `manifest.json`.

Se corre con `python -m src.datos` desde `pset_3/`. Es reanudable: los textos de OpenAlex quedan en caché en `data/raw/*.jsonl` y los embeddings que ya existen no se recalculan. `--solo-pares` genera los pares sin descargas ni embeddings (1 minuto, para depurar).

## 2. Archivos que quedan en `data/processed/` (no se versionan)

| Archivo | Contenido |
|---|---|
| `pares_train.parquet` | 804 250 filas, 160 850 positivos, 39 474 publicaciones (2017–2024) |
| `pares_test.parquet` | 143 960 filas, 28 792 positivos, 7 795 publicaciones (2025). **Se usa una sola vez**, en `05_seleccion` |
| `manifest.json` | tabla de origen, fecha de corte, sha256, conteos, cobertura de texto, negativos difíciles por año |
| `emb_works.parquet` | 47 269 filas: `work_key` + 384 columnas `e000`..`e383` (float32, normalizados) |
| `emb_awards.parquet` | 34 376 filas: `award_key` + 384 columnas `e000`..`e383`. **Solo hay fila para los awards con texto** |
| `negativos_tipo.parquet` | `work_key`, `award_key`, `tipo` (`azar` o `dificil`). Solo para el EDA: **nunca entra a los modelos**, porque revelaría el target |

## 3. Esquema de los pares

57 columnas, una fila por `(work_key, award_key)`, sin duplicados, filas mezcladas dentro de cada año.

- **Llaves y target:** `work_key`, `award_key` (texto), `label` (int64, 0 o 1). El grupo para `GroupKFold` es `work_key`.
- **Award:** `funder_award_id`, `award_title`, `has_description`, `funder_scheme`, `funding_type`, `funder_key`, `funder_name`, `funder_country_code`, `is_global_south` (vacía al 100 %), `institution_key`, `lead_institution_name`, `lead_institution_country_code`, `lead_institution_type`, `award_topic_key`, `award_topic_name`, `award_subfield_name`, `award_field_name`, `award_domain_name`, `start_date_key`, `award_start_date`, `award_start_year`, `award_us_fiscal_year`, `end_date_key`, `amount`, `currency`, `amount_usd`, `duration_days`, `funded_outputs_count`, `institutions_count`, `is_amount_invalid`, `has_inconsistent_dates`, `award_description`.
- **Publicación:** `work_doi`, `work_title`, `has_abstract`, `publication_year`, `publication_date_key`, `publication_date`, `work_type`, `language`, `cited_by_count`, `fwci`, `is_retracted`, `authors_count`, `work_topic_key`, `work_topic_name`, `work_subfield_name`, `work_field_name`, `work_domain_name`, `linked_authors_count`, `first_author_key`, `first_author_name`, `author_country_codes` (lista JSON compacta en texto, por ejemplo `["CN", "US"]`), `work_abstract`.

Convenciones: los textos faltantes son `None`, no cadena vacía. Los awards sin topic traen `award_topic_key = "UNKNOWN"`. Las fechas sin dato en las llaves numéricas vienen como `-1`.

## 4. API de `src/datos.py`

```python
datos.cargar_pares("train" | "test", columnas=None)  # columnas=[...] lee solo esas; ver advertencia de memoria
datos.leer_manifest()                               # dict del manifest (o el de la muestra)
datos.separar_xy(df)                                # X sin label, y (numpy), grupos work_key (numpy)
datos.cargar_embeddings("works" | "awards")         # (llaves, matriz float32 n x 384)
datos.usar_muestra()                                # True si GRANTMATCH_MUESTRA=1 o no existen los datos reales
datos.cargar_obt(); datos.columnas_award(obt); datos.columnas_work(obt)
datos.generar_negativos(obt)                        # DataFrame work_key, award_key, label=0, tipo
datos.descargar_abstracts(work_keys); datos.descargar_descripciones(award_keys)
datos.construir_embeddings(pares); datos.construir_splits(descargar=True, embeddings=True)
```

**Advertencia de memoria.** Los textos pesan más de 1 GB al cargar `pares_train` completo. Si el modelo no usa texto, lee solo las columnas que necesita (`columnas=[...]`) o recorre el parquet por bloques con `pyarrow.parquet.ParquetFile(...).iter_batches`. Los embeddings en columnas anchas pesan unos 300 MB en conjunto.

## 5. Cómo se generan los negativos (decisiones que importan para los modelos)

- **Cuántos:** `C.NEGATIVOS_POR_POSITIVO = 4`, semilla `C.SEMILLA = 42`. Por construcción, 20 % de las filas son positivas: un ranking al azar da `average precision` y `precision@3` de 0.20.
- **Un negativo nunca es un award que la publicación ya recibió.**
- **Vigencia.** Solo el 18 % de los awards trae fechas, así que se infiere una ventana por award: con fechas, `[inicio, fin + 3 años]`; sin ellas, desde 3 años antes de su primera publicación observada hasta 3 años después de la última; sin fin, `inicio + 4 años`. Los positivos caen siempre dentro de la ventana. Para construir el train solo se usan publicaciones hasta 2024; para el test, hasta 2025.
- **Difíciles (49 % de los negativos):** el país del funder está entre los países de los autores, o el award tiene el mismo `topic_key` que la publicación. Solo el 2.4 % de los positivos tiene topic de award, así que el topic pesa `PESO_TEMA = 0.05` de las extracciones: sin eso el 10.3 % de los negativos tenía topic contra 2.4 % de los positivos, y un modelo aprendía "award con topic = negativo". Casi todos los difíciles lo son por el país. Se logró el 98 % de los difíciles pedidos; si una publicación no tiene candidatos difíciles, se completa con azar.

## 6. Hallazgos del EDA que afectan a los demás roles

1. **Techo de precision@3 = 0.694.** El 36.0 % de las publicaciones tiene 1 award, el 19.9 % tiene 2, el 44.1 % tiene 3 o más. Los modelos se mueven entre 0.20 y 0.69.
2. **Publicaciones con muchísimos awards.** Mediana de 2, máximo de 1 609. El 1 % de las publicaciones concentra el 14.2 % de los positivos. `GroupKFold` por `work_key` evita que una publicación se parta entre train y validación; considerar pesos por publicación.
3. **`funded_outputs_count` es fuga. No usarla como feature.** Es el número de publicaciones del award, incluida la de este par y las posteriores. AUC univariado de 0.586 (mediana 25 en positivos contra 16 en negativos); ninguna otra variable pasa de 0.48–0.50. Si se quiere popularidad del award, calcularla solo con años anteriores y dentro de cada fold.
4. **El texto del award casi no existe.** 78.1 % de los pares tiene abstract de la publicación, pero solo 23.0 % tiene título de award y 17.1 % descripción. Solo el 22.8 % de los awards tiene embedding, y los dos lados tienen embedding en el 23.0 % de los pares. Una feature de similitud de embeddings debe llevar un indicador de "award con texto" y un valor neutro cuando falta.
5. **Colas largas.** `cited_by_count` (asimetría 50.7, 9.8 % de atípicos por IQR, máximo 14 045), `authors_count` (máximo 5 000), `amount_usd` (94.4 % vacío). Usar transformaciones log o por rangos.
6. **Los negativos no son idénticos a los positivos en metadatos.** Con fecha de inicio: 17.8 % de los positivos contra 24.0 % de los negativos; con descripción: 13.7 % contra 18.8 %; con topic: 2.4 % contra 5.0 %. Los positivos favorecen awards muy productivos, que suelen venir sin fechas ni descripción. Las features que dependen de que el award tenga fecha, descripción o topic (`mismo_topic`, `anios_desde_inicio_award`) tienen una señal parcialmente artificial: mirar su importancia con cuidado y reportarlo como limitación.
7. **Deriva temporal.** Las publicaciones por año crecen de 2 754 (2017) a 6 825 (2024); el test de 2025 tiene 7 795.

## 7. Garantías verificadas por las pruebas (`tests/test_datos.py`)

- 4 negativos por positivo; ningún negativo coincide con un positivo de la misma publicación.
- Todo negativo es un award vigente en el año de la publicación, y la ventana incluye los años de sus positivos.
- Un award sin fechas cuya única publicación es de 2025 no tiene ventana en el train.
- Los negativos difíciles comparten país o topic; la generación es reproducible con la misma semilla.
- Train solo tiene 2017–2024, test solo 2025, ninguna publicación está en los dos, 2026 no aparece, y la tasa de positivos es 20 %.
- La descarga pide lotes de 100 y reutiliza el caché sin llamadas nuevas.

## 8. Limitaciones conocidas

- Los positivos son solo los links que OpenAlex registra: un award que financió una publicación sin registro aparece como negativo.
- La vigencia es inferida para el 82 % de los awards.
- El texto del award falta en casi 8 de cada 10 pares, lo que acota a los modelos basados en embeddings.
- La API gratuita de OpenAlex tiene un presupuesto diario por IP; la descarga es reanudable, pero el contenido de OpenAlex puede cambiar con el tiempo.
- Las pruebas y la generación se corrieron con Python 3.13; el proyecto usa 3.11.

## 9. Cómo reproducir

```bash
cd pset_3
# 1) bajar obt_award_work_full.csv a data/raw/ y comprobar su sha256 (ver README)
# 2) pip install -r requirements.txt       (incluye sentence-transformers y torch)
# 3) opcional: OPENALEX_MAILTO en .env (cola prioritaria de OpenAlex)
python -m src.datos                         # ~3 horas la primera vez; se puede interrumpir y retomar
python -m pytest tests -x
```

Mientras no existan los datos reales, `datos.cargar_pares()` usa la muestra de `tests/fixtures/` (solo para desarrollar, no para reportar métricas).
