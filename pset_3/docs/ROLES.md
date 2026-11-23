# Roles, ownership y contrato de interfaces (PSet 3)

Cada rol es dueño de sus archivos. **No editen archivos de otro rol sin avisar.** Cada uno trabaja en su rama (`pset3/rol1-datos`, `pset3/rol2-features`, `pset3/rol3-avanzados`) y abre PR contra `main`; el Tech Lead revisa, integra y deja todo corriendo.

| Rol | Dueño de | Entrega |
|---|---|---|
| 1. Datos | `src/datos.py`, `notebooks/01_eda.ipynb` | Memo: datos, negativos, EDA |
| 2. Features y clásicos | `src/pipeline.py`, `src/candidatos_clasicos.py`, `notebooks/02_features.ipynb` | Memo: features (sin fuga), baselines, lineal y bagging |
| 3. Avanzados | `src/candidatos_avanzados.py` | Memo: boosting, red neuronal y SOTA con embeddings |
| 4. Tech Lead | `src/contrato.py`, `src/tracking.py`, `src/llm.py`, `docker-compose.yml`, `notebooks/03–05`, `README.md`, `docs/`, memo final | Memo: problema y métricas, MLflow, tuning, selección, limitaciones |

Dependencias nuevas: cada rol las agrega a `requirements.txt` con versión fija en su PR.

## Problema

Clasificador de pares **publicación (work) – award**: dado un work, ¿cuáles awards lo financiaron? Para recomendar, se puntúan los candidatos de cada work y se muestran los 3 mejores.

- **Positivos:** las filas de `OBT.OBT_AWARD_WORK` (snapshot en `data/raw/obt_award_work_full.csv`).
- **Negativos:** 4 por positivo entre los awards vigentes en el año de publicación; mitad al azar y mitad difíciles (mismo topic o país del funder).
- **Split temporal:** train/CV = publicaciones 2017–2024, test = 2025 (se usa **una sola vez**, en `05_seleccion`). 2026 queda fuera.

## Contrato de interfaces

Las constantes viven en [`src/contrato.py`](../src/contrato.py). Si algo de aquí cambia, se cambia ahí y se avisa.

**Datos (Rol 1 produce, todos consumen):**
- `data/processed/pares_train.parquet`, `pares_test.parquet`: una fila por `(work_key, award_key)`, columna `label` (0/1), todas las columnas de award y de work de la OBT, más `work_abstract` y `award_description` (texto, puede venir vacío).
- `data/processed/manifest.json`: `tabla_origen`, `fecha_corte`, `sha256`, `filas`, `positivos`, `publicaciones`. `tracking` lo registra en cada corrida.
- `data/processed/emb_works.parquet`, `emb_awards.parquet`: embeddings `all-MiniLM-L6-v2` por llave.
- Se generan con `python -m src.datos`. Mientras no existan, `datos.cargar_pares()` usa la muestra `tests/fixtures/pares_muestra.parquet` (negativos al azar, sin abstracts: **solo para desarrollar**).

**Modelos (Roles 2 y 3 producen, Tech Lead consume):**
- `pipeline.modelo(estimador)` devuelve un `Pipeline` que recibe las columnas crudas (sin `label`) y hace todo: features, imputación, codificación, escalado y estimador. Ese pipeline completo es lo que se registra en MLflow y se sirve en el PSet 4.
- `candidatos_*.candidatos()` devuelve `{nombre: {"familia", "estimador", "espacio"}}`. `familia` ∈ `contrato.FAMILIAS`; `espacio` usa el prefijo `modelo__` para `RandomizedSearchCV`.
- El estimador debe tener `predict_proba` o `decision_function`.

**Evaluación (Tech Lead, igual para todos):**
- `GroupKFold(5)` por `work_key`: una publicación nunca está a la vez en train y en validación.
- Métrica técnica: **average precision** (`ap`). Métrica de negocio: **precision@3 por publicación** (`p_at_3`), con empates al azar.
- Todo modelo se registra con `tracking.registrar_corrida(...)`; nadie llama a `mlflow.log_*` por su cuenta.

**LLM:** el prompting con Claude (familia `sota`) lo corre solo el Tech Lead, que tiene la llave. Las respuestas quedan cacheadas y como artefacto en MLflow, así las métricas se pueden revisar sin la llave.

## Flujo de PRs

1. Partir de `main` actualizado: `git switch -c pset3/<rol> origin/main`.
2. Desarrollar contra la muestra (`GRANTMATCH_MUESTRA=1`) hasta que existan los datos reales.
3. Antes del PR: `python -m pytest tests -x` en verde.
4. PR con qué cambia y cómo se probó. El Tech Lead revisa, integra y corre los notebooks de punta a punta.

---

## Checklist de entrega (rúbrica del PSet)
- [ ] EDA y features (15 %): `01_eda`, `02_features`
- [ ] Preparación sin fuga y esquema de validación (15 %): `pipeline.py`, GroupKFold por `work_key`, split temporal
- [ ] Línea base y comparación de candidatos (15 %): Dummy, regla y una corrida por familia en `03`
- [ ] Ajuste de hiperparámetros (15 %): ≥ 30 configuraciones × 3 modelos, corridas anidadas en `04`
- [ ] Tracking y registro en MLflow (20 %): params, datos, métricas, tags, modelo con firma, alias; `docs/corridas.csv`
- [ ] Selección del modelo y métrica de negocio (15 %): precision@3 y evaluación única en test en `05`
- [ ] Reproducibilidad y documento técnico (5 %): Restart & Run All con outputs, README, memo
