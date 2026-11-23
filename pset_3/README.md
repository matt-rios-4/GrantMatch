# PSet 3: modelado de GrantMatch

Clasificador de pares publicación–award para recomendar qué awards financiaron una publicación. Roles, contrato de interfaces y checklist: [docs/ROLES.md](docs/ROLES.md).

## Setup

Python 3.11.

```bash
cd pset_3
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.txt
# o: python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env
```

## Datos

El PSet 2 terminó en `OBT.OBT_AWARD_WORK` en Snowflake (cuenta de prueba, ya vencida). La tabla se exportó completa a CSV antes de que venciera; ese snapshot es la fuente del PSet 3:

- Bajar `obt_award_work_full.csv` (155 MB, [Drive del equipo](https://drive.google.com/drive/folders/1R5fdg2A-RmjSpoBZN5QAoK0HI69aw6cr)) a `pset_3/data/raw/`. No se versiona: pasa el límite de 100 MB de GitHub.
- Comprobar que es el mismo snapshot: `shasum -a 256 data/raw/obt_award_work_full.csv` debe dar `ef2b43871376f294d3f15613890694c8017ba53f61d6ec00089cd3d0d4ac3052`.
- `python -m src.datos` genera los pares con negativos, los abstracts de OpenAlex y los embeddings en `data/processed/` (ver ROLES).
- Sin esos archivos, todo corre sobre `tests/fixtures/pares_muestra.parquet`, una muestra de 400 publicaciones con negativos al azar. Sirve para desarrollar, **no** para reportar métricas.

## MLflow

```bash
docker compose up -d          # postgres + servidor MLflow
open http://127.0.0.1:5050
```

Experimento `grantmatch`, modelo registrado `grantmatch-modelo` (alias `champion` y `challenger`). Las corridas se registran solo a través de `src/tracking.py`. Al final, `python scripts/exportar_corridas.py` deja `docs/corridas.csv`.

## Notebooks

En orden, cada uno con *Restart & Run All* y outputs guardados:

1. `01_eda`: datos, target y cobertura de texto.
2. `02_features`: features y revisión de fuga.
3. `03_baseline_candidatos`: baselines y una corrida por familia.
4. `04_tuning`: búsqueda de hiperparámetros de los 3 mejores.
5. `05_seleccion`: modelo final, evaluación única en test 2025 y registro.

## Pruebas

```bash
.venv/bin/python -m pytest tests -x
```
