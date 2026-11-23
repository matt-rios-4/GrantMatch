"""Contrato de interfaces del PSet 3. Lo mantiene el Tech Lead: si cambias algo aquí, avisa al equipo."""
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")  # MLFLOW_TRACKING_URI, OPENALEX_MAILTO, ...
DATA = RAIZ / "data"
CRUDOS = DATA / "raw"
PROCESADOS = DATA / "processed"
FIXTURES = RAIZ / "tests" / "fixtures"

# Snapshot de la OBT del PSet 2 (OBT.OBT_AWARD_WORK exportada de Snowflake a CSV)
CSV_OBT = CRUDOS / "obt_award_work_full.csv"
TABLA_ORIGEN = "OBT.OBT_AWARD_WORK"

# Salidas del Rol 1
PARES_TRAIN = PROCESADOS / "pares_train.parquet"
PARES_TEST = PROCESADOS / "pares_test.parquet"
MANIFEST = PROCESADOS / "manifest.json"
EMB_WORKS = PROCESADOS / "emb_works.parquet"
EMB_AWARDS = PROCESADOS / "emb_awards.parquet"
MODELO_EMBEDDINGS = "sentence-transformers/all-MiniLM-L6-v2"

# Muestra chica para desarrollar sin los datos reales (no sirve para reportar métricas)
MUESTRA = FIXTURES / "pares_muestra.parquet"
MANIFEST_MUESTRA = FIXTURES / "manifest_muestra.json"

# Llaves y target
GRUPO = "work_key"
LLAVES = ["work_key", "award_key"]
TARGET = "label"
COLUMNAS_TEXTO = ["award_title", "award_description", "work_title", "work_abstract"]

# Corte temporal: train/CV con 2017-2024, prueba con 2025; 2026 queda fuera (año incompleto)
ANIOS_TRAIN = (2017, 2024)
ANIO_TEST = 2025
NEGATIVOS_POR_POSITIVO = 4
SEMILLA = 42

# Validación y métricas
N_FOLDS = 5            # GroupKFold por work_key
K = 3                  # top-3 avisos por publicación, como en el PSet 1
METRICA_TECNICA = "ap"  # average precision (PR-AUC)
METRICA_NEGOCIO = "p_at_3"

# MLflow
EXPERIMENTO = "grantmatch"
MODELO_REGISTRADO = "grantmatch-modelo"
FASES = ("baseline", "candidato", "tuning", "final")
FAMILIAS = ("dummy", "regla", "lineal", "bagging", "boosting", "red_neuronal", "sota")
