"""Preparación de features. Dueño: Rol 2 (Features).

Versión mínima de arranque para que el resto del equipo pueda correr modelos sobre la muestra.
Rol 2 la reemplaza con las features definitivas (similitud de embeddings, etc.) y documenta en
02_features.ipynb por qué cada una existe en el momento de la inferencia.

Contrato: `modelo(estimador)` recibe las columnas crudas de `datos.cargar_pares` (sin `label`) y
devuelve un Pipeline que hace todo: features, imputación, codificación, escalado y el estimador.
"""
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

NIVELES_TEMA = ["topic_name", "subfield_name", "field_name", "domain_name"]

FEATURES_NUM = [
    "mismo_topic", "mismo_subfield", "mismo_field", "mismo_domain",
    "pais_funder_en_autores", "anios_desde_inicio_award", "duration_days",
    "institutions_count", "authors_count",
]
FEATURES_CAT = ["funding_type", "work_type"]


def construir_features(df: pd.DataFrame) -> pd.DataFrame:
    """Features derivadas del par. Solo usa columnas que existen al recomendar (sin `label`)."""
    out = df.copy()
    # al servir, MLflow entrega el texto como dtype "string" (con pd.NA): se vuelve a object/None
    for col in out.select_dtypes(include="string").columns:
        out[col] = out[col].astype(object).where(out[col].notna(), None)
    for nivel in NIVELES_TEMA:
        corto = nivel.removesuffix("_name")
        out[f"mismo_{corto}"] = (
            out[f"award_{nivel}"].notna() & (out[f"award_{nivel}"] == out[f"work_{nivel}"])
        ).astype(float)
    # author_country_codes viene como lista JSON en texto: '["CN", "US"]'
    paises = out["author_country_codes"].fillna("").astype(str)
    funder = out["funder_country_code"].fillna("").astype(str)
    out["pais_funder_en_autores"] = [
        float(bool(f) and f'"{f}"' in p) for f, p in zip(funder, paises)
    ]
    out["anios_desde_inicio_award"] = out["publication_year"] - out["award_start_year"]
    return out[FEATURES_NUM + FEATURES_CAT]


def preprocesador() -> Pipeline:
    numericas = make_pipeline(SimpleImputer(strategy="median"), StandardScaler())
    categoricas = make_pipeline(
        SimpleImputer(strategy="constant", fill_value="desconocido"),
        OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=20),
    )
    return Pipeline([
        ("features", FunctionTransformer(construir_features)),
        ("columnas", ColumnTransformer([
            ("num", numericas, FEATURES_NUM),
            ("cat", categoricas, FEATURES_CAT),
        ])),
    ])


def modelo(estimador) -> Pipeline:
    """Pipeline completo: lo que se entrena, se registra en MLflow y se sirve en el PSet 4."""
    return Pipeline([("prep", preprocesador()), ("modelo", estimador)])
