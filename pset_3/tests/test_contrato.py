import os

import pandas as pd
import pytest

os.environ["GRANTMATCH_MUESTRA"] = "1"

from src import contrato as C  # noqa: E402
from src import datos, pipeline  # noqa: E402


@pytest.fixture(scope="module")
def pares():
    return datos.cargar_pares("train")


def test_muestra_cumple_el_contrato(pares):
    for col in C.LLAVES + [C.TARGET, "publication_year"] + C.COLUMNAS_TEXTO:
        assert col in pares.columns
    assert set(pares[C.TARGET].unique()) == {0, 1}
    assert not pares.duplicated(C.LLAVES).any()
    assert pares["publication_year"].between(*C.ANIOS_TRAIN).all()


def test_cada_publicacion_tiene_positivos_y_negativos(pares):
    por_work = pares.groupby(C.GRUPO)[C.TARGET].agg(["sum", "count"])
    assert (por_work["sum"] >= 1).all()
    assert (por_work["count"] > por_work["sum"]).all()


def test_manifest_trae_lo_que_registra_mlflow():
    m = datos.leer_manifest()
    assert {"tabla_origen", "fecha_corte", "sha256"} <= m.keys()


def test_features_no_usan_el_target(pares):
    X, _, _ = datos.separar_xy(pares)
    feats = pipeline.construir_features(X)
    assert list(feats.columns) == pipeline.FEATURES_NUM + pipeline.FEATURES_CAT
    assert C.TARGET not in feats.columns


def test_pais_funder_en_autores():
    fila = pd.DataFrame({
        "author_country_codes": ['["CN", "US"]', '["CN"]', None],
        "funder_country_code": ["US", "US", "US"],
    })
    base = {c: None for c in ["publication_year", "award_start_year"]}
    for nivel in pipeline.NIVELES_TEMA:
        base[f"award_{nivel}"] = base[f"work_{nivel}"] = None
    for c in ["duration_days", "institutions_count", "authors_count", "funding_type", "work_type"]:
        base[c] = None
    fila = fila.assign(**base).astype({"publication_year": float, "award_start_year": float})
    assert pipeline.construir_features(fila)["pais_funder_en_autores"].tolist() == [1.0, 0.0, 0.0]
