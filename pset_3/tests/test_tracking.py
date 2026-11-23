import os

import mlflow
import numpy as np
import pytest
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression

os.environ["GRANTMATCH_MUESTRA"] = "1"

from src import contrato as C  # noqa: E402
from src import datos, pipeline, tracking  # noqa: E402


def test_precision_at_k_ordena_por_puntaje_dentro_de_cada_grupo():
    y = np.array([1, 0, 0, 0, 0, 1, 1, 0])
    s = np.array([.9, .8, .7, .1, .9, .8, .7, .1])
    g = np.array([1, 1, 1, 1, 2, 2, 2, 2])
    # grupo 1: top-3 tiene 1 pertinente; grupo 2: 2 pertinentes
    assert tracking.precision_at_k(y, s, g, k=3) == pytest.approx((1 / 3 + 2 / 3) / 2)


def test_precision_at_k_no_depende_del_orden_con_empates():
    y = np.array([1] * 5 + [0] * 45)
    g = np.zeros(50)
    ordenado = tracking.precision_at_k(y, np.zeros(50), g, k=3)
    invertido = tracking.precision_at_k(y[::-1], np.zeros(50), g, k=3)
    assert ordenado < 1.0 and invertido < 1.0


@pytest.fixture(scope="module")
def xyg():
    return datos.separar_xy(datos.cargar_pares("train"))


def test_validar_devuelve_media_y_desviacion(xyg):
    X, y, g = xyg
    res = tracking.validar(pipeline.modelo(LogisticRegression(max_iter=500)), X, y, g)
    assert {"cv_ap_mean", "cv_ap_std", "cv_p_at_3_mean", "cv_p_at_3_std"} <= res.keys()
    assert 0 <= res["cv_ap_mean"] <= 1


def test_registrar_corrida_deja_todo_lo_de_la_rubrica(xyg, tmp_path):
    X, y, g = xyg
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
    mlflow.set_experiment(C.EXPERIMENTO)
    run_id, res = tracking.registrar_corrida(
        pipeline.modelo(DummyClassifier(strategy="prior")), "dummy", "dummy", "baseline", X, y, g
    )
    run = mlflow.get_run(run_id)
    assert {"fase", "familia", "git_commit"} <= run.data.tags.keys()
    assert {"datos_tabla", "datos_corte", "datos_hash", "semilla"} <= run.data.params.keys()
    for m in ("train_ap", "cv_ap_mean", "cv_ap_std", "segundos_entrenamiento", "brecha_p_at_3"):
        assert m in run.data.metrics
    cargado = mlflow.sklearn.load_model(f"runs:/{run_id}/modelo")
    assert cargado.predict_proba(X.head(3)).shape == (3, 2)
    # el Dummy no ordena: su p@3 en CV queda cerca de la tasa de positivos
    assert res["cv_p_at_3_mean"] == pytest.approx(y.mean(), abs=0.05)


def test_rechaza_fase_desconocida(xyg):
    X, y, g = xyg
    with pytest.raises(ValueError):
        tracking.registrar_corrida(DummyClassifier(), "x", "dummy", "exploracion", X, y, g)
