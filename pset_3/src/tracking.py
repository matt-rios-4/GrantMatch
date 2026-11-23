"""Evaluación y registro en MLflow. Dueño: Tech Lead.

Todos los modelos pasan por `registrar_corrida`, así comparten validación, métricas y lo que
pide la rúbrica: parámetros, datos (tabla, corte, hash), métricas de train y CV, tiempo, tags
(fase, familia, commit) y el pipeline con firma y ejemplo de entrada.
"""
import os
import subprocess
import time

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.base import clone
from sklearn.metrics import average_precision_score
from sklearn.model_selection import GroupKFold

from src import contrato as C
from src import datos


def precision_at_k(y, puntaje, grupos, k: int = C.K, semilla: int = C.SEMILLA) -> float:
    """Promedio por publicación de la fracción de pertinentes entre sus k mejores candidatos.

    Los empates se rompen al azar: con un puntaje constante (Dummy) el orden de las filas no
    debe decidir el resultado.
    """
    desempate = np.random.default_rng(semilla).random(len(y))
    df = pd.DataFrame({"y": y, "s": puntaje, "g": grupos, "d": desempate})
    df = df.sort_values(["g", "s", "d"], ascending=[True, False, True])
    top = df.groupby("g", sort=False).head(k)
    return float(top.groupby("g")["y"].mean().mean())


def puntuar(modelo, X) -> np.ndarray:
    if hasattr(modelo, "predict_proba"):
        return modelo.predict_proba(X)[:, 1]
    return np.asarray(modelo.decision_function(X), dtype=float)


def metricas(y, puntaje, grupos) -> dict:
    return {
        C.METRICA_TECNICA: float(average_precision_score(y, puntaje)),
        C.METRICA_NEGOCIO: precision_at_k(y, puntaje, grupos),
    }


def validar(modelo, X, y, grupos, n_folds: int = C.N_FOLDS) -> dict:
    """GroupKFold por work_key: una publicación nunca está a la vez en train y en validación."""
    por_fold = []
    for idx_tr, idx_va in GroupKFold(n_splits=n_folds).split(X, y, grupos):
        m = clone(modelo).fit(X.iloc[idx_tr], y[idx_tr])
        por_fold.append(metricas(y[idx_va], puntuar(m, X.iloc[idx_va]), grupos[idx_va]))
    folds = pd.DataFrame(por_fold)
    res = {}
    for nombre in folds.columns:
        res[f"cv_{nombre}_mean"] = float(folds[nombre].mean())
        res[f"cv_{nombre}_std"] = float(folds[nombre].std(ddof=1))
    return res


def commit_git() -> str:
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=C.RAIZ, text=True).strip()
        sucio = subprocess.check_output(["git", "status", "--porcelain", "--", "src"], cwd=C.RAIZ, text=True)
        return sha + ("-dirty" if sucio.strip() else "")
    except (OSError, subprocess.CalledProcessError):
        return "desconocido"


def _params_planos(modelo) -> dict:
    params = modelo.get_params(deep=True)
    return {k: str(v)[:250] for k, v in params.items() if not hasattr(v, "get_params")}


def _ejemplo_entrada(X: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """MLflow no infiere el tipo de una columna de texto que viene toda en None: se declara str."""
    ejemplo = X.head(n).copy()
    texto = ejemplo.select_dtypes(include="object").columns
    ejemplo[texto] = ejemplo[texto].astype("string")
    return ejemplo


def configurar_mlflow() -> None:
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5050"))
    mlflow.set_experiment(C.EXPERIMENTO)


def registrar_corrida(
    modelo,
    nombre: str,
    familia: str,
    fase: str,
    X: pd.DataFrame,
    y: np.ndarray,
    grupos: np.ndarray,
    params_extra: dict | None = None,
    nested: bool = False,
    guardar_modelo: bool = True,
) -> tuple[str, dict]:
    """Valida con CV, entrena con todo train, registra la corrida y devuelve (run_id, métricas)."""
    if fase not in C.FASES:
        raise ValueError(f"fase debe ser una de {C.FASES}")
    if familia not in C.FAMILIAS:
        raise ValueError(f"familia debe ser una de {C.FAMILIAS}")

    resultado = validar(modelo, X, y, grupos)
    inicio = time.perf_counter()
    final = clone(modelo).fit(X, y)
    resultado["segundos_entrenamiento"] = time.perf_counter() - inicio
    puntaje_train = puntuar(final, X)
    for k, v in metricas(y, puntaje_train, grupos).items():
        resultado[f"train_{k}"] = v
    for k in (C.METRICA_TECNICA, C.METRICA_NEGOCIO):
        resultado[f"brecha_{k}"] = resultado[f"train_{k}"] - resultado[f"cv_{k}_mean"]

    manifest = datos.leer_manifest()
    with mlflow.start_run(run_name=nombre, nested=nested) as run:
        mlflow.set_tags({"fase": fase, "familia": familia, "git_commit": commit_git()})
        mlflow.log_params({
            "semilla": C.SEMILLA,
            "n_folds": C.N_FOLDS,
            "esquema_cv": "GroupKFold(work_key)",
            "datos_tabla": manifest["tabla_origen"],
            "datos_corte": manifest["fecha_corte"],
            "datos_hash": manifest["sha256"],
            "datos_filas": len(X),
            **_params_planos(modelo),
            **(params_extra or {}),
        })
        mlflow.log_metrics(resultado)
        if guardar_modelo:
            ejemplo = _ejemplo_entrada(X)
            mlflow.sklearn.log_model(
                final,
                artifact_path="modelo",
                signature=infer_signature(ejemplo, puntuar(final, ejemplo)),
                input_example=ejemplo,
            )
        return run.info.run_id, resultado
