"""Baselines y modelos clásicos. Dueño: Rol 2.

Contrato: `candidatos()` devuelve {nombre: {"familia", "estimador", "espacio"}}.
- `familia` es una de contrato.FAMILIAS.
- `estimador` es el estimador suelto: 03_baseline_candidatos lo envuelve con pipeline.modelo().
- `espacio` son las distribuciones para RandomizedSearchCV en 04_tuning, con prefijo "modelo__"
  (vacío en los baselines).

Los dos de abajo son ejemplos para que 03 corra desde el día 1. Rol 2 agrega la regla de negocio
(familia "regla"), ajusta el lineal y agrega bagging (RandomForest / ExtraTrees).
"""
from scipy.stats import loguniform
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression

from src import contrato as C


def candidatos() -> dict[str, dict]:
    return {
        "dummy_prior": {
            "familia": "dummy",
            "estimador": DummyClassifier(strategy="prior"),
            "espacio": {},
        },
        "logistica": {
            "familia": "lineal",
            "estimador": LogisticRegression(max_iter=1000, random_state=C.SEMILLA),
            "espacio": {
                "modelo__C": loguniform(1e-3, 1e2),
                "modelo__class_weight": [None, "balanced"],
            },
        },
    }
