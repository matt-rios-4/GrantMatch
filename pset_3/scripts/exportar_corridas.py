"""Exporta todas las corridas del experimento a docs/corridas.csv (entregable de la rúbrica).

    python scripts/exportar_corridas.py
"""
import sys
from pathlib import Path

import mlflow

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import contrato as C  # noqa: E402
from src import tracking  # noqa: E402


def main() -> None:
    tracking.configurar_mlflow()
    corridas = mlflow.search_runs(experiment_names=[C.EXPERIMENTO], order_by=["start_time ASC"])
    columnas = (
        ["run_id", "tags.mlflow.runName", "tags.fase", "tags.familia", "tags.git_commit",
         "tags.mlflow.parentRunId", "start_time", "status"]
        + sorted(c for c in corridas.columns if c.startswith("metrics."))
        + sorted(c for c in corridas.columns if c.startswith("params."))
    )
    destino = C.RAIZ / "docs" / "corridas.csv"
    corridas.reindex(columns=columnas).to_csv(destino, index=False)
    print(f"{len(corridas)} corridas -> {destino}")


if __name__ == "__main__":
    main()
