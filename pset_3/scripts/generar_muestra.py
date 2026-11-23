"""Genera tests/fixtures/pares_muestra.parquet: unas 400 publicaciones de 2017-2024 con sus awards
(positivos) y 4 awards al azar por positivo (negativos simples).

Es solo para desarrollar y probar código mientras el Rol 1 construye los datos reales: los
negativos no tienen la lógica final (vigencia + difíciles) y no trae abstracts.

    python scripts/generar_muestra.py [ruta_csv]
"""
import hashlib
import json
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from src import contrato as C  # noqa: E402

N_WORKS = 400


def main(ruta_csv) -> None:
    obt = pd.read_csv(ruta_csv, low_memory=False)
    corte = str(obt["_obt_built_at"].max())
    cols = list(obt.columns)
    cols_award = ["award_key"] + cols[cols.index("funder_award_id"): cols.index("has_inconsistent_dates") + 1]
    cols_work = ["work_key"] + cols[cols.index("work_doi"): cols.index("author_country_codes") + 1]

    awards = obt[cols_award].drop_duplicates("award_key").reset_index(drop=True)
    works = obt[cols_work].drop_duplicates("work_key")
    en_rango = works["publication_year"].between(*C.ANIOS_TRAIN)
    elegidas = works[en_rango].sample(N_WORKS, random_state=C.SEMILLA)

    pos = obt[obt["work_key"].isin(elegidas["work_key"])][C.LLAVES]
    rng = np.random.default_rng(C.SEMILLA)
    recibidos = pos.groupby("work_key")["award_key"].apply(set)
    negativos = []
    for work, n_pos in pos["work_key"].value_counts().items():
        candidatos = awards["award_key"].to_numpy()[rng.choice(len(awards), 4 * n_pos * 2, replace=False)]
        candidatos = [a for a in candidatos if a not in recibidos[work]][: 4 * n_pos]
        negativos += [(work, a) for a in candidatos]
    neg = pd.DataFrame(negativos, columns=C.LLAVES)

    pares = pd.concat([pos.assign(label=1), neg.assign(label=0)], ignore_index=True)
    pares = pares.merge(works, on="work_key").merge(awards, on="award_key")
    pares["work_abstract"] = None
    pares["award_description"] = None
    pares = pares.sample(frac=1, random_state=C.SEMILLA).reset_index(drop=True)

    C.FIXTURES.mkdir(parents=True, exist_ok=True)
    pares.to_parquet(C.MUESTRA, index=False)
    C.MANIFEST_MUESTRA.write_text(json.dumps({
        "tabla_origen": C.TABLA_ORIGEN,
        "fecha_corte": corte,
        "sha256": hashlib.sha256(C.MUESTRA.read_bytes()).hexdigest(),
        "filas": len(pares),
        "positivos": int(pares["label"].sum()),
        "publicaciones": int(pares["work_key"].nunique()),
        "nota": "muestra de desarrollo con negativos al azar; no sirve para reportar métricas",
    }, indent=2, ensure_ascii=False) + "\n")
    print(f"{len(pares)} filas, {pares['label'].mean():.1%} positivos -> {C.MUESTRA}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else C.CSV_OBT)
