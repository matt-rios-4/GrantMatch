"""Datos del PSet 3. Dueño: Rol 1 (Datos).

Ya implementado (contrato): `cargar_pares` y `leer_manifest`.
Pendiente (Rol 1): las funciones con `NotImplementedError`, que producen los archivos de
`contrato.PROCESADOS`. Corre todo con `python -m src.datos` desde `pset_3/`.
"""
import json
import os

import pandas as pd

from src import contrato as C


def usar_muestra() -> bool:
    """GRANTMATCH_MUESTRA=1 obliga a usar la muestra; sin datos procesados también se usa."""
    return os.getenv("GRANTMATCH_MUESTRA") == "1" or not C.PARES_TRAIN.exists()


def cargar_pares(split: str = "train") -> pd.DataFrame:
    """Pares (work, award) con `label` 1/0. split: 'train' (2017-2024) o 'test' (2025)."""
    if split not in ("train", "test"):
        raise ValueError("split debe ser 'train' o 'test'")
    if usar_muestra():
        if split == "test":
            raise FileNotFoundError("La muestra no trae test: corre `python -m src.datos` (Rol 1).")
        return pd.read_parquet(C.MUESTRA)
    return pd.read_parquet(C.PARES_TRAIN if split == "train" else C.PARES_TEST)


def leer_manifest() -> dict:
    """Tabla de origen, fecha de corte y hash del dataset, para registrarlos en MLflow."""
    ruta = C.MANIFEST_MUESTRA if usar_muestra() else C.MANIFEST
    return json.loads(ruta.read_text())


def separar_xy(df: pd.DataFrame):
    """X (todas las columnas menos el target), y, grupos para GroupKFold."""
    return df.drop(columns=[C.TARGET]), df[C.TARGET].to_numpy(), df[C.GRUPO].to_numpy()


# ---------------------------------------------------------------- Rol 1


def cargar_obt() -> pd.DataFrame:
    """Lee `C.CSV_OBT` (un positivo por fila) y separa columnas del award y de la publicación."""
    raise NotImplementedError("Rol 1")


def descargar_abstracts(work_keys) -> pd.DataFrame:
    """Columnas `work_key`, `work_abstract` desde la API de OpenAlex (abstract_inverted_index).

    Lotes de hasta 100 IDs, `mailto=OPENALEX_MAILTO` (polite pool), caché en `C.CRUDOS`.
    Hacer lo mismo con la descripción de los awards (`award_description`) si existe.
    """
    raise NotImplementedError("Rol 1")


def generar_negativos(obt: pd.DataFrame) -> pd.DataFrame:
    """`C.NEGATIVOS_POR_POSITIVO` negativos por positivo, semilla `C.SEMILLA`.

    Solo awards vigentes en el año de la publicación y que esa publicación no recibió.
    Mitad al azar, mitad difíciles (mismo tema o mismo país).
    """
    raise NotImplementedError("Rol 1")


def construir_embeddings(pares: pd.DataFrame) -> None:
    """Escribe `C.EMB_WORKS` y `C.EMB_AWARDS` con `C.MODELO_EMBEDDINGS` (título + texto)."""
    raise NotImplementedError("Rol 1")


def construir_splits() -> None:
    """Escribe `C.PARES_TRAIN`, `C.PARES_TEST` y `C.MANIFEST` (tabla, corte, sha256, filas)."""
    raise NotImplementedError("Rol 1")


if __name__ == "__main__":
    construir_splits()
