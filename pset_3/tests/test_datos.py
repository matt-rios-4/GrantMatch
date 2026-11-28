"""Pruebas de src/datos.py (Rol 1). Usan una OBT sintética de pocas filas: no leen el CSV real."""
import json
import os

os.environ["GRANTMATCH_MUESTRA"] = "1"

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from src import contrato as C  # noqa: E402
from src import datos  # noqa: E402

COLUMNAS_OBT = [
    "award_key", "work_key", "label", "funder_award_id", "award_title", "has_description", "funder_scheme",
    "funding_type", "funder_key", "funder_name", "funder_country_code", "is_global_south", "institution_key",
    "lead_institution_name", "lead_institution_country_code", "lead_institution_type", "award_topic_key",
    "award_topic_name", "award_subfield_name", "award_field_name", "award_domain_name", "start_date_key",
    "award_start_date", "award_start_year", "award_us_fiscal_year", "end_date_key", "amount", "currency",
    "amount_usd", "duration_days", "funded_outputs_count", "institutions_count", "is_amount_invalid",
    "has_inconsistent_dates", "work_doi", "work_title", "has_abstract", "publication_year",
    "publication_date_key", "publication_date", "work_type", "language", "cited_by_count", "fwci",
    "is_retracted", "authors_count", "work_topic_key", "work_topic_name", "work_subfield_name",
    "work_field_name", "work_domain_name", "linked_authors_count", "first_author_key", "first_author_name",
    "author_country_codes", "_obt_built_at",
]


def obt_sintetica(n_awards=120, n_works=60, semilla=0) -> pd.DataFrame:
    """Awards y works inventados: años 2016-2026, algunos awards con fechas y otros sin ellas."""
    rng = np.random.default_rng(semilla)
    paises = ["US", "CN", "JP", "DE"]
    inicio = rng.integers(2012, 2025, n_awards).astype(float)
    inicio[rng.random(n_awards) < 0.6] = np.nan  # la mayoría de los awards no trae fechas
    awards = pd.DataFrame({
        "award_key": [f"G{i}" for i in range(n_awards)],
        "award_start_year": inicio,
        "end_date_key": np.where(np.isnan(inicio), -1, (inicio + 4) * 10000 + 1231).astype(int),
        "funder_country_code": rng.choice(paises, n_awards),
        "award_topic_key": rng.choice(["UNKNOWN", "T1", "T2"], n_awards, p=[0.8, 0.1, 0.1]),
        "award_title": [f"award {i}" for i in range(n_awards)],
    })
    filas = []
    for i in range(n_works):
        anio = int(rng.integers(2016, 2027))
        for j in rng.choice(n_awards, int(rng.integers(1, 4)), replace=False):
            filas.append({
                "award_key": f"G{j}", "work_key": f"W{i}", "publication_year": float(anio),
                "work_topic_key": rng.choice(["T1", "T2", "T3"]), "work_title": f"work {i}",
                "author_country_codes": json.dumps(list(rng.choice(paises, 2, replace=False))),
            })
    obt = pd.DataFrame(filas).merge(awards, on="award_key")
    obt["label"] = 1
    obt["has_abstract"] = True
    obt["has_description"] = False
    obt["_obt_built_at"] = "2026-10-23 14:58:29.260618"
    for col in COLUMNAS_OBT:
        if col not in obt.columns:
            obt[col] = None
    return obt[COLUMNAS_OBT]


@pytest.fixture(scope="module")
def obt():
    return obt_sintetica()


@pytest.fixture(scope="module")
def negativos(obt):
    return datos.generar_negativos(obt)


def test_columnas_se_separan_en_award_y_work(obt):
    award, work = datos.columnas_award(obt), datos.columnas_work(obt)
    assert award[0] == "award_key" and "funder_country_code" in award and "work_key" not in award
    assert work[0] == "work_key" and "author_country_codes" in work and C.TARGET not in work + award
    assert not set(award[1:]) & set(work[1:])


def test_cuatro_negativos_por_positivo(obt, negativos):
    positivos = obt[obt["publication_year"].between(*C.ANIOS_TRAIN) | (obt["publication_year"] == C.ANIO_TEST)]
    por_work_pos = positivos.groupby("work_key").size()
    por_work_neg = negativos.groupby("work_key").size()
    assert (por_work_neg == C.NEGATIVOS_POR_POSITIVO * por_work_pos.reindex(por_work_neg.index)).all()
    assert set(negativos["work_key"]) == set(positivos["work_key"])
    assert (negativos[C.TARGET] == 0).all()


def test_un_negativo_nunca_es_un_award_que_la_publicacion_recibio(obt, negativos):
    pares = pd.concat([obt[C.LLAVES], negativos[C.LLAVES]])
    assert not pares.duplicated().any()


def test_los_negativos_son_awards_vigentes_ese_anio(obt, negativos):
    ventanas = {c: datos._ventanas(obt, c) for c in (C.ANIOS_TRAIN[1], C.ANIO_TEST)}
    anio = obt.drop_duplicates("work_key").set_index("work_key")["publication_year"]
    for fila in negativos.itertuples():
        a = anio[fila.work_key]
        v = ventanas[C.ANIOS_TRAIN[1] if a <= C.ANIOS_TRAIN[1] else C.ANIO_TEST].loc[fila.award_key]
        assert v["inicio"] <= a <= v["fin"], (fila, v)


def test_la_ventana_de_cada_award_incluye_los_anios_de_sus_positivos(obt):
    for corte in (C.ANIOS_TRAIN[1], C.ANIO_TEST):
        v = datos._ventanas(obt, corte)
        vistos = obt[obt["publication_year"] <= corte]
        unidos = vistos.join(v, on="award_key")
        assert (unidos["inicio"] <= unidos["publication_year"]).all()
        assert (unidos["publication_year"] <= unidos["fin"]).all()


def test_el_train_no_usa_el_anio_de_prueba_para_la_vigencia(obt):
    """Un award sin fechas cuya única publicación es de 2025 no tiene ventana para construir el train."""
    solo_2025 = obt.copy()
    solo_2025["award_start_year"] = np.nan
    solo_2025["end_date_key"] = -1
    solo_2025.loc[solo_2025["award_key"] == "G0", "publication_year"] = 2025.0
    solo_2025 = solo_2025[(solo_2025["award_key"] != "G0") | (solo_2025["publication_year"] == 2025.0)]
    ventana = datos._ventanas(solo_2025, C.ANIOS_TRAIN[1]).loc["G0"]
    assert np.isnan(ventana["inicio"]) and np.isnan(ventana["fin"])


def test_negativos_dificiles_comparten_pais_o_tema(obt, negativos):
    paises = obt.drop_duplicates("work_key").set_index("work_key")["author_country_codes"].map(json.loads)
    temas = obt.drop_duplicates("work_key").set_index("work_key")["work_topic_key"]
    awards = obt.drop_duplicates("award_key").set_index("award_key")
    dificiles = negativos[negativos["tipo"] == "dificil"]
    assert len(dificiles) > 0
    for fila in dificiles.itertuples():
        a = awards.loc[fila.award_key]
        comparte = a["funder_country_code"] in paises[fila.work_key] or a["award_topic_key"] == temas[fila.work_key]
        assert comparte, fila


def test_los_negativos_son_reproducibles(obt, negativos):
    otra = datos.generar_negativos(obt)
    pd.testing.assert_frame_equal(negativos, otra)


def test_reconstruir_abstract_ordena_las_palabras():
    indice = {"graph": [2], "A": [0], "neural": [1], "networks": [3, 5], "on": [4]}
    assert datos._reconstruir_abstract(indice) == "A neural graph networks on networks"
    assert datos._reconstruir_abstract(None) == ""


def test_texto_completo_une_titulo_y_cuerpo():
    titulo = pd.Series(["Un titulo", "Solo titulo", None, None])
    cuerpo = pd.Series(["cuerpo", None, "solo cuerpo", None])
    assert datos._texto_completo(titulo, cuerpo).tolist() == ["Un titulo. cuerpo", "Solo titulo", "solo cuerpo", ""]


class RespuestaFalsa:
    status_code = 200

    def __init__(self, resultados):
        self._resultados = resultados

    def json(self):
        return {"results": self._resultados}


def test_descargar_abstracts_pide_lotes_de_100_y_usa_el_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(datos, "CACHE_ABSTRACTS", tmp_path / "abstracts.jsonl")
    monkeypatch.setattr(datos.time, "sleep", lambda s: None)
    llamadas = []

    def get_falso(url, params, timeout):
        ids = params["filter"].split(":", 1)[1].split("|")
        llamadas.append(ids)
        # OpenAlex no devuelve W3 (no existe) y W1 no tiene abstract
        return RespuestaFalsa([
            {"id": f"https://openalex.org/{i}", "abstract_inverted_index": None if i == "W1" else {"hola": [0], "mundo": [1]}}
            for i in ids if i != "W3"
        ])

    monkeypatch.setattr(datos.requests, "get", get_falso)
    llaves = [f"W{i}" for i in range(250)]
    resultado = datos.descargar_abstracts(llaves)

    assert [len(lote) for lote in llamadas] == [100, 100, 50]
    assert resultado["work_key"].tolist() == llaves
    por_llave = resultado.set_index("work_key")["work_abstract"]
    assert por_llave["W0"] == "hola mundo"
    assert por_llave["W1"] is None and por_llave["W3"] is None

    datos.descargar_abstracts(llaves)  # todo en caché: no hay llamadas nuevas
    assert len(llamadas) == 3


def test_construir_splits_separa_por_anio_y_no_mezcla_publicaciones(tmp_path, monkeypatch):
    obt = obt_sintetica()
    csv = tmp_path / "obt.csv"
    obt.to_csv(csv, index=False)
    procesados = tmp_path / "processed"
    monkeypatch.setattr(C, "CSV_OBT", csv)
    monkeypatch.setattr(C, "PROCESADOS", procesados)
    monkeypatch.setattr(C, "PARES_TRAIN", procesados / "pares_train.parquet")
    monkeypatch.setattr(C, "PARES_TEST", procesados / "pares_test.parquet")
    monkeypatch.setattr(C, "MANIFEST", procesados / "manifest.json")
    monkeypatch.setattr(datos, "NEGATIVOS_TIPO", procesados / "negativos_tipo.parquet")

    datos.construir_splits(descargar=False, embeddings=False)
    train, test = pd.read_parquet(C.PARES_TRAIN), pd.read_parquet(C.PARES_TEST)

    assert train["publication_year"].between(*C.ANIOS_TRAIN).all()
    assert (test["publication_year"] == C.ANIO_TEST).all()
    assert not set(train["work_key"]) & set(test["work_key"])
    de_2026 = set(obt.loc[obt["publication_year"] == 2026, "work_key"])
    assert de_2026 and not de_2026 & (set(train["work_key"]) | set(test["work_key"]))
    for pares in (train, test):
        assert not pares.duplicated(C.LLAVES).any()
        assert pares[C.TARGET].mean() == pytest.approx(1 / (1 + C.NEGATIVOS_POR_POSITIVO))
        for col in C.COLUMNAS_TEXTO:
            assert col in pares.columns
    manifest = json.loads(C.MANIFEST.read_text())
    assert {"tabla_origen", "fecha_corte", "sha256", "filas", "positivos", "publicaciones"} <= manifest.keys()
    assert manifest["filas"] == len(train) and manifest["test"]["filas"] == len(test)
