"""Datos del PSet 3. Dueño: Rol 1 (Datos).

Convierte el snapshot de la OBT (positivos) en los pares de entrenamiento y prueba:

    python -m src.datos                # pares + abstracts + embeddings + manifest
    python -m src.datos --solo-pares   # solo pares (sin descargas ni embeddings), para depurar

Deja en `contrato.PROCESADOS`:

    pares_train.parquet, pares_test.parquet   una fila por (work_key, award_key), con `label`
    manifest.json                             tabla de origen, fecha de corte, hashes y conteos
    emb_works.parquet, emb_awards.parquet     embeddings all-MiniLM-L6-v2 (`work_key`/`award_key`
                                              y 384 columnas float32 e000..e383, normalizados)
    negativos_tipo.parquet                    solo para el EDA: de qué tipo es cada negativo
                                              (nunca entra a los pares: revelaría el target)

`cargar_pares` y `leer_manifest` son del contrato: las usa todo el equipo.
"""
import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import contrato as C

# ------------------------------------------------------------------ constantes propias del Rol 1
API_OPENALEX = "https://api.openalex.org"
LOTE_API = 100                  # IDs por consulta (máximo de OpenAlex)
MAX_CARACTERES_TEXTO = 2000     # abstracts y descripciones se recortan: el modelo de embeddings lee ~256 tokens
GRACIA_ANIOS = 3                # un award sigue "vigente" para publicar 3 años después de su fin
ANTES_DEL_PRIMER_REGISTRO = 3   # sin fecha de inicio: se supone 3 años antes de su primera publicación
DURACION_TIPICA = 4             # sin fecha de fin: se supone inicio + 4 años (mediana en la OBT)
# Peso del "mismo topic" entre las fuentes de negativos difíciles. Solo el 2.6 % de los awards trae topic:
# si el topic compitiera de igual a igual con los países, el 10 % de los negativos tendría topic contra
# el 2.4 % de los positivos, y el modelo aprendería "award con topic = negativo" (un atajo, no pertinencia).
PESO_TEMA = 0.05
INTENTOS_API = 5
CACHE_ABSTRACTS = C.CRUDOS / "openalex_abstracts.jsonl"
CACHE_DESCRIPCIONES = C.CRUDOS / "openalex_descripciones.jsonl"
NEGATIVOS_TIPO = C.PROCESADOS / "negativos_tipo.parquet"
LOTE_EMBEDDINGS = 64
DIM_EMBEDDING = 384


# ------------------------------------------------------------------ contrato (ya implementado)
def usar_muestra() -> bool:
    """GRANTMATCH_MUESTRA=1 obliga a usar la muestra; sin datos procesados también se usa."""
    return os.getenv("GRANTMATCH_MUESTRA") == "1" or not C.PARES_TRAIN.exists()


def cargar_pares(split: str = "train", columnas=None) -> pd.DataFrame:
    """Pares (work, award) con `label` 1/0. split: 'train' (2017-2024) o 'test' (2025).

    `columnas` (opcional) lee solo esas columnas: los textos (abstracts, descripciones) pesan
    más de 1 GB en memoria, y para un EDA o un modelo sin texto no hacen falta.
    """
    if split not in ("train", "test"):
        raise ValueError("split debe ser 'train' o 'test'")
    if usar_muestra():
        if split == "test":
            raise FileNotFoundError("La muestra no trae test: corre `python -m src.datos` (Rol 1).")
        return pd.read_parquet(C.MUESTRA, columns=columnas)
    return pd.read_parquet(C.PARES_TRAIN if split == "train" else C.PARES_TEST, columns=columnas)


def leer_manifest() -> dict:
    """Tabla de origen, fecha de corte y hash del dataset, para registrarlos en MLflow."""
    ruta = C.MANIFEST_MUESTRA if usar_muestra() else C.MANIFEST
    return json.loads(ruta.read_text())


def separar_xy(df: pd.DataFrame):
    """X (todas las columnas menos el target), y, grupos para GroupKFold."""
    return df.drop(columns=[C.TARGET]), df[C.TARGET].to_numpy(), df[C.GRUPO].to_numpy()


def cargar_embeddings(tipo: str):
    """Embeddings de 'works' o 'awards': (llaves, matriz float32 n x 384)."""
    ruta = {"works": C.EMB_WORKS, "awards": C.EMB_AWARDS}[tipo]
    df = pd.read_parquet(ruta)
    llave = df.columns[0]
    return df[llave].to_numpy(), df.drop(columns=[llave]).to_numpy(dtype=np.float32)


# ------------------------------------------------------------------ Rol 1: OBT
def _compactar_json(texto):
    """'[\\n  "US"\\n]' -> '["US"]': la OBT guarda las listas con saltos de línea."""
    if not isinstance(texto, str):
        return None
    try:
        return json.dumps(json.loads(texto), ensure_ascii=False)
    except ValueError:
        return texto


def columnas_award(obt: pd.DataFrame) -> list:
    cols = list(obt.columns)
    return ["award_key"] + cols[cols.index("funder_award_id"): cols.index("has_inconsistent_dates") + 1]


def columnas_work(obt: pd.DataFrame) -> list:
    cols = list(obt.columns)
    return ["work_key"] + cols[cols.index("work_doi"): cols.index("author_country_codes") + 1]


def cargar_obt(ruta=None) -> pd.DataFrame:
    """Lee `C.CSV_OBT` (un positivo por fila) y separa columnas del award y de la publicación.

    Devuelve la OBT completa con `label` = 1 (todas sus filas son positivos). Usa
    `columnas_award(obt)` y `columnas_work(obt)` para separar los dos lados del par.
    """
    ruta = Path(ruta) if ruta else C.CSV_OBT
    if not ruta.exists():
        raise FileNotFoundError(f"No encuentro {ruta}: baja el snapshot al Drive del equipo (ver README).")
    obt = pd.read_csv(ruta, dtype={"award_key": str, "work_key": str}, low_memory=False)
    if obt.duplicated(C.LLAVES).any():
        raise ValueError("La OBT trae pares (work_key, award_key) repetidos")
    if not (obt[C.TARGET] == 1).all():
        raise ValueError("Todas las filas de la OBT deben ser positivos (label = 1)")
    compactas = {u: _compactar_json(u) for u in obt["author_country_codes"].dropna().unique()}
    obt["author_country_codes"] = obt["author_country_codes"].map(compactas)
    return obt


def _ventanas(obt: pd.DataFrame, hasta_anio: int) -> pd.DataFrame:
    """Años en que cada award está vigente (`inicio`, `fin`), con lo que se sabe hasta `hasta_anio`.

    El 84 % de los awards no trae fechas, así que la vigencia se completa con sus publicaciones
    observadas (hasta `hasta_anio`: nada del año de prueba entra al train). Dentro de la ventana
    caen siempre los años en que el award tiene un positivo.
    """
    awards = obt.drop_duplicates("award_key").set_index("award_key")
    inicio = awards["award_start_year"]
    fin = awards["end_date_key"].where(awards["end_date_key"] > 0) // 10000
    vistos = obt[obt["publication_year"] <= hasta_anio].groupby("award_key")["publication_year"].agg(["min", "max"])
    vistos = vistos.reindex(awards.index)

    ini = inicio.fillna(vistos["min"] - ANTES_DEL_PRIMER_REGISTRO).fillna(fin - DURACION_TIPICA)
    fin_est = fin.fillna(inicio + DURACION_TIPICA).fillna(vistos["max"])
    ini = pd.Series(np.fmin(ini, vistos["min"]), index=awards.index)
    fin_est = pd.Series(np.fmax(fin_est, vistos["max"]), index=awards.index) + GRACIA_ANIOS
    return pd.DataFrame({"inicio": ini, "fin": fin_est})


def generar_negativos(obt: pd.DataFrame) -> pd.DataFrame:
    """`C.NEGATIVOS_POR_POSITIVO` negativos por positivo, semilla `C.SEMILLA`.

    Solo awards vigentes en el año de la publicación y que esa publicación no recibió.
    Mitad al azar, mitad difíciles (mismo tema o mismo país).

    Devuelve `work_key`, `award_key`, `label` (0) y `tipo` ('azar' o 'dificil'), para los
    positivos de 2017 a 2025. Un negativo es difícil si el award tiene el mismo topic que la
    publicación o si el país de su funder está entre los países de los autores (el topic pesa
    `PESO_TEMA`, ver arriba). Cuando una
    publicación no tiene suficientes candidatos difíciles, se completa con azar.
    `resultado.attrs['estadisticas']` guarda los conteos por año.
    """
    rng = np.random.default_rng(C.SEMILLA)
    n_neg = C.NEGATIVOS_POR_POSITIVO
    positivos = obt[obt["publication_year"].between(C.ANIOS_TRAIN[0], C.ANIO_TEST)]

    awards = obt.drop_duplicates("award_key")[["award_key", "funder_country_code", "award_topic_key"]]
    awards = awards.reset_index(drop=True)
    indice = pd.Series(awards.index.to_numpy(), index=awards["award_key"])
    llaves = awards["award_key"].to_numpy(object)
    pais = awards["funder_country_code"].fillna("").to_numpy(object)
    tema = awards["award_topic_key"].where(awards["award_topic_key"] != "UNKNOWN").fillna("").to_numpy(object)
    ventanas = {
        corte: _ventanas(obt, corte).reindex(awards["award_key"])
        for corte in (C.ANIOS_TRAIN[1], C.ANIO_TEST)
    }
    paises_de = {}  # texto JSON de países de autores -> lista

    filas, estadisticas = [], []
    for anio in range(C.ANIOS_TRAIN[0], C.ANIO_TEST + 1):
        v = ventanas[C.ANIOS_TRAIN[1] if anio <= C.ANIOS_TRAIN[1] else C.ANIO_TEST]
        vigente = ((v["inicio"] <= anio) & (anio <= v["fin"])).to_numpy()
        pool = np.flatnonzero(vigente)
        tabla = pd.DataFrame({"i": pool, "p": pais[pool], "t": tema[pool]})
        por_pais = {p: g["i"].to_numpy() for p, g in tabla[tabla["p"] != ""].groupby("p")}
        por_tema = {t: g["i"].to_numpy() for t, g in tabla[tabla["t"] != ""].groupby("t")}

        del_anio = positivos[positivos["publication_year"] == anio]
        del_anio = del_anio.assign(_i=indice.reindex(del_anio["award_key"]).to_numpy())
        works = del_anio.drop_duplicates("work_key").set_index("work_key")[["work_topic_key", "author_country_codes"]]
        de_cada_work = del_anio.groupby("work_key", sort=True)["_i"].agg(list)

        pedidos = logrados = sin_dificiles = 0
        for work, premiados in de_cada_work.items():
            n = len(premiados)
            quiere_dificiles = (n_neg // 2) * n
            quiere_azar = n_neg * n - quiere_dificiles
            elegidos = set(premiados)

            texto_paises = works.at[work, "author_country_codes"]
            if not isinstance(texto_paises, str):
                paises = []
            else:
                if texto_paises not in paises_de:
                    paises_de[texto_paises] = json.loads(texto_paises)
                paises = paises_de[texto_paises]
            fuentes_pais = [por_pais[p] for p in paises if p in por_pais]
            fuente_tema = por_tema.get(works.at[work, "work_topic_key"])

            dificiles, intentos = [], 0
            while (fuentes_pais or fuente_tema is not None) and len(dificiles) < quiere_dificiles                     and intentos < 30 * quiere_dificiles:
                if fuente_tema is not None and (not fuentes_pais or rng.random() < PESO_TEMA):
                    fuente = fuente_tema
                else:
                    fuente = fuentes_pais[rng.integers(len(fuentes_pais))]
                candidato = fuente[rng.integers(len(fuente))]
                if candidato not in elegidos:
                    elegidos.add(candidato)
                    dificiles.append(candidato)
                intentos += 1
            pedidos += quiere_dificiles
            logrados += len(dificiles)
            sin_dificiles += 0 if dificiles else 1

            azar, intentos = [], 0
            falta = n_neg * n - len(dificiles)
            while len(azar) < falta and intentos < 30 * falta:
                candidato = pool[rng.integers(len(pool))]
                if candidato not in elegidos:
                    elegidos.add(candidato)
                    azar.append(candidato)
                intentos += 1
            for a in dificiles:
                filas.append((work, a, "dificil"))
            for a in azar:
                filas.append((work, a, "azar"))
        estadisticas.append({
            "anio": anio, "awards_vigentes": int(len(pool)), "publicaciones": int(len(de_cada_work)),
            "dificiles_pedidos": int(pedidos), "dificiles_logrados": int(logrados),
            "publicaciones_sin_dificiles": int(sin_dificiles),
        })
        print(f"  {anio}: {len(de_cada_work)} publicaciones, {len(pool)} awards vigentes, "
              f"difíciles {logrados}/{pedidos}", flush=True)

    neg = pd.DataFrame(filas, columns=["work_key", "award_key_i", "tipo"])
    neg["award_key"] = llaves[neg.pop("award_key_i").to_numpy()]
    neg[C.TARGET] = 0
    neg = neg[["work_key", "award_key", C.TARGET, "tipo"]]
    neg.attrs["estadisticas"] = estadisticas
    return neg


# ------------------------------------------------------------------ Rol 1: texto (OpenAlex)
def _reconstruir_abstract(indice_invertido) -> str:
    """OpenAlex entrega el abstract como {palabra: [posiciones]}: se vuelve a poner en orden."""
    if not indice_invertido:
        return ""
    palabras = {}
    for palabra, posiciones in indice_invertido.items():
        for posicion in posiciones:
            palabras[posicion] = palabra
    return " ".join(palabras[p] for p in sorted(palabras))


def _limpiar_texto(texto) -> str:
    texto = " ".join(str(texto or "").split())
    return texto[:MAX_CARACTERES_TEXTO]


def _leer_cache(ruta: Path) -> dict:
    if not ruta.exists():
        return {}
    cache = {}
    with open(ruta, encoding="utf-8") as archivo:
        for linea in archivo:
            if linea.strip():
                fila = json.loads(linea)
                cache[fila["id"]] = fila["texto"]
    return cache


def _pedir(ruta_api: str, parametros: dict) -> dict:
    """GET a OpenAlex con reintentos. Si se acaba el presupuesto diario, falla con un mensaje claro."""
    mailto = os.getenv("OPENALEX_MAILTO")
    if mailto:
        parametros = {**parametros, "mailto": mailto}
    for intento in range(INTENTOS_API):
        try:
            respuesta = requests.get(f"{API_OPENALEX}/{ruta_api}", params=parametros, timeout=60)
        except requests.RequestException:
            respuesta = None
        if respuesta is not None and respuesta.status_code == 200:
            return respuesta.json()
        if respuesta is not None and respuesta.status_code in (402, 403, 429) and intento >= 2:
            raise RuntimeError(
                f"OpenAlex respondió {respuesta.status_code}: probablemente se acabó el presupuesto diario "
                f"(1000 consultas por IP sin llave). Lo descargado ya quedó en {C.CRUDOS}; vuelve a "
                f"correr `python -m src.datos` mañana o con otra red y continúa donde se quedó."
            )
        time.sleep(2 ** intento)
    raise RuntimeError(f"OpenAlex no respondió bien a {ruta_api} después de {INTENTOS_API} intentos")


def _descargar_textos(llaves, cache: Path, ruta_api: str, campo: str, texto_de) -> pd.DataFrame:
    """Descarga en lotes de 100 los textos que no están en el caché y devuelve llave -> texto."""
    cache.parent.mkdir(parents=True, exist_ok=True)
    guardados = _leer_cache(cache)
    pendientes = [k for k in dict.fromkeys(llaves) if k not in guardados]
    print(f"  {len(guardados)} en caché, {len(pendientes)} por descargar ({ruta_api})", flush=True)
    prefijo = "openalex" if ruta_api == "works" else "id"
    with open(cache, "a", encoding="utf-8") as archivo:
        for inicio in range(0, len(pendientes), LOTE_API):
            lote = pendientes[inicio: inicio + LOTE_API]
            datos = _pedir(ruta_api, {
                "filter": f"{prefijo}:" + "|".join(lote),
                "select": f"id,{campo}",
                "per-page": LOTE_API,
            })
            recibidos = {r["id"].rsplit("/", 1)[-1]: _limpiar_texto(texto_de(r)) for r in datos.get("results", [])}
            for llave in lote:  # lo que OpenAlex no devuelve se guarda vacío para no volver a pedirlo
                texto = recibidos.get(llave, "")
                guardados[llave] = texto
                archivo.write(json.dumps({"id": llave, "texto": texto}, ensure_ascii=False) + "\n")
            archivo.flush()
            if (inicio // LOTE_API) % 20 == 0:
                print(f"    {min(inicio + LOTE_API, len(pendientes))}/{len(pendientes)}", flush=True)
            time.sleep(0.1)
    return pd.Series({k: guardados[k] for k in dict.fromkeys(llaves)})


def descargar_abstracts(work_keys) -> pd.DataFrame:
    """Columnas `work_key`, `work_abstract` desde la API de OpenAlex (abstract_inverted_index).

    Lotes de hasta 100 IDs, `mailto=OPENALEX_MAILTO` (polite pool), caché en `C.CRUDOS`.
    Un work sin abstract queda con `work_abstract` vacío (None). Se puede interrumpir y repetir:
    lo ya descargado no se vuelve a pedir.
    """
    textos = _descargar_textos(
        list(work_keys), CACHE_ABSTRACTS, "works", "abstract_inverted_index",
        lambda r: _reconstruir_abstract(r.get("abstract_inverted_index")),
    )
    return pd.DataFrame({"work_key": textos.index, "work_abstract": textos.replace("", None).to_numpy()})


def descargar_descripciones(award_keys) -> pd.DataFrame:
    """Columnas `award_key`, `award_description`: lo mismo que `descargar_abstracts`, para awards."""
    textos = _descargar_textos(
        list(award_keys), CACHE_DESCRIPCIONES, "awards", "description", lambda r: r.get("description"),
    )
    return pd.DataFrame({"award_key": textos.index, "award_description": textos.replace("", None).to_numpy()})


# ------------------------------------------------------------------ Rol 1: embeddings
def _texto_completo(titulo: pd.Series, cuerpo: pd.Series) -> pd.Series:
    """'título. cuerpo' (lo que exista de los dos); vacío si no hay ninguno."""
    titulo = titulo.fillna("").astype(str).str.strip()
    cuerpo = cuerpo.fillna("").astype(str).str.strip()
    junto = (titulo + ". " + cuerpo).where((titulo != "") & (cuerpo != ""), titulo + cuerpo)
    return junto.str.slice(0, MAX_CARACTERES_TEXTO)


def _embeber(textos: pd.Series, ruta: Path, nombre_llave: str) -> None:
    """Calcula los embeddings que faltan en `ruta` (se puede retomar) y la reescribe completa."""
    textos = textos[textos != ""]
    previos = pd.read_parquet(ruta) if ruta.exists() else None
    if previos is not None:
        textos = textos[~textos.index.isin(previos[nombre_llave])]
    print(f"  {nombre_llave}: {0 if previos is None else len(previos)} ya calculados, {len(textos)} por calcular", flush=True)
    if len(textos):
        from sentence_transformers import SentenceTransformer  # import perezoso: carga torch

        modelo = SentenceTransformer(C.MODELO_EMBEDDINGS)
        bloques = []
        for inicio in range(0, len(textos), 4000):
            parte = textos.iloc[inicio: inicio + 4000]
            matriz = modelo.encode(
                parte.tolist(), batch_size=LOTE_EMBEDDINGS, normalize_embeddings=True,
                convert_to_numpy=True, show_progress_bar=False,
            ).astype(np.float32)
            bloque = pd.DataFrame(matriz, columns=[f"e{i:03d}" for i in range(matriz.shape[1])])
            bloque.insert(0, nombre_llave, parte.index.to_numpy())
            bloques.append(bloque)
            print(f"    {min(inicio + 4000, len(textos))}/{len(textos)}", flush=True)
        nuevos = pd.concat(bloques, ignore_index=True)
        todos = nuevos if previos is None else pd.concat([previos, nuevos], ignore_index=True)
        ruta.parent.mkdir(parents=True, exist_ok=True)
        todos.to_parquet(ruta, index=False)


def _embeber_tablas(works: pd.DataFrame, awards: pd.DataFrame) -> None:
    """works indexado por work_key (work_title, work_abstract); awards por award_key (award_title, award_description)."""
    _embeber(_texto_completo(works["work_title"], works["work_abstract"]), C.EMB_WORKS, "work_key")
    _embeber(_texto_completo(awards["award_title"], awards["award_description"]), C.EMB_AWARDS, "award_key")


def construir_embeddings(pares: pd.DataFrame) -> None:
    """Escribe `C.EMB_WORKS` y `C.EMB_AWARDS` con `C.MODELO_EMBEDDINGS` (título + texto).

    `pares` necesita `work_key`, `work_title`, `work_abstract`, `award_key`, `award_title` y
    `award_description`. Un embedding por llave; los works o awards sin ningún texto no tienen
    fila (quien los use debe tratarlos como faltantes).
    """
    _embeber_tablas(pares.drop_duplicates("work_key").set_index("work_key"),
                    pares.drop_duplicates("award_key").set_index("award_key"))


# ------------------------------------------------------------------ Rol 1: pares y splits
def _sha256(ruta: Path) -> str:
    resumen = hashlib.sha256()
    with open(ruta, "rb") as archivo:
        for bloque in iter(lambda: archivo.read(1 << 20), b""):
            resumen.update(bloque)
    return resumen.hexdigest()


def _esquema(df: pd.DataFrame) -> pa.Schema:
    """Esquema fijo para que todos los bloques del parquet coincidan, aunque una columna llegue vacía."""
    campos = []
    for col, dtype in df.dtypes.items():
        if dtype.kind == "b":
            tipo = pa.bool_()
        elif dtype.kind in "iu":
            tipo = pa.int64()
        elif dtype.kind == "f":
            tipo = pa.float64()
        else:
            tipo = pa.string()
        campos.append(pa.field(col, tipo))
    return pa.schema(campos)


def construir_splits(descargar: bool = True, embeddings: bool = True) -> None:
    """Escribe `C.PARES_TRAIN`, `C.PARES_TEST` y `C.MANIFEST` (tabla, corte, sha256, filas).

    Train/CV = publicaciones 2017-2024, test = 2025; 2026 queda fuera (año incompleto).
    Cada publicación va completa a un solo split (el año decide), así que ninguna aparece en los dos.
    """
    C.PROCESADOS.mkdir(parents=True, exist_ok=True)
    print("Leyendo la OBT...", flush=True)
    obt = cargar_obt()
    fecha_corte = str(obt["_obt_built_at"].max())
    cols_award, cols_work = columnas_award(obt), columnas_work(obt)

    print("Generando negativos...", flush=True)
    negativos = generar_negativos(obt)
    estadisticas = negativos.attrs["estadisticas"]
    negativos[["work_key", "award_key", "tipo"]].to_parquet(NEGATIVOS_TIPO, index=False)

    positivos = obt[obt["publication_year"].between(C.ANIOS_TRAIN[0], C.ANIO_TEST)]
    works = positivos.drop_duplicates("work_key")[cols_work].set_index("work_key")
    awards = obt.drop_duplicates("award_key")[cols_award].set_index("award_key")
    en_pares = pd.Index(pd.concat([positivos["award_key"], negativos["award_key"]]).unique())

    works["work_abstract"] = None
    awards["award_description"] = None
    cobertura = {}
    if descargar:
        print("Descargando abstracts de OpenAlex...", flush=True)
        con_abstract = works.index[works["has_abstract"].fillna(False).astype(bool)]
        abstracts = descargar_abstracts(con_abstract).set_index("work_key")["work_abstract"]
        works.loc[abstracts.index, "work_abstract"] = abstracts
        print("Descargando descripciones de awards...", flush=True)
        con_descripcion = en_pares[awards.reindex(en_pares)["has_description"].fillna(False).astype(bool)]
        descripciones = descargar_descripciones(con_descripcion).set_index("award_key")["award_description"]
        awards.loc[descripciones.index, "award_description"] = descripciones
        cobertura = {
            "works_con_abstract": int(works["work_abstract"].notna().sum()),
            "works_total": int(len(works)),
            "awards_con_descripcion": int(awards.loc[en_pares, "award_description"].notna().sum()),
            "awards_en_pares": int(len(en_pares)),
        }

    columnas = (["work_key", "award_key", C.TARGET] + [c for c in cols_award if c != "award_key"]
                + [c for c in cols_work if c != "work_key"] + ["work_abstract", "award_description"])
    todos = pd.concat([positivos[C.LLAVES].assign(**{C.TARGET: 1}), negativos[C.LLAVES + [C.TARGET]]], ignore_index=True)
    anio_de = works["publication_year"]
    todos["_anio"] = anio_de.reindex(todos["work_key"]).to_numpy()

    esquema = None
    escritores, conteos = {}, {"train": [0, 0, set()], "test": [0, 0, set()]}  # filas, positivos, works
    rutas = {"train": C.PARES_TRAIN, "test": C.PARES_TEST}
    try:
        for anio in range(C.ANIOS_TRAIN[0], C.ANIO_TEST + 1):
            split = "train" if anio <= C.ANIOS_TRAIN[1] else "test"
            bloque = todos[todos["_anio"] == anio].drop(columns="_anio")
            bloque = bloque.merge(works, left_on="work_key", right_index=True, how="left")
            bloque = bloque.merge(awards, left_on="award_key", right_index=True, how="left")
            bloque = bloque[columnas].sample(frac=1, random_state=C.SEMILLA + anio).reset_index(drop=True)
            if esquema is None:
                esquema = _esquema(bloque)
            if split not in escritores:
                escritores[split] = pq.ParquetWriter(rutas[split], esquema, compression="zstd")
            escritores[split].write_table(pa.Table.from_pandas(bloque, schema=esquema, preserve_index=False))
            conteos[split][0] += len(bloque)
            conteos[split][1] += int(bloque[C.TARGET].sum())
            conteos[split][2].update(bloque["work_key"].unique())
            print(f"  {anio} -> {split}: {len(bloque)} pares", flush=True)
            del bloque
    finally:
        for escritor in escritores.values():
            escritor.close()

    if embeddings:
        print("Calculando embeddings...", flush=True)
        _embeber_tablas(works, awards.loc[en_pares])

    manifest = {
        "tabla_origen": C.TABLA_ORIGEN,
        "fecha_corte": fecha_corte,
        "sha256": _sha256(C.PARES_TRAIN),
        "filas": conteos["train"][0],
        "positivos": conteos["train"][1],
        "publicaciones": len(conteos["train"][2]),
        "test": {
            "sha256": _sha256(C.PARES_TEST), "filas": conteos["test"][0],
            "positivos": conteos["test"][1], "publicaciones": len(conteos["test"][2]),
        },
        "sha256_obt_csv": _sha256(C.CSV_OBT),
        "anios_train": list(C.ANIOS_TRAIN),
        "anio_test": C.ANIO_TEST,
        "negativos_por_positivo": C.NEGATIVOS_POR_POSITIVO,
        "semilla": C.SEMILLA,
        "modelo_embeddings": C.MODELO_EMBEDDINGS,
        "max_caracteres_texto": MAX_CARACTERES_TEXTO,
        "cobertura_texto": cobertura,
        "negativos_dificiles_por_anio": estadisticas,
    }
    C.MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print(f"Listo: {conteos['train'][0]} pares de train y {conteos['test'][0]} de test en {C.PROCESADOS}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Construye los pares, abstracts y embeddings del PSet 3")
    ap.add_argument("--solo-pares", action="store_true", help="sin descargas de OpenAlex ni embeddings")
    args = ap.parse_args()
    construir_splits(descargar=not args.solo_pares, embeddings=not args.solo_pares)
