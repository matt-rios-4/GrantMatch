"""
Pruebas locales de build_obt.py, sin Snowflake: se reemplaza la lectura de GOLD por
DataFrames sintéticos y se comprueba que las validaciones de grain hagan su trabajo.

    docker compose run --rm --no-deps spark-master /opt/spark/bin/spark-submit \
        --master "local[2]" /opt/spark-apps/tests/test_build_obt.py

Limitación conocida: FakeGold no ejecuta los filtros `where` (*_IN_SCOPE), que corren en
Snowflake; esos se validan en la corrida real, donde un filtro equivocado aparece como
huérfano y aborta el job.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

import build_obt  # noqa: E402

spark = SparkSession.builder.master("local[2]").appName("test-build-obt").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def gold_tables(overrides=None):
    t = {
        "FACT_AWARD_WORKS": [
            # funder_key aquí es una copia vieja a propósito: la OBT debe usar la de FACT_AWARDS.
            ("G1", "W1", "UNKNOWN", "T1", 20240115),
            ("G1", "W2", "UNKNOWN", "T2", 20240301),
            ("G2", "W1", "F2", "T1", 20240115),   # W1 financiado por dos awards
            ("G2", "W3", "F2", "T2", 20240301),   # W3 no tiene autorías con ID
        ],
        "FACT_AWARDS": [
            ("G1", "F1", "T1", "I1", 20230101, 20251231, 500000.0, "USD", 500000.0, 1095, 2, 1, False, False),
            ("G2", "F2", "UNKNOWN", "UNKNOWN", -1, -1, None, None, None, None, 2, 0, False, False),
        ],
        "DIM_AWARD": [
            ("G1", "NSF-123", "Can AI predict floods?", "Climate models", True, "CAREER", "grant"),
            ("G2", None, None, None, False, None, None),
        ],
        "DIM_FUNDER": [("F1", "NSF", "US", False), ("F2", "ANID", "CL", True), ("UNKNOWN", "Desconocido", None, None)],
        "DIM_INSTITUTION": [("I1", "MIT", "US", "education"), ("UNKNOWN", "Institución desconocida", None, None)],
        "DIM_WORK": [
            ("W1", "10.1/x", "Policy in the U.S.", "Abstract uno", True, 2024, "article", "en", 10, 1.2, False, 3),
            ("W2", None, "Paper dos", None, False, 2024, "article", "en", 0, None, False, 1),
            ("W3", None, "Paper tres", "Abstract tres", True, 2024, "article", "en", 0, None, False, 0),
        ],
        "DIM_TOPIC": [
            ("T1", "Climate ML", "AI", "CS", "Physical"), ("T2", "Ecology", "Bio", "Life", "Life"),
            ("UNKNOWN", "Tema desconocido", None, None, None),
        ],
        "DIM_DATE": [(20230101, None, 2023, 2023), (20240115, None, 2024, 2024), (20240301, None, 2024, 2024),
                     (20251231, None, 2025, 2026), (-1, None, None, None)],
        "BRG_WORK_AUTHOR": [
            ("W1", "A1", "first", True, "US"), ("W1", "A2", "middle", False, "CL"), ("W1", "A3", "last", False, "US"),
            # W2 tiene dos autorías marcadas "first" (dato real posible en OpenAlex)
            ("W2", "A2", "first", True, "CL"), ("W2", "A9", "first", False, "AR"),
        ],
        "DIM_AUTHOR": [("A1", "Ana"), ("A2", "Zoe"), ("A3", "Caro"), ("A9", "Beto")],
    }
    t.update(overrides or {})
    return t


SCHEMAS = {
    "FACT_AWARD_WORKS": "award_key string, work_key string, funder_key string, topic_key string, publication_date_key int",
    "FACT_AWARDS": "award_key string, funder_key string, topic_key string, institution_key string, "
                   "start_date_key int, end_date_key int, amount double, currency string, amount_usd double, "
                   "duration_days int, funded_outputs_count int, institutions_count int, "
                   "is_amount_invalid boolean, has_inconsistent_dates boolean",
    "DIM_AWARD": "award_key string, funder_award_id string, award_title string, award_description string, "
                 "has_description boolean, funder_scheme string, funding_type string",
    "DIM_FUNDER": "funder_key string, funder_name string, country_code string, is_global_south boolean",
    "DIM_INSTITUTION": "institution_key string, institution_name string, country_code string, institution_type string",
    "DIM_WORK": "work_key string, doi string, title string, abstract_text string, has_abstract boolean, "
                "publication_year int, work_type string, language string, cited_by_count int, fwci double, "
                "is_retracted boolean, authors_count int",
    "DIM_TOPIC": "topic_key string, topic_name string, subfield_name string, field_name string, domain_name string",
    "DIM_DATE": "date_key int, full_date date, year int, us_fiscal_year int",
    "BRG_WORK_AUTHOR": "work_key string, author_key string, author_position string, is_corresponding boolean, "
                       "institution_country_code string",
    "DIM_AUTHOR": "author_key string, author_name string",
}


class FakeGold(build_obt.Gold):
    """Gold sin Snowflake: mismas columnas y persistencia, datos sintéticos."""

    def __init__(self, tables):  # noqa: super().__init__ pediría credenciales
        self.tables = tables
        self._persisted = []

    def table(self, name, *columns, where=None):
        df = spark.createDataFrame(self.tables[name], SCHEMAS[name])
        return self.persist(df.select(*columns) if columns else df)


def run(overrides=None):
    metrics = {}
    obt = build_obt.build_obt(FakeGold(gold_tables(overrides)), metrics)
    build_obt.validate_grain(obt, metrics)
    return obt, metrics


failures = []


def check(name, condition):
    print(("PASS " if condition else "FAIL ") + name)
    if not condition:
        failures.append(name)


try:
    # 1) Caso feliz: el grain se conserva aunque W1 tenga 3 autores y 2 awards.
    obt, metrics = run()
    rows = {(r.award_key, r.work_key): r for r in obt.collect()}
    check("conteo OBT = conteo hecho (4)", metrics["obt_rows"] == 4 == metrics["base_rows"])
    check("autores agregados, no multiplicados", rows[("G1", "W1")].linked_authors_count == 3)
    check("first_author correcto", rows[("G1", "W1")].first_author_name == "Ana")
    w2 = rows[("G1", "W2")]
    check("dos 'first': key y nombre del MISMO autor",
          (w2.first_author_key, w2.first_author_name) == ("A9", "Beto"))
    check("países distintos ordenados", rows[("G1", "W1")].author_country_codes == ["CL", "US"])
    w3 = rows[("G2", "W3")]
    check("work sin autores: listas vacías y conteo 0",
          w3.author_keys == [] and w3.linked_authors_count == 0 and w3.first_author_key is None)
    check("funder vigente de FACT_AWARDS, no la copia de FACT_AWARD_WORKS",
          rows[("G1", "W1")].funder_name == "NSF")
    check("miembro UNKNOWN resuelve sin huérfanos",
          rows[("G2", "W1")].lead_institution_name == "Institución desconocida")
    check("título con '?' no duplica separador",
          rows[("G1", "W1")].award_text == "Can AI predict floods? Climate models")
    check("título con 'U.S.' conserva el punto",
          rows[("G1", "W1")].work_text == "Policy in the U.S. Abstract uno")
    check("título sin cuerpo -> solo título", rows[("G1", "W2")].work_text == "Paper dos")
    check("award sin título ni descripción -> NULL, no ''", rows[("G2", "W1")].award_text is None)
    check("label = 1", all(r.label == 1 for r in rows.values()))

    def expect_failure(name, overrides, fragment):
        try:
            run(overrides)
            check(name, False)
        except build_obt.ValidationError as exc:
            check(f"{name} -> '{exc}'", fragment in str(exc))

    # 2) Tabla con llave repetida: el job debe abortar ANTES de duplicar filas.
    expect_failure("dim_funder duplicada aborta",
                   {"DIM_FUNDER": gold_tables()["DIM_FUNDER"] + [("F1", "NSF (copia)", "US", False)]},
                   "dim_funder")
    expect_failure("dim_author duplicada aborta",
                   {"DIM_AUTHOR": gold_tables()["DIM_AUTHOR"] + [("A1", "Ana bis")]},
                   "dim_author")

    # 3) FK sin dimensión: el job debe abortar en vez de dejar atributos nulos.
    expect_failure("FK huérfana aborta",
                   {"DIM_WORK": [r for r in gold_tables()["DIM_WORK"] if r[0] != "W2"]},
                   "dim_work")
    expect_failure("autoría con autor inexistente aborta",
                   {"DIM_AUTHOR": [r for r in gold_tables()["DIM_AUTHOR"] if r[0] != "A3"]},
                   "dim_author")

    # 4) Hecho base con pares repetidos.
    expect_failure("hecho base duplicado aborta",
                   {"FACT_AWARD_WORKS": gold_tables()["FACT_AWARD_WORKS"] + [("G1", "W1", "F1", "T1", 20240115)]},
                   "repetidas")

    # 5) Llave RSA cifrada: mensaje claro en vez de un error opaco del conector.
    with tempfile.NamedTemporaryFile("w", suffix=".p8", delete=False) as fh:
        fh.write("-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIabc\n-----END ENCRYPTED PRIVATE KEY-----\n")
    try:
        build_obt._private_key_body(fh.name)
        check("llave cifrada se rechaza", False)
    except SystemExit as exc:
        check("llave cifrada se rechaza con mensaje claro", "nocrypt" in str(exc))
    finally:
        os.unlink(fh.name)
finally:
    spark.stop()

print(f"\n{'OK' if not failures else 'FALLARON'}: {len(failures)} fallas")
sys.exit(1 if failures else 0)
