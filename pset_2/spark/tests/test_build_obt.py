"""
Pruebas locales de build_obt.py, sin Snowflake: se reemplaza la lectura de GOLD por
DataFrames sintéticos y se comprueba que las validaciones de grain hagan su trabajo.

    docker compose run --rm --no-deps spark-master /opt/spark/bin/spark-submit \
        --master "local[2]" /opt/spark-apps/tests/test_build_obt.py
"""

import sys

sys.path.insert(0, "/opt/spark-apps/jobs")

from pyspark.sql import SparkSession  # noqa: E402

import build_obt  # noqa: E402

spark = SparkSession.builder.master("local[2]").appName("test-build-obt").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def gold_tables(overrides=None):
    t = {
        "FACT_AWARD_WORKS": [
            ("G1", "W1", "F1", "T1", 20240115),
            ("G1", "W2", "F1", "T2", 20240301),
            ("G2", "W1", "F2", "T1", 20240115),  # W1 financiado por dos awards
        ],
        "FACT_AWARDS": [
            ("G1", "T1", "I1", 20230101, 20251231, 500000.0, "USD", 500000.0, 1095, 2, 1, False, False),
            ("G2", "UNKNOWN", "UNKNOWN", -1, -1, None, None, None, None, 1, 0, False, False),
        ],
        "DIM_AWARD": [
            ("G1", "NSF-123", "Deep learning for climate", "<p>Climate</p> models", True, "CAREER", "grant"),
            ("G2", None, "Small grant", None, False, None, None),
        ],
        "DIM_FUNDER": [("F1", "NSF", "US", False), ("F2", "ANID", "CL", True), ("UNKNOWN", "Desconocido", None, None)],
        "DIM_INSTITUTION": [("I1", "MIT", "US", "education"), ("UNKNOWN", "Institución desconocida", None, None)],
        "DIM_WORK": [
            ("W1", "10.1/x", "Paper uno", "Abstract uno", True, 2024, "article", "en", 10, 1.2, False, 3),
            ("W2", None, "Paper dos", None, False, 2024, "article", "en", 0, None, False, 1),
        ],
        "DIM_TOPIC": [
            ("T1", "Climate ML", "AI", "CS", "Physical"), ("T2", "Ecology", "Bio", "Life", "Life"),
            ("UNKNOWN", "Tema desconocido", None, None, None),
        ],
        "DIM_DATE": [(20230101, None, 2023, 2023), (20240115, None, 2024, 2024), (20240301, None, 2024, 2024),
                     (20251231, None, 2025, 2026), (-1, None, None, None)],
        "BRG_WORK_AUTHOR": [
            ("W1", "A1", "first", True, "US"), ("W1", "A2", "middle", False, "CL"), ("W1", "A3", "last", False, "US"),
            ("W2", "A2", "first", True, "CL"),
        ],
        "DIM_AUTHOR": [("A1", "Ana"), ("A2", "Beto"), ("A3", "Caro")],
    }
    t.update(overrides or {})
    return t


SCHEMAS = {
    "FACT_AWARD_WORKS": "award_key string, work_key string, funder_key string, topic_key string, publication_date_key int",
    "FACT_AWARDS": "award_key string, topic_key string, institution_key string, start_date_key int, end_date_key int, "
                   "amount double, currency string, amount_usd double, duration_days int, funded_outputs_count int, "
                   "institutions_count int, is_amount_invalid boolean, has_inconsistent_dates boolean",
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


class FakeGold:
    def __init__(self, tables):
        self.tables = tables

    def table(self, name, *columns, where=None):
        # El filtro `where` corre en Snowflake; aquí no hace falta porque los datos ya son el subconjunto.
        df = spark.createDataFrame(self.tables[name], SCHEMAS[name])
        return df.select(*columns) if columns else df


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


# 1) Caso feliz: el grain se conserva aunque W1 tenga 3 autores y 2 awards.
obt, metrics = run()
rows = {(r.award_key, r.work_key): r for r in obt.collect()}
check("conteo OBT = conteo hecho (3)", metrics["obt_rows"] == 3 == metrics["base_rows"])
check("autores agregados, no multiplicados", rows[("G1", "W1")].linked_authors_count == 3)
check("first_author correcto", rows[("G1", "W1")].first_author_name == "Ana")
check("países distintos ordenados", rows[("G1", "W1")].author_country_codes == ["CL", "US"])
check("miembro UNKNOWN resuelve sin huérfanos", rows[("G2", "W1")].funder_name == "ANID"
      and rows[("G2", "W1")].lead_institution_name == "Institución desconocida")
check("award_text concatena título y descripción", rows[("G1", "W1")].award_text.startswith("Deep learning"))
check("label = 1", all(r.label == 1 for r in rows.values()))


def expect_failure(name, overrides, fragment):
    try:
        run(overrides)
        check(name, False)
    except build_obt.ValidationError as exc:
        check(f"{name} -> '{exc}'", fragment in str(exc))


# 2) Dimensión con llave repetida: el job debe abortar ANTES de duplicar filas.
expect_failure("dim_funder duplicada aborta",
               {"DIM_FUNDER": gold_tables()["DIM_FUNDER"] + [("F1", "NSF (copia)", "US", False)]},
               "dim_funder")

# 3) FK sin dimensión: el job debe abortar en vez de dejar atributos nulos.
expect_failure("FK huérfana aborta",
               {"DIM_WORK": [r for r in gold_tables()["DIM_WORK"] if r[0] != "W2"]},
               "dim_work")

# 4) Hecho base con pares repetidos.
expect_failure("hecho base duplicado aborta",
               {"FACT_AWARD_WORKS": gold_tables()["FACT_AWARD_WORKS"] + [("G1", "W1", "F1", "T1", 20240115)]},
               "repetidos")

spark.stop()
print(f"\n{'OK' if not failures else 'FALLARON'}: {len(failures)} fallas")
sys.exit(1 if failures else 0)
