"""
[ROL 3] One Big Table para el recomendador de "Subvenciones con Ruta".

Lee el star schema de GOLD (Snowflake), lo aplana en OBT.OBT_AWARD_WORK y lo vuelve a
escribir en Snowflake.

GRAIN: una fila por publicación financiada por un award (award_key + work_key), el mismo
grain de GOLD.FACT_AWARD_WORKS. Cada fila es un ejemplo positivo del recomendador: junta
el texto del award (lo que se financió) con el de la publicación (lo que produjo el
investigador), que es el par que comparará el modelo de embeddings.

Garantías (si alguna falla, el job aborta SIN escribir la OBT):
  1. Cada dimensión es única en su llave antes del JOIN (la causa raíz de duplicados).
  2. Ningún JOIN deja huérfanos: todas las FKs del hecho encuentran su dimensión.
  3. COUNT(OBT) = COUNT(FACT_AWARD_WORKS) y (award_key, work_key) es único en la OBT.
Los autores (muchos por work) se agregan a una fila por work ANTES del JOIN, para no
multiplicar filas.

Uso (desde pset_2/):
  docker compose exec spark-master /opt/spark-apps/run_obt.sh
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
OBT_TABLE = os.environ.get("OBT_TABLE", "OBT_AWARD_WORK")
VALIDATION_TABLE = f"{OBT_TABLE}_VALIDATION"


# --------------------------------------------------------------------------- conexión
def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"Falta la variable de entorno {name} (revisa pset_2/.env)")
    return value


def _private_key_body(path: str) -> str:
    """El conector espera la llave PKCS#8 sin encabezados ni saltos de línea."""
    with open(path, encoding="utf-8") as fh:
        return "".join(line.strip() for line in fh if "-----" not in line)


def snowflake_options(schema: str) -> dict[str, str]:
    options = {
        "sfURL": f"{_require('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": _require("SNOWFLAKE_USER"),
        "sfRole": _require("SNOWFLAKE_ROLE"),
        "sfWarehouse": _require("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": _require("SNOWFLAKE_DATABASE"),
        "sfSchema": schema,
        "query_tag": "pset2_spark_obt",
    }
    # Llave RSA si está configurada (Snowflake bloquea password sin MFA en cuentas nuevas);
    # si no, password como en el resto del equipo.
    key_file = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE", "").strip()
    if key_file:
        options["pem_private_key"] = _private_key_body(key_file)
    else:
        options["sfPassword"] = _require("SNOWFLAKE_PASSWORD")
    return options


class Gold:
    """Lectura de tablas GOLD.

    `where` se ejecuta dentro de Snowflake: así solo viajan a Spark las filas que la OBT
    usa (p. ej. ~130 mil de los 17 M de awards), en vez de copiar tablas completas.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.options = snowflake_options(os.environ.get("SNOWFLAKE_SCHEMA_GOLD", "GOLD"))

    def table(self, name: str, *columns: str, where: str | None = None) -> DataFrame:
        query = f"select {', '.join(columns) or '*'} from {name}" + (f" where {where}" if where else "")
        df = self.spark.read.format(SNOWFLAKE_SOURCE).options(**self.options).option("query", query).load()
        # Snowflake devuelve identificadores en mayúsculas: se normalizan a minúsculas.
        # persist(): los chequeos y el JOIN reutilizan la lectura en vez de repetir la consulta.
        return df.toDF(*[c.lower() for c in df.columns]).persist(StorageLevel.MEMORY_AND_DISK)


# Subconjuntos de GOLD que participan en la OBT (filtros que corren en Snowflake).
AWARDS_IN_SCOPE = "award_key in (select award_key from FACT_AWARD_WORKS)"
WORKS_IN_SCOPE = "work_key in (select work_key from FACT_AWARD_WORKS)"
INSTITUTIONS_IN_SCOPE = f"institution_key in (select institution_key from FACT_AWARDS where {AWARDS_IN_SCOPE})"
AUTHORS_IN_SCOPE = f"author_key in (select author_key from BRG_WORK_AUTHOR where {WORKS_IN_SCOPE})"


# --------------------------------------------------------------------------- validaciones
class ValidationError(RuntimeError):
    pass


def assert_unique(df: DataFrame, key: str, name: str, metrics: dict) -> None:
    total = df.count()
    distinct = df.select(key).distinct().count()
    metrics[f"{name}_rows"] = total
    print(f"[check] {name}: {total:,} filas, {distinct:,} llaves distintas")
    if total != distinct:
        raise ValidationError(f"{name} tiene {total - distinct:,} llaves '{key}' repetidas: el JOIN duplicaría filas")


def assert_no_orphans(df: DataFrame, flag_columns: dict[str, str], metrics: dict) -> None:
    """flag_columns: {nombre_join: columna que es NULL solo si el JOIN no encontró pareja}."""
    counts = df.select(
        *[F.sum(F.col(col).isNull().cast("int")).alias(name) for name, col in flag_columns.items()]
    ).first().asDict()
    for name, orphans in counts.items():
        orphans = orphans or 0
        metrics[f"orphans_{name}"] = orphans
        print(f"[check] huérfanos en join {name}: {orphans:,}")
    bad = {k: v for k, v in counts.items() if v}
    if bad:
        raise ValidationError(f"JOINs con filas sin pareja (FK sin dimensión): {bad}")


# --------------------------------------------------------------------------- construcción
def _join_text(title: str, body: str):
    """'Título. Cuerpo' sin duplicar el punto si el título ya termina en puntuación."""
    clean_title = F.regexp_replace(F.trim(F.col(title)), r"[.\s]+$", "")
    return F.concat_ws(". ", clean_title, F.col(body))


def build_obt(gold: Gold, metrics: dict) -> DataFrame:
    # Hecho base: define el grain.
    base = gold.table(
        "FACT_AWARD_WORKS", "award_key", "work_key", "funder_key", "topic_key", "publication_date_key"
    ).withColumnRenamed("topic_key", "work_topic_key")

    base_rows = base.count()
    base_distinct = base.select("award_key", "work_key").distinct().count()
    metrics.update(base_rows=base_rows, base_distinct_keys=base_distinct)
    print(f"[check] FACT_AWARD_WORKS: {base_rows:,} filas, {base_distinct:,} pares distintos")
    if base_rows != base_distinct:
        raise ValidationError("FACT_AWARD_WORKS ya trae pares (award_key, work_key) repetidos")
    if base_rows == 0:
        raise ValidationError("FACT_AWARD_WORKS está vacía: no hay nada que aplanar")

    # Dimensiones y hecho de awards (todas many-to-one respecto del grain).
    awards = gold.table(
        "FACT_AWARDS", "award_key", "topic_key", "institution_key", "start_date_key", "end_date_key",
        "amount", "currency", "amount_usd", "duration_days", "funded_outputs_count",
        "institutions_count", "is_amount_invalid", "has_inconsistent_dates",
        where=AWARDS_IN_SCOPE,
    ).withColumnRenamed("topic_key", "award_topic_key")

    dim_award = gold.table(
        "DIM_AWARD", "award_key", "funder_award_id", "award_title", "award_description",
        "has_description", "funder_scheme", "funding_type",
        where=AWARDS_IN_SCOPE,
    )
    dim_funder = gold.table("DIM_FUNDER", "funder_key", "funder_name", "country_code", "is_global_south") \
        .withColumnRenamed("country_code", "funder_country_code")
    dim_institution = gold.table(
        "DIM_INSTITUTION", "institution_key", "institution_name", "country_code", "institution_type",
        where=INSTITUTIONS_IN_SCOPE,
    ).withColumnRenamed("country_code", "lead_institution_country_code") \
     .withColumnRenamed("institution_name", "lead_institution_name") \
     .withColumnRenamed("institution_type", "lead_institution_type")
    dim_work = gold.table(
        "DIM_WORK", "work_key", "doi", "title", "abstract_text", "has_abstract", "publication_year",
        "work_type", "language", "cited_by_count", "fwci", "is_retracted", "authors_count",
        where=WORKS_IN_SCOPE,
    ).withColumnRenamed("title", "work_title").withColumnRenamed("doi", "work_doi")

    dim_topic = gold.table("DIM_TOPIC", "topic_key", "topic_name", "subfield_name", "field_name", "domain_name")
    dim_date = gold.table("DIM_DATE", "date_key", "full_date", "year", "us_fiscal_year")

    def topic_role(prefix: str) -> DataFrame:
        return dim_topic.select(
            F.col("topic_key").alias(f"{prefix}_topic_key"),
            *[F.col(c).alias(f"{prefix}_{c}") for c in ("topic_name", "subfield_name", "field_name", "domain_name")],
        )

    start_date = dim_date.select(
        F.col("date_key").alias("start_date_key"),
        F.col("full_date").alias("award_start_date"),
        F.col("year").alias("award_start_year"),
        F.col("us_fiscal_year").alias("award_us_fiscal_year"),
    )
    publication_date = dim_date.select(
        F.col("date_key").alias("publication_date_key"),
        F.col("full_date").alias("publication_date"),
    )

    # Autores: muchos por work -> se agregan a UNA fila por work antes del JOIN.
    authorships = gold.table(
        "BRG_WORK_AUTHOR", "work_key", "author_key", "author_position", "is_corresponding",
        "institution_country_code", where=WORKS_IN_SCOPE,
    )
    authors = gold.table("DIM_AUTHOR", "author_key", "author_name", where=AUTHORS_IN_SCOPE)
    authors_by_work = (
        authorships.join(authors, "author_key", "left")
        .groupBy("work_key")
        .agg(
            F.array_sort(F.collect_set("author_key")).alias("author_keys"),
            F.countDistinct("author_key").alias("linked_authors_count"),
            F.max(F.when(F.col("author_position") == "first", F.col("author_key"))).alias("first_author_key"),
            F.max(F.when(F.col("author_position") == "first", F.col("author_name"))).alias("first_author_name"),
            F.array_sort(F.collect_set(F.when(F.col("is_corresponding"), F.col("author_key")))).alias("corresponding_author_keys"),
            F.array_sort(F.collect_set("institution_country_code")).alias("author_country_codes"),
        )
    )

    # Garantía 1: todo lo que se une es único en su llave.
    for df, key, name in [
        (awards, "award_key", "fact_awards"),
        (dim_award, "award_key", "dim_award"),
        (dim_funder, "funder_key", "dim_funder"),
        (dim_institution, "institution_key", "dim_institution"),
        (dim_work, "work_key", "dim_work"),
        (dim_topic, "topic_key", "dim_topic"),
        (dim_date, "date_key", "dim_date"),
        (authors_by_work, "work_key", "authors_by_work"),
    ]:
        assert_unique(df, key, name, metrics)

    # Marcadores de pareja: columnas NOT NULL en su tabla que solo quedan NULL si no hubo match.
    awards = awards.withColumn("_m_fact_awards", F.lit(1))
    dim_award = dim_award.withColumn("_m_dim_award", F.lit(1))
    dim_funder = dim_funder.withColumn("_m_dim_funder", F.lit(1))
    dim_institution = dim_institution.withColumn("_m_dim_institution", F.lit(1))
    dim_work = dim_work.withColumn("_m_dim_work", F.lit(1))
    award_topic = topic_role("award").withColumn("_m_award_topic", F.lit(1))
    work_topic = topic_role("work").withColumn("_m_work_topic", F.lit(1))
    start_date = start_date.withColumn("_m_start_date", F.lit(1))
    publication_date = publication_date.withColumn("_m_publication_date", F.lit(1))

    obt = (
        base
        .join(awards, "award_key", "left")
        .join(dim_award, "award_key", "left")
        .join(dim_funder, "funder_key", "left")
        .join(dim_institution, "institution_key", "left")
        .join(dim_work, "work_key", "left")
        .join(award_topic, "award_topic_key", "left")
        .join(work_topic, "work_topic_key", "left")
        .join(start_date, "start_date_key", "left")
        .join(publication_date, "publication_date_key", "left")
        .join(authors_by_work, "work_key", "left")
    )

    # Garantía 2: ningún JOIN quedó sin pareja.
    markers = {c[len("_m_"):]: c for c in obt.columns if c.startswith("_m_")}  # Python 3.8 (imagen Spark)
    obt = obt.cache()
    assert_no_orphans(obt, markers, metrics)

    empty_array = F.array().cast("array<string>")
    return obt.select(
        # Grain
        "award_key", "work_key",
        # Etiqueta: todo par de esta tabla es un financiamiento real observado
        F.lit(1).alias("label"),
        # Lado award (la "convocatoria")
        "funder_award_id", "award_title", "award_description", "has_description",
        "funder_scheme", "funding_type",
        "funder_key", "funder_name", "funder_country_code", "is_global_south",
        "institution_key", "lead_institution_name", "lead_institution_country_code", "lead_institution_type",
        "award_topic_key", "award_topic_name", "award_subfield_name", "award_field_name", "award_domain_name",
        "start_date_key", "award_start_date", "award_start_year", "award_us_fiscal_year", "end_date_key",
        "amount", "currency", "amount_usd", "duration_days", "funded_outputs_count", "institutions_count",
        "is_amount_invalid", "has_inconsistent_dates",
        # Lado publicación (el "perfil" del investigador)
        "work_doi", "work_title", "abstract_text", "has_abstract", "publication_year",
        "publication_date_key", "publication_date", "work_type", "language",
        "cited_by_count", "fwci", "is_retracted", "authors_count",
        "work_topic_key", "work_topic_name", "work_subfield_name", "work_field_name", "work_domain_name",
        # Autores agregados por work (para pasar luego a pares investigador-award)
        F.coalesce("author_keys", empty_array).alias("author_keys"),
        F.coalesce("linked_authors_count", F.lit(0)).alias("linked_authors_count"),
        "first_author_key", "first_author_name",
        F.coalesce("corresponding_author_keys", empty_array).alias("corresponding_author_keys"),
        F.coalesce("author_country_codes", empty_array).alias("author_country_codes"),
        # Textos listos para vectorizar con Cortex en la siguiente etapa
        _join_text("award_title", "award_description").alias("award_text"),
        _join_text("work_title", "abstract_text").alias("work_text"),
        F.current_timestamp().alias("_obt_built_at"),
    )


def validate_grain(obt: DataFrame, metrics: dict) -> None:
    """Garantía 3: el aplanado no cambió el número de observaciones."""
    rows = obt.count()
    distinct = obt.select("award_key", "work_key").distinct().count()
    metrics.update(obt_rows=rows, obt_distinct_keys=distinct)
    print(f"[check] OBT: {rows:,} filas, {distinct:,} pares distintos (base: {metrics['base_rows']:,})")
    if rows != metrics["base_rows"]:
        raise ValidationError(f"La OBT tiene {rows:,} filas y FACT_AWARD_WORKS {metrics['base_rows']:,}")
    if rows != distinct:
        raise ValidationError(f"La OBT tiene {rows - distinct:,} pares (award_key, work_key) repetidos")


# --------------------------------------------------------------------------- escritura
def write(df: DataFrame, table: str, mode: str) -> None:
    options = snowflake_options(os.environ.get("SNOWFLAKE_SCHEMA_OBT", "OBT"))
    df.toDF(*[c.upper() for c in df.columns]).write.format(SNOWFLAKE_SOURCE) \
        .options(**options).option("dbtable", table).mode(mode).save()


VALIDATION_SCHEMA = (
    "run_at timestamp, status string, base_rows long, obt_rows long, obt_distinct_keys long, "
    "orphans_total long, error string, metrics_json string"
)


def save_validation(spark: SparkSession, metrics: dict, status: str, error: str) -> None:
    """Una fila por corrida, con esquema fijo (el conector mapea columnas por posición)."""
    row = (
        datetime.now(timezone.utc).replace(tzinfo=None),
        status,
        metrics.get("base_rows", -1),
        metrics.get("obt_rows", -1),
        metrics.get("obt_distinct_keys", -1),
        sum(v for k, v in metrics.items() if k.startswith("orphans_")),
        error,
        json.dumps(metrics, sort_keys=True),
    )
    write(spark.createDataFrame([row], VALIDATION_SCHEMA), VALIDATION_TABLE, "append")


def main() -> int:
    spark = SparkSession.builder.appName("pset2-build-obt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    metrics: dict[str, int] = {}
    try:
        obt = build_obt(Gold(spark), metrics)
        validate_grain(obt, metrics)
        # Overwrite: la OBT es derivada de GOLD. Re-ejecutar el batch la reconstruye igual
        # (idempotente); el conector escribe en una tabla temporal y la intercambia al final.
        write(obt, OBT_TABLE, "overwrite")
        save_validation(spark, metrics, "OK", "")
        print(f"[ok] OBT.{OBT_TABLE} escrita: {metrics['obt_rows']:,} filas")
        return 0
    except ValidationError as exc:
        print(f"[error] Validación fallida, la OBT NO se escribió: {exc}", file=sys.stderr)
        save_validation(spark, metrics, "FAILED", str(exc))
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
