"""
[ROL 3] One Big Table para el recomendador de "Subvenciones con Ruta".

Lee el star schema de GOLD (Snowflake), lo aplana en OBT.OBT_AWARD_WORK y lo vuelve a
escribir en Snowflake.

GRAIN: una fila por publicación financiada por un award (award_key + work_key), el mismo
grain de GOLD.FACT_AWARD_WORKS. Cada fila es un ejemplo positivo del recomendador: junta
el texto del award (lo que se financió) con el de la publicación (lo que produjo el
investigador), que es el par que comparará el modelo de embeddings.

Garantías (si alguna falla, el job aborta SIN escribir la OBT):
  1. Cada tabla que se une es única en su llave antes del JOIN (la causa raíz de duplicados).
  2. Ningún JOIN deja huérfanos: todas las FKs encuentran su dimensión, incluido autor.
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
from pyspark.sql import Column, DataFrame, SparkSession
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
    """El conector espera la llave PKCS#8 SIN cifrar, sin encabezados ni saltos de línea."""
    with open(path, encoding="utf-8") as fh:
        pem = fh.read()
    if "ENCRYPTED" in pem:
        sys.exit(f"La llave {path} está cifrada con passphrase; genérala con "
                 "`openssl pkcs8 -topk8 -nocrypt` (ver spark/README.md)")
    if "BEGIN PRIVATE KEY" not in pem:
        sys.exit(f"La llave {path} no es PKCS#8 ('BEGIN PRIVATE KEY'); conviértela con "
                 "`openssl pkcs8 -topk8 -nocrypt -in vieja.pem -out snowflake_rsa_key.p8`")
    return "".join(line.strip() for line in pem.splitlines() if "-----" not in line)


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
    # si no, password como en el resto del equipo. run_obt.sh la oculta de la UI de Spark.
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
        self._persisted: list[DataFrame] = []

    def table(self, name: str, *columns: str, where: str | None = None) -> DataFrame:
        query = f"select {', '.join(columns) or '*'} from {name}" + (f" where {where}" if where else "")
        df = self.spark.read.format(SNOWFLAKE_SOURCE).options(**self.options).option("query", query).load()
        # Snowflake devuelve identificadores en mayúsculas: se normalizan a minúsculas.
        # persist(): los chequeos y el JOIN reutilizan la lectura en vez de repetir la consulta.
        return self.persist(df.toDF(*[c.lower() for c in df.columns]))

    def persist(self, df: DataFrame) -> DataFrame:
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        self._persisted.append(df)
        return df

    def release(self) -> None:
        """Libera las lecturas una vez que la OBT ya está materializada en caché."""
        for df in self._persisted:
            df.unpersist()
        self._persisted.clear()


# Subconjuntos de GOLD que participan en la OBT (filtros que corren en Snowflake).
AWARDS_IN_SCOPE = "award_key in (select award_key from FACT_AWARD_WORKS)"
WORKS_IN_SCOPE = "work_key in (select work_key from FACT_AWARD_WORKS)"
INSTITUTIONS_IN_SCOPE = f"institution_key in (select institution_key from FACT_AWARDS where {AWARDS_IN_SCOPE})"
AUTHORS_IN_SCOPE = f"author_key in (select author_key from BRG_WORK_AUTHOR where {WORKS_IN_SCOPE})"


# --------------------------------------------------------------------------- validaciones
class ValidationError(RuntimeError):
    pass


def assert_unique(df: DataFrame, keys: str | list[str], name: str, metrics: dict) -> int:
    """Una sola pasada: filas totales vs. llaves distintas (una llave nula también cuenta como error)."""
    keys = [keys] if isinstance(keys, str) else keys
    total, distinct = df.agg(F.count(F.lit(1)), F.countDistinct(*keys)).first()
    metrics[f"{name}_rows"] = total
    print(f"[check] {name}: {total:,} filas, {distinct:,} llaves distintas")
    if total != distinct:
        raise ValidationError(f"{name} tiene {total - distinct:,} llaves {keys} repetidas o nulas: "
                              "el JOIN duplicaría filas")
    return total


def assert_no_orphans(df: DataFrame, flag_columns: dict[str, str], metrics: dict) -> None:
    """flag_columns: {nombre_join: columna que es NULL solo si el JOIN no encontró pareja}."""
    counts = df.select(
        *[F.sum(F.col(col).isNull().cast("int")).alias(name) for name, col in flag_columns.items()]
    ).first().asDict()
    report_orphans(counts, metrics)


def report_orphans(counts: dict[str, int], metrics: dict) -> None:
    for name, orphans in counts.items():
        orphans = orphans or 0
        metrics[f"orphans_{name}"] = orphans
        print(f"[check] huérfanos en join {name}: {orphans:,}")
    bad = {k: v for k, v in counts.items() if v}
    if bad:
        raise ValidationError(f"JOINs con filas sin pareja (FK sin dimensión): {bad}")


# --------------------------------------------------------------------------- construcción
_END_PUNCTUATION = r"[.!?:;…]$"


def join_text(title: str, body: str) -> Column:
    """'Título. Cuerpo' para el embedding.

    - Si el título ya termina en puntuación (., ?, !, :), solo agrega un espacio.
    - Si falta uno de los dos, devuelve el otro.
    - Si faltan ambos (o son vacíos), devuelve NULL, no una cadena vacía.
    """
    t = F.trim(F.col(title))
    t = F.when(F.length(t) > 0, t)
    b = F.trim(F.col(body))
    b = F.when(F.length(b) > 0, b)
    return (
        F.when(t.isNull(), b)
        .when(b.isNull(), t)
        .when(t.rlike(_END_PUNCTUATION), F.concat(t, F.lit(" "), b))
        .otherwise(F.concat(t, F.lit(". "), b))
    )


def build_authors_by_work(gold: Gold, metrics: dict) -> DataFrame:
    """Autores: muchos por work -> UNA fila por work, para unirla sin multiplicar filas."""
    authorships = gold.table(
        "BRG_WORK_AUTHOR", "work_key", "author_key", "author_position", "is_corresponding",
        "institution_country_code", where=WORKS_IN_SCOPE,
    )
    authors = gold.table("DIM_AUTHOR", "author_key", "author_name", where=AUTHORS_IN_SCOPE)

    assert_unique(authorships, ["work_key", "author_key"], "brg_work_author", metrics)
    assert_unique(authors, "author_key", "dim_author", metrics)
    report_orphans({"dim_author": authorships.join(authors, "author_key", "left_anti").count()}, metrics)

    first = F.col("author_position") == "first"
    by_work = (
        authorships.join(authors, "author_key", "inner")
        .groupBy("work_key")
        .agg(
            F.array_sort(F.collect_set("author_key")).alias("author_keys"),
            # key y nombre del MISMO autor (si hubiera dos "first", gana el mayor author_key).
            F.max(F.when(first, F.struct("author_key", "author_name"))).alias("_first_author"),
            F.array_sort(F.collect_set(F.when(F.col("is_corresponding"), F.col("author_key"))))
             .alias("corresponding_author_keys"),
            F.array_sort(F.collect_set("institution_country_code")).alias("author_country_codes"),
        )
        .select(
            "work_key",
            "author_keys",
            F.size("author_keys").alias("linked_authors_count"),
            F.col("_first_author.author_key").alias("first_author_key"),
            F.col("_first_author.author_name").alias("first_author_name"),
            "corresponding_author_keys",
            "author_country_codes",
        )
    )
    return gold.persist(by_work)


def build_obt(gold: Gold, metrics: dict) -> DataFrame:
    # Hecho base: define el grain y el número de filas esperado.
    base = gold.table(
        "FACT_AWARD_WORKS", "award_key", "work_key", "topic_key", "publication_date_key"
    ).withColumnRenamed("topic_key", "work_topic_key")
    base_rows = assert_unique(base, ["award_key", "work_key"], "base", metrics)
    metrics["base_rows"] = base_rows
    if base_rows == 0:
        raise ValidationError("FACT_AWARD_WORKS está vacía: no hay nada que aplanar")

    # Lado award. funder_key se toma de FACT_AWARDS (estado vigente del award, se actualiza
    # por merge) y no de FACT_AWARD_WORKS (copia hecha cuando se insertó el enlace).
    awards = gold.table(
        "FACT_AWARDS", "award_key", "funder_key", "topic_key", "institution_key", "start_date_key",
        "end_date_key", "amount", "currency", "amount_usd", "duration_days", "funded_outputs_count",
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
    ).toDF("institution_key", "lead_institution_name", "lead_institution_country_code", "lead_institution_type")

    # Lado publicación.
    dim_work = gold.table(
        "DIM_WORK", "work_key", "doi", "title", "abstract_text", "has_abstract", "publication_year",
        "work_type", "language", "cited_by_count", "fwci", "is_retracted", "authors_count",
        where=WORKS_IN_SCOPE,
    ).withColumnRenamed("title", "work_title").withColumnRenamed("doi", "work_doi")

    # Dimensiones compartidas, usadas con dos roles (award_ / work_, inicio / publicación).
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

    # Fuente única de verdad de los JOINs obligatorios: de esta lista salen el chequeo de
    # unicidad (garantía 1), el marcador de huérfanos (garantía 2) y el JOIN mismo. El orden
    # importa: fact_awards aporta las llaves de funder, institución, tema y fecha del award.
    required_joins = [
        ("fact_awards", awards, "award_key"),
        ("dim_award", dim_award, "award_key"),
        ("dim_funder", dim_funder, "funder_key"),
        ("dim_institution", dim_institution, "institution_key"),
        ("dim_work", dim_work, "work_key"),
        ("award_topic", topic_role("award"), "award_topic_key"),
        ("work_topic", topic_role("work"), "work_topic_key"),
        ("start_date", start_date, "start_date_key"),
        ("publication_date", publication_date, "publication_date_key"),
    ]

    obt = base
    for name, df, key in required_joins:
        assert_unique(df, key, name, metrics)
        obt = obt.join(df.withColumn(f"_m_{name}", F.lit(1)), key, "left")

    # Autores: opcional (un work puede no tener autorías con ID), queda como listas vacías.
    obt = obt.join(build_authors_by_work(gold, metrics), "work_key", "left")

    obt = obt.cache()
    assert_no_orphans(obt, {name: f"_m_{name}" for name, _, _ in required_joins}, metrics)
    gold.release()  # la OBT ya está en caché: las lecturas de entrada sobran

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
        # Textos listos para vectorizar con Cortex en la siguiente etapa (NULL si no hay texto)
        join_text("award_title", "award_description").alias("award_text"),
        join_text("work_title", "abstract_text").alias("work_text"),
        F.current_timestamp().alias("_obt_built_at"),
    )


def validate_grain(obt: DataFrame, metrics: dict) -> None:
    """Garantía 3: el aplanado no cambió el número de observaciones."""
    rows, distinct = obt.agg(F.count(F.lit(1)), F.countDistinct("award_key", "work_key")).first()
    metrics.update(obt_rows=rows, obt_distinct_keys=distinct)
    print(f"[check] OBT: {rows:,} filas, {distinct:,} pares distintos (base: {metrics['base_rows']:,})")
    if rows != metrics["base_rows"]:
        raise ValidationError(f"La OBT tiene {rows:,} filas y FACT_AWARD_WORKS {metrics['base_rows']:,}")
    if rows != distinct:
        raise ValidationError(f"La OBT tiene {rows - distinct:,} pares (award_key, work_key) repetidos")


# --------------------------------------------------------------------------- escritura
def write(df: DataFrame, table: str, mode: str, **extra_options: str) -> None:
    options = {**snowflake_options(os.environ.get("SNOWFLAKE_SCHEMA_OBT", "OBT")), **extra_options}
    df.toDF(*[c.upper() for c in df.columns]).write.format(SNOWFLAKE_SOURCE) \
        .options(**options).option("dbtable", table).mode(mode).save()


VALIDATION_SCHEMA = (
    "run_at timestamp, status string, base_rows long, obt_rows long, obt_distinct_keys long, "
    "orphans_total long, error string, metrics_json string"
)


def save_validation(spark: SparkSession, metrics: dict, status: str, error: str) -> None:
    """Una fila por corrida. Los valores -1 significan "ese chequeo no llegó a ejecutarse"."""
    orphan_counts = [v for k, v in metrics.items() if k.startswith("orphans_")]
    row = (
        datetime.now(timezone.utc),  # con zona: PySpark lo convierte bien sin importar el TZ del contenedor
        status,
        metrics.get("base_rows", -1),
        metrics.get("obt_rows", -1),
        metrics.get("obt_distinct_keys", -1),
        sum(orphan_counts) if orphan_counts else -1,
        error,
        json.dumps(metrics, sort_keys=True),
    )
    # column_mapping=name: si el esquema cambia, no se desalinean columnas en silencio.
    write(spark.createDataFrame([row], VALIDATION_SCHEMA), VALIDATION_TABLE, "append", column_mapping="name")


def try_save_validation(spark: SparkSession, metrics: dict, status: str, error: str) -> bool:
    """El registro es de auditoría: si falla, se avisa, pero no oculta el resultado real."""
    try:
        save_validation(spark, metrics, status, error)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] No se pudo registrar la corrida en {VALIDATION_TABLE}: {exc}", file=sys.stderr)
        return False


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
        print(f"[ok] OBT.{OBT_TABLE} escrita: {metrics['obt_rows']:,} filas")
        # La OBT ya quedó escrita y validada: si solo falla el registro, el job sigue siendo OK.
        try_save_validation(spark, metrics, "OK", "")
        return 0
    except ValidationError as exc:
        print(f"[error] Validación fallida, la OBT NO se escribió: {exc}", file=sys.stderr)
        try_save_validation(spark, metrics, "FAILED", str(exc))
        return 1
    except Exception as exc:  # noqa: BLE001 — conexión, columnas renombradas, memoria...
        print(f"[error] El job falló antes de escribir la OBT: {type(exc).__name__}: {exc}", file=sys.stderr)
        try_save_validation(spark, metrics, "FAILED", f"{type(exc).__name__}: {str(exc)[:2000]}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
