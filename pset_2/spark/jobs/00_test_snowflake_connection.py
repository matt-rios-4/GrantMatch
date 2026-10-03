"""Smoke test: verifica que Spark puede leer de Snowflake. [ROL 3] reemplazar por los jobs reales."""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pset2-smoke-test").getOrCreate()

sf_options = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA_GOLD", "GOLD"),
}

df = (
    spark.read.format("net.snowflake.spark.snowflake")
    .options(**sf_options)
    .option("query", "SELECT CURRENT_VERSION() AS version, CURRENT_DATABASE() AS db")
    .load()
)
df.show(truncate=False)
spark.stop()
