"""Smoke test: verifica que Spark llega a Snowflake con las credenciales del .env.

Usa las mismas opciones de conexión que build_obt.py (password o llave RSA):
    docker compose exec spark-master /opt/spark-apps/run_obt.sh   # job real
    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --packages net.snowflake:spark-snowflake_2.12:3.1.1 --conf spark.jars.ivy=/opt/ivy \\
        /opt/spark-apps/jobs/00_test_snowflake_connection.py
"""
import os

from pyspark.sql import SparkSession

from build_obt import SNOWFLAKE_SOURCE, snowflake_options

spark = SparkSession.builder.appName("pset2-smoke-test").getOrCreate()
df = (
    spark.read.format(SNOWFLAKE_SOURCE)
    .options(**snowflake_options(os.environ.get("SNOWFLAKE_SCHEMA_GOLD", "GOLD")))
    .option("query", "SELECT CURRENT_VERSION() AS version, CURRENT_DATABASE() AS db, CURRENT_ROLE() AS rol")
    .load()
)
df.show(truncate=False)
spark.stop()
