#!/usr/bin/env bash
# [ROL 3] Ejecuta el job de la OBT contra el cluster Spark del docker-compose.
#   Manual:  docker compose exec spark-master /opt/spark-apps/run_obt.sh
#   Kestra:  docker exec pset2-spark-master /opt/spark-apps/run_obt.sh   (ver spark/README.md)
# Sale con código 1 si algo falla (y en ese caso no escribe la OBT).
# Las dos confs de redacción ocultan la llave RSA y el password en la UI y los logs de Spark.
# Executor de 2g: con 1g la OBT de ~1.9 M filas (textos largos) moría por OutOfMemoryError.
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages net.snowflake:spark-snowflake_2.12:3.1.1 \
  --conf spark.jars.ivy=/opt/ivy \
  --conf spark.sql.session.timeZone=UTC \
  --conf "spark.redaction.regex=(?i)secret|password|token|access[.]key|private_key|pem" \
  --conf "spark.sql.redaction.options.regex=(?i)url|password|private_key|pem" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  /opt/spark-apps/jobs/build_obt.py "$@"
