# Spark (ROL 3)
Jobs PySpark que leen el Star Schema (GOLD) y escriben la OBT en el esquema OBT de Snowflake.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages net.snowflake:spark-snowflake_2.12:3.1.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark-apps/jobs/00_test_snowflake_connection.py
```
Las credenciales llegan como variables de entorno (SNOWFLAKE_*). Validar grain y conteos tras cada join.
