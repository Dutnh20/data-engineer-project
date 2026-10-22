#!/usr/bin/env bash
set -euo pipefail

# Driver container phải dùng tên edai-silver-job (hoặc cùng SPARK_DRIVER_HOST).
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-1g}" \
  --executor-cores 2 \
  --total-executor-cores 2 \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-edai-silver-job}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=37071 \
  --conf spark.blockManager.port=37072 \
  /opt/app/spark_jobs/dp2_bronze_to_silver.py "$@"
