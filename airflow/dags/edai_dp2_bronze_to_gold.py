"""DP2: Bronze -> Silver -> Gold (Spark), mỗi zone có stage ingest và validate.

ingest_silver : DP2 Spark, chuẩn hóa/khử trùng/SCD2, ghi stg_* và _SUCCESS.json.
ingest_gold   : DP3 (star schema) Spark, dựng dim_/fact_/agg_ với quality gate FK.
validate_silver, validate_gold : đọc lại marker/report; đủ bảng và rows > 0.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from edai_common import BASE_DAG, spark_task, validate_task, var

with DAG(dag_id="edai_dp2_bronze_to_gold", description="DP2 Bronze -> Silver -> Gold", start_date=datetime(2026, 10, 1),
         params={}, tags=["edai", "dp2", "silver", "gold"], **BASE_DAG) as dag:
    bronze, silver, gold = var("bronze_version", "smoke_v1"), var("silver_version", "smoke_v1"), var("gold_version", "gold_v2")
    ingest_silver = spark_task(DockerOperator, "ingest_silver",
                               f"dp2_bronze_to_silver.py --bronze-version {bronze} --silver-version {silver} --out /data/silver/{silver}")
    validate_silver = validate_task(DockerOperator, "validate_silver", "silver", silver)
    ingest_gold = spark_task(DockerOperator, "ingest_gold",
                             f"dp3_silver_to_gold.py --silver-version {silver} --gold-version {gold} --out /data/gold/{gold}")
    validate_gold = validate_task(DockerOperator, "validate_gold", "gold", gold)
    ingest_silver >> validate_silver >> ingest_gold >> validate_gold
