"""DP1: kéo dữ liệu từ source-department (MinIO) về Bronze, rồi validate.

ingest_to_bronze : chép từng Parquet nguồn sang Bronze, kiểm SHA256/kích thước/schema/số dòng, ghi _SUCCESS.json.
validate_bronze  : đọc lại marker + report đã công bố; so verified_files với expected_files.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from edai_common import BASE_DAG, COMMON, GENERATOR_IMAGE, S3_ENV, validate_task, var

with DAG(dag_id="edai_dp1_ingest_bronze", description="DP1 nguồn -> Bronze", start_date=datetime(2026, 10, 1),
         params={}, tags=["edai", "dp1", "bronze"], **BASE_DAG) as dag:
    ingest = DockerOperator(
        task_id="ingest_to_bronze", image=GENERATOR_IMAGE, entrypoint=["python"],
        command=("-m pipelines.dp1_source_to_bronze --source-prefix " + var("source_prefix", "synthetic/smoke") +
                 " --dataset-version " + var("bronze_version", "smoke_v1") +
                 " --out /data/bronze/" + var("bronze_version", "smoke_v1")),
        environment=S3_ENV, **COMMON)
    validate = validate_task(DockerOperator, "validate_bronze", "bronze", var("bronze_version", "smoke_v1"))
    ingest >> validate
