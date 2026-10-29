"""DP3: tính bảng offline feature feat_customer_orders_<N>d từ Gold, rồi validate.

build_features    : feat_customer_orders.py; cột event_timestamp + created, point-in-time correct.
validate_features : đọc lại marker/report; kiểm tra quality check, có event_timestamp và created.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from edai_common import BASE_DAG, spark_task, validate_task, var

with DAG(dag_id="edai_dp3_feature_table", description="DP3 Gold -> feat_ table", start_date=datetime(2026, 10, 1),
         params={}, tags=["edai", "dp3", "feature"], **BASE_DAG) as dag:
    gold, feat = var("gold_version", "gold_v2"), var("feat_version", "feat_v1")
    window, snaps = var("feat_window_days", "90"), var("feat_snapshots", "1")
    build = spark_task(DockerOperator, "build_features",
                       f"feat_customer_orders.py --gold-version {gold} --feat-version {feat} "
                       f"--window-days {window} --snapshots {snaps} --out /data/features/{feat}")
    validate = validate_task(DockerOperator, "validate_features", "feat", feat, extra=f"--window-days {window}")
    build >> validate
