"""DP4: Kafka -> Silver streaming (Spark Structured Streaming), loại trùng và chịu event muộn.

Cần Kafka đang chạy và đã có dữ liệu (generator plan-stream + produce). Job tự đối soát và fail nếu còn trùng/mất event.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from edai_common import BASE_DAG, spark_task, var

with DAG(dag_id="edai_dp4_stream_kafka", description="DP4 Kafka -> Silver", start_date=datetime(2026, 10, 1),
         params={}, tags=["edai", "dp4", "streaming"], **BASE_DAG) as dag:
    stream = var("stream_version", "stream_v1")
    spark_task(DockerOperator, "stream_kafka_to_silver",
               f"dp4_stream_to_silver.py --stream-version {stream} --topic-prefix " + var("topic_prefix", "edai") +
               f" --bootstrap {var('kafka_bootstrap', 'kafka:9092')} --out /data/stream/{stream}",
               timeout_hours=1,
               spark_opts="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 --conf spark.jars.ivy=/tmp/.ivy2 ")
