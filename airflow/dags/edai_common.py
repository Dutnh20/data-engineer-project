"""Cấu hình dùng chung cho các DAG EDAI.

Mọi kết nối và biến nằm trong Airflow, không ghi cứng trong DAG:
- Connection `edai_minio`  : endpoint, access key, secret key của MinIO (đặt qua AIRFLOW_CONN_EDAI_MINIO).
- Variables `edai_*`       : phiên bản dữ liệu, prefix nguồn, Kafka... (đặt qua AIRFLOW_VAR_EDAI_* hoặc UI Admin > Variables).
Mỗi tham số có thể bị ghi đè khi Trigger DAG w/ config, ví dụ {"gold_version": "gold_v3"}.
Các hàm dưới đây trả về chuỗi Jinja để Airflow dựng giá trị lúc chạy (không đọc DB lúc parse DAG).
"""
import os
from datetime import timedelta

from docker.types import Mount

GENERATOR_IMAGE = os.environ.get("EDAI_GENERATOR_IMAGE", "edai-synthetic-generator:1.1.0")
SPARK_IMAGE = os.environ.get("EDAI_SPARK_IMAGE", "edai-spark-lab:3.5.6")
NETWORK = os.environ.get("EDAI_DOCKER_NETWORK", "edai-synthetic_default")
HOST_OUTPUT = os.environ["EDAI_HOST_OUTPUT_DIR"]  # đường dẫn trên máy chủ, không phải trong container Airflow

SPARK_OPTS = "--master local[2] --driver-memory 1g"


def var(name, default):
    """Giá trị tham số: dag_run.conf > Airflow Variable `edai_<name>` > mặc định."""
    return "{{ dag_run.conf.get('%s', var.value.get('edai_%s', '%s')) }}" % (name, name, default)


# Credentials lấy từ Airflow Connection `edai_minio`, truyền vào container task.
S3_ENV = {
    "S3_ENDPOINT_URL": "{{ conn.edai_minio.extra_dejson.endpoint_url }}",
    "AWS_ACCESS_KEY_ID": "{{ conn.edai_minio.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.edai_minio.password }}",
    "AWS_EC2_METADATA_DISABLED": "true",
}

COMMON = dict(
    docker_url="unix://var/run/docker.sock",
    network_mode=NETWORK,
    mounts=[Mount(source=HOST_OUTPUT, target="/data", type="bind")],
    mount_tmp_dir=False,
    auto_remove="success",   # giữ container lỗi để đọc log
    tty=False,
    retries=0,
)

DEFAULT_ARGS = {"owner": "edai"}
BASE_DAG = dict(schedule=None, catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS)


def spark_task(DockerOperator, task_id, script_args, timeout_hours=4, extra_env=None, spark_opts=""):
    """Task chạy spark-submit (local mode) trong image Spark."""
    return DockerOperator(
        task_id=task_id, image=SPARK_IMAGE, entrypoint=["/opt/spark/bin/spark-submit"],
        command=f"{SPARK_OPTS} {spark_opts}/opt/app/spark_jobs/{script_args}",
        environment={**S3_ENV, **(extra_env or {})}, execution_timeout=timedelta(hours=timeout_hours), **COMMON)


def validate_task(DockerOperator, task_id, zone, version_template, extra=""):
    """Task validate: kiểm tra marker/report của zone (không cần Spark)."""
    return DockerOperator(
        task_id=task_id, image=SPARK_IMAGE, entrypoint=["python3", "-m", "spark_jobs.validate_zone"],
        command=f"--zone {zone} --version {version_template} --out /data/validation {extra}".strip(),
        environment=S3_ENV, execution_timeout=timedelta(minutes=10), **COMMON)
