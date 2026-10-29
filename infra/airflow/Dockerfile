# Airflow chạy DAG điều phối; mỗi bước DP chạy trong container riêng qua Docker socket.
FROM apache/airflow:2.10.5-python3.12
# Phiên bản provider lấy từ constraints chính thức của Airflow 2.10.5 để tránh xung đột dependency.
RUN pip install --no-cache-dir "apache-airflow-providers-docker==4.0.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
