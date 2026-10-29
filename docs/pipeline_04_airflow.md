# Orchestration: 3 pipeline trên Airflow (+ stream)

Mỗi pipeline là một DAG riêng, mỗi DAG có stage **ingest** và stage **validate**. Task chạy trong container
của image đã build (Airflow chỉ điều phối). Mọi kết nối và biến nằm trong Airflow, không ghi cứng trong DAG.

| DAG | Rubric | Task | Việc làm |
|---|---|---|---|
| `edai_dp1_ingest_bronze` | DP1 | `ingest_to_bronze` → `validate_bronze` | Chép Parquet nguồn sang Bronze, kiểm SHA256/schema/số dòng, ghi `_SUCCESS.json`; validate đọc lại marker/report, `verified_files == expected_files` |
| `edai_dp2_bronze_to_gold` | DP2 | `ingest_silver` → `validate_silver` → `ingest_gold` → `validate_gold` | Silver: chuẩn hóa, khử trùng, SCD2 `stg_*`. Gold: `dim_`/`fact_`/`agg_`, quality gate FK. Validate: đủ bảng, rows > 0 |
| `edai_dp3_feature_table` | DP3 | `build_features` → `validate_features` | Tạo `feat_customer_orders_90d` có `event_timestamp` + `created`; validate checks của job và cột bắt buộc |
| `edai_dp4_stream_kafka` | thêm | `stream_kafka_to_silver` | Kafka → Silver, loại trùng `event_id`, chịu event muộn (xem `pipeline_05_stream.md`) |

> Đổi tên so với tài liệu cũ: "DP2" Bronze→Silver và "DP3" Silver→Gold trong code nay gộp thành **DP2** của rubric;
> **DP3** của rubric là bảng feature offline.

## Connection và Variables (đặt trong Airflow)

- Connection `edai_minio` (Admin → Connections): login, password và `endpoint_url` trong extra. Task lấy qua `{{ conn.edai_minio... }}`.
- Variables `edai_*` (Admin → Variables): `source_prefix`, `bronze_version`, `silver_version`, `gold_version`, `feat_version`,
  `feat_window_days`, `feat_snapshots`, `stream_version`, `topic_prefix`, `kafka_bootstrap`.
- Khi Trigger DAG w/ config, giá trị trong config ghi đè Variable, ví dụ `{"gold_version": "gold_v3"}`.
- Giá trị khởi tạo được đặt trong `compose.airflow.yaml` (`AIRFLOW_CONN_EDAI_MINIO`, `AIRFLOW_VAR_EDAI_*`).

## Chuẩn bị và bật Airflow

```powershell
cd D:\edai-synthetic-generator
Remove-Item .\airflow\dags\edai_medallion.py -ErrorAction SilentlyContinue   # DAG cũ, đã được thay bằng 4 DAG
docker compose build generator
docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml build spark-job
$env:HOST_OUTPUT_DIR = ((Get-Location).Path -replace '\\','/') + '/output'
docker compose -f compose.yaml -f compose.airflow.yaml build airflow
docker compose -f compose.yaml -f compose.airflow.yaml up -d --force-recreate minio airflow
```

Mở http://localhost:8085 (`admin` / `admin`, chỉ dùng cho lab cục bộ). Chờ khoảng 1 phút để thấy 4 DAG.
Phải build lại `spark-job` vì `feat_customer_orders.py` và `validate_zone.py` được COPY vào image lúc build.

## Chạy

Trigger theo thứ tự `edai_dp1_ingest_bronze` → `edai_dp2_bronze_to_gold` → `edai_dp3_feature_table`.
Chạy lại cùng tham số là idempotent. Nếu đổi quy tắc/code của một zone, dùng version mới cho zone đó.

## Bằng chứng cần chụp

- Trang DAGs có 3 pipeline (và DP4), mỗi DAG một ảnh Graph View xanh.
- Admin → Connections (`edai_minio`) và Admin → Variables: chứng minh không ghi cứng.
- Log các task `validate_*` (mỗi dòng check `ok: true`) và file `output\validation\validation_*.json`.

## Lỗi thường gặp

| Lỗi | Cách xử lý |
|---|---|
| `HOST_OUTPUT_DIR` báo thiếu khi `up` | Đặt biến trong cùng cửa sổ PowerShell |
| `Cannot connect to the Docker daemon` | Docker Desktop phải chạy; kiểm tra mount `/var/run/docker.sock` |
| `network ... not found` | `docker network ls`, sửa `EDAI_DOCKER_NETWORK` |
| Lỗi mount/`invalid mount path` | Thử `HOST_OUTPUT_DIR=/run/desktop/mnt/host/d/edai-synthetic-generator/output` |
| `No such image` | Build lại hai image ở trên |
| DAG Import Error | Mở banner đỏ trên UI hoặc `docker compose ... logs airflow` |
| `validate_*` đỏ | Mở log task, dòng có `"ok": false` cho biết kiểm tra nào hỏng |

Không dùng `docker compose down -v` (xóa volume MinIO). Task lỗi giữ lại container để đọc log.

## Phạm vi kiểm thử

Cú pháp DAG, chuỗi lệnh render từ Jinja (Variables, config ghi đè, Connection), validator (4 test) và logic feature
(6 test, Spark local) đã được kiểm tra. Chưa chạy được Docker/Airflow thật trong môi trường soạn thảo, nên cần chạy bước
"Chạy" ở trên để xác nhận tích hợp.
