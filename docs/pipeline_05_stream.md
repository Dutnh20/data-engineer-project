# Pipeline 5 (DP4): Kafka → Silver bằng Spark Structured Streaming

Job `spark_jobs/dp4_stream_to_silver.py` đọc `edai.clickstream` và `edai.payment_callbacks`, loại event trùng
theo `event_id`, chịu được event đến muộn và ghi parquet sạch vào MinIO. Chạy kiểu bounded (`availableNow`):
xử lý hết dữ liệu hiện có trong Kafka rồi dừng, nên chạy được từ Airflow và lặp lại an toàn.

## Cách hoạt động

| Vấn đề rubric | Xử lý |
|---|---|
| Late arrival (mobile trễ 5–10 phút) | `withWatermark("event_ts", "15 minutes")`; 15 phút lớn hơn độ trễ tối đa 10 phút nên không mất event |
| Callback gửi lại | `dropDuplicatesWithinWatermark(["event_id"])` (Spark ≥ 3.5) |
| Schema đổi v1 → v2 (`failure_reason`) | Schema đọc có `failure_reason` nullable; báo cáo đếm theo `schema_version` |
| JSON hỏng / thiếu `event_id` | Loại khỏi bảng sạch, đếm ở `unparseable_rows` |
| Chạy lại | Checkpoint nằm cạnh output nên không đọc lại offset cũ |

Output: `s3a://silver/_streaming/<stream_version>/clickstream_clean` và `.../payment_callbacks_clean`
(prefix bắt đầu bằng `_` để không lẫn với snapshot Silver theo version).

## Chạy tay

Cần Kafka đã có dữ liệu (`plan-stream` + `produce`). Build lại image Spark vì code được COPY lúc build:

```powershell
docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml build spark-job
docker compose up -d --wait kafka minio
docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml run --rm --no-deps -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 --entrypoint /opt/spark/bin/spark-submit spark-job --master "local[2]" --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 --conf spark.jars.ivy=/tmp/.ivy2 /opt/app/spark_jobs/dp4_stream_to_silver.py --stream-version stream_v1 --topic-prefix edai --out /data/stream/stream_v1
```

Lần đầu container cần Internet để tải jar Kafka (`--packages`), mất vài phút.

## Đọc báo cáo

```powershell
Get-ChildItem .\output\stream\stream_v1 -Recurse -Filter stream_report.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content
```

Đạt khi `"passed": true`. Các số cần ghi vào bằng chứng, mỗi topic:
- `kafka_rows`, `duplicate_rows_in_kafka` (callback: khoảng 1,5%)
- `clean_rows_written` = `distinct_event_ids_in_kafka`, `duplicates_in_clean_output` = 0
- `dropped_as_too_late` = 0
- `out_of_order_rows` và `out_of_order_lateness_seconds` (clickstream: kỳ vọng max khoảng 300–600)

Thử `--watermark "1 minutes"` với `--stream-version stream_late` để thấy `dropped_as_too_late > 0`: đây là bằng chứng
watermark thật sự có tác dụng.

## Trong Airflow

DAG `edai_medallion` có thêm task `dp4_kafka_to_silver` chạy sau `verify_markers`. Tham số `stream_version` và
`topic_prefix` đổi được khi Trigger. Muốn chạy lại từ đầu, dùng `stream_version` mới.

## Phạm vi kiểm thử

Logic parse, loại dòng hỏng, đếm trùng và đo out-of-order đã thử trên dữ liệu mẫu bằng Spark local.
Phần đọc Kafka thật, watermark và checkpoint trên MinIO chưa chạy được ở môi trường soạn tài liệu (không có broker);
cần chạy trên máy bạn để xác nhận. Dữ liệu replay lịch sử dùng event_ts của nguồn, nên độ trễ tính trong
đồng hồ mô phỏng, không phải `now - event_ts`.
