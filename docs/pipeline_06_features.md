# Bảng feature offline `feat_customer_orders_90d` (DP3 rubric)

Job: `spark_jobs/feat_customer_orders.py`. Đọc `fact_orders` và `dim_customer` của Gold đã công bố, ghi bảng
`feat_customer_orders_<N>d` vào `s3a://gold/_features/<feat_version>/runs/<run_id>/` kèm `feat_report.json` và marker
`_features/<feat_version>/_SUCCESS.json` (ghi một lần).

## Lược đồ

| Cột | Ý nghĩa |
|---|---|
| `customer_id` | Khóa thực thể (mọi khách trong `dim_customer`, kể cả khách chưa có đơn) |
| `event_timestamp` | Mốc "as of" của feature; chỉ dùng đơn có `order_ts <= event_timestamp` (point-in-time correct) |
| `f_customer_total_orders_90d` | Số đơn trong (event_timestamp − 90 ngày, event_timestamp] |
| `f_customer_completed_orders_90d` | Số đơn hoàn tất trong cửa sổ |
| `f_customer_total_spend_vnd_90d` | Tổng tiền đơn hoàn tất trong cửa sổ |
| `f_customer_avg_order_value_vnd_90d` | Chi tiêu / số đơn hoàn tất; NULL nếu chưa có |
| `f_customer_days_since_last_order` | Số ngày từ đơn gần nhất (mọi thời điểm ≤ mốc) đến mốc; NULL nếu chưa có |
| `created` | Thời điểm pipeline tạo hàng |
| `_gold_version`, `_pipeline_run_id` | Truy vết |

Mốc `event_timestamp` mặc định là nửa đêm UTC sau ngày có đơn cuối. `--snapshots N` tạo N mốc cách nhau 1 ngày để có lịch sử
feature cho huấn luyện. Cột feature được tính theo `--window-days` (mặc định 90).

## Quality checks (job fail nếu sai)

rows = khách × số mốc; không trùng `(customer_id, event_timestamp)`; không NULL ở khóa/`event_timestamp`/`created`;
feature đếm không âm; `completed <= total`.

## Chạy tay

```powershell
$c = "docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml"
Invoke-Expression "$c run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-job --master local[2] --driver-memory 1g /opt/app/spark_jobs/feat_customer_orders.py --gold-version gold_v2 --feat-version feat_v1 --out /data/features/feat_v1"
```

Hoặc chạy qua DAG `edai_dp3_feature_table`. Validate thủ công:

```powershell
Invoke-Expression "$c run --rm --no-deps --entrypoint python3 spark-job -m spark_jobs.validate_zone --zone feat --version feat_v1"
```

Kiểm thử: `tests_spark/test_features.py` (cửa sổ 90 ngày, không rò rỉ tương lai, khách không có đơn, nhiều mốc).
