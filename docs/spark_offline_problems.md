# Spark xử lý các vấn đề dữ liệu offline (schema evolution, trùng lặp, SCD2)

Phần xử lý skew và so sánh baseline → tối ưu nằm ở [spark_optimization.md](spark_optimization.md). Tài liệu này giải thích hai vấn đề còn lại
mà `spark_jobs/dp2_bronze_to_silver.py` và `silver_transforms.py` xử lý, và cách tích hợp vào Airflow.

## 1. Schema evolution của `payments`

**Vấn đề.** Nguồn đổi schema giữa chừng: đơn có `order_id` ≤ `last_v1_order_id` ghi Parquet v1 (không có `failure_reason`), từ `first_v2_order_id` trở đi ghi v2 (có cột này).
Một thư mục `payments` chứa cả hai loại file.

**Baseline (cách làm ngây thơ).** `spark.read.parquet(thư_mục)` lấy schema từ một file; nếu file đó là v1, **cột `failure_reason` biến mất mà không có cảnh báo**,
nên mọi lý do thanh toán thất bại ở file v2 bị mất. Tái hiện bằng:

```powershell
$c = "docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml"
Invoke-Expression "$c run --rm --no-deps -v ${PWD}/scripts:/opt/app/scripts:ro --entrypoint /opt/spark/bin/spark-submit spark-job /opt/app/scripts/demo_schema_evolution.py --payments-dir /data/<run>/source/offline/payments"
```

(`<run>` là thư mục generator đã sinh, ví dụ `ml_history`.) Kết quả ba cách đọc, thử trên dữ liệu tổng hợp 99 dòng (49 dòng v1, 50 dòng v2):

| Cách đọc | Có cột `failure_reason` | Số `failure_reason` khác NULL | Nhận xét |
|---|---|---:|---|
| `default` | không | — | Mất cột: lỗi im lặng |
| `mergeSchema` | có | 50 | Đúng, nhưng phải đọc footer mọi file; kiểu cột xung đột sẽ làm job lỗi muộn |
| `explicit` (cách DP2 dùng) | có | 50 | Schema cố định `schema_for("payments")`; file v1 đọc ra NULL |

Đây là kết quả trên dữ liệu thử; hãy chạy lệnh trên với dữ liệu thật của bạn và dán bảng số thật vào mục này.

**Cách DP2 xử lý.** `read_bronze` đọc với schema cố định đã khai báo trong `silver_contracts.py` (`failure_reason:string?`, dấu `?` = nullable),
file v1 cho giá trị NULL. Sau đó quy tắc `reason_only_for_failed` kiểm tra lý do chỉ xuất hiện ở thanh toán `failed`, nên NULL ở v1 hợp lệ nhưng
lý do ở thanh toán thành công thì bị chặn. Báo cáo Silver ghi nhận các phiên bản schema quan sát được.

## 2. Duplicate offline ở `order_items` (khoảng 2%)

**Vấn đề.** Nguồn gửi lại một số dòng `order_items` với `created_ts` lệch +1 giây, nên cùng `order_item_id` xuất hiện nhiều lần. Nếu cộng thẳng, doanh thu phình
(ở `ml_history`: thô 8.170.786.576.000 VND so với 8.008.621.030.000 VND sau khử trùng, thừa 162.165.546.000 VND, 10.208 dòng dư).

**Baseline.** Không khử trùng: `sum(line_amount)` tính cả bản sao.

**Cách DP2 xử lý.** `deduplicate_items` dùng `row_number()` theo `(_dataset_version, order_item_id)`, sắp `created_ts` giảm dần rồi tên file giảm dần
(tie-break ổn định), giữ dòng đầu. Sau đó `monetary_sum` đối soát: tổng tiền thô trừ tổng sau khử trùng phải bằng tiền của các bản sao bị loại,
ghi trong `silver_report.json` mục `amount_reconciliation_vnd`. Quality gate chặn công bố snapshot nếu còn trùng hoặc số liệu không khớp,
và đưa dòng lỗi vào thư mục quarantine để điều tra.

## 3. Lịch sử khách hàng SCD2

`build_customer_history` biến `customer_changes` thành các khoảng `[valid_from_ts, valid_to_ts)`; bỏ cập nhật không đổi thuộc tính;
quality gate kiểm tra không chồng/không hở, mỗi khách đúng một bản hiện tại và đơn hàng luôn khớp đúng một phiên bản.

## Tích hợp Airflow

Cả ba xử lý chạy trong task `ingest_silver` của DAG `edai_dp2_bronze_to_gold`; task `validate_silver` kiểm tra marker/report đã công bố
([pipeline_04_airflow.md](pipeline_04_airflow.md)). Số partition lấy từ tham số `--shuffle-partitions` của job, đặt sau khi đo ở
[spark_optimization.md](spark_optimization.md).
