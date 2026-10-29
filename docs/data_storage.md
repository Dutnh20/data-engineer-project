# Tối ưu lưu trữ: Lakehouse và Data Warehouse

Hai thử nghiệm đo trước/sau trên cùng dữ liệu Gold. Mọi bảng số bên dưới để trống cho đến khi bạn chạy và điền số thật (không dùng số mẫu).

## A. Lakehouse: compaction và partition

Job `spark_jobs/lakehouse_compact.py` dựng ba bố cục của `fact_orders` (Parquet trên MinIO):

| Bố cục | Mô tả | Mô phỏng vấn đề |
|---|---|---|
| `small_files` | 200 file Parquet nhỏ | Ghi micro-batch/streaming sinh nhiều file nhỏ |
| `compacted` | Gom thành vài file ~128 MB | Compaction |
| `partitioned` | Gom file và `partitionBy(order_month)` | Partition pruning theo tháng |

Với mỗi bố cục job in số file, kích thước trung bình, thời gian truy vấn quét toàn bảng và truy vấn lọc theo một tháng (tháng có nhiều đơn nhất, đặt bằng `--month`).
Job kiểm tra kết quả của ba bố cục **giống hệt nhau** (số dòng, tổng tiền); nếu khác thì thoát lỗi. Z-order không có trong Parquet thuần (cần Delta/Iceberg), nên không dùng.

```powershell
$c = "docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml"
docker compose up -d minio
Invoke-Expression "$c run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-job --master local[2] --driver-memory 2g /opt/app/spark_jobs/lakehouse_compact.py --gold-version ml_gold_v1 --runs 3 --out /data/lakehouse/ml_gold_v1"
Get-Content .\output\lakehouse\ml_gold_v1\lakehouse_report.json
```

| Bố cục | Số file | TB/file | Quét toàn bảng (s) | Lọc 1 tháng (s) |
|---|---:|---:|---:|---:|
| small_files | | | | |
| compacted | | | | |
| partitioned | | | | |

Thử trên dữ liệu tổng hợp 80.000 đơn (local, 100 file nhỏ ban đầu): 100 → 1 file (compacted) và 5 file (partitioned), quét toàn bảng nhanh hơn khoảng 2,5 lần
sau compaction, truy vấn lọc tháng nhanh nhất ở bố cục partition; kết quả cả ba giống hệt nhau. Con số này chỉ chứng minh job hoạt động; hãy dùng số của bạn.
Lưu ý: với dữ liệu nhỏ, lợi ích đến chủ yếu từ ít file hơn (ít overhead mở file và lập lịch task), và partition chỉ có ích khi truy vấn lọc theo cột partition.

## B. Data warehouse: indexing trên PostgreSQL

Gold được xuất CSV rồi nạp vào PostgreSQL (`compose.warehouse.yaml`). Bốn truy vấn đại diện trong `warehouse_sql/queries.sql` chạy `EXPLAIN (ANALYZE, BUFFERS)`
**trước** và **sau** khi tạo index (`warehouse_sql/indexes.sql`).

```powershell
$c = "docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml"
Invoke-Expression "$c run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-job --master local[2] --driver-memory 2g /opt/app/spark_jobs/export_warehouse.py --gold-version ml_gold_v1 --out /data/warehouse_csv"
docker compose -f compose.yaml -f compose.warehouse.yaml up -d --wait warehouse
$w = "docker compose -f compose.yaml -f compose.warehouse.yaml exec -T warehouse psql -U edai -d edai_dw"
Invoke-Expression "$w -f /csv/schema.sql"
Invoke-Expression "$w -f /sql/load.sql"
Invoke-Expression "$w -c 'SELECT COUNT(*) FROM fact_orders'"
# Trước khi tạo index:
Invoke-Expression "$w -f /sql/queries.sql" | Tee-Object .\output\warehouse_before.txt
# Tạo index rồi đo lại:
Invoke-Expression "$w -f /sql/indexes.sql"
Invoke-Expression "$w -f /sql/queries.sql" | Tee-Object .\output\warehouse_after.txt
```

Trong mỗi file kết quả, lấy dòng `Execution Time` và dạng node đầu của plan (`Seq Scan` hay `Index Scan`/`Index Only Scan`/`Bitmap Heap Scan`).

| Truy vấn | Index dùng | Trước: plan / ms | Sau: plan / ms |
|---|---|---|---|
| Q1 tra cứu `order_id` | `ix_fact_orders_order_id` | | |
| Q2 doanh thu theo thành phố | `ix_fact_orders_status_ts` (covering) | | |
| Q3 đơn của một khách | `ix_dim_customer_customer_id`, `ix_fact_orders_customer_sk` | | |
| Q4 dòng hàng của sản phẩm | `ix_fact_items_product_id` (covering) | | |

Cần nêu rõ trong báo cáo: index tăng tốc truy vấn chọn lọc cao (Q1, Q3), còn truy vấn quét phần lớn bảng không hưởng lợi và index tốn thêm dung lượng
và thời gian ghi. Đo dung lượng bằng `SELECT pg_size_pretty(pg_total_relation_size('fact_orders'))` trước và sau.

## Phạm vi kiểm thử

`lakehouse_compact.py` đã chạy thành công trên dữ liệu tổng hợp ở Spark local; `export_warehouse.py` mới được thử phần sinh DDL; các file SQL PostgreSQL chưa chạy được trong môi trường soạn tài liệu
(không có PostgreSQL). Nếu `\copy` báo lỗi kiểu dữ liệu hoặc số cột, dán lỗi để chỉnh DDL sinh tự động.
