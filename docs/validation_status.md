# Phạm vi xác minh của bản bàn giao

## Đã thực thi — phiên bản 1.1.0, ngày 21/10/2026

- Python 3.12.14 trên môi trường Linux; dependency theo requirements.lock.
- Sinh và đọc lại bộ smoke: 2.000 customers, 1.000 products, 12.000 orders,
  30.591 order_items kể cả bản sao, 13.523 payments, 2.150 customer_changes; tổng Parquet 981.634 byte.
- Validator quét Parquet, kiểm tra SHA256, PK/FK, timestamp, tổng tiền, duplicate,
  phân bố và hai schema vật lý; kết quả passed=true.
- Sinh kế hoạch streaming cho 2.000 order: 14.042 clickstream, 2.268 callback gốc,
  34 callback gửi lại; 1.530 clickstream mobile trễ 300–600 giây.
- Sinh và kiểm tra profile ML: 200.000 order, 20.000 khách, 10.000 sản phẩm,
  510.418 order_items, 225.533 payments, 21.500 customer_changes; phủ 347,22 ngày,
  tổng Parquet 16.746.269 byte. Không tạo streaming cho toàn bộ bộ ML ở lần kiểm tra này.
- 16 kiểm thử tự động đã qua: checkpoint/resume, file chưa commit, config mismatch, file hỏng,
  byte target thật ở quy mô nhỏ, tính tái tạo, khóa liên kết streaming, dịch đồng hồ,
  truy vấn đúng phiên bản khách theo thời gian, tắt cập nhật khách và độ dài lịch sử ML.
- Kiểm thử luồng ACK/thất bại của producer và upload/skip/ready marker bằng fake
  transports; các test đó không kết nối broker hoặc MinIO thật.
- Kiểm tra cú pháp Python và parse YAML; đọc lại metadata và checksum Parquet.

Các kết quả cụ thể nằm trong thư mục evidence của gói mã nguồn. Thư viện pytz
được khai báo trong requirements.lock để DuckDB đọc timestamp có múi giờ.
Các script cuối đã chạy thành công sau khi cài đầy đủ dependency này.

## Chưa thực thi trong môi trường bàn giao

- Sinh đủ 100GB. Bản full có code và cấu hình target; người chạy phải sinh và
  xác nhận bằng physical_source_bytes trên dữ liệu thật của mình.
- Build/run Docker image và Compose: môi trường kiểm tra không có Docker CLI/daemon.
- Kafka broker/MinIO thật, producer wall-clock chờ đủ 5–10 phút qua mạng thực.
- Sinh/chạy trên máy Windows của người học.
- Spark trên cluster Docker/S3A tới MinIO thật, Gold/Flink downstream, Airflow,
  DataHub và các nền tảng ML/Agent.

Do đó cần chạy các bước Docker trong README, giữ upload_report, delivery_report,
Kafka sample và bằng chứng volume từ máy thực hành. Không dùng kết quả fake
transport hay smoke để thay cho các bằng chứng tích hợp/100GB.

## Sửa image storage ngày 21/10/2026

Đã kiểm tra tag, manifest và entrypoint của image PGSTY Silo công khai.
Compose đổi image cho service minio; dữ liệu generator và kết quả Python trước đó không thay đổi.
Chưa chạy container hoặc kết nối S3 thật sau thay đổi. Xem minio_image_fix.md.

## Bổ sung DP1 và Spark Silver ngày 21/10/2026

- DP1: thêm 13 test, tổng suite generator/DP1 là 29 test; S3 transport mô phỏng.
- DP2a: 7 test mới bằng Spark 3.5.6 local[2], Java 17. Parquet được đọc/ghi thật,
  metadata S3 được mô phỏng qua filesystem adapter.
- Bộ smoke: loại 611 bản retry, giữ 29.980 order_items; tổng tiền sạch khớp
  orders; cả payment schema v1/v2 được đọc chung; 2.150 phiên bản lịch sử khách.
- 48 kiểm tra chất lượng qua. Một bản ghi quantity=0 được quarantine và chặn
  công bố. Rerun dùng lại snapshot, không append; file sửa bị phát hiện.
- Đã xác nhận Spark/Hadoop là 3.5.6/3.3.4 và JVM tải được các class S3A/AWS SDK
  với các JAR đã kiểm SHA256. Chưa thử kết nối S3A tới storage thật.
- Code, YAML và các đường dẫn COPY của Dockerfile đã được kiểm tra tĩnh.
  Docker Compose/master-worker và image build vẫn phải được xác minh trên máy lab.

Xem `evidence/dp2_validation.json`, `evidence/dp2_smoke_report.json` và
[hướng dẫn Spark Silver](pipeline_02_silver.md). Các kết quả này không phải
bằng chứng chạy 100GB, benchmark skew hay hoàn tất Airflow/DataHub.
