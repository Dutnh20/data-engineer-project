# Bằng chứng chạy bản 1.1.0

- unit_tests.txt: 16 kiểm thử tự động.
- dp1_validation.json: bổ sung DP1, toàn bộ 29 test đã pass (16 generator + 13 pipeline); ghi rõ giới hạn transport fake và lần thử live Silo bị chặn.
- dp2_validation.json, dp2_smoke_report.json, dp2_spark_tests.txt: 7 test mới chạy bằng Spark 3.5.6 local và Parquet thật, metadata S3 mô phỏng; chưa phải kết quả cluster Docker/100GB.
- smoke_run.txt, smoke_manifest.json, smoke_validation.json: bộ 12.000 đơn.
- stream_plan_report.json: lịch sự kiện cho 2.000 đơn đầu của smoke; chưa gửi Kafka.
- ml_history_run.txt, ml_history_manifest.json, ml_history_validation.json: bộ 200.000 đơn.
- packaging_checks.json: kiểm tra cú pháp và cấu trúc gói.

Các file run.txt dùng --resume để kiểm tra lại nguồn đã sinh trong lần chạy này.
Các kiểm thử cũng xác nhận resume tái tạo đúng các file so với lần chạy liên tục.
Kafka/MinIO trong unit test dùng đối tượng giả để kiểm tra luồng xử lý.
Chưa chạy Docker, broker/MinIO thật hoặc sinh 100 GB trong môi trường bàn giao.
Không dùng các kết quả nhỏ này thay bằng chứng chạy đủ yêu cầu trên máy lab.
