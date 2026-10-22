# Đối chiếu phần Data Generator với rubric

Nguồn yêu cầu: workbook người học cung cấp `EDAI K11 - DE(1).xlsx`, sheet
`edai-1 (50%)`. Phần generator ở dòng 5–10. Workbook không tự động chấm dự án này;
bảng dưới mô tả cách tạo bằng chứng để giảng viên kiểm tra.

| Dòng / yêu cầu | Code | Cách chạy / quan sát | Trạng thái cần có khi nộp |
|---|---|---|---|
| 5 — Simulate skew | model.customer/product; validate | offline; validate; đọc shipping_city_counts và category_counts | Có tỷ lệ đo thực tế và giải thích ảnh hưởng group-by/join |
| 5 — Offline tối thiểu 100GB | offline.generate; Config.target_bytes | offline --config configs/full_100gb.json | physical_source_bytes >= 100000000000, không chỉ cấu hình |
| 6 — Schema evolution | schemas.PAYMENTS_V2; offline cắt lô tại boundary | preview một file v1 và v2 | File v1 thiếu cột; v2 có cột nullable; reader xử lý được |
| 7 — Vấn đề offline khác | duplicate_copy_due; offline | validate | Duplicate count/rate đo lại và bằng chứng overcount trước dedup |
| 8 — Store generated data before Bronze | storage.upload_run | upload; mở MinIO | Parquet tại source-department, _READY.json, upload_report passed |
| 9 — Late arrivals | streaming.plan_stream + produce_stream | plan-stream; produce; consumer/Flink | Có events đến muộn, giải thích đồng hồ và mức delay |
| 10 — Vấn đề streaming khác | callback copies trong plan_stream | produce; downstream dedup theo event_id | Callback lặp được đưa vào Kafka; ACK report và consumer evidence |
| D5/D9 — Documentation | manifest + validator + plan/delivery report | lưu các báo cáo và ảnh có chú thích | Volume, skew, cardinality, schema, duplicate, lateness |

## Checklist bằng chứng

1. Config sử dụng: seed, số lượng, target, batch size và tỷ lệ lỗi.
2. Manifest run full và báo cáo validator độc lập.
3. Ảnh console/JSON có byte count thực tế; không cộng local + MinIO + Bronze.
4. Hai schema payments và tên file đại diện.
5. Đếm duplicate và chênh lệch tổng tiền trước/sau khử trùng.
6. Ảnh MinIO với source prefix và marker hoàn tất.
7. Plan report cho late/duplicates và delivery report có Kafka ACK.
8. Kafka sample/ảnh consumer; ghi rõ topic prefix và có replay lại hay không.
9. Giải thích: producer tạo lỗi; Spark/Flink mới chịu trách nhiệm xử lý lỗi.

## Phần nào chưa được hoàn thành chỉ bằng gói này?

Các tiêu chí Spark/Flink optimization, Bronze/Silver/Gold, warehouse modeling,
Airflow pipelines, DataHub, SCD2, feature store, ML/LLM platforms và ý tưởng mới
vẫn cần triển khai riêng. Validator dùng SQL để kiểm tra source không thay thế
cho các pipeline Spark/Flink mà rubric yêu cầu.

Gói có hai Dockerfile để thực hành multi-stage, nhưng kích thước image trước/sau
phải đo từ image thực tế trên máy chạy. Không có số đo image giả trong tài liệu.
