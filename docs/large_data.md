# Chạy quy mô lớn và hiểu đúng yêu cầu 100GB

## Đơn vị và điều kiện dừng

Gói chọn định nghĩa có thể kiểm chứng: **100GB = 100.000.000.000 byte của các file
Parquet nguồn đã ghi xong**, cộng năm bảng chính customers/products/orders/order_items/payments và bảng nguồn customer_changes.
Rubric không ghi rõ trước/sau nén; cấu hình này đo trực tiếp file vật lý để tránh
báo volume từ số dòng nhân với một kích thước ước lượng.

Không tính manifest, inventory, journal, stream plan, bản upload MinIO, bản sao
Bronze/Silver/Gold hay dữ liệu đệm. Những bản sao 2% trong order_items là lỗi dữ
liệu chủ động và được tính như một phần nguồn, nhưng không nhân bản cả dataset
để lấy dung lượng.

Profile `full_100gb.json`:

| Tham số | Giá trị | Vai trò |
|---|---:|---|
| customers | 1.000.000 | Bảng khách hàng |
| products | 2.000.000 | Danh mục lớn cho bài high-cardinality join |
| orders | 2.000.000 | Mức tối thiểu, không phải giới hạn trên |
| target_gb | 100 | Mức tối thiểu file Parquet vật lý |
| batch_orders | 100.000 | Số đơn xử lý mỗi lô |
| dimension_batch | 100.000 | Số bản ghi dimension mỗi lô |
| schema_change_after_order | 1.000.000 | Ranh giới schema theo thứ tự ghi nguồn |
| compression | NONE | Parquet không nén; không có padding |

Cả hai điều kiện số đơn tối thiểu và byte target phải đúng mới kết thúc. Một lô
hoàn chỉnh được commit trước khi kiểm tra nên tổng cuối có thể lớn hơn 100GB.
Nếu chọn ZSTD/Snappy, chương trình sẽ sinh nhiều bản ghi hơn để file sau nén vẫn
đạt target. Báo cáo có thêm Arrow logical bytes để tham khảo; đó không phải số
byte được dùng để kết luận đạt 100GB.

## Chạy calibration trước

```powershell
Measure-Command { docker compose run --rm generator offline --config configs/calibrate.json --out /data/calibrate }
```

Calibration dừng ở ít nhất 0,1GB file vật lý. Dùng thời gian thực tế trên máy bạn
để ước lượng sơ bộ; tốc độ còn phụ thuộc filesystem Windows/WSL, CPU, số file,
compression và tải hệ thống. Không có cam kết thời gian sinh 100GB từ một lần
đo nhỏ. Kết quả chạy thực tế của phiên bản này nằm trong evidence/; chưa chạy 100GB.

## RAM và disk

Chương trình không giữ cả tập dữ liệu trong RAM. Nó giữ một lô order cùng item/
payment rồi giải phóng, và tính dimension record từ ID thay vì giữ danh mục
2 triệu sản phẩm trong một dictionary. RAM tăng theo batch size.

Profile full có batch lớn hơn smoke. Nếu máy thiếu RAM, giảm batch_orders và
dimension_batch **trước khi bắt đầu run mới**. Sau khi đã có checkpoint, phải
giữ nguyên cấu hình để resume. Batch nhỏ tạo nhiều file nhỏ hơn; đây là đánh đổi
giữa RAM và số lượng file. Compaction có thể là bài Spark ở bước sau.

Local nguồn cần hơn 100GB. Khi upload thêm một bản lên MinIO chạy trên cùng máy,
tổng storage tăng thêm khoảng dung lượng nguồn. Docker image/volumes và dữ liệu
tạm của validator cũng cần chỗ. Vì vậy cần dự trù hơn 200GB cho cả local + MinIO,
và thêm dung lượng kiểm tra/xử lý; kiểm tra cả ổ chứa output và vị trí Docker disk.

`min_free_gb` là ngưỡng dừng trước khi bắt đầu lô, không phải cơ chế bảo đảm lô
tiếp theo luôn vừa phần trống. Nếu ổ đầy giữa lô, giải phóng chỗ rồi resume.
Validator DuckDB full scan có thể dùng disk tạm trong `_validation_tmp/`; các phép
DISTINCT, join, window trên quy mô lớn vẫn cần tài nguyên đáng kể.

## Resume hoạt động thế nào?

1. Generator ghi từng shard vào file tạm, đóng file rồi đổi tên atomically.
2. Sau khi mọi bảng của một lô đã ghi xong, commit metadata lô trong SQLite.
3. Metadata chứa đường dẫn, số dòng, kích thước, SHA256 và counters.
4. Khi resume, kiểm tra fingerprint config và kích thước file đã commit.
5. File chưa commit được tái sinh đúng tên và dữ liệu; phần đã commit được bỏ qua.

Resume không đọc lại SHA256 của toàn bộ 100GB mỗi lần. Dùng `validate --checksums`
nếu cần xác minh toàn bộ byte. Chỉ chạy một writer cho một thư mục output; các
run độc lập có thể dùng thư mục khác. Gói không cung cấp ghi đồng thời vào cùng run.

Muốn thử resume nhanh:

```powershell
docker compose run --rm generator offline --config configs/smoke.json --out /data/resume_demo --stop-after-chunks 2
docker compose run --rm generator offline --config configs/smoke.json --out /data/resume_demo --resume
docker compose run --rm generator validate --out /data/resume_demo --checksums
```

## Giới hạn nghiệp vụ ở quy mô benchmark

Event time được ánh xạ vào một cửa sổ lịch sử cố định: khi vượt history_days,
lịch được lặp theo chu kỳ nhưng order/item/payment ID vẫn mới. Điều này tạo
dữ liệu giao dịch mới cho benchmark, không copy file cũ. Lượng giao dịch và
hành vi là kịch bản tổng hợp; không nên diễn giải thành quy mô thật của cửa hàng.

Không có SCD2 dimension trong source generator này. SCD2 và dim/fact là bước
modeling downstream trong rubric. Không có nhãn tương lai hoặc cờ đáp án injected
trong bảng nguồn để pipeline bỏ qua việc xử lý dữ liệu.
