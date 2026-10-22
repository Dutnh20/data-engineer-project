# Bắt đầu với code Python sinh dữ liệu

Đây là bước **tạo nguồn dữ liệu** cho đồ án thương mại điện tử. Toàn bộ bản ghi
được Python tự sinh. Bạn có thể dùng nguồn này cho Spark, Flink và các bước ML
sau đó. Gói đã có DP1 và bước Spark Silver; Flink và mô hình ML sẽ thêm sau.

Nếu đã sinh và upload dữ liệu, chuyển đến [hướng dẫn DP1 nguồn → Bronze](docs/pipeline_01_bronze.md).
Pipeline Python/S3 này đã được thêm vào gói; dùng lại image generator sau khi build lại.

Nếu Bronze đã có `_SUCCESS.json`, đọc [hướng dẫn Spark Bronze → Silver](docs/pipeline_02_silver.md).

## 1. Chạy bản nhỏ trước

Chạy từng lệnh sau trong PowerShell:

```powershell
cd D:\edai-lab\synthetic_generator
Get-ChildItem
docker version
docker compose build generator
docker compose run --rm --entrypoint python generator generate_data.py --config configs/smoke.json --out /data/smoke --checksums
```

Lệnh cuối chạy Python trong container, thực hiện:

1. Sinh sáu bảng nguồn, ghi Parquet theo lô.
2. Đọc lại file để kiểm tra dữ liệu và SHA256.
3. Tạo clickstream/callback cùng lịch sự kiện đến, lưu JSONL.

Ở bước này chưa cần bật Kafka hoặc MinIO. Python trong container dùng các
thư viện đã cài lúc build; bạn không cần chạy pip trên Python Windows đang lỗi.

Khi hoàn tất, màn hình phải có `"hoan_thanh": true` và
`"kiem_tra_du_lieu": true`. `"da_du_100gb": false` là đúng với bản nhỏ.

## 2. Tìm dữ liệu vừa sinh

Container ghi vào `/data/smoke`, tương ứng thư mục Windows:

```text
D:\edai-lab\synthetic_generator\output\smoke
```

| Đường dẫn bên trong | Nội dung |
|---|---|
| `source/offline/customers/` | 2.000 khách hàng, ảnh chụp lúc đăng ký |
| `source/offline/products/` | 1.000 sản phẩm |
| `source/offline/orders/` | 12.000 đơn hàng |
| `source/offline/order_items/` | Chi tiết đơn, gồm bản sao được chèn |
| `source/offline/payments/` | Các lần thanh toán; có hai phiên bản schema |
| `source/offline/customer_changes/` | 2.150 ảnh chụp/lần đổi thông tin khách hàng |
| `stream/events.jsonl` | Sự kiện cho 2.000 đơn đầu, có lịch đến muộn và callback trùng |
| `manifest.json` | Số lượng và số byte Parquet thực tế |
| `evidence/validation.json` | Kết quả kiểm tra file độc lập |
| `stream/plan_report.json` | Thống kê sự kiện; chưa phải bằng chứng đã gửi Kafka |

Xem vài dòng Parquet ngay trong terminal:

```powershell
docker compose run --rm generator preview /data/smoke/source/offline/orders/part-000000000001.parquet
docker compose run --rm generator preview /data/smoke/source/offline/customer_changes/part-000000000001.parquet --rows 25
```

## 3. Hiểu dữ liệu được sinh như thế nào

| Dữ liệu | Quy tắc chính |
|---|---|
| Khách hàng | ID duy nhất; ngày đăng ký trước khi đặt đơn; khoảng 80% ở TP.HCM |
| Sản phẩm | 75% danh mục là electronics trong các profile đi kèm |
| Đơn hàng | Chọn customer_id đã tồn tại; mỗi đơn có 1–4 sản phẩm khác nhau |
| Chi tiết đơn | Giá theo sản phẩm; `line_amount = quantity * unit_price - discount_amount` |
| Thanh toán | Cùng order_id, số tiền bằng tổng đơn; một số đơn có lần thử thất bại trước khi thành công |
| Lịch sử khách | 5% khách đổi segment; một nửa nhóm này đổi lần thứ hai |

Code chủ động chèn các vấn đề để bạn xử lý trong lab:

- Khoảng 2% dòng cuối của `order_items` là bản sao cùng ID, khác `created_ts` 1 giây.
- Payment v1 **không có** cột `failure_reason`; v2 bổ sung cột này.
- Khoảng 15% clickstream từ mobile đến muộn 5–10 phút trong đồng hồ mô phỏng.
- Khoảng 1,5% callback cuối là bản gửi lại với cùng event/payment ID.

Các bảng dùng chung một logic nghiệp vụ để khóa ngoại và số tiền khớp nhau.
Seed giúp chạy lại với cùng cấu hình và phiên bản code cho cùng dữ liệu.

## 4. Đọc code theo thứ tự

1. **`generate_data.py`**: nhìn luồng ba bước trước.
2. **`configs/smoke.json`**: xem số khách, số đơn, seed, tỷ lệ lỗi.
3. **`edai_generator/model.py`**: đọc `customer()`, `product()`, `make_order()`.
   Sau đó đọc `customer_changes()` để hiểu nguồn cho SCD2.
4. **`edai_generator/schemas.py`**: xem cột và kiểu dữ liệu Parquet.
5. **`edai_generator/offline.py`**: xem vòng lặp theo lô, chèn bản sao và checkpoint.
6. **`edai_generator/validate.py`**: xem các câu SQL dùng kiểm tra kết quả.
7. **`edai_generator/streaming.py`**: xem cách tạo event time, arrival time và gửi Kafka.
8. **`edai_generator/storage.py`**: xem upload file nguồn lên MinIO.

Ví dụ dòng `orders: 12000` trong config là số đơn tối thiểu. Khi `target_gb`
là `null`, chương trình dừng đúng số đơn đó. Khi có mục tiêu byte, chương trình
tiếp tục thêm đơn tới khi đủ cả số đơn tối thiểu và dung lượng thực tế.

## 5. Đổi quy mô dữ liệu

| Config | Mục đích |
|---|---|
| `smoke.json` | Kiểm tra code nhanh; lịch sử đơn khoảng 3 giờ 20 phút |
| `ml_history.json` | 200.000 đơn trải khoảng 347 ngày, để luyện feature/label theo thời gian |
| `calibrate.json` | Đo thử mục tiêu Parquet 100 MB trên máy bạn |
| `full_100gb.json` | Sinh tối thiểu 100 GB Parquet nguồn thực tế |

Sinh bộ có lịch sử dài:

```powershell
docker compose run --rm --entrypoint python generator generate_data.py --config configs/ml_history.json --out /data/ml_history --stream-orders 0 --checksums
```

`--stream-orders 0` bỏ qua streaming. Khi cần lịch sử clickstream cho cả bộ ML:

```powershell
docker compose run --rm generator plan-stream --out /data/ml_history --orders 200000
```

Lệnh này tạo nhiều sự kiện và cần thêm dung lượng. Chỉ lập lịch cho 2.000 đơn
đầu không tạo ra đủ lịch sử streaming cho toàn bộ 347 ngày.

Sinh bộ chính sau khi bản nhỏ chạy đúng và máy đủ chỗ trống:

```powershell
docker compose run --rm generator offline --config configs/full_100gb.json --out /data/full
docker compose run --rm generator validate --out /data/full --checksums
```

Không suy ra 100 GB từ số dòng. Xem `physical_source_bytes` trong manifest và
validator. Dung lượng lưu thêm trên MinIO, Bronze và Silver không cộng vào nguồn.
Gói ZIP chứa code và kết quả kiểm tra nhỏ, **không chứa sẵn 100 GB**.

## 6. Tiếp tục sau khi bị dừng

Nếu lệnh sinh trước đó bị dừng, chạy lại đúng config và output, thêm `--resume`:

```powershell
docker compose run --rm --entrypoint python generator generate_data.py --config configs/smoke.json --out /data/smoke --checksums --resume
```

Chương trình bỏ qua các lô đã commit, tiếp tục phần còn lại rồi kiểm tra.
Nếu bạn sửa seed, config hoặc đổi phiên bản code, dùng thư mục output mới.
Không chạy hai tiến trình ghi vào cùng một output.

## 7. Khi đã đọc được Parquet

Tiếp tục phần MinIO và Kafka theo [README.md](README.md). Nếu đã có nền móng
`edai-foundation`, dùng `compose.addon.yaml` theo [docs/run_windows.md](docs/run_windows.md).

Các tệp config được COPY vào image. Sau khi sửa config hoặc code, chạy lại
`docker compose build generator` trước khi chạy container.

Nếu dùng trực tiếp Python 3.12 đã có venv hoạt động, có thể chạy:

```powershell
python -m pip install -r requirements.txt
python generate_data.py --config configs/smoke.json --out output/smoke --checksums
```

Hai lệnh này chỉ áp dụng khi `python` trỏ đúng venv, không áp dụng vào Python
do uv quản lý hoặc WindowsApps alias chưa được cấu hình.
