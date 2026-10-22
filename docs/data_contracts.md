# Data contracts và cách chèn lỗi

## 1. Seed, tính nhất quán và khả năng tái tạo

Mỗi entity có RNG riêng từ seed, ID và namespace bằng integer mixer ổn định.
Không dùng `hash()` phụ thuộc tiến trình. Giữ nguyên cấu hình, code và môi trường
dependency để tái tạo file. Các file có fingerprint và SHA256 trong inventory.
Thay seed/config hoặc sửa logic code thì bắt đầu một thư mục run mới.

## 2. Skew

Thành phố của khách được ánh xạ với xác suất xấp xỉ 80% TP.HCM, 10% Hà Nội,
10% Đà Nẵng. Order giao theo thành phố khách. Khoảng 60% đơn thuộc nhóm 20%
khách loyal. Vì lấy mẫu hữu hạn, tỷ lệ đơn TP.HCM không luôn chính xác 80%.
Đo tỷ lệ thực tế bằng SQL group-by trong validator.

3/4 ID sản phẩm thuộc electronics; với 1.000 hoặc 2.000.000 sản phẩm,
tỷ lệ chính xác là 75%. Đây là phân bố **danh mục sản phẩm**. Khi đo skew của
join/group-by, còn phải quan sát phân bố khóa phía fact; validator cũng đo
cardinality của order_items.product_id và orders.customer_id.

## 3. Schema evolution thật trên file

Với smoke:

- Các order_id 1–6.000 tạo payment v1: không có trường failure_reason.
- Từ order_id 6.001 tạo payment v2: có trường string nullable failure_reason.
- Một lô được cắt ngay tại ranh giới; một file không lẫn hai schema vật lý.
- V2 chỉ có failure_reason khi status=failed. Các status khác nhận null.

Ranh giới là **thứ tự sinh/ghi nguồn theo order ID**, không phải thời gian giao
dịch lịch sử. Các file cùng nằm trong thư mục payments. Reader phải union schema
theo tên cột; validator dùng DuckDB `union_by_name=true`. Spark ở bước sau có
thể dùng mergeSchema hoặc schema đích tường minh và điền null cho file cũ.

## 4. Duplicate offline

Mục tiêu r=0,02 là `extra_copies / final_rows`. Sau N dòng sạch, số bản sao là
`floor(N*r/(1-r))`. Ví dụ với 980 dòng sạch thì có 20 bản sao, tổng 1.000 dòng.
Mỗi dòng sạch được thêm tối đa một bản sao. Chỉ created_ts tăng 1 giây, các trường
nghiệp vụ và order_item_id giữ nguyên. Tỷ lệ sai lệch tối đa khoảng một dòng do làm tròn.

Không ghi cột `is_duplicate` vào bảng nguồn để downstream khỏi dựa vào cờ đáp án.
Manifest ghi số liệu đối chiếu. Validator đếm lại `COUNT(*)-COUNT(DISTINCT id)`,
kiểm tra nội dung bản sao, khử trùng trong view để đối chiếu tổng tiền.
Nó không sửa file dirty; Bronze vẫn cần lưu nguồn nguyên trạng ở giai đoạn sau.

## 5. Late arrivals và duplicate streaming

`plan-stream` mô phỏng một phần clickstream mobile được thiết bị gửi muộn:

- Khoảng 15% sự kiện mobile có delay 300–600 giây.
- Clickstream còn lại delay 0–2 giây.
- Callback bình thường delay 0,1–2 giây; bản gửi lại thêm 2–10 giây.
- Khoảng 1,5% tổng callback cuối là bản sao; clickstream không chèn duplicate.

SQLite sắp lịch theo simulated_arrival_ts trên đĩa. Producer phát theo lịch
đến, không sắp lại theo event_ts. Vì vậy có out-of-order. Callback bản sao giữ
nguyên event_id, payment_id, result_code và payload. Producer idempotence chỉ
xử lý retry vận chuyển; hai lần gọi produce có chủ đích vẫn là hai Kafka records.

## 6. Hai chế độ thời gian

| Chế độ | Lệnh | Ý nghĩa |
|---|---|---|
| Historical replay | produce --clock historical --speed 60 | event_ts là lịch sử nguồn; 60 giây logic phát trong khoảng 1 giây thật |
| Nhanh không chờ | produce --clock historical --speed 0 | Giữ thứ tự đến, bỏ chờ; vẫn bị giới hạn max-rate |
| Wall-clock demo | produce --clock wall --speed 1 | Dịch event_ts bằng một offset chung sang hiện tại; đợi đủ lịch đến thật |

Historical replay phù hợp để thử event-time window và nối lại nguồn offline.
Không lấy `now - event_ts` của lịch sử năm 2025 rồi gọi đó là độ trễ 5 phút.
Độ trễ kế hoạch là `simulated_arrival_ts - event_ts` trong cùng đồng hồ mô phỏng.

Wall-clock mode giữ timestamp gốc trong `source_event_ts` và `source_callback_ts`,
đồng thời ghi replay_time_shift_ms. Dùng event_ts đã dịch cho demo Flink theo
đồng hồ hiện tại. Dùng source_event_ts khi dựng feature/label cùng lịch sử offline;
không trộn hai hệ thời gian. Scheduling/broker/network có thể tăng thêm độ trễ.
Wall-clock mode yêu cầu speed=1 để ý nghĩa 5–10 phút thực không bị thay đổi.

`out_of_order_relative_to_topic_max_event_ts` chỉ đo thứ tự lịch phát theo topic.
Nó không khẳng định watermark hay allowed lateness của Flink đã được cấu hình.
Flink, side output cho sự kiện quá muộn và dedup state là phần pipeline tiếp theo.

## 7. Nguồn trước Bronze

Local: source/offline/<table>/part-....parquet.
MinIO: source-department/synthetic/<run>/source/offline/....

Inventory, config và manifest là metadata; checkpoint SQLite không upload làm
bảng nghiệp vụ. Chỉ ghi _READY.json khi tất cả file nguồn đã được xác nhận.
Không dùng cùng prefix cho một bộ dữ liệu khác; uploader từ chối xung đột.

## 8. Lịch sử khách hàng (bản 1.1)

customer_changes có một ảnh INSERT cho mỗi khách và các ảnh UPDATE khi đổi segment.
customers là ảnh lúc đăng ký. Quy tắc lịch sử, SCD2 và profile ML nằm trong
[customer_history_and_ml.md](customer_history_and_ml.md).
