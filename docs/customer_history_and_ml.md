# Nguồn lịch sử khách hàng và bộ thực hành ML

## Customer changes là nguồn cho SCD2

`customers` lưu một dòng cho mỗi khách hàng tại thời điểm đăng ký.
`customer_changes` lưu một dòng cho mỗi ảnh chụp ban đầu hoặc lần đổi segment.
Đây là dữ liệu nguồn, chưa phải bảng dimension đã xử lý bằng Spark.

| Cột | Ý nghĩa |
|---|---|
| `change_id` | ID duy nhất, ổn định của lần thay đổi |
| `customer_id` | Khóa tham chiếu tới customers |
| `customer_name`, `city`, `segment`, `device_type` | Ảnh đầy đủ của khách tại phiên bản đó |
| `change_type` | INSERT cho ảnh đầu, UPDATE cho lần đổi segment |
| `effective_ts` | Thời điểm thay đổi có hiệu lực |
| `source_updated_ts` | Thời điểm nguồn ghi nhận thay đổi |

Mỗi khách có một INSERT. Với N khách và r=`customer_change_fraction`, có đúng
`floor(N*r)` khách được chọn đều theo ID để đổi segment một lần. Một nửa trong
số được chọn đổi lần nữa. Với smoke: 2.000 INSERT + 100 UPDATE đầu + 50 UPDATE sau.

Thời điểm UPDATE đầu và sau lần lượt ở 25% và 60% khoảng thời gian được phủ bởi
số đơn tối thiểu trong config, trước lần quay vòng thời gian đầu tiên.
Cách này giúp smoke cũng có lịch sử thay đổi và giữ kết quả resume ổn định.
Mốc thực tế nằm trong `manifest.json.customer_history`.

Segment đổi qua lại giữa loyal và standard. Thành phố và device không đổi.
Việc chọn khách đặt đơn dùng phân nhóm ID ban đầu, **không mô phỏng tác động nhân
quả của thay đổi segment lên hành vi mua hàng**.

Khi xử lý Spark, sắp theo customer_id và effective_ts, dùng LEAD để tạo
`valid_to_ts`; phiên bản cuối có `is_current=true`. Khi nối với sự kiện tại t:

```sql
event.customer_id = dim.customer_id
AND event.event_ts >= dim.valid_from_ts
AND (event.event_ts < dim.valid_to_ts OR dim.valid_to_ts IS NULL)
```

Khoảng hiệu lực đóng bên trái, mở bên phải. Không nối mọi đơn lịch sử với
segment hiện tại vì sẽ đưa thông tin tương lai vào dữ liệu cũ. Khi mô phỏng
thông tin hệ thống thực sự biết ở thời điểm dự đoán, còn phải kiểm tra
`source_updated_ts <= prediction_ts` và chính sách xử lý cập nhật tới muộn.

## Vì sao có profile ML riêng?

`history_days` chỉ đặt chiều dài chu kỳ; nó không tự làm ít đơn trải đều cả chu kỳ.
Ví dụ 12.000 đơn cách nhau 1 giây chỉ phủ 11.999 giây. Không thể dùng bộ đó để
đánh giá feature 90 ngày hoặc nhãn mua trong 7 ngày.

`ml_history.json` sinh 200.000 đơn cách nhau 150 giây, phủ khoảng 347,22 ngày và
chưa quay vòng trong chu kỳ 365 ngày. Có 20.000 khách, 10.000 sản phẩm.
Đây là profile để luyện xử lý dữ liệu và huấn luyện, không phải dữ liệu thực chứng
minh mô hình có ích cho một doanh nghiệp thật.

Khi dựng dataset dự đoán mua hàng 7 ngày ở bước sau:

1. Chốt các trạng thái đơn được tính là mua, ví dụ completed.
2. Lấy feature chỉ từ dữ liệu có trước thời điểm dự đoán t.
3. Tạo nhãn từ đơn trong khoảng tương lai đã định nghĩa rõ, ví dụ `(t, t+7 ngày]`.
4. Không gán 0 cho các ngày cuối chưa có đủ 7 ngày quan sát nhãn.
5. Có thể dành 90 ngày đầu làm thời gian tích lũy feature, hoặc dùng giá trị thiếu
   và cờ khách mới theo thiết kế. Không tự động loại mọi khách chưa có lịch sử.
6. Chia train/validation/test theo thời gian. Chỉ dùng nhãn đã quan sát đầy đủ
   tại thời điểm huấn luyện; chừa khoảng 7 ngày ở ranh giới nếu cách chia cần nó.
7. Muốn kết hợp streaming features thì phải lập lịch/lưu snapshot cho toàn bộ
   khoảng lịch sử tương ứng, không chỉ các sự kiện mẫu đầu bộ dữ liệu.

Code sinh dữ liệu chưa tính feature, label hoặc huấn luyện model. Không dùng
`order_id`, thứ tự sinh hoặc thông tin chèn lỗi làm feature hành vi khách hàng.

## Tính dung lượng và phiên bản

Bản 1.1 tính tổng byte vật lý của **sáu bảng nguồn**: năm bảng chính cộng
customer_changes. Manifest khai báo cách tính này. Stream JSONL, báo cáo và
bản sao nguồn trên MinIO không được cộng thêm. Những file nhỏ về lịch sử khách
không thay thế việc sinh đủ >=100.000.000.000 byte khi chạy profile 100GB.

Fingerprint bao gồm phiên bản 1.1.0 và config. Không resume output của bản 1.0.
