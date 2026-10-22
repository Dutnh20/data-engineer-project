# Domain và schema

## Phạm vi

Một cửa hàng mô phỏng bán hàng online. Nguồn dữ liệu 100% synthetic, không có
thông tin khách hàng thật. Tiền là số nguyên VND. Timestamp timezone-aware UTC.
Các đặc điểm dữ liệu được chủ động tạo để học Data Engineering.

## Bảng offline

| Bảng | Grain / khóa | Cột và ý nghĩa |
|---|---|---|
| customers | Một khách hàng; PK customer_id | customer_name: tên giả; signup_ts: ngày đăng ký; city: thành phố; segment: loyal/standard; device_type: mobile/desktop |
| customer_changes | Một ảnh chụp ban đầu hoặc lần đổi segment; PK change_id; FK customer_id | customer_name, city, segment, device_type, change_type, effective_ts, source_updated_ts |
| products | Một sản phẩm; PK product_id | product_name, category, brand: thông tin danh mục giả; base_price: giá VND; is_active; created_ts; description: mô tả phục vụ thực hành truy xuất |
| orders | Một đơn; PK order_id; FK customer_id | order_ts, status, shipping_city, coupon_code, total_amount |
| order_items | Một dòng sản phẩm trong đơn trước khi chèn lỗi; business PK order_item_id; FK order_id/product_id | quantity, unit_price, discount_amount, line_amount, created_ts. Khi chèn duplicate, business PK bị lặp có chủ đích |
| payments | Một lần thử thanh toán; PK payment_id; FK order_id | attempt_no, payment_ts, method, provider, amount, status; v2 có failure_reason nullable |

ID là int64 dương. Customer/product/order IDs liên tục từ 1. Item ID bằng
`order_id*10 + position` (position 1–4); payment ID bằng `order_id*10 + attempt_no`
(attempt 1–2). Vì vậy item/payment IDs có khoảng trống hợp lệ; không yêu cầu ID
liên tục cho hai bảng này.

Mỗi order có 1–4 sản phẩm khác nhau, số lượng 1–3. Có coupon thì giảm 10% giá trị
dòng hàng. `line_amount = quantity*unit_price - discount_amount`.
`total_amount` bằng tổng các dòng **sau khử trùng**. Chưa mô phỏng thuế/phí giao hàng.

Đơn completed có một lần thanh toán thành công cuối cùng; có thể có lần thất bại
trước đó. Đơn pending có payment pending. Đơn cancelled có payment failed.
Đăng ký khách hàng và tạo sản phẩm luôn trước các đơn. Payment sau thời điểm order.
Đây là các giả định mô phỏng, không phải mô tả mọi quy trình thương mại điện tử.

## Sự kiện streaming

| Topic mặc định | Grain | Payload |
|---|---|---|
| edai.clickstream | Một hành động trên website/app; event_id duy nhất | schema_version, session_id, customer_id, product_id, order_id nullable, event_type, page_type, event_ts, device_type |
| edai.payment_callbacks | Một thông báo cho một payment attempt; có thể được giao nhiều lần | event_id, schema_version, payment_id, order_id, event_ts, callback_ts, result_code, provider, amount; v2 có failure_reason |

`order_placed` là thời điểm tạo đơn, chưa có nghĩa thanh toán đã thành công.
Session mua hàng có view/cart trước khi đặt. Có thêm session chỉ xem và không
đặt hàng. Callback tham chiếu các payment thật trong nguồn offline vừa sinh.
Một record JSON bị gửi lại có cùng event_id và cùng nội dung nghiệp vụ.

## Có thể nối sang bài ML nào?

Có thể lập snapshot đặc trưng và nhãn khách hàng đặt đơn trong 7 ngày tiếp theo.
Nguồn có customer_id, lịch sử order và sự kiện view/cart. Không dùng trạng thái
tương lai làm đặc trưng; chỉ tạo nhãn khi đã quan sát đủ cửa sổ 7 ngày. Chia tập
theo thời gian, xử lý cả khoảng nhãn chồng lấn ở ranh giới train/validation/test.

Generator hiện tạo khác biệt tần suất mua theo phân nhóm loyal/standard ban đầu và có funnel cơ
bản. Nó chưa mô phỏng đầy đủ sở thích, mùa vụ, phản ứng khuyến mãi hoặc churn.
Kết quả ML trên bộ này đánh giá một thế giới mô phỏng, không chứng minh hiệu quả
với khách hàng thật. Profile smoke chỉ phủ vài giờ, nên không đủ lịch sử cho
bài nhãn 7 ngày. Profile ml_history trải khoảng 347 ngày và chưa quay vòng. Xem customer_history_and_ml.md để dựng nhãn/feature theo thời gian. Profile full_100gb phục vụ benchmark khối lượng và dùng chu kỳ lịch sử tối đa 180 ngày.
