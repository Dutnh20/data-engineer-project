# Đặc tính dữ liệu sau khi sinh

Nguồn: `output/ml_history` (config `6edecded9bdb…`, generator 1.1.0). Tài liệu này được sinh tự động bằng `scripts/make_data_characteristics.py` từ `manifest.json` và `validation.json`, validator độc lập báo `passed = True`.

## Dung lượng

- Dung lượng Parquet thực tế trên đĩa: **16,746,269 byte (0.0167 GB)**.
- Mốc rubric 100 GB: **chưa đạt** ở lần chạy này (target_gb = None). Để đạt phải dùng `configs/full_100gb.json` (xem `docs/large_data.md`).

| Bảng | Số dòng |
|---|---:|
| `customers` | 20,000 |
| `customer_changes` | 21,500 |
| `products` | 10,000 |
| `orders` | 200,000 |
| `order_items` | 510,418 |
| `payments` | 225,533 |

Khoảng thời gian đơn hàng: 2024-12-31T19:00:00-05:00 → 2025-12-14T00:17:30-05:00 (347.2 ngày).

## Cột bị skew

| Cột | Giá trị | Số dòng | Tỉ lệ |
|---|---|---:|---:|
| `orders.shipping_city` | Ho Chi Minh | 159,533 | 79.77% |
| `orders.shipping_city` | Ha Noi | 19,678 | 9.84% |
| `orders.shipping_city` | Da Nang | 20,789 | 10.39% |
| `products.category` | home | 1,250 | 12.50% |
| `products.category` | electronics | 7,500 | 75.00% |
| `products.category` | fashion | 1,250 | 12.50% |

Skew thành phố làm các phép group/join theo `shipping_city` lệch; skew `order_items.product_id` (một số sản phẩm rất hot) được thấy ở benchmark Spark (`docs/spark_optimization.md`).

## Cardinality (số giá trị khác nhau / tổng số dòng)

| Cột | Giá trị khác nhau | Số dòng | Tỉ lệ |
|---|---:|---:|---:|
| `customers.customer_id` | 20,000 | 20,000 | 1.0000 |
| `customer_changes.change_id` | 21,500 | 21,500 | 1.0000 |
| `products.product_id` | 10,000 | 10,000 | 1.0000 |
| `orders.order_id` | 200,000 | 200,000 | 1.0000 |
| `order_items.order_item_id` | 500,210 | 510,418 | 0.9800 |
| `payments.payment_id` | 225,533 | 225,533 | 1.0000 |
| `orders.customer_id` | 19,879 | 200,000 | 0.0994 |
| `order_items.product_id` | 10,000 | 510,418 | 0.0196 |
| `payments.order_id` | 200,000 | 225,533 | 0.8868 |

Tỉ lệ gần 1 là cột high-cardinality (khóa chính). `order_items.order_item_id` < 1 vì có duplicate; `order_items.product_id` và `orders.customer_id` thấp vì mỗi giá trị lặp nhiều lần.

## Duplicate offline

- Khóa trùng: `order_item_id`, bản sao thay đổi `created_ts (+1 second)`.
- Dòng sạch 500,210, bản sao thừa 10,208, tổng 510,418; tỉ lệ **2.00%** (cấu hình 2.00%).
- Nếu không khử trùng, tổng tiền thô 8,170,786,576,000 VND so với 8,008,621,030,000 VND sau khử trùng: **phình thêm 162,165,546,000 VND**. Order value, not recognized revenue; cancelled/pending orders are included.

## Schema evolution

Bảng `payments`: đơn có `order_id` ≤ 100,000 dùng schema v1, từ 100,001 dùng v2 (source generation order, not historical event time). Phiên bản quan sát được: [1, 2].

Schema trước (v1):

| Cột | Kiểu | Cho phép NULL |
|---|---|---|
| `payment_id` | int64 | không |
| `order_id` | int64 | không |
| `attempt_no` | int64 | không |
| `payment_ts` | timestamp[us, tz=UTC] | không |
| `method` | string | không |
| `provider` | string | không |
| `amount` | int64 | không |
| `status` | string | không |

Schema sau (v2):

| Cột | Kiểu | Cho phép NULL |
|---|---|---|
| `payment_id` | int64 | không |
| `order_id` | int64 | không |
| `attempt_no` | int64 | không |
| `payment_ts` | timestamp[us, tz=UTC] | không |
| `method` | string | không |
| `provider` | string | không |
| `amount` | int64 | không |
| `status` | string | không |
| `failure_reason` | string | có |

## Vấn đề streaming (Kafka)

- Clickstream mobile đến muộn: 15.00% event mobile, trễ 300–600 giây.
- Callback thanh toán gửi lại: 1.50%.
- Số event thật sự phát vào Kafka và số bản trùng: `output/<run>/stream/plan_report.json` và `delivery_report.json`; kết quả xử lý ở `docs/pipeline_05_stream.md`.
