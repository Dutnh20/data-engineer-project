-- Index chọn theo mẫu truy vấn trong queries.sql; chạy rồi ANALYZE và đo lại.
CREATE INDEX IF NOT EXISTS ix_fact_orders_order_id      ON fact_orders (order_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_status_ts     ON fact_orders (status, order_ts) INCLUDE (shipping_city, total_amount);
CREATE INDEX IF NOT EXISTS ix_fact_orders_customer_sk   ON fact_orders (customer_sk);
CREATE INDEX IF NOT EXISTS ix_dim_customer_customer_id  ON dim_customer (customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_items_product_id     ON fact_order_items (product_id) INCLUDE (line_amount);
ANALYZE;
