-- Nạp CSV (xuất bởi export_warehouse.py) vào PostgreSQL. Chạy sau schema.sql.
\copy "dim_customer" FROM '/csv/dim_customer/dim_customer.csv' WITH (FORMAT csv, HEADER true)
\copy "dim_product" FROM '/csv/dim_product/dim_product.csv' WITH (FORMAT csv, HEADER true)
\copy "dim_date" FROM '/csv/dim_date/dim_date.csv' WITH (FORMAT csv, HEADER true)
\copy "fact_orders" FROM '/csv/fact_orders/fact_orders.csv' WITH (FORMAT csv, HEADER true)
\copy "fact_order_items" FROM '/csv/fact_order_items/fact_order_items.csv' WITH (FORMAT csv, HEADER true)
ANALYZE;
