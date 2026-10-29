-- 4 truy vấn đại diện. Mỗi truy vấn chạy EXPLAIN (ANALYZE, BUFFERS) để xem plan và thời gian thực thi.
\timing on
\echo '--- Q1: tra cứu một đơn theo order_id'
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM fact_orders WHERE order_id = (SELECT order_id FROM fact_orders ORDER BY order_id OFFSET 5000 LIMIT 1);

\echo '--- Q2: doanh thu theo thành phố của một khoảng ngày (đơn hoàn tất)'
EXPLAIN (ANALYZE, BUFFERS) SELECT shipping_city, SUM(total_amount) AS revenue, COUNT(*) AS orders
FROM fact_orders
WHERE status = 'completed' AND order_ts >= (SELECT MIN(order_ts) FROM fact_orders) + INTERVAL '30 days'
  AND order_ts < (SELECT MIN(order_ts) FROM fact_orders) + INTERVAL '37 days'
GROUP BY shipping_city ORDER BY revenue DESC;

\echo '--- Q3: đơn của một khách hàng (qua dim_customer)'
EXPLAIN (ANALYZE, BUFFERS) SELECT o.order_id, o.order_ts, o.total_amount
FROM dim_customer c JOIN fact_orders o ON o.customer_sk = c.customer_sk
WHERE c.customer_id = (SELECT customer_id FROM dim_customer ORDER BY customer_id OFFSET 500 LIMIT 1) AND c.is_current;

\echo '--- Q4: các dòng hàng của một sản phẩm'
EXPLAIN (ANALYZE, BUFFERS) SELECT COUNT(*), SUM(line_amount)
FROM fact_order_items WHERE product_id = (SELECT product_id FROM fact_order_items ORDER BY product_id OFFSET 20000 LIMIT 1);
