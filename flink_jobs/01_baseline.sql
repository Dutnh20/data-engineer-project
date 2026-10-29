-- BASELINE (chưa tối ưu): watermark không có độ trễ cho phép, không loại trùng.
-- Hậu quả dự kiến: event mobile đến muộn bị bỏ khỏi cửa sổ; callback gửi lại bị đếm hai lần.
SET 'execution.checkpointing.interval' = '5 s';
SET 'parallelism.default' = '1';
-- Phát watermark mỗi 1 ms và đọc từng lô nhỏ để độ trễ của event muộn thể hiện được khi replay nhanh từ Kafka
SET 'pipeline.auto-watermark-interval' = '1 ms';

CREATE TABLE clicks (
  event_id STRING, schema_version INT, session_id STRING, customer_id BIGINT, product_id BIGINT, order_id BIGINT,
  event_type STRING, page_type STRING, event_ts STRING, device_type STRING,
  ts AS TO_TIMESTAMP(REPLACE(SUBSTR(event_ts, 1, 19), 'T', ' ')),
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'kafka', 'topic' = 'edai.clickstream', 'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-baseline', 'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset', 'properties.max.poll.records' = '50', 'format' = 'json', 'json.ignore-parse-errors' = 'true');

CREATE TABLE callbacks (
  event_id STRING, schema_version INT, payment_id BIGINT, order_id BIGINT, event_ts STRING, callback_ts STRING,
  result_code STRING, provider STRING, amount BIGINT, failure_reason STRING,
  ts AS TO_TIMESTAMP(REPLACE(SUBSTR(event_ts, 1, 19), 'T', ' ')),
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'kafka', 'topic' = 'edai.payment_callbacks', 'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-baseline', 'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset', 'properties.max.poll.records' = '50', 'format' = 'json', 'json.ignore-parse-errors' = 'true');

CREATE TABLE click_windows_out (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), device_type STRING, event_type STRING, cnt BIGINT
) WITH ('connector' = 'filesystem', 'path' = 'file:///data/flink_out/baseline/click_windows', 'format' = 'csv');

CREATE TABLE callback_windows_out (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), result_code STRING, cnt BIGINT, total_amount BIGINT
) WITH ('connector' = 'filesystem', 'path' = 'file:///data/flink_out/baseline/callback_windows', 'format' = 'csv');

EXECUTE STATEMENT SET
BEGIN
  -- Cửa sổ tumbling 5 phút theo event time
  INSERT INTO click_windows_out
  SELECT window_start, window_end, device_type, event_type, COUNT(*) AS cnt
  FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '5' MINUTES))
  GROUP BY window_start, window_end, device_type, event_type;

  INSERT INTO callback_windows_out
  SELECT window_start, window_end, result_code, COUNT(*) AS cnt, SUM(amount) AS total_amount
  FROM TABLE(TUMBLE(TABLE callbacks, DESCRIPTOR(ts), INTERVAL '5' MINUTES))
  GROUP BY window_start, window_end, result_code;
END;
