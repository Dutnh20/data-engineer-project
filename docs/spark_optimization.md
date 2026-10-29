# Tối ưu Spark trên dữ liệu skew (baseline → tối ưu)

Công cụ: `spark_jobs/bench_offline.py`. Mỗi lần chạy là một Spark app riêng (`EDAI-BENCH-<mode>`), nên có thể so sánh từng app trên
Spark History Server (http://localhost:18081). Kết quả số liệu nằm ở `output\bench\<gold_version>\bench_results.jsonl`.

## Workload

`fact_order_items` JOIN `fact_orders` JOIN `dim_product`, GROUP BY `product_id, category`, tính doanh thu và số dòng.
`product_id` bị skew (một số sản phẩm rất hot) nên join và group theo khóa này làm một vài task nặng hơn hẳn phần còn lại.

## Các mức (mỗi mức thêm một thay đổi so với mức trước, trừ salting)

| Mode | Cấu hình | Giả thuyết |
|---|---|---|
| `baseline` | AQE tắt, không broadcast, 200 shuffle partition | Điểm xuất phát: nhiều partition nhỏ, partition của khóa hot rất lớn |
| `aqe` | AQE bật | Gộp partition nhỏ; giảm overhead lập lịch |
| `aqe_skew` | AQE + skew join (ngưỡng hạ xuống để thấy tác dụng ở quy mô lab) | Tách partition lệch thành nhiều task |
| `broadcast` | AQE + broadcast `dim_product` | Bỏ shuffle phía bảng lớn khi join theo `product_id` |
| `salting` | Cấu hình baseline + muối khóa hot | Chia đều khóa hot thủ công, không phụ thuộc AQE |

Mọi mode phải cho **cùng checksum** (`revenue_checksum`, `lines_checksum`, `result_rows`); nếu khác nhau là bản tối ưu sai.

## Chạy

Cần Gold của bộ lớn. Dựng bộ `ml_history` qua Airflow bằng cách Trigger DP1 và DP2 với config
`{"source_prefix":"synthetic/ml_history","bronze_version":"ml_v1","silver_version":"ml_v1","gold_version":"ml_gold_v1"}`
(sau khi `generator offline/validate/upload` bộ `ml_history`, xem `docs/large_data.md`), rồi:

```powershell
$c = "docker compose -f compose.yaml -f compose.spark.yaml -f compose.sparkfix.yaml"
docker compose up -d minio
foreach ($m in "baseline","aqe","aqe_skew","broadcast","salting") {
  Invoke-Expression "$c run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-job --master local[2] --driver-memory 2g /opt/app/spark_jobs/bench_offline.py --mode $m --gold-version ml_gold_v1 --runs 2 --out /data/bench/ml_gold_v1"
}
Get-Content .\output\bench\ml_gold_v1\bench_results.jsonl | ForEach-Object { $_ | ConvertFrom-Json } |
  Select-Object mode, seconds_best, shuffle_read_bytes, memory_spilled_bytes, max_task_to_median_ratio, result_rows, revenue_checksum | Format-Table
```

`--runs 2` đo hai lần và lấy lần nhanh nhất để giảm nhiễu. Có thể thử nhanh trên bộ nhỏ bằng `--gold-version gold_v2`.

## Đọc Spark UI (chụp ảnh cho từng mode)

Mở http://localhost:18081, bấm app `EDAI-BENCH-<mode>`:
1. **SQL / DataFrame**: xem plan. `baseline` dùng `SortMergeJoin`; `broadcast` dùng `BroadcastHashJoin`; mode AQE có nhãn `AQEShuffleRead` (gộp partition) hoặc `skew`.
2. **Stages** → bấm stage có shuffle read: bảng *Summary Metrics for tasks* cho `Duration` Median vs Max. Tỉ lệ Max/Median lớn là dấu hiệu skew; đây chính là cột `max_task_to_median_ratio`.
3. Cột **Shuffle Read** và **Spill** của stage; số task của stage (200 task nhỏ so với ít task đã được gộp).

## Bảng kết quả (điền từ lần chạy thật, không dùng số mẫu)

| Mode | Thời gian (s) | Shuffle read | Spill | Max/Median task | Ghi chú sau khi xem UI |
|---|---|---|---|---|---|
| baseline | | | | | |
| aqe | | | | | |
| aqe_skew | | | | | |
| broadcast | | | | | |
| salting | | | | | |

Cách viết giải thích cho rubric: "Do `product_id` skew, Spark UI của stage X cho thấy task chậm nhất gấp N lần median; tôi thử A, B, C;
thời gian giảm từ T0 xuống T1 (−P%), shuffle read giảm từ S0 xuống S1".

## Lưu ý trung thực khi diễn giải

- Chạy `local[2]` nên chỉ 2 task song song; cải thiện do skew sẽ nhỏ hơn trên cluster nhiều core. Nếu `baseline` chậm chủ yếu vì 200 partition nhỏ,
  hãy ghi rõ mức cải thiện đến từ gộp partition (AQE), không phải từ skew.
- Thử trên dữ liệu skew tổng hợp nhỏ (180.000 dòng, local) cho thấy cả 5 mode cùng checksum, `broadcast` giảm shuffle mạnh nhất và baseline có
  Max/Median ≈ 18; số này chỉ chứng minh harness đúng, **không** dùng làm kết quả của bạn.
- Tích hợp pipeline (rubric 2 điểm): phần cấu hình tối ưu được chọn sau benchmark (AQE, số partition) cần đưa vào DP2/DP3 qua `--shuffle-partitions`
  và biến Airflow; ghi rõ bước này trong tài liệu sau khi có số liệu.
