# Flink: xử lý streaming (baseline → tối ưu)

Flink SQL đọc hai topic Kafka do generator tạo: `edai.clickstream` (event muộn 5–10 phút ở thiết bị mobile) và `edai.payment_callbacks`
(callback gửi lại, ~1,5% trùng). Hai job so sánh trên cùng dữ liệu:

| File | Vai trò | Cấu hình |
|---|---|---|
| `flink_jobs/01_baseline.sql` | Baseline | `WATERMARK FOR ts AS ts` (không cho trễ), không loại trùng |
| `flink_jobs/02_optimized.sql` | Tối ưu | watermark trễ 15 phút, `ROW_NUMBER()` loại trùng theo `event_id`, state TTL 2 giờ |

Cả hai đọc ở chế độ bounded (`scan.bounded.mode = latest-offset`), nên job tự kết thúc và kết quả so sánh được.

## Các vấn đề và cách xử lý

| Vấn đề | Baseline gây ra | Tối ưu |
|---|---|---|
| **Late arrival** | Watermark = event time lớn nhất; event đến sau khi cửa sổ đã đóng bị **bỏ** khỏi kết quả | Cho phép trễ 15 phút (> 10 phút trễ tối đa), event muộn vẫn vào đúng cửa sổ. Đo bằng số event thiếu so với số event thật trong Kafka |
| **Duplicate callback** | Mỗi bản trùng được đếm/cộng tiền thêm một lần | `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts)`, giữ bản đầu |
| **State phình to** (vấn đề phát sinh khi loại trùng) | Không có state, nhưng dedup tối ưu cần nhớ mọi `event_id` | `table.exec.state.ttl = 2 h` giới hạn bộ nhớ; TTL phải lớn hơn khoảng cách tối đa giữa bản gốc và bản gửi lại (vài giây đến vài phút) |
| **Window processing** | — | Cửa sổ tumbling 5 phút theo event time (xem đoạn code bên dưới) |

### Đoạn code xử lý window

```sql
INSERT INTO click_windows_out
SELECT window_start, window_end, device_type, event_type, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE clicks_dedup, DESCRIPTOR(ts), INTERVAL '5' MINUTES))
GROUP BY window_start, window_end, device_type, event_type;
```

`TUMBLE` chia event theo `ts` (event time, parse từ `event_ts`) vào các cửa sổ 5 phút không chồng lấn. Cửa sổ chỉ được phát kết quả khi watermark vượt `window_end`.
Watermark của job tối ưu là `ts - 15 phút`, nên cửa sổ đóng muộn hơn 15 phút so với event mới nhất; đổi lại event muộn không bị mất. Với `callbacks_dedup`,
cùng cách này cho tổng số callback và tổng tiền theo `result_code` mỗi 5 phút.

## Chạy

Cần Kafka đã có dữ liệu (generator `plan-stream` + `produce`) và báo cáo DP4 để đối chiếu số event thật.

```powershell
docker compose -f compose.yaml -f compose.flink.yaml build jobmanager
docker compose -f compose.yaml -f compose.flink.yaml up -d --wait kafka jobmanager taskmanager
Remove-Item -Recurse -Force .\output\flink_out -ErrorAction SilentlyContinue
docker compose -f compose.yaml -f compose.flink.yaml exec jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/01_baseline.sql
docker compose -f compose.yaml -f compose.flink.yaml exec jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/02_optimized.sql
```

Mở http://localhost:8082 (Flink UI): tab **Running Jobs / Completed Jobs**, bấm job → **Watermarks**, **Task Metrics** và **Checkpoints**.
Chờ cả hai job `FINISHED` rồi so sánh:

```powershell
$run = Get-ChildItem .\output\stream -Recurse -Filter stream_report.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1
docker compose run --rm --no-deps --entrypoint python -v "${PWD}:/work" -w /work generator scripts/summarize_flink.py --report "$($run.FullName -replace '\\','/')"
```

Nếu máy có Python 3, chạy trực tiếp `python scripts/summarize_flink.py --report <đường dẫn>` cũng được.

## Bằng chứng cần chụp (đính vào tài liệu này)

1. Flink UI của job baseline và tối ưu: sơ đồ operator, tab Watermarks (độ lệch watermark), Checkpoints (hoàn tất).
2. Kết quả `summarize_flink.py`: baseline thiếu N event ở cửa sổ clickstream và đếm thừa M callback; tối ưu thiếu 0 và thừa 0.
3. Một cửa sổ cụ thể: tệp CSV baseline so với optimized cho cùng `window_start`.

Điền số thật vào bảng này sau khi chạy:

| Chỉ số | Baseline | Tối ưu |
|---|---|---|
| Event clickstream được đếm / thật | | |
| Callback được đếm / duy nhất | | |
| Tổng tiền callback | | |
| Thời gian job (Flink UI) | | |

## Phạm vi kiểm thử

Các file SQL chưa chạy được trên Flink thật trong môi trường soạn tài liệu (không có Docker). Điểm cần chú ý khi chạy lần đầu:
(1) ghi file dùng checkpoint nên cần `execution.checkpointing.interval`; (2) nếu `ROW_NUMBER` làm mất thuộc tính thời gian của `ts`, Flink báo lỗi khi
dùng `TUMBLE` trên `*_dedup`; khi đó dán lỗi để chuyển sang cách loại trùng bằng `GROUP BY event_id` trước cửa sổ.
Jar connector tải từ Maven Central lúc build image và chưa kiểm checksum.
