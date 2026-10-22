# Hướng dẫn Windows, VS Code và Docker

## Cách ưu tiên: chạy toàn bộ Python trong Docker

Các lệnh trong README dùng Docker nên không cần `py`, không cần kích hoạt venv,
không cài package vào Python do uv quản lý. Dockerfile chọn Python 3.12 và cài
requirements.lock trong image. Dữ liệu ở bind mount `output/` nên vẫn còn khi
container `run --rm` kết thúc.

Mở đúng thư mục dự án. `cd` nhận đường dẫn thư mục, không nhận đường dẫn file .py.
Ví dụ:

```powershell
cd D:\edai-lab\synthetic_generator
Get-ChildItem
docker compose config --services
docker compose build generator
```

Phải nhìn thấy compose.yaml, Dockerfile và thư mục edai_generator.

## Cổng của stack độc lập

| Từ Windows | Đích | Từ container generator |
|---|---|---|
| http://localhost:9010 | MinIO S3 API | http://minio:9000 |
| http://localhost:9011 | MinIO console | Không cần cho uploader |
| localhost:19093 | Kafka bootstrap | kafka:9092 |

Các cổng này được chọn khác stack nền móng trước (9000/9001/19092).
Trong container, localhost là chính container; do đó uploader/producer dùng tên
service. Compose đã truyền đúng các biến môi trường đó.

## Dùng lại nền móng đã chạy trước đó

Nếu thư mục nền móng nằm ở `D:\edai-foundation`, khởi động MinIO và Kafka ở đó:

```powershell
cd D:\edai-foundation
docker compose up -d minio
docker compose up -d --wait kafka
```

Sau đó quay về thư mục generator, dùng file addon và .env của nền móng:

```powershell
cd D:\edai-lab\synthetic_generator
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env build generator
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env run --rm generator offline --config configs/smoke.json --out /data/smoke
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env run --rm generator validate --out /data/smoke --checksums
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env run --rm generator upload --out /data/smoke
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env run --rm generator plan-stream --out /data/smoke --orders 2000
docker compose -f compose.addon.yaml --env-file D:\edai-foundation\.env run --rm generator produce --out /data/smoke --speed 60
```

Chọn cách standalone hoặc addon cho cùng một lần thực hành. Addon kết nối
network `edai-foundation_default`; nếu đã đổi tên project, đặt
`$env:FOUNDATION_NETWORK = "ten_network_thuc_te"` sau khi kiểm tra `docker network ls`.
Tài khoản MinIO của addon lấy từ .env nền móng, không dùng tài khoản demo của gói mới.

## Xem log và topic

Với stack standalone:

```powershell
docker compose ps -a
docker compose logs --tail 50 kafka
docker compose logs --tail 50 minio
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

`kafka-init` kết thúc với mã 0 là bình thường: nó chỉ chuẩn bị quyền ghi volume.
`generator` là tác vụ chạy rồi thoát; không cần luôn có trạng thái running.
Service kafka cần healthy trước khi phát sự kiện. Nếu upload báo kết nối chưa
sẵn sàng ngay sau lần khởi động đầu, xem log MinIO rồi chạy lại lệnh upload.

## Hiểu kết quả streaming

`plan_report.json` ghi số lượng được mô phỏng. Bằng chứng đã gửi Kafka nằm trong
`delivery_report.json`: `passed: true`, số ACK và hash của đúng plan.
Không suy ra đã gửi Kafka chỉ vì có file events.jsonl.

Chạy producer lại với cùng plan sẽ gửi lại các event ID. Đây là replay toàn bộ,
khác với 1,5% duplicate được chèn trong một lần chạy. Khi đối soát, dùng topic
prefix mới cho mỗi thí nghiệm, ví dụ:

```powershell
docker compose run --rm generator plan-stream --out /data/smoke --orders 8000 --topic-prefix edai.experiment2
docker compose run --rm generator produce --out /data/smoke --speed 60
```

8.000 order này đi qua ranh giới schema 6.000 nên callback có cả v1/v2.
Consumer sample dùng group mới và đọc earliest; nếu topic có nhiều lần replay,
mẫu sẽ gồm dữ liệu cũ. Trước khi so sánh đếm tổng, phải xác định phạm vi thí nghiệm.

## Dừng và chạy tiếp

`Ctrl+C` dừng lệnh đang chạy. Với offline generator, chạy lại **cùng config,
cùng output**, thêm `--resume`. Với producer, một lần phát bị dừng có thể đã có
message được ACK; lần phát lại có thể tạo thêm bản sao. Downstream phải xử lý
idempotent theo event_id.

```powershell
docker compose stop kafka minio
docker compose up -d minio
docker compose up -d --wait kafka
```

Named volumes giữ dữ liệu Kafka/MinIO qua các lần stop/start. File local ở
output/ cũng được giữ lại.

## Chạy Python trực tiếp nếu đã có Python 3.12

Chỉ dùng phần này khi `python --version` thật sự là Python 3.12.x. Nếu chưa có
môi trường này, dùng Docker ở trên là đủ.

```powershell
python --version
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements.lock
& .\.venv\Scripts\python.exe -m edai_generator offline --config configs/smoke.json --out output/smoke
& .\.venv\Scripts\python.exe -m edai_generator validate --out output/smoke --checksums
```

Đường dẫn .venv chỉ tồn tại sau khi lệnh tạo venv chạy thành công. Các SDK có
wheel theo Python/platform; không copy site-packages từ Python 3.13 sang 3.14.
Chạy producer trực tiếp trên Windows dùng bootstrap `localhost:19093` cho stack
standalone, hoặc `localhost:19092` cho foundation. Đặt AWS_ACCESS_KEY_ID và
AWS_SECRET_ACCESS_KEY bằng tài khoản MinIO khi upload trực tiếp.
