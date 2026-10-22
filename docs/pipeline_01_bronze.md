# Pipeline 1: đưa dữ liệu nguồn vào Bronze

Bạn đã sinh dữ liệu bằng generator và mở được giao diện MinIO/Silo. Bước này
đóng gói các thao tác ingest thành một chương trình chạy được, có kiểm tra và
báo cáo. Tên pipeline là **DP1: source-department → bronze**.

Mã chính: `pipelines/dp1_source_to_bronze.py`. Chạy bằng Python trong container
`generator` hiện có; thư viện `boto3` và `pyarrow` đã nằm trong requirements.
Spark sẽ dùng ở bước xử lý Silver/Gold. Airflow sẽ điều phối sau khi job độc lập
chạy đúng; DP1 hiện tại chưa có lịch tự chạy.

## 1. Pipeline làm những gì?

| Bước | Thực hiện | Mục đích |
|---|---|---|
| 1 | Đọc `_READY.json` của nguồn | Chỉ nhận dataset đã upload hoàn tất |
| 2 | Kiểm tra config, manifest, file index và fingerprint | Xác nhận đang đọc đúng một phiên bản nguồn |
| 3 | Kiểm tra đủ sáu bảng, số file, byte và tổng dòng khai báo | Phát hiện nguồn thiếu hoặc metadata không nhất quán |
| 4 | Copy từng file sang Bronze bằng S3 API | Giữ nguyên dữ liệu và schema vật lý |
| 5 | Đọc lại từng file Bronze: SHA256, số dòng, schema Parquet | Kiểm chứng nội dung thực, không chỉ tin metadata |
| 6 | Ghi ledger và báo cáo, sau cùng ghi `_SUCCESS.json` | Cho pipeline tiếp theo biết snapshot nào đã được xác nhận |

**Chưa khử trùng ở bước này.** Các bản sao trong `order_items`, phân bố lệch,
và hai schema của `payments` được giữ nguyên để xử lý và đo hiệu quả ở Silver.
Pipeline không cộng thêm các bản sao Bronze vào điều kiện nguồn tối thiểu 100GB.

## 2. Cập nhật code vào project đang dùng

Giải nén gói ZIP mới vào một thư mục tạm. Trong thư mục gốc của project cũ,
thêm nguyên thư mục `pipelines/` và file `tests/test_bronze_pipeline.py`.
Thêm tài liệu này vào `docs/`. Trong cả `Dockerfile` và `Dockerfile.baseline`,
thêm dòng dưới ngay sau dòng COPY `edai_generator`:

```dockerfile
COPY --chown=generator:generator pipelines /app/pipelines
```

Nếu bạn đang dùng nguyên gói trước đó, có thể chép đè các file code/Dockerfile
tương ứng từ ZIP mới. Giữ `.env`, thư mục `output/`, cấu hình Compose và volume
storage hiện có. Code DP1 không cần tạo lại generator data.

Mở đúng thư mục chứa `compose.yaml` trong PowerShell:

```powershell
cd D:\edai-lab\synthetic_generator
Test-Path .\compose.yaml
Test-Path .\pipelines\dp1_source_to_bronze.py
docker compose build generator
docker compose up -d minio
```

Thay đường dẫn Windows nếu bạn đặt project ở chỗ khác. Cả hai `Test-Path`
phải trả về `True`. `cd` trỏ đến thư mục, không trỏ đến file Python.

Nếu bạn dùng `compose.addon.yaml` với stack nền móng đã có, giữ cách gọi
`docker compose -f compose.addon.yaml ...` và credentials hiện có; storage phải
đang chạy ở stack nền móng. Các lệnh dưới giả định stack độc lập `compose.yaml`.

## 3. Kiểm tra nguồn trước khi chạy

Trong UI `http://localhost:9011`, vào bucket `source-department`, mở
`synthetic/smoke/`. Cần có các object sau:

| Object hoặc prefix | Nội dung |
|---|---|
| `_READY.json` | Tín hiệu nguồn đã upload hoàn tất |
| `metadata/manifest.json` | Tổng số dòng, file, byte và phiên bản generator |
| `metadata/config.json` | Cấu hình sinh dữ liệu |
| `metadata/file_index.jsonl` | Danh sách file, SHA256, bảng, schema version |
| `source/offline/` | Sáu thư mục bảng chứa file Parquet |

Nếu mới tạo bucket rỗng hoặc kéo thả vài file qua UI, nguồn chưa đủ hợp đồng
đầu vào. Hãy chạy uploader có sẵn để đưa cả metadata và marker lên:

```powershell
docker compose run --rm generator upload --out /data/smoke --prefix synthetic/smoke
```

Lệnh này giả định `output\smoke\` đã được generator hoàn tất. Nếu `_READY.json`
đã tồn tại và nguồn đầy đủ, chuyển sang bước tiếp theo. Không tự viết marker
chỉ để vượt qua kiểm tra thiếu dữ liệu.

## 4. Chạy DP1 lần đầu

Dán nguyên dòng vào PowerShell:

```powershell
docker compose run --rm --entrypoint python generator -m pipelines.dp1_source_to_bronze --source-prefix synthetic/smoke --dataset-version smoke_v1 --out /data/bronze/smoke_v1
```

| Tham số | Ý nghĩa |
|---|---|
| `--source-prefix synthetic/smoke` | Nguồn trong bucket `source-department` |
| `--dataset-version smoke_v1` | Prefix đầu ra trong bucket `bronze` |
| `--out /data/bronze/smoke_v1` | Lưu báo cáo vào `output\bronze\smoke_v1\` trên Windows |
| `--entrypoint python` | Dùng Python để chạy module pipeline trong image hiện có |

Pipeline tự tạo bucket `bronze` nếu chưa có. Nếu bucket đã tồn tại, pipeline
dùng lại. Credentials lấy từ environment trong Compose, giống lệnh upload.
Trong container, endpoint là `http://minio:9000`; cổng UI `9011` không phải S3 API.

Với đúng profile `configs/smoke.json` của generator 1.1.0, đầu ra thành công
có các giá trị sau (run ID và thời gian sẽ khác):

```json
{
  "passed": true,
  "copied_files": 39,
  "skipped_files": 0,
  "verified_files": 39,
  "verified_bytes": 981634
}
```

Đây là số kỳ vọng của profile mẫu, không phải báo cáo đã chạy trên máy bạn.
Nếu bạn đã thay đổi config, đọc tổng file/byte trong manifest của chính bộ đó.
Lỗi trả về exit code khác 0; không được coi log `copied_files` riêng lẻ là đã xong.

## 5. Mở UI và kiểm tra đầu ra

Vào `bronze` → `smoke_v1` → `source_files`. Phải thấy sáu thư mục:

| Bảng | Số dòng profile smoke |
|---|---:|
| `customers` | 2.000 |
| `products` | 1.000 |
| `orders` | 12.000 |
| `order_items` | 30.591, gồm cả dòng trùng |
| `payments` | 13.523 |
| `customer_changes` | 2.150 |

Ở `bronze/smoke_v1/`, kiểm tra thêm:

- `_DATASET.json`: gắn phiên bản này với đúng nguồn, tránh ghi đè nhầm dataset.
- `_runs/<run_id>/ingest_ledger.jsonl`: mỗi dòng là một file đã xác nhận, có
  đường dẫn nguồn/đích, SHA256, số dòng và thời gian ingest.
- `_runs/<run_id>/ingestion_report.json`: báo cáo tổng hợp lần chạy.
- `_runs/<run_id>/source_...`: bản chụp metadata nguồn phục vụ truy vết.
- `_SUCCESS.json`: marker được ghi cuối cùng, trỏ đến báo cáo và ledger thành công.

Bucket chỉ có file Parquet mà thiếu `_SUCCESS.json` có thể là một lần chạy
đang dở. Pipeline Silver sau này cần kiểm tra marker trước khi đọc.

Trên Windows, báo cáo cũng được lưu ở
`output\bronze\smoke_v1\<run_id>\ingestion_report.json`.

Mở báo cáo mới nhất bằng PowerShell:

```powershell
$bronzeReport = Get-ChildItem .\output\bronze\smoke_v1 -Filter ingestion_report.json -Recurse | Sort-Object LastWriteTime -Descending | Select-Object -First 1
Get-Content $bronzeReport.FullName
```

## 6. Chạy lại để kiểm tra không nhân đôi dữ liệu

Chạy lại đúng lệnh ở bước 4. Nếu không có file thay đổi, kỳ vọng:

```json
{
  "passed": true,
  "copied_files": 0,
  "skipped_files": 39,
  "verified_files": 39
}
```

Pipeline dùng lại đường dẫn file cố định và **vẫn đọc lại byte để kiểm chứng**.
Chỉ thư mục báo cáo `_runs/` có thêm một lần chạy; `source_files/` giữ nguyên số
file và số dòng. Đây là tính idempotent: chạy lại không nhân đôi dữ liệu nghiệp vụ.

Nếu tiến trình bị ngắt, chạy lại cùng lệnh: file hợp lệ đã copy được kiểm tra
lại, file thiếu được copy tiếp. Nếu phát hiện file hỏng hoặc thuộc nguồn khác,
pipeline dừng để bạn kiểm tra, không âm thầm ghi đè.

Mỗi `dataset_version` chỉ chạy một job tại một thời điểm. Mẫu này chưa có
distributed lock; khi thêm Airflow, đặt `max_active_runs=1` cho DAG ingest.
Giữ nguồn bất biến sau `_READY.json`; bộ dữ liệu mới dùng prefix và version mới.

Nếu từng có một lần thành công rồi lần chạy lại thất bại, marker cũ vẫn ghi
`run_id` của lần thành công trước. Đối chiếu đúng run ID và báo cáo lần hiện tại.
Không sửa tay object thuộc snapshot đã công bố.

## 7. Đọc code để tự viết pipeline tiếp theo

Đọc `pipelines/dp1_source_to_bronze.py` theo thứ tự sau:

1. `main()`: đọc tham số terminal, tạo kết nối S3 và trả exit code.
2. `run_pipeline()`: nối các bước theo thứ tự, tạo run ID, xử lý báo cáo lỗi.
3. `load_source_contract()`: kiểm tra hợp đồng dữ liệu đầu vào.
4. `bind_dataset()`: khóa danh tính nguồn cho phiên bản đầu ra bằng conditional PUT.
5. Vòng `for entry in entries`: copy hoặc verify-skip từng file.
6. `verify_parquet()`: đọc lại file, kiểm SHA256, số dòng và schema vật lý.
7. Phần cuối `run_pipeline()`: kiểm nguồn không đổi, lưu ledger và ghi marker cuối.

S3 copy dùng server-side copy. Để kiểm chứng nội dung, Python sau đó đọc từng
shard vào một file tạm, hash từng khối 8 MiB, đọc footer Parquet rồi xóa file tạm.
Không nạp cả dataset 100GB vào RAM. Đĩa tạm cần đủ chỗ cho **một shard lớn nhất**;
metadata tối đa 64 MiB mỗi file. Bộ 100GB sẽ phát sinh đọc lại 100GB qua S3 ở mỗi
lần xác minh, kể cả rerun. Cần tính thời gian và dung lượng đĩa trước khi tăng quy mô.

Code giữ audit ở file riêng, không thêm cột vào file nguồn. Nếu rubric yêu cầu
cột kỹ thuật trong bảng Bronze/Iceberg, thêm lớp bảng đó sau raw Bronze này;
không được coi thư mục Parquet hiện tại là một bảng Iceberg hoàn chỉnh.

## 8. Xử lý các lỗi thường gặp

| Lỗi | Kiểm tra/cách xử lý |
|---|---|
| `No module named pipelines` | Chép đủ thư mục, thêm COPY rồi build lại `generator` |
| `no such service: generator` | Chạy ở đúng thư mục chứa Compose của gói này |
| `Missing ... _READY.json` | Kiểm tra prefix; chạy uploader cho bộ nguồn đã hoàn tất |
| Connection refused / endpoint unreachable | `docker compose ps`; bật service `minio`; dùng endpoint container đúng |
| `AccessDenied` / signature error | Credentials của generator phải trùng storage trong `.env` |
| `different source` | Dùng version đầu ra mới cho bộ nguồn khác; giữ phiên bản cũ |
| SHA256 / schema / row count mismatch | Xem file trong báo cáo, kiểm tra source/config/index; không bỏ qua validation |
| `untracked objects` | Có object ngoài inventory trong `source_files`; kiểm tra trước khi sửa, hoặc chọn version mới |
| No space left on device | Kiểm tra đĩa Docker Desktop, storage volume và chỗ cho một shard tạm |

Không dùng `docker compose down -v` để xử lý lỗi job; lệnh đó có thể xóa volume
dữ liệu của lab. Nếu nguồn đúng và chỉ lỗi mạng tạm thời, chạy lại cùng lệnh.

## 9. Tiêu chí hoàn thành và bước tiếp theo

DP1 xong khi báo cáo `passed=true`, đủ sáu bảng, `_SUCCESS.json` trỏ đúng lần
chạy và rerun có `copied_files=0` mà vẫn kiểm chứng toàn bộ file. Chụp UI và lưu
báo cáo/ledger làm bằng chứng cho bài lab.

Tiếp theo là **DP2 Bronze → Silver bằng Spark**: thống nhất schema payments,
khử trùng order_items theo ID và quy tắc thời gian, kiểm tra chất lượng/FK,
chuẩn hóa dữ liệu, xây lịch sử khách hàng SCD2 và ghi bảng Silver. Sau khi job
đơn lẻ chạy đúng mới nối DP1 → DP2 bằng Airflow và thêm các bước Gold/warehouse.

Code và hướng dẫn bước này đã có tại [pipeline_02_silver.md](pipeline_02_silver.md).

## 10. Phạm vi kiểm thử của bản bàn giao

29 test tự động đã chạy thành công: 16 test generator hiện có và 13 test DP1.
DP1 có kiểm thử copy giữ nguyên byte, rerun, resume sau ngắt, thiếu marker/file,
inventory bị sửa, xung đột phiên bản, dữ liệu hỏng dù metadata đúng, thay đổi
nguồn khi copy, sai số dòng/schema và file ngoài inventory.

Transport S3 trong các test này là fake có điều kiện ETag/conditional PUT.
Đã thử khởi động binary Silo để kiểm thử tích hợp, nhưng môi trường bàn giao
chặn truy vấn giao diện mạng (`netlinkrib: operation not permitted`), server
không khởi động được. Vì vậy chưa xác nhận live S3/Docker Desktop hay tải 100GB.
Bạn dùng bước 4–6 để xác nhận thực tế trên storage đang chạy của mình.

Chạy lại test trong image khi cần sửa code:

```powershell
docker compose run --rm --entrypoint python generator -m unittest discover -s tests -v
```

Tài liệu API tham chiếu:

- [Boto3 managed S3 copy](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/copy.html)
- [Boto3 get_object và điều kiện IfMatch](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/get_object.html)
- [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)
