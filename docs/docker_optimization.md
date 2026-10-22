# Docker multi-stage và bằng chứng kích thước

Dockerfile.baseline dùng Python đầy đủ. Dockerfile tối ưu tạo wheel ở builder,
sau đó chỉ cài wheel vào runtime Python slim. Hai image dùng cùng requirements,
source và entrypoint, chạy user không phải root.

Wheel được bind-mount trong bước build nên không lưu thư mục wheel vào image
runtime. .dockerignore loại dữ liệu output, credentials và file bằng chứng khỏi
build context. Các package runtime libstdc++/libgomp được cài rõ ràng.

Đo thực tế trong PowerShell:

```powershell
docker build -f Dockerfile.baseline -t edai-generator:baseline .
docker build -f Dockerfile -t edai-synthetic-generator:1.1.0 .
docker run --rm edai-generator:baseline --help
docker run --rm edai-synthetic-generator:1.1.0 --help
$before = [long](docker image inspect edai-generator:baseline --format '{{.Size}}')
$after = [long](docker image inspect edai-synthetic-generator:1.1.0 --format '{{.Size}}')
[pscustomobject]@{
    baseline_bytes = $before
    optimized_bytes = $after
    baseline_MiB = [math]::Round($before / 1MB, 2)
    optimized_MiB = [math]::Round($after / 1MB, 2)
    reduction_percent = [math]::Round(100 * ($before - $after) / $before, 2)
} | Export-Csv -Path evidence\image_sizes.csv -NoTypeInformation -Encoding UTF8
```

Chỉ đo sau khi cả hai build thành công. Size ở đây là kích thước image Docker
inspect báo, không phải kích thước tải nén từ registry. Lưu image ID/platform
cùng kết quả nếu cần tái hiện. Bản bàn giao chưa có Docker daemon để build/đo,
nên không cung cấp con số giảm kích thước chưa được kiểm chứng.

Các image phiên bản cụ thể được dùng cho local lab, không được giới thiệu là
phiên bản mới nhất hay baseline bảo mật cho production. MinIO dùng tag có trong
bộ nền móng trước để giữ nhất quán môi trường học.
