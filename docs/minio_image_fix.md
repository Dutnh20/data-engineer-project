# Sửa lỗi tải image MinIO: 401 UNAUTHORIZED

Cập nhật ngày 21/10/2026. Image MinIO cũ trên Quay không còn tải công khai.
Compose trong bản bàn giao đã được sửa để dùng:

```yaml
image: docker.io/pgsty/silo:RELEASE.2026-10-18T00-00-00Z
```

PGSTY Silo là bản fork cộng đồng của MinIO, độc lập với MinIO Inc. Bản này giữ
S3 API, các biến MINIO_ROOT_USER/MINIO_ROOT_PASSWORD, cổng và tham số khởi động
đang dùng trong lab. Service vẫn tên minio để các job tiếp tục truy cập
object storage theo cấu hình hiện có; endpoint storage là http://minio:9000.
Giao diện có thể hiển thị tên SILO.

Nếu đã giải nén bản trước, sửa dòng image trong service minio của compose.yaml,
lưu file rồi chạy PowerShell tại thư mục chứa Compose:

```powershell
cd D:\edai-lab\synthetic_generator
docker compose pull minio
docker compose up -d minio
docker compose ps minio
```

Mở http://localhost:9011, đăng nhập bằng tài khoản cấu hình trong .env; nếu dùng
mặc định của gói, tài khoản là labadmin / local_demo_change_this_2026.
Sau khi dịch vụ hoạt động:

```powershell
docker compose run --rm generator upload --out /data/smoke --prefix synthetic/smoke
```

Nếu container thoát, lấy log bằng `docker compose logs --tail 50 minio`.
Với compose.addon.yaml, image storage nằm trong stack nền móng; sửa service
minio ở Compose của nền móng thay vì thêm một storage mới vào addon.

Đã xác minh tag qua Docker Hub API, lấy được manifest và config image công khai
trên Docker Registry. Image cung cấp linux/amd64 và linux/arm64. Chưa chạy Docker
daemon/container hoặc thử lại upload S3 trong môi trường bàn giao.

Nguồn:
- [IBM: MinIO image bị gỡ khỏi Quay](https://www.ibm.com/support/pages/node/7289585)
- [Nhà phát hành PGSTY: hướng dẫn Docker và Compose](https://silo.pgsty.com/download/)
- [PGSTY: tương thích cấu hình và entrypoint](https://silo.pgsty.com/compatibility/server/)
