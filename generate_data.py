"""Điểm bắt đầu: sinh Parquet -> kiểm tra -> lập lịch sự kiện streaming.

Chạy bằng Python 3.12 trong môi trường đã cài requirements.lock:
    python generate_data.py --config configs/smoke.json --out output/smoke

Hoặc dùng Docker như README. Script không tự gửi Kafka hay upload MinIO.
"""

import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).parent / "configs/smoke.json")
    parser.add_argument("--out", type=Path, default=Path("output/smoke"))
    parser.add_argument("--resume", action="store_true",
                        help="Tiếp tục cùng thư mục, cùng cấu hình sau khi bị dừng")
    parser.add_argument("--stream-orders", type=int, default=2000,
                        help="Sinh sự kiện từ N đơn đầu tiên; 0 để bỏ qua")
    parser.add_argument("--checksums", action="store_true",
                        help="Đọc lại toàn bộ file để kiểm tra SHA256")
    args = parser.parse_args()
    if args.stream_orders < 0:
        parser.error("--stream-orders phải >= 0")

    try:
        from edai_generator.config import load_config
        from edai_generator.offline import generate
        from edai_generator.streaming import plan_stream
        from edai_generator.validate import validate_run

        # Bước 1: đọc config. Seed giúp chạy lại cho ra cùng dữ liệu.
        cfg = load_config(args.config)
        print("[1/3] Sinh các bảng nguồn thành file Parquet...", flush=True)
        manifest = generate(cfg, args.out, resume=args.resume)

        # Bước 2: kiểm tra PK/FK, tổng tiền, duplicate, schema và lịch sử.
        print("[2/3] Kiểm tra độc lập dữ liệu đầu ra...", flush=True)
        validation = validate_run(args.out, checksums=args.checksums)
        if not validation["passed"]:
            print(json.dumps(validation, ensure_ascii=False, indent=2))
            return 1

        # Bước 3: lập lịch theo thời gian đến. Chưa kết nối Kafka ở bước này.
        if args.stream_orders:
            print("[3/3] Tạo lịch clickstream và payment callbacks...", flush=True)
            plan_stream(args.out, n_orders=min(args.stream_orders, manifest["orders_generated"]))
        else:
            print("[3/3] Bỏ qua streaming theo --stream-orders 0.")

        print(json.dumps({
            "hoan_thanh": True,
            "thu_muc": str(args.out.resolve()),
            "so_dong_moi_bang": validation["rows"],
            "kiem_tra_du_lieu": validation["passed"],
            "so_ngay_lich_su_don_hang": validation["order_time_range"]["span_days"],
            "dung_luong_parquet_gb": validation["physical_source_gb"],
            "da_du_100gb": validation["rubric_100gb_met"],
            "da_gui_kafka": False,
        }, ensure_ascii=False, indent=2))
        return 0
    except KeyboardInterrupt:
        print("Đã dừng. Chạy lại cùng lệnh và thêm --resume để tiếp tục.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"LỖI: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
