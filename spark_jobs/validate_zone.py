"""Validate stage cho từng zone: đọc marker + report đã công bố trên MinIO, kiểm tra tính toàn vẹn.

Không cần Spark (chỉ boto3), chạy nhanh làm task `validate` trong Airflow.
Zone: bronze | silver | gold | feat. Thoát mã 1 nếu có kiểm tra thất bại; luôn in JSON kết quả.
"""
import argparse
import json
import os
import sys
from pathlib import Path

from spark_jobs.snapshot_store import S3Store, digest, required_read, validate_version

SILVER_TABLES = ["stg_customers", "stg_products", "stg_orders", "stg_order_items", "stg_payments",
                 "stg_customer_history"]
GOLD_TABLES = ["dim_customer", "dim_product", "dim_date", "fact_orders", "fact_order_items",
               "fact_payment_attempts"]


def check(results, name, ok, detail=""):
    results.append({"check": name, "ok": bool(ok), "detail": str(detail)})


def load(store, marker_key):
    marker = json.loads(required_read(store, marker_key))
    report_bytes = required_read(store, marker["report_key"])
    if "report_sha256" in marker and digest(report_bytes) != marker["report_sha256"]:
        raise ValueError("Report checksum không khớp marker")
    return marker, json.loads(report_bytes)


def tables_rows(report):
    return {name: info.get("rows") for name, info in report.get("tables", {}).items()}


def validate(zone, endpoint, version, bucket, window_days):
    results = []
    store = S3Store(endpoint, bucket)
    marker_key = f"_features/{version}/_SUCCESS.json" if zone == "feat" else f"{version}/_SUCCESS.json"
    try:
        marker, report = load(store, marker_key)
    except Exception as exc:
        check(results, "marker + report đọc được, checksum khớp", False, f"{type(exc).__name__}: {exc}")
        return results
    check(results, "marker + report đọc được, checksum khớp", True, marker.get("run_id"))
    check(results, "report.passed == true", report.get("passed") is True)
    check(results, "run_id marker == run_id report", marker.get("run_id") == report.get("run_id"),
          f"{marker.get('run_id')} vs {report.get('run_id')}")
    if zone == "bronze":
        check(results, "verified_files == expected_files", marker.get("verified_files") == report.get("expected_files"),
              f"{marker.get('verified_files')} vs {report.get('expected_files')}")
        check(results, "verified_files > 0", (marker.get("verified_files") or 0) > 0)
    elif zone in ("silver", "gold"):
        rows = tables_rows(report)
        wanted = SILVER_TABLES if zone == "silver" else GOLD_TABLES
        for table in wanted:
            check(results, f"{table} tồn tại và rows > 0", (rows.get(table) or 0) > 0, rows.get(table))
        if zone == "gold":
            check(results, "dim_customer >= số khách (SCD2 giữ mọi phiên bản)",
                  (rows.get("dim_customer") or 0) >= 1, rows.get("dim_customer"))
    elif zone == "feat":
        table = f"feat_customer_orders_{window_days}d"
        check(results, f"{table} có trong marker", table in marker.get("tables", {}))
        check(results, "rows > 0", (report.get("rows") or 0) > 0, report.get("rows"))
        check(results, "mọi quality check của job đều ok", all(c["ok"] for c in report.get("checks", [])),
              [c["check"] for c in report.get("checks", []) if not c["ok"]])
        cols = set(report.get("columns", []))
        check(results, "có cột event_timestamp và created", {"event_timestamp", "created"} <= cols)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zone", required=True, choices=["bronze", "silver", "gold", "feat"])
    parser.add_argument("--version", required=True)
    parser.add_argument("--bucket", help="Mặc định: trùng tên zone (feat dùng bucket gold)")
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    parser.add_argument("--out", help="Thư mục ghi validation_<zone>.json")
    args = parser.parse_args()
    validate_version(args.version)
    bucket = args.bucket or ("gold" if args.zone == "feat" else args.zone)
    results = validate(args.zone, args.endpoint, args.version, bucket, args.window_days)
    passed = all(r["ok"] for r in results)
    summary = {"zone": args.zone, "version": args.version, "bucket": bucket, "passed": passed, "checks": results}
    text = json.dumps(summary, indent=2, ensure_ascii=False)
    print(text)
    if args.out:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        (Path(args.out) / f"validation_{args.zone}_{args.version}.json").write_text(text, encoding="utf-8")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
