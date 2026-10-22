"""DP3: Silver → Gold. Submit bằng spark-submit, không chạy qua generator CLI."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import uuid

from spark_jobs.snapshot_store import (S3Store, digest, encode_json, required_read, validate_version)
from spark_jobs.gold_transforms import transform_gold


PIPELINE_VERSION = "1.0.0"


def now():
    return datetime.now(timezone.utc).isoformat()


def code_fingerprint():
    """SHA256 của source code các file liên quan đến DP3."""
    root = Path(__file__).parent
    names = ("gold_contracts.py", "gold_transforms.py", "snapshot_store.py", "dp3_silver_to_gold.py")
    return digest(b"\n".join(name.encode() + b"\n" + (root / name).read_bytes() for name in names))


def read_silver_success(silver_store, silver_version):
    """Đọc Silver _SUCCESS.json và report, trả về (marker, silver_paths).

    silver_paths: dict tên bảng → đường dẫn Parquet (s3a:// hoặc file://).
    """
    validate_version(silver_version)
    marker_data = required_read(silver_store, f"{silver_version}/_SUCCESS.json")
    marker = json.loads(marker_data)

    report_key = marker.get("report_key")
    if not report_key:
        raise ValueError("Silver _SUCCESS.json thiếu trường report_key")

    report_data = required_read(silver_store, report_key)
    # Kiểm tra checksum nếu có
    if "report_sha256" in marker and digest(report_data) != marker["report_sha256"]:
        raise ValueError("Silver report checksum không khớp")

    silver_report = json.loads(report_data)
    if silver_report.get("passed") is not True:
        raise ValueError("Silver report chưa được công bố thành công (passed != true)")

    # Lấy paths từ report tables
    tables_info = silver_report.get("tables", {})
    required_tables = [
        "stg_customers", "stg_products", "stg_orders",
        "stg_order_items", "stg_payments", "stg_customer_history",
    ]
    silver_paths = {}
    for tbl in required_tables:
        info = tables_info.get(tbl)
        if not info:
            raise ValueError(f"Silver report thiếu bảng: {tbl}")
        path = info.get("path")
        if not path:
            raise ValueError(f"Silver report thiếu path cho bảng: {tbl}")
        silver_paths[tbl] = path

    return marker, silver_paths


def published_gold_report(gold_store, gold_version, identity):
    """Kiểm tra xem Gold version đã được công bố chưa. Trả về report hoặc None."""
    marker_data = gold_store.read(f"{gold_version}/_SUCCESS.json")
    if marker_data is None:
        return None
    marker = json.loads(marker_data)
    if marker.get("identity") != identity:
        raise ValueError("Gold version đã công bố với identity khác; chọn --gold-version mới")
    run_id = marker["run_id"]
    validate_version(run_id)
    expected_key = f"{gold_version}/_runs/{run_id}/gold_report.json"
    if marker.get("report_key") != expected_key:
        raise ValueError("Gold marker trỏ ngoài run của nó")
    report_data = required_read(gold_store, expected_key)
    if digest(report_data) != marker["report_sha256"]:
        raise ValueError("Gold report checksum không khớp")
    report = json.loads(report_data)
    if report.get("passed") is not True or report.get("run_id") != run_id:
        raise ValueError("Gold report không phải publication thành công")
    return report


def bind_gold_version(gold_store, gold_version, identity):
    """Ghi _DATASET.json để claim version; idempotent nếu identity khớp."""
    key = f"{gold_version}/_DATASET.json"
    if not gold_store.write(key, encode_json(identity), if_absent=True):
        if json.loads(required_read(gold_store, key)) != identity:
            raise ValueError("Gold version thuộc về input/rules khác; chọn --gold-version mới")


def run_pipeline(silver_store, gold_store, *, silver_version, gold_version, out, spark_factory):
    """Một version được công bố đúng một lần; rerun kiểm tra và tái sử dụng snapshot."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    local = Path(out) / run_id
    local.mkdir(parents=True, exist_ok=True)
    report = {
        "pipeline": "dp3_silver_to_gold",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": run_id,
        "started_at": now(),
        "passed": False,
        "silver_bucket": silver_store.bucket,
        "silver_version": silver_version,
        "gold_bucket": gold_store.bucket,
        "gold_version": gold_version,
    }
    spark = None
    storage_ready = False
    try:
        validate_version(silver_version)
        validate_version(gold_version)
        if silver_store.bucket == gold_store.bucket:
            raise ValueError("Silver và Gold phải dùng bucket riêng biệt")

        # Đọc Silver SUCCESS và lấy table paths
        print(f"[DP3] Đọc Silver SUCCESS: {silver_version}", flush=True)
        silver_marker, silver_paths = read_silver_success(silver_store, silver_version)

        identity = {
            "silver_bucket": silver_store.bucket,
            "silver_version": silver_version,
            "silver_run_id": silver_marker.get("run_id", ""),
            "gold_version": gold_version,
            "pipeline_version": PIPELINE_VERSION,
            "code_sha256": code_fingerprint(),
            "spark_version": "3.5.6",
        }
        report.update(identity=identity, silver_run_id=silver_marker.get("run_id", ""))

        gold_store.ensure()
        bind_gold_version(gold_store, gold_version, identity)
        storage_ready = True

        # Kiểm tra idempotency
        existing = published_gold_report(gold_store, gold_version, identity)
        if existing is not None:
            report.update(
                passed=True,
                status="already_published",
                reused_run_id=existing["run_id"],
                tables=existing["tables"],
                finished_at=now(),
                verification="Existing Gold output matches the published report",
            )
            (local / "gold_report.json").write_bytes(encode_json(report))
            return report

        # Khởi động Spark
        spark = spark_factory()
        if spark.version != "3.5.6":
            raise ValueError(f"Expected Spark 3.5.6, got {spark.version}")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        spark.sparkContext.setLogLevel("WARN")
        report["spark"] = {
            "version": spark.version,
            "application_id": spark.sparkContext.applicationId,
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        }

        run_prefix = f"{gold_version}/_runs/{run_id}"
        tables_root = gold_store.uri(f"{run_prefix}/tables")
        quarantine_root = gold_store.uri(f"{run_prefix}/quarantine")

        print(f"[DP3] Bắt đầu transform_gold → {tables_root}", flush=True)
        transform_gold(spark, silver_paths, tables_root, quarantine_root,
                       gold_version, run_id, report["started_at"], report)

        inventory = gold_store.listing(f"{run_prefix}/tables/")
        if not inventory or not any(row["key"].endswith(".parquet") for row in inventory):
            raise ValueError("Gold output inventory không có dữ liệu Parquet")

        report.update(passed=True, status="published", finished_at=now(), output_inventory=inventory)
        report_key = f"{run_prefix}/gold_report.json"
        report_bytes = encode_json(report)
        gold_store.write(report_key, report_bytes)

        marker = {
            "identity": identity,
            "run_id": run_id,
            "completed_at": report["finished_at"],
            "report_key": report_key,
            "report_sha256": digest(report_bytes),
            "tables": {name: info["path"] for name, info in report["tables"].items()},
        }
        if not gold_store.write(f"{gold_version}/_SUCCESS.json", encode_json(marker), if_absent=True):
            raise ValueError("Job khác đã công bố version này; chọn một writer và rerun")

        (local / "gold_report.json").write_bytes(report_bytes)
        print(f"[DP3] Hoàn thành. Run ID: {run_id}", flush=True)
        return report

    except Exception as exc:
        report.update(passed=False, status="failed", finished_at=now(), error=f"{type(exc).__name__}: {exc}")
        data = encode_json(report)
        (local / "gold_report.json").write_bytes(data)
        if storage_ready:
            try:
                gold_store.write(f"{gold_version}/_runs/{run_id}/failed_report.json", data)
            except Exception as write_error:
                print(f"Không lưu được failure report từ xa: {type(write_error).__name__}", file=sys.stderr)
        print(f"DP3 thất bại; local report: {local / 'gold_report.json'}", file=sys.stderr, flush=True)
        raise

    finally:
        if spark is not None:
            spark.stop()


def make_spark(endpoint, shuffle_partitions):
    """Tạo SparkSession với cấu hình S3A cho MinIO."""
    from pyspark.sql import SparkSession
    if shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions phải > 0")
    builder = (SparkSession.builder.appName("EDAI-DP3-Silver-to-Gold")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.files.maxRecordsPerFile", "500000")
               .config("spark.hadoop.fs.s3a.endpoint", endpoint)
               .config("spark.hadoop.fs.s3a.path.style.access", "true")
               .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                       str(endpoint.startswith("https://")).lower())
               .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                       "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
    eventlog = os.getenv("SPARK_EVENTLOG_DIR")
    if eventlog:
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", eventlog)
    spark = builder.getOrCreate()
    hadoop = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    if hadoop != "3.3.4":
        spark.stop()
        raise ValueError(f"Expected Hadoop 3.3.4, got {hadoop}")
    return spark


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-version", required=True)
    parser.add_argument("--gold-version", required=True)
    parser.add_argument("--silver-bucket", default="silver")
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--out", default="/data/gold")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    args = parser.parse_args(argv)
    try:
        if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
            raise ValueError("Đặt thông tin xác thực lưu trữ trong môi trường Compose")
        result = run_pipeline(
            S3Store(args.endpoint, args.silver_bucket),
            S3Store(args.endpoint, args.gold_bucket),
            silver_version=args.silver_version,
            gold_version=args.gold_version,
            out=args.out,
            spark_factory=lambda: make_spark(args.endpoint, args.shuffle_partitions),
        )
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    summary = {k: result[k] for k in ("passed", "status", "run_id", "silver_version", "gold_version")}
    summary["table_rows"] = {name: info["rows"] for name, info in result["tables"].items()}
    if "reused_run_id" in result:
        summary["reused_run_id"] = result["reused_run_id"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

