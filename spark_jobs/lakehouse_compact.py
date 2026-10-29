"""Tối ưu lưu trữ lakehouse: gom file nhỏ (compaction) và chia partition theo tháng, đo trước/sau.

Dựng ba bố cục của cùng một bảng (mặc định fact_orders của Gold):
  small_files  : nhiều file Parquet nhỏ (mô phỏng ghi streaming/micro-batch, chưa tối ưu)
  compacted    : cùng dữ liệu gom thành vài file lớn
  partitioned  : gom file và chia partition theo tháng đặt hàng (order_month)
Với mỗi bố cục: đếm file, kích thước trung bình, thời gian truy vấn quét toàn bảng và truy vấn lọc theo tháng.
Kết quả: <out>/lakehouse_report.json. Z-order không có trong Parquet thuần (cần Delta/Iceberg), nên bài này dùng compaction + partition.
"""
import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def make_spark(endpoint):
    b = SparkSession.builder.appName("EDAI-LAKEHOUSE-COMPACT").config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.adaptive.enabled", "true")
    if endpoint:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", endpoint).config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
    eventlog = os.getenv("SPARK_EVENTLOG_DIR")
    if eventlog:
        b = b.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", eventlog)
    return b.getOrCreate()


def file_stats(path):
    """Số file Parquet và tổng byte dưới một thư mục (local hoặc s3a://)."""
    if path.startswith("s3a://"):
        import boto3
        bucket, _, prefix = path[len("s3a://"):].partition("/")
        client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"], region_name="us-east-1")
        count = total = 0
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    count += 1
                    total += obj["Size"]
    else:
        files = [p for p in Path(path).rglob("*.parquet")]
        count, total = len(files), sum(p.stat().st_size for p in files)
    return {"files": count, "total_bytes": total, "avg_file_bytes": int(total / count) if count else 0}


def time_query(spark, path, month_filter, runs):
    """Thời gian tốt nhất (giây) cho truy vấn quét toàn bảng và truy vấn lọc theo tháng."""
    def best(build):
        durations = []
        for _ in range(runs):
            t0 = time.perf_counter()
            result = build().collect()
            durations.append(time.perf_counter() - t0)
        return round(min(durations), 3), result
    full_s, full = best(lambda: spark.read.parquet(path).agg(F.count("*").alias("n"), F.sum("total_amount").alias("s")))

    def filtered():
        df = spark.read.parquet(path)
        start, end = month_filter
        cond = (F.col("order_ts") >= start) & (F.col("order_ts") < end)
        if "order_month" in df.columns:
            cond = cond & (F.col("order_month") == start[:7])
        return df.filter(cond).agg(F.count("*").alias("n"), F.sum("total_amount").alias("s"))
    month_s, month = best(filtered)
    return {"full_scan_seconds": full_s, "month_filter_seconds": month_s,
            "full_rows": full[0]["n"], "full_sum": str(full[0]["s"]),
            "month_rows": month[0]["n"], "month_sum": str(month[0]["s"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-version")
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--source-dir", help="Thư mục Parquet local của bảng nguồn (thử nhanh, thay cho --gold-version)")
    parser.add_argument("--work-root", help="Nơi ghi các bố cục (mặc định s3a://<bucket>/_lakehouse/<version>)")
    parser.add_argument("--small-files", type=int, default=200, help="Số file nhỏ mô phỏng")
    parser.add_argument("--target-file-mb", type=int, default=128)
    parser.add_argument("--month", help="Tháng lọc YYYY-MM (mặc định: tháng có nhiều đơn nhất)")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if not args.source_dir and not args.gold_version:
        parser.error("cần --gold-version hoặc --source-dir")

    spark = make_spark(None if args.source_dir else args.endpoint)
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.source_dir:
            source, root = args.source_dir, args.work_root or "/tmp/lakehouse_work"
        else:
            sys.path.insert(0, "/opt/app")
            from spark_jobs.snapshot_store import S3Store, required_read
            store = S3Store(args.endpoint, args.gold_bucket)
            marker = json.loads(required_read(store, f"{args.gold_version}/_SUCCESS.json"))
            tables = json.loads(required_read(store, marker["report_key"]))["tables"]
            source = tables["fact_orders"]["path"]
            root = args.work_root or f"s3a://{args.gold_bucket}/_lakehouse/{args.gold_version}"
        df = spark.read.parquet(source).select("order_id", "customer_sk", "order_ts", "status", "shipping_city", "total_amount")
        df = df.withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
        month = args.month or df.groupBy("order_month").count().orderBy(F.col("count").desc()).first()["order_month"]
        start = f"{month}-01"
        y, m = int(month[:4]), int(month[5:])
        end = f"{y + (m == 12)}-{1 if m == 12 else m + 1:02d}-01"
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base = f"{root.rstrip('/')}/{run_id}"

        df.repartition(args.small_files).write.mode("errorifexists").parquet(f"{base}/small_files")
        small = spark.read.parquet(f"{base}/small_files")
        total_bytes = file_stats(f"{base}/small_files")["total_bytes"]
        n_files = max(1, math.ceil(total_bytes / (args.target_file_mb * 1024 * 1024)))
        small.coalesce(n_files).write.mode("errorifexists").parquet(f"{base}/compacted")
        small.repartition("order_month").write.mode("errorifexists").partitionBy("order_month").parquet(f"{base}/partitioned")

        report = {"source": source, "work_root": base, "month_filter": month, "runs_per_query": args.runs,
                  "layouts": {}, "generated_at": datetime.now(timezone.utc).isoformat()}
        for layout in ("small_files", "compacted", "partitioned"):
            path = f"{base}/{layout}"
            report["layouts"][layout] = {**file_stats(path), **time_query(spark, path, (start, end), args.runs)}
        sums = {(v["full_rows"], v["full_sum"], v["month_rows"], v["month_sum"]) for v in report["layouts"].values()}
        report["results_identical_across_layouts"] = len(sums) == 1
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "lakehouse_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        if not report["results_identical_across_layouts"]:
            sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
