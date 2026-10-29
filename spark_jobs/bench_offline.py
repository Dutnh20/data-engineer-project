"""Benchmark tối ưu Spark cho dữ liệu skew: một mode mỗi lần chạy (mỗi lần = một Spark app riêng để so sánh trên Spark UI).

Workload (giống bước dựng agg theo sản phẩm của Gold): fact_order_items JOIN fact_orders JOIN dim_product,
GROUP BY product_id, category. product_id bị skew (một số sản phẩm rất hot) nên join/group theo product_id lệch.

Modes:
  baseline   AQE tắt, không broadcast, 200 shuffle partition (mặc định Spark)
  aqe        AQE bật (gộp partition nhỏ), vẫn không broadcast
  aqe_skew   AQE + skew join (tách partition lệch), vẫn không broadcast
  broadcast  AQE + broadcast dim_product (loại bỏ shuffle phía bảng lớn khi join theo product_id)
  salting    Baseline conf nhưng muối khóa hot theo product_id để chia đều partition
Kết quả: 1 dòng JSON/mode vào <out>/bench_results.jsonl, gồm thời gian và số liệu stage lấy từ Spark REST API.
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MODES = ["baseline", "aqe", "aqe_skew", "broadcast", "salting"]


def build_spark(mode, endpoint, partitions):
    b = (SparkSession.builder.appName(f"EDAI-BENCH-{mode}")
         .config("spark.sql.shuffle.partitions", str(partitions))
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.adaptive.skewJoin.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC"))
    if mode in ("aqe", "aqe_skew", "broadcast"):
        b = b.config("spark.sql.adaptive.enabled", "true")
    if mode == "aqe_skew":
        b = (b.config("spark.sql.adaptive.skewJoin.enabled", "true")
             .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
             .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1MB")
             .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4MB"))
    if mode == "broadcast":
        b = b.config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
    if endpoint:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
    eventlog = os.getenv("SPARK_EVENTLOG_DIR")
    if eventlog:
        b = b.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", eventlog)
    return b.getOrCreate()


def load_tables(spark, args):
    if args.tables_dir:
        base = Path(args.tables_dir)
        read = lambda name: spark.read.parquet(str(base / name))  # noqa: E731
    else:
        sys.path.insert(0, "/opt/app")
        from spark_jobs.snapshot_store import S3Store, required_read
        store = S3Store(args.endpoint, args.gold_bucket)
        marker = json.loads(required_read(store, f"{args.gold_version}/_SUCCESS.json"))
        tables = json.loads(required_read(store, marker["report_key"]))["tables"]
        read = lambda name: spark.read.parquet(tables[name]["path"])  # noqa: E731
    return read("fact_order_items"), read("fact_orders"), read("dim_product")


def workload(mode, items, orders, products, hot_keys, salt_buckets):
    """Trả về DataFrame kết quả (đã aggregate)."""
    items = items.select("order_id", "product_id", "line_amount")
    orders = orders.select("order_id", "customer_sk")
    products = products.select("product_id", "category")
    joined = items.join(orders, "order_id")
    if mode == "broadcast":
        joined = joined.join(F.broadcast(products), "product_id")
    elif mode == "salting":
        # Khóa hot được chia thành salt_buckets nhóm; dim_product nhân bản tương ứng
        is_hot = F.col("product_id").isin(hot_keys)
        salted = joined.withColumn("salt", F.when(is_hot, (F.rand(42) * salt_buckets).cast("int")).otherwise(F.lit(0)))
        salts = F.when(F.col("product_id").isin(hot_keys), F.sequence(F.lit(0), F.lit(salt_buckets - 1))) \
                 .otherwise(F.array(F.lit(0)))
        prod_rows = products.withColumn("salt", F.explode(salts))
        joined = salted.join(prod_rows, ["product_id", "salt"]).drop("salt")
    else:
        joined = joined.join(products, "product_id")
    return (joined.groupBy("product_id", "category")
            .agg(F.sum("line_amount").alias("revenue"), F.count("*").alias("lines")))


def find_hot_keys(items, top_n):
    rows = items.groupBy("product_id").count().orderBy(F.col("count").desc()).limit(top_n).collect()
    return [r["product_id"] for r in rows]


def stage_metrics(spark):
    """Lấy số liệu stage từ REST API của driver; trả về dict hoặc {'unavailable': lý do}."""
    try:
        base = spark.sparkContext.uiWebUrl
        app = spark.sparkContext.applicationId
        get = lambda path: json.loads(urllib.request.urlopen(f"{base}/api/v1/applications/{app}{path}", timeout=10).read())  # noqa: E731
        stages = get("/stages?status=complete")
        shuffle_read = sum(s.get("shuffleReadBytes", 0) for s in stages)
        shuffle_write = sum(s.get("shuffleWriteBytes", 0) for s in stages)
        spilled = sum(s.get("memoryBytesSpilled", 0) for s in stages)
        worst = 0.0
        for s in stages:
            if s.get("shuffleReadBytes", 0) <= 0 or s.get("numCompleteTasks", 0) < 2:
                continue
            q = get(f"/stages/{s['stageId']}/{s['attemptId']}/taskSummary?quantiles=0.5,1.0")
            median, mx = q["executorRunTime"][0], q["executorRunTime"][1]
            if median > 0:
                worst = max(worst, mx / median)
        return {"stages": len(stages), "shuffle_read_bytes": shuffle_read, "shuffle_write_bytes": shuffle_write,
                "memory_spilled_bytes": spilled, "max_task_to_median_ratio": round(worst, 2),
                "app_id": app}
    except Exception as exc:  # UI có thể tắt
        return {"unavailable": f"{type(exc).__name__}: {exc}"}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--gold-version")
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--tables-dir", help="Thư mục local chứa fact_order_items, fact_orders, dim_product (để thử nhanh)")
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--partitions", type=int, default=200, help="spark.sql.shuffle.partitions (mặc định Spark: 200)")
    parser.add_argument("--hot-keys", type=int, default=20)
    parser.add_argument("--salt-buckets", type=int, default=16)
    parser.add_argument("--runs", type=int, default=1, help="Số lần đo; lấy lần nhanh nhất để giảm nhiễu")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if not args.tables_dir and not args.gold_version:
        parser.error("cần --gold-version hoặc --tables-dir")

    spark = build_spark(args.mode, None if args.tables_dir else args.endpoint, args.partitions)
    spark.sparkContext.setLogLevel("WARN")
    try:
        items, orders, products = load_tables(spark, args)
        hot = find_hot_keys(items, args.hot_keys) if args.mode == "salting" else []
        durations, rows = [], 0
        for _ in range(args.runs):
            result = workload(args.mode, items, orders, products, hot, args.salt_buckets)
            t0 = time.perf_counter()
            result.write.format("noop").mode("overwrite").save()
            durations.append(time.perf_counter() - t0)
        rows = result.count()
        checksum = result.agg(F.sum("revenue").cast("decimal(38,0)").alias("r"), F.sum("lines").alias("l")).first()
        record = {"mode": args.mode, "seconds_best": round(min(durations), 2),
                  "seconds_all": [round(d, 2) for d in durations], "result_rows": rows,
                  "revenue_checksum": str(checksum["r"]), "lines_checksum": checksum["l"],
                  "shuffle_partitions": args.partitions, "hot_keys": len(hot),
                  "finished_at": datetime.now(timezone.utc).isoformat(), **stage_metrics(spark)}
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        with (out / "bench_results.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(json.dumps(record, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
