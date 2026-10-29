"""DP4: Kafka -> Silver streaming (Spark Structured Streaming, chạy bounded bằng AvailableNow).

- Đọc 2 topic: <prefix>.clickstream và <prefix>.payment_callbacks (JSON).
- Parse theo schema; dòng hỏng/thiếu event_id/event_ts bị loại và đếm riêng.
- Watermark theo event_ts (mặc định 15 phút, > độ trễ 5-10 phút của mobile) + dropDuplicatesWithinWatermark(event_id).
- Ghi parquet vào s3a://<bucket>/_streaming/<stream-version>/...; checkpoint cùng nơi nên chạy lại không nhân đôi dữ liệu.
- Cuối job đối soát với Kafka (đọc batch) và ghi stream_report.json.
Kết quả đối soát là bằng chứng cho rubric: late arrival được nhận, callback trùng bị loại.
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CLICK_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("schema_version", T.IntegerType()),
    T.StructField("session_id", T.StringType()), T.StructField("customer_id", T.LongType()),
    T.StructField("product_id", T.LongType()), T.StructField("order_id", T.LongType()),
    T.StructField("event_type", T.StringType()), T.StructField("page_type", T.StringType()),
    T.StructField("event_ts", T.StringType()), T.StructField("device_type", T.StringType()),
])
CALLBACK_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("schema_version", T.IntegerType()),
    T.StructField("payment_id", T.LongType()), T.StructField("order_id", T.LongType()),
    T.StructField("event_ts", T.StringType()), T.StructField("callback_ts", T.StringType()),
    T.StructField("result_code", T.StringType()), T.StructField("provider", T.StringType()),
    T.StructField("amount", T.LongType()), T.StructField("failure_reason", T.StringType()),
])
TOPICS = {"clickstream": CLICK_SCHEMA, "payment_callbacks": CALLBACK_SCHEMA}


def make_spark(endpoint):
    return (SparkSession.builder.appName("EDAI-DP4-Kafka-to-Silver")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
            .getOrCreate())


def kafka_reader(spark, fmt, bootstrap, topic, **extra):
    reader = (spark.readStream if fmt == "stream" else spark.read).format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic) \
        .option("startingOffsets", "earliest").option("failOnDataLoss", "false")
    for key, value in extra.items():
        reader = reader.option(key, value)
    return reader.load()


def parse(raw, schema):
    parsed = raw.select(
        F.col("value").cast("string").alias("json"), F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_ts"),
    ).withColumn("d", F.from_json("json", schema))
    good = parsed.filter(F.col("d").isNotNull() & F.col("d.event_id").isNotNull() & F.col("d.event_ts").isNotNull())
    cols = [F.col("d." + f.name).alias(f.name) for f in schema.fields if f.name not in ("event_ts", "callback_ts")]
    cols.append(F.col("d.event_ts").cast("timestamp").alias("event_ts"))
    if "callback_ts" in schema.fieldNames():
        cols.append(F.col("d.callback_ts").cast("timestamp").alias("callback_ts"))
    return good.select(*cols, "kafka_partition", "kafka_offset", "kafka_ts") \
               .filter(F.col("event_ts").isNotNull())


def start_query(spark, name, args, endpoint_bucket):
    topic = f"{args.topic_prefix}.{name}"
    base = f"s3a://{endpoint_bucket}/_streaming/{args.stream_version}"
    extra = {"maxOffsetsPerTrigger": args.max_offsets_per_trigger}
    clean = (parse(kafka_reader(spark, "stream", args.bootstrap, topic, **extra), TOPICS[name])
             .withWatermark("event_ts", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"]))
    return (clean.writeStream.format("parquet").outputMode("append")
            .option("path", f"{base}/{name}_clean")
            .option("checkpointLocation", f"{base}/_checkpoints/{name}")
            .trigger(availableNow=True).queryName(name).start())


def reconcile(spark, name, args, bucket):
    """Đối soát batch: Kafka (nguồn thật) so với bảng sạch đã ghi."""
    topic = f"{args.topic_prefix}.{name}"
    base = f"s3a://{bucket}/_streaming/{args.stream_version}"
    raw = kafka_reader(spark, "batch", args.bootstrap, topic)
    kafka_rows = raw.count()
    parsed = parse(raw, TOPICS[name]).cache()
    parsed_rows = parsed.count()
    distinct_ids = parsed.select("event_id").distinct().count()
    try:
        out = spark.read.parquet(f"{base}/{name}_clean")
        out_rows = out.count()
        out_distinct = out.select("event_id").distinct().count()
    except Exception as exc:  # chưa có file nào được ghi
        out, out_rows, out_distinct = None, 0, 0
        print(f"[DP4] {name}: chưa có dữ liệu sạch ({type(exc).__name__})")
    report = {
        "topic": topic,
        "kafka_rows": kafka_rows,
        "unparseable_rows": kafka_rows - parsed_rows,
        "duplicate_rows_in_kafka": parsed_rows - distinct_ids,
        "distinct_event_ids_in_kafka": distinct_ids,
        "clean_rows_written": out_rows,
        "clean_distinct_event_ids": out_distinct,
        "dropped_as_too_late": max(0, distinct_ids - out_distinct),
        "duplicates_in_clean_output": out_rows - out_distinct,
    }
    if out is not None and out_rows:
        # Out-of-order: event_ts nhỏ hơn event_ts lớn nhất đã thấy trước đó trong cùng partition (theo offset).
        w = Window.partitionBy("kafka_partition").orderBy("kafka_offset").rowsBetween(Window.unboundedPreceding, -1)
        ordered = out.withColumn("prev_max", F.max("event_ts").over(w))
        late = ordered.filter(F.col("prev_max").isNotNull() & (F.col("event_ts") < F.col("prev_max")))
        lateness = late.select((F.col("prev_max").cast("long") - F.col("event_ts").cast("long")).alias("s"))
        stats = lateness.agg(F.count("*").alias("n"), F.min("s").alias("min_s"),
                             F.percentile_approx("s", 0.5).alias("p50_s"), F.max("s").alias("max_s")).first()
        report["out_of_order_rows"] = int(stats["n"])
        report["out_of_order_lateness_seconds"] = {"min": stats["min_s"], "p50": stats["p50_s"], "max": stats["max_s"]}
        report["schema_versions"] = {str(r["schema_version"]): r["count"]
                                     for r in out.groupBy("schema_version").count().collect()}
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-version", default="stream_v1")
    parser.add_argument("--topic-prefix", default="edai")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--bucket", default="silver")
    parser.add_argument("--watermark", default="15 minutes")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=2000)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.stream_version):
        raise ValueError("--stream-version chỉ gồm chữ, số, '_' và '-'")

    spark = make_spark(os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    spark.sparkContext.setLogLevel("WARN")
    started = datetime.now(timezone.utc)
    report = {"stream_version": args.stream_version, "topic_prefix": args.topic_prefix,
              "watermark": args.watermark, "started_at": started.isoformat(), "passed": False}
    try:
        queries = [start_query(spark, name, args, args.bucket) for name in TOPICS]
        for query in queries:
            query.awaitTermination()
            if query.exception():
                raise RuntimeError(str(query.exception()))
        report["topics"] = {name: reconcile(spark, name, args, args.bucket) for name in TOPICS}
        checks = []
        for name, r in report["topics"].items():
            checks.append((f"{name}: không còn trùng event_id trong bảng sạch", r["duplicates_in_clean_output"] == 0))
            checks.append((f"{name}: có dữ liệu sạch", r["clean_rows_written"] > 0))
            checks.append((f"{name}: không mất event hợp lệ do quá muộn", r["dropped_as_too_late"] == 0))
        report["checks"] = [{"check": c, "ok": ok} for c, ok in checks]
        report["passed"] = all(ok for _, ok in checks)
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        out_dir = Path(args.out) / started.strftime("%Y%m%dT%H%M%SZ")
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "stream_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        print(f"DP4 {'thành công' if report['passed'] else 'thất bại'}; report: {out_dir / 'stream_report.json'}")
        spark.stop()
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
