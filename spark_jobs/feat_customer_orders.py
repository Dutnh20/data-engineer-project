"""DP3 (rubric): tính bảng offline feature `feat_customer_orders_<N>d` từ Gold.

Đầu vào : Gold đã công bố (`fact_orders`, `dim_customer`).
Đầu ra  : s3a://<bucket>/_features/<feat-version>/runs/<run_id>/feat_customer_orders_<N>d (Parquet)
          + feat_report.json + marker _features/<feat-version>/_SUCCESS.json (ghi một lần).
Mỗi hàng = (customer_id, event_timestamp) với event_timestamp là mốc "as of" của feature và
`created` là thời điểm pipeline tạo hàng, đúng quy ước feature store (point-in-time correct):
chỉ dùng đơn có order_ts <= event_timestamp.
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.snapshot_store import S3Store, digest, encode_json, required_read, validate_version


def feature_names(window_days):
    """Tên các cột feature theo cửa sổ (ngày)."""
    w = f"{window_days}d"
    return {
        "orders": f"f_customer_total_orders_{w}",
        "completed": f"f_customer_completed_orders_{w}",
        "spend": f"f_customer_total_spend_vnd_{w}",
        "aov": f"f_customer_avg_order_value_vnd_{w}",
        "recency": "f_customer_days_since_last_order",
    }


def build_feat_customer_orders(spark, orders_df, customers_df, as_of_list, window_days,
                               gold_version, run_id, created_ts):
    """Tính feature cho mọi (khách hàng x mốc as_of); khách chưa có đơn vẫn có hàng với giá trị 0/NULL.

    orders_df   : customer_id, order_ts, is_completed, total_amount
    customers_df: customer_id
    as_of_list  : danh sách datetime (UTC, naive)
    """
    if window_days < 1:
        raise ValueError("window_days phải >= 1")
    names = feature_names(window_days)
    as_of = spark.createDataFrame([(t,) for t in as_of_list], "as_of timestamp")
    universe = customers_df.select("customer_id").distinct().crossJoin(as_of).alias("u")
    orders = orders_df.select("customer_id", "order_ts", "is_completed",
                              F.col("total_amount").cast("long").alias("amount")).alias("o")
    joined = universe.join(
        orders,
        (F.col("u.customer_id") == F.col("o.customer_id")) & (F.col("o.order_ts") <= F.col("u.as_of")),
        "left")
    in_window = F.col("o.order_ts") > (F.col("u.as_of") - F.expr(f"INTERVAL {int(window_days)} DAYS"))
    done = in_window & F.coalesce(F.col("o.is_completed"), F.lit(False))
    agg = (joined.groupBy(F.col("u.customer_id").alias("customer_id"), F.col("u.as_of").alias("event_timestamp"))
           .agg(F.sum(F.when(in_window, 1).otherwise(0)).cast("long").alias(names["orders"]),
                F.sum(F.when(done, 1).otherwise(0)).cast("long").alias(names["completed"]),
                F.sum(F.when(done, F.col("o.amount")).otherwise(0)).cast("long").alias(names["spend"]),
                F.max("o.order_ts").alias("_last_order_ts")))
    return (agg
            .withColumn(names["aov"], F.when(F.col(names["completed"]) > 0,
                                             F.col(names["spend"]) / F.col(names["completed"])).cast("double"))
            .withColumn(names["recency"], F.datediff(F.col("event_timestamp"), F.col("_last_order_ts")))
            .withColumn("created", F.lit(created_ts).cast("timestamp"))
            .withColumn("_gold_version", F.lit(gold_version))
            .withColumn("_pipeline_run_id", F.lit(run_id))
            .select("customer_id", "event_timestamp", names["orders"], names["completed"], names["spend"],
                    names["aov"], names["recency"], "created", "_gold_version", "_pipeline_run_id"))


def quality_checks(feat_df, n_customers, n_snapshots, window_days):
    """Kiểm tra chất lượng bảng feature; trả về list {check, ok, detail}."""
    names = feature_names(window_days)
    total = feat_df.count()
    stats = feat_df.agg(
        F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer"),
        F.sum(F.col("event_timestamp").isNull().cast("int")).alias("null_event_ts"),
        F.sum(F.col("created").isNull().cast("int")).alias("null_created"),
        F.sum((F.col(names["orders"]) < 0).cast("int")).alias("negative_orders"),
        F.sum((F.col(names["completed"]) > F.col(names["orders"])).cast("int")).alias("completed_gt_total"),
    ).first()
    distinct_keys = feat_df.select("customer_id", "event_timestamp").distinct().count()
    expected = n_customers * n_snapshots
    checks = [
        ("rows == customers x snapshots", total == expected, f"{total} vs {expected}"),
        ("không trùng khóa (customer_id, event_timestamp)", distinct_keys == total, f"{distinct_keys} vs {total}"),
        ("customer_id/event_timestamp/created không NULL",
         not (stats["null_customer"] or stats["null_event_ts"] or stats["null_created"]), "đã đếm NULL"),
        ("feature đếm không âm", not stats["negative_orders"], f"{stats['negative_orders']} dòng âm"),
        ("completed <= total", not stats["completed_gt_total"], f"{stats['completed_gt_total']} dòng sai"),
    ]
    return [{"check": c, "ok": bool(ok), "detail": d} for c, ok, d in checks]


def default_as_of(max_order_ts):
    """Nửa đêm UTC ngay sau ngày có đơn cuối, để mọi đơn đều nằm trong mốc as_of cuối."""
    day = max_order_ts.date() + timedelta(days=1)
    return datetime(day.year, day.month, day.day)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-version", required=True)
    parser.add_argument("--feat-version", required=True)
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--as-of", help="Mốc as_of cuối (ISO, UTC); mặc định ngay sau đơn cuối cùng")
    parser.add_argument("--snapshots", type=int, default=1, help="Số mốc as_of liên tiếp, cách nhau 1 ngày")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    validate_version(args.gold_version)
    validate_version(args.feat_version)
    if not 1 <= args.snapshots <= 400:
        raise ValueError("--snapshots phải trong khoảng 1..400")

    store = S3Store(args.endpoint, args.gold_bucket)
    store.ensure()
    marker_key = f"_features/{args.feat_version}/_SUCCESS.json"
    existing = store.read(marker_key)
    if existing:
        marker = json.loads(existing)
        if marker.get("gold_version") != args.gold_version or marker.get("window_days") != args.window_days:
            raise ValueError("feat-version đã công bố với Gold/cửa sổ khác; chọn --feat-version mới")
        print(json.dumps({"passed": True, "reused": True, "run_id": marker["run_id"]}, indent=2))
        return 0

    gold_marker = json.loads(required_read(store, f"{args.gold_version}/_SUCCESS.json"))
    gold_report = json.loads(required_read(store, gold_marker["report_key"]))
    if gold_report.get("passed") is not True:
        raise ValueError("Gold report chưa passed")
    tables = gold_report["tables"]

    from spark_jobs.dp3_silver_to_gold import make_spark
    spark = make_spark(args.endpoint, args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:12]}"
    started = datetime.now(timezone.utc)
    report = {"passed": False, "run_id": run_id, "feat_version": args.feat_version,
              "gold_version": args.gold_version, "gold_run_id": gold_marker["run_id"],
              "window_days": args.window_days, "started_at": started.isoformat()}
    try:
        fact = spark.read.parquet(tables["fact_orders"]["path"])
        dim = spark.read.parquet(tables["dim_customer"]["path"])
        orders = (fact.join(dim.select("customer_sk", "customer_id"), "customer_sk")
                  .select("customer_id", "order_ts", "is_completed", "total_amount"))
        max_ts = fact.agg(F.max("order_ts")).first()[0]
        if max_ts is None:
            raise ValueError("fact_orders rỗng")
        last = datetime.fromisoformat(args.as_of.replace("Z", "")) if args.as_of else default_as_of(max_ts)
        as_of_list = sorted(last - timedelta(days=k) for k in range(args.snapshots))
        customers = dim.select("customer_id").distinct()
        n_customers = customers.count()

        feat = build_feat_customer_orders(spark, orders, customers, as_of_list, args.window_days,
                                          args.gold_version, run_id, started.replace(tzinfo=None))
        table = f"feat_customer_orders_{args.window_days}d"
        path = store.uri(f"_features/{args.feat_version}/runs/{run_id}/{table}")
        feat.write.mode("errorifexists").parquet(path)
        written = spark.read.parquet(path)
        checks = quality_checks(written, n_customers, args.snapshots, args.window_days)
        report.update(table=table, path=path, rows=written.count(), customers=n_customers,
                      as_of=[t.isoformat() for t in as_of_list], checks=checks,
                      columns=written.columns)
        if not all(c["ok"] for c in checks):
            raise ValueError("Feature quality checks thất bại: " +
                             "; ".join(c["check"] for c in checks if not c["ok"]))
        report.update(passed=True, finished_at=datetime.now(timezone.utc).isoformat())
        report_key = f"_features/{args.feat_version}/runs/{run_id}/feat_report.json"
        data = encode_json(report)
        store.write(report_key, data)
        marker = {"run_id": run_id, "gold_version": args.gold_version, "window_days": args.window_days,
                  "report_key": report_key, "report_sha256": digest(data), "rows": report["rows"],
                  "tables": {table: path}, "completed_at": report["finished_at"]}
        if not store.write(marker_key, encode_json(marker), if_absent=True):
            raise ValueError("Job khác đã công bố feat-version này")
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
    finally:
        out = Path(args.out) / run_id
        out.mkdir(parents=True, exist_ok=True)
        (out / "feat_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str),
                                              encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        spark.stop()
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
