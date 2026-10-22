"""Xem nhanh Gold snapshot đã được công bố. Chạy bằng spark-submit, không ghi dữ liệu."""

import argparse
import json
import os

from pyspark.sql import functions as F
from spark_jobs.dp3_silver_to_gold import make_spark
from spark_jobs.snapshot_store import S3Store, required_read, validate_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-version", required=True)
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    parser.add_argument("--customer-id", type=int, default=1,
                        help="ID khách hàng để xem lịch sử mua hàng")
    args = parser.parse_args()

    validate_version(args.gold_version)
    store = S3Store(args.endpoint, args.gold_bucket)

    # Đọc Gold SUCCESS marker
    marker_data = required_read(store, f"{args.gold_version}/_SUCCESS.json")
    marker = json.loads(marker_data)
    print(f"Gold run_id: {marker['run_id']}")
    print(f"Completed at: {marker['completed_at']}")

    # Đọc Gold report
    report_data = required_read(store, marker["report_key"])
    report = json.loads(report_data)
    table_rows = {name: info["rows"] for name, info in report["tables"].items()}
    print("\nRow counts:")
    print(json.dumps(table_rows, indent=2, ensure_ascii=False))

    spark = make_spark(args.endpoint, 4)
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setJobDescription("Read-only Gold inspection")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    tables = report["tables"]
    try:
        # Xem mẫu dim_customer
        print("\n=== dim_customer (10 hàng) ===")
        dim_customer = spark.read.parquet(tables["dim_customer"]["path"])
        dim_customer.select(
            "customer_sk", "customer_id", "customer_name", "city", "segment", "device_type"
        ).orderBy("customer_id").show(10, truncate=False)

        # Xem mẫu fact_orders
        print("\n=== fact_orders (10 hàng) ===")
        fact_orders = spark.read.parquet(tables["fact_orders"]["path"])
        fact_orders.select(
            "order_id", "customer_sk", "date_sk", "status", "total_amount", "is_completed", "has_coupon"
        ).orderBy("order_id").show(10, truncate=False)

        # Xem agg_daily_sales
        print("\n=== agg_daily_sales (10 hàng, sắp xếp theo ngày giảm dần) ===")
        agg_daily = spark.read.parquet(tables["agg_daily_sales"]["path"])
        agg_daily.select(
            "agg_date", "category", "shipping_city",
            "orders_count", "completed_orders_count",
            "gross_revenue_vnd", "net_revenue_vnd",
        ).orderBy(F.col("agg_date").desc()).show(10, truncate=False)

        # Xem agg_customer_ltv cho khách hàng cụ thể
        print(f"\n=== agg_customer_ltv cho customer_id={args.customer_id} ===")
        agg_ltv = spark.read.parquet(tables["agg_customer_ltv"]["path"])
        agg_ltv.filter(F.col("customer_id") == args.customer_id).show(5, truncate=False)

        # Thống kê tổng quan
        print("\n=== Thống kê tổng quan ===")
        completed = fact_orders.filter("is_completed").agg(
            F.count("order_id").alias("completed_orders"),
            F.sum(F.col("total_amount").cast("decimal(38,0)")).alias("total_revenue_vnd"),
        ).first()
        print(f"Tổng đơn completed: {completed['completed_orders']}")
        print(f"Tổng doanh thu: {completed['total_revenue_vnd']:,} VND")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

