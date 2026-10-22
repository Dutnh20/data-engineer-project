"""Read a published Silver snapshot and show a small sample. No data writes."""
import argparse
import json
import os

from pyspark.sql import functions as F
from spark_jobs.dp2_bronze_to_silver import make_spark
from spark_jobs.snapshot_store import S3Store, published_report, required_read, validate_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-version", required=True)
    parser.add_argument("--silver-bucket", default="silver")
    parser.add_argument("--customer-id", type=int, default=20)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    args = parser.parse_args()
    validate_version(args.silver_version)
    store = S3Store(args.endpoint, args.silver_bucket)
    marker = json.loads(required_read(store, f"{args.silver_version}/_SUCCESS.json"))
    report = published_report(store, args.silver_version, marker["identity"])
    print("Published run:", report["run_id"])
    print("Validated table counts:", json.dumps({name: value["rows"] for name, value in report["tables"].items()}, indent=2))
    spark = make_spark(args.endpoint, 4)
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setJobDescription("Read-only Silver inspection")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        items = spark.read.parquet(report["tables"]["stg_order_items"]["path"])
        print("Ten cleaned order items:")
        items.select("order_item_id", "order_id", "product_id", "quantity", "line_amount", "created_ts").orderBy("order_item_id").show(10, truncate=False)
        history = spark.read.parquet(report["tables"]["stg_customer_history"]["path"])
        print(f"Customer {args.customer_id} history (UTC):")
        history.filter(F.col("customer_id") == args.customer_id).select(
            "customer_id", "segment", "valid_from_ts", "valid_to_ts", "is_current"
        ).orderBy("valid_from_ts").show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
