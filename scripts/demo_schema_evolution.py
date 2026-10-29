"""Minh họa vấn đề schema evolution của `payments` (v1 thiếu `failure_reason`, v2 có) và cách DP2 xử lý.

So sánh ba cách đọc cùng một tập Parquet trộn v1 và v2:
  default       spark.read.parquet(...)            -> schema lấy từ file đầu/ngẫu nhiên, có thể mất cột `failure_reason`
  mergeSchema   option("mergeSchema", true)        -> đúng cột, nhưng tốn thêm bước đọc footer mọi file và kiểu có thể xung đột
  explicit      spark.read.schema(schema_for(...)) -> cách DP2 dùng: schema cố định, file v1 đọc ra NULL
Chạy trong image Spark: spark-submit /opt/app/scripts/demo_schema_evolution.py --payments-dir <thư mục Parquet payments>.
"""
import argparse
import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/app")
sys.path.insert(0, ".")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payments-dir", required=True, help="Thư mục chứa các file Parquet payments (v1 và v2 trộn)")
    args = parser.parse_args()
    from spark_jobs.silver_contracts import schema_for
    spark = SparkSession.builder.master("local[2]").appName("EDAI-demo-schema-evolution") \
        .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    path = args.payments_dir.rstrip("/") + "/*.parquet"
    readers = {
        "default": lambda: spark.read.parquet(path),
        "mergeSchema": lambda: spark.read.option("mergeSchema", "true").parquet(path),
        "explicit": lambda: spark.read.schema(schema_for("payments")).parquet(path),
    }
    report = {}
    for name, read in readers.items():
        t0 = time.perf_counter()
        df = read()
        has_col = "failure_reason" in df.columns
        info = {"columns": df.columns, "has_failure_reason": has_col}
        if has_col:
            info["failure_reason_non_null"] = df.filter(F.col("failure_reason").isNotNull()).count()
        info["rows"] = df.count()
        info["seconds"] = round(time.perf_counter() - t0, 2)
        report[name] = info
    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
