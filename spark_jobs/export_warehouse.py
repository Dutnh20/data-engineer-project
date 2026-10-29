"""Xuất các bảng Gold sang CSV và sinh DDL PostgreSQL để nạp vào data warehouse.

Mỗi bảng -> <out>/<bảng>/<bảng>.csv (có header, 1 file) và <out>/schema.sql (CREATE TABLE sinh từ schema Spark).
Dùng cho thử nghiệm indexing trong warehouse_sql/. Chỉ xuất các bảng cần cho truy vấn phân tích.
"""
import argparse
import json
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import types as T

TABLES = ["dim_customer", "dim_product", "dim_date", "fact_orders", "fact_order_items"]


def pg_type(dtype):
    """Ánh xạ kiểu Spark sang PostgreSQL."""
    if isinstance(dtype, T.DecimalType):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    return {T.LongType: "BIGINT", T.IntegerType: "INTEGER", T.ShortType: "SMALLINT", T.StringType: "TEXT",
            T.DoubleType: "DOUBLE PRECISION", T.FloatType: "REAL", T.BooleanType: "BOOLEAN",
            T.DateType: "DATE", T.TimestampType: "TIMESTAMP"}.get(type(dtype), "TEXT")


def create_table_sql(name, schema):
    """CREATE TABLE cho một schema Spark (mọi cột cho phép NULL, index tạo riêng sau khi nạp)."""
    cols = ",\n  ".join(f'"{f.name}" {pg_type(f.dataType)}' for f in schema.fields)
    return f'DROP TABLE IF EXISTS "{name}";\nCREATE TABLE "{name}" (\n  {cols}\n);\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-version", required=True)
    parser.add_argument("--gold-bucket", default="gold")
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    sys.path.insert(0, "/opt/app")
    from spark_jobs.dp3_silver_to_gold import make_spark
    from spark_jobs.snapshot_store import S3Store, required_read
    store = S3Store(args.endpoint, args.gold_bucket)
    marker = json.loads(required_read(store, f"{args.gold_version}/_SUCCESS.json"))
    tables = json.loads(required_read(store, marker["report_key"]))["tables"]
    spark = make_spark(args.endpoint, 8)
    spark.sparkContext.setLogLevel("WARN")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    ddl, counts = [], {}
    try:
        for name in TABLES:
            df = spark.read.parquet(tables[name]["path"])
            tmp = out / f"_tmp_{name}"
            shutil.rmtree(tmp, ignore_errors=True)
            df.coalesce(1).write.option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .option("nullValue", "").csv(str(tmp))
            part = next(tmp.glob("part-*.csv"))
            (out / name).mkdir(exist_ok=True)
            shutil.move(str(part), str(out / name / f"{name}.csv"))
            shutil.rmtree(tmp, ignore_errors=True)
            ddl.append(create_table_sql(name, df.schema))
            counts[name] = tables[name].get("rows")
        (out / "schema.sql").write_text("\n".join(ddl), encoding="utf-8")
        (out / "export_report.json").write_text(json.dumps({"gold_version": args.gold_version, "rows": counts}, indent=2))
        print(json.dumps({"passed": True, "gold_version": args.gold_version, "rows": counts, "out": str(out)}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
