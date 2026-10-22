"""DP2a: raw Bronze -> validated Silver. Submit bằng spark-submit, không chạy qua generator CLI."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import uuid

from spark_jobs.snapshot_store import (S3Store, assert_bronze_unchanged, bind_version, digest, encode_json,
                                      inspect_bronze, published_report, validate_version)
from spark_jobs.silver_transforms import transform_snapshot


PIPELINE_VERSION = "1.0.0"


def now():
    return datetime.now(timezone.utc).isoformat()


def code_fingerprint():
    root = Path(__file__).parent
    names = ("silver_contracts.py", "silver_transforms.py", "snapshot_store.py", "dp2_bronze_to_silver.py")
    return digest(b"\n".join(name.encode() + b"\n" + (root / name).read_bytes() for name in names))


def run_pipeline(bronze, silver, *, bronze_version, silver_version, out, spark_factory):
    """Một version được công bố đúng một lần; rerun kiểm tra và tái sử dụng snapshot."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    local = Path(out) / run_id
    local.mkdir(parents=True, exist_ok=True)
    report = {"pipeline": "dp2a_bronze_to_silver", "pipeline_version": PIPELINE_VERSION,
              "run_id": run_id, "started_at": now(), "passed": False,
              "bronze_bucket": bronze.bucket, "bronze_version": bronze_version,
              "silver_bucket": silver.bucket, "silver_version": silver_version}
    spark = None
    storage_ready = False
    try:
        validate_version(bronze_version)
        validate_version(silver_version)
        if bronze.bucket == silver.bucket:
            raise ValueError("Bronze and Silver must use separate buckets")
        contract = inspect_bronze(bronze, bronze_version)
        identity = {"bronze_bucket": bronze.bucket, "bronze_version": bronze_version,
                    "silver_version": silver_version, **contract["identity"],
                    "pipeline_version": PIPELINE_VERSION, "code_sha256": code_fingerprint(),
                    "spark_version": "3.5.6"}
        report.update(identity=identity, bronze_run_id=contract["marker"]["run_id"])
        silver.ensure()
        bind_version(silver, silver_version, identity)
        storage_ready = True
        existing = published_report(silver, silver_version, identity)
        if existing is not None:
            assert_bronze_unchanged(bronze, bronze_version, contract)
            report.update(passed=True, status="already_published", reused_run_id=existing["run_id"],
                          tables=existing["tables"], finished_at=now(),
                          verification="Existing output inventory (keys, lengths and ETags) matches the published report")
            (local / "silver_report.json").write_bytes(encode_json(report))
            return report

        spark = spark_factory()
        if spark.version != "3.5.6":
            raise ValueError(f"Expected Spark 3.5.6, got {spark.version}")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        spark.sparkContext.setLogLevel("WARN")
        report["spark"] = {"version": spark.version, "application_id": spark.sparkContext.applicationId,
                           "master": spark.sparkContext.master,
                           "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                           "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled")}
        run_prefix = f"{silver_version}/_runs/{run_id}"
        transform_snapshot(spark, contract["paths"], contract["rows"],
                           silver.uri(f"{run_prefix}/tables"), silver.uri(f"{run_prefix}/quarantine"),
                           silver_version, run_id, report["started_at"], report)
        assert_bronze_unchanged(bronze, bronze_version, contract)
        inventory = silver.listing(f"{run_prefix}/tables/")
        if not inventory or not any(row["key"].endswith(".parquet") for row in inventory):
            raise ValueError("Silver output inventory has no Parquet data")
        report.update(passed=True, status="published", finished_at=now(), output_inventory=inventory)
        report_key = f"{run_prefix}/silver_report.json"
        report_bytes = encode_json(report)
        silver.write(report_key, report_bytes)
        marker = {"identity": identity, "run_id": run_id, "completed_at": report["finished_at"],
                  "report_key": report_key, "report_sha256": digest(report_bytes),
                  "tables": {name: info["path"] for name, info in report["tables"].items()}}
        if not silver.write(f"{silver_version}/_SUCCESS.json", encode_json(marker), if_absent=True):
            raise ValueError("Another job published this version; use one writer and rerun to inspect its result")
        (local / "silver_report.json").write_bytes(report_bytes)
        return report
    except Exception as exc:
        report.update(passed=False, status="failed", finished_at=now(), error=f"{type(exc).__name__}: {exc}")
        data = encode_json(report)
        (local / "silver_report.json").write_bytes(data)
        if storage_ready:
            try:
                silver.write(f"{silver_version}/_runs/{run_id}/failed_report.json", data)
            except Exception as write_error:
                print(f"Could not persist remote failure report: {type(write_error).__name__}", file=sys.stderr)
        print(f"DP2 failed; local report: {local / 'silver_report.json'}", file=sys.stderr, flush=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


def make_spark(endpoint, shuffle_partitions):
    from pyspark.sql import SparkSession
    if shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions must be positive")
    builder = (SparkSession.builder.appName("EDAI-DP2-Bronze-to-Silver")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.files.maxRecordsPerFile", "500000")
               .config("spark.hadoop.fs.s3a.endpoint", endpoint)
               .config("spark.hadoop.fs.s3a.path.style.access", "true")
               .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
               .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
    eventlog = os.getenv("SPARK_EVENTLOG_DIR")
    if eventlog:
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", eventlog)
    spark = builder.getOrCreate()
    hadoop = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    if hadoop != "3.3.4":
        spark.stop()
        raise ValueError(f"Expected Hadoop 3.3.4 to match S3A jars, got {hadoop}")
    return spark


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-version", required=True)
    parser.add_argument("--silver-version", required=True)
    parser.add_argument("--bronze-bucket", default="bronze")
    parser.add_argument("--silver-bucket", default="silver")
    parser.add_argument("--out", default="/data/silver")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"))
    args = parser.parse_args(argv)
    try:
        if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
            raise ValueError("Set the storage credentials in the Compose environment")
        result = run_pipeline(S3Store(args.endpoint, args.bronze_bucket), S3Store(args.endpoint, args.silver_bucket),
                              bronze_version=args.bronze_version, silver_version=args.silver_version, out=args.out,
                              spark_factory=lambda: make_spark(args.endpoint, args.shuffle_partitions))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    summary = {k: result[k] for k in ("passed", "status", "run_id", "bronze_version", "silver_version")}
    summary["table_rows"] = {name: info["rows"] for name, info in result["tables"].items()}
    if "duplicate_amount_removed_vnd" in result:
        summary["duplicate_amount_removed_vnd"] = result["duplicate_amount_removed_vnd"]
    if "reused_run_id" in result:
        summary["reused_run_id"] = result["reused_run_id"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
