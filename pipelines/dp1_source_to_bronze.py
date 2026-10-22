"""DP1: sao chép nguồn Parquet đã hoàn tất sang Bronze, kiểm chứng từng file.

Chạy: python -m pipelines.dp1_source_to_bronze --help
Mỗi dataset_version chỉ có một writer. Không làm sạch hay khử trùng ở Bronze.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as ClientConfig
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from edai_generator.schemas import SCHEMAS, schema_for


PIPELINE_VERSION = "1.0.0"
METADATA_LIMIT = 64 * 1024 * 1024
READ_CHUNK = 8 * 1024 * 1024
MISSING = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def head_optional(s3, bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in MISSING:
            raise
        return None


def read_metadata(s3, bucket, key):
    """Metadata được giới hạn 64 MiB; dữ liệu Parquet đi qua hàm streaming riêng."""
    result = s3.get_object(Bucket=bucket, Key=key)
    body = result["Body"]
    try:
        data = body.read(METADATA_LIMIT + 1)
    finally:
        body.close()
    if len(data) > METADATA_LIMIT:
        raise ValueError(f"Metadata exceeds 64 MiB: {key}")
    return data


def validate_prefix(value, label):
    # Giới hạn tên rõ ràng, tránh đường dẫn '..' và ký tự điều khiển.
    if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", value):
        raise ValueError(f"{label}: use letters, digits, '_' or '-' separated by '/'")


def load_source_contract(s3, bucket, prefix):
    """Kiểm tra marker, fingerprint, inventory và tổng số file/dòng/byte."""
    snapshots = {}
    ready_key = f"{prefix}/_READY.json"
    if head_optional(s3, bucket, ready_key) is None:
        raise ValueError(f"Missing s3://{bucket}/{ready_key}; finish the generator upload first")
    for suffix in ("_READY.json", "metadata/manifest.json", "metadata/config.json", "metadata/file_index.jsonl"):
        snapshots[suffix] = read_metadata(s3, bucket, f"{prefix}/{suffix}")
    ready = json.loads(snapshots["_READY.json"])
    manifest = json.loads(snapshots["metadata/manifest.json"])
    config = json.loads(snapshots["metadata/config.json"])
    index_bytes = snapshots["metadata/file_index.jsonl"]
    index_hash = sha256(index_bytes)
    if index_hash != ready.get("file_index_sha256"):
        raise ValueError("Source file_index SHA256 does not match _READY.json")
    if manifest.get("status") != "complete":
        raise ValueError("Source manifest is not complete")
    if "generator_version" not in manifest or "generator_version" in config:
        raise ValueError("Invalid generator version in source contract")
    fingerprint = sha256(json.dumps(
        {"generator_version": manifest["generator_version"], **config}, sort_keys=True
    ).encode())
    if fingerprint != ready.get("source_fingerprint") or fingerprint != manifest.get("config_fingerprint"):
        raise ValueError("Source config/manifest/marker fingerprints disagree")
    if ready.get("source_prefix") != f"{prefix}/source/offline/" or ready.get("manifest_key") != f"{prefix}/metadata/manifest.json":
        raise ValueError("_READY.json points to a different source prefix")

    entries = [json.loads(line) for line in index_bytes.decode("utf-8").splitlines() if line.strip()]
    totals = defaultdict(lambda: {"rows": 0, "physical_bytes": 0, "files": 0})
    seen = set()
    for entry in entries:
        table, path = entry.get("table"), entry.get("path", "")
        if table not in SCHEMAS or not re.fullmatch(
            rf"source/offline/{re.escape(table)}/part-[0-9]{{12,}}\.parquet", path
        ):
            raise ValueError(f"Invalid source inventory path/table: {path}")
        if path in seen:
            raise ValueError(f"Repeated inventory path: {path}")
        seen.add(path)
        if any(type(entry.get(key)) is not int or entry[key] <= 0 for key in ("rows", "bytes")):
            raise ValueError(f"Invalid row/byte count: {path}")
        if not re.fullmatch(r"[0-9a-f]{64}", entry.get("sha256", "")):
            raise ValueError(f"Invalid SHA256: {path}")
        versions = (1, 2) if table == "payments" else (1,)
        if type(entry.get("schema_version")) is not int or entry["schema_version"] not in versions:
            raise ValueError(f"Unsupported schema version: {path}")
        totals[table]["rows"] += entry["rows"]
        totals[table]["physical_bytes"] += entry["bytes"]
        totals[table]["files"] += 1
    if set(totals) != set(SCHEMAS) or set(manifest.get("tables", {})) != set(SCHEMAS):
        raise ValueError("Expected all six generator tables in inventory and manifest")
    for table, counts in totals.items():
        if any(manifest["tables"][table].get(key) != value for key, value in counts.items()):
            raise ValueError(f"Inventory and manifest totals disagree: {table}")
    total_bytes = sum(e["bytes"] for e in entries)
    if ready.get("files") != len(entries) or ready.get("physical_source_bytes") != total_bytes or manifest.get("physical_source_bytes") != total_bytes:
        raise ValueError("Source file/byte totals disagree")
    return entries, dict(totals), snapshots, fingerprint, index_hash


def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in MISSING:
            raise
        s3.create_bucket(Bucket=bucket)


def bind_dataset(s3, bucket, prefix, identity):
    """Một phiên bản Bronze chỉ được gắn với một snapshot nguồn."""
    key = f"{prefix}/_DATASET.json"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json_bytes(identity),
                      ContentType="application/json", IfNoneMatch="*")
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in {"412", "PreconditionFailed"}:
            raise
        if json.loads(read_metadata(s3, bucket, key)) != identity:
            raise ValueError("Bronze dataset_version belongs to a different source; choose a new version") from exc


def assert_object_metadata(head, entry, fingerprint, key, index_hash=None):
    if head is None:
        raise ValueError(f"Missing object: {key}")
    expected = {"sha256": entry["sha256"], "source-fingerprint": fingerprint}
    if index_hash is not None:
        expected["source-index-sha256"] = index_hash
    if head["ContentLength"] != entry["bytes"] or any(head.get("Metadata", {}).get(k) != v for k, v in expected.items()):
        raise ValueError(f"Object size/ownership/checksum metadata mismatch: {key}")


def verify_parquet(s3, bucket, key, entry, head):
    """Đọc lại toàn bộ byte, SHA256, số dòng và schema; chỉ giữ một shard trên đĩa."""
    result = s3.get_object(Bucket=bucket, Key=key, IfMatch=head["ETag"])
    body = result["Body"]
    digest = hashlib.sha256()
    size = 0
    try:
        with tempfile.TemporaryFile() as tmp:
            while True:
                chunk = body.read(READ_CHUNK)
                if not chunk:
                    break
                size += len(chunk)
                if size > entry["bytes"]:
                    raise ValueError(f"Object is larger than expected: {key}")
                digest.update(chunk)
                tmp.write(chunk)
            if size != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
                raise ValueError(f"Actual byte SHA256 mismatch: {key}")
            tmp.seek(0)
            parquet = pq.ParquetFile(tmp)
            if parquet.metadata.num_rows != entry["rows"]:
                raise ValueError(f"Parquet row count mismatch: {key}")
            if not parquet.schema_arrow.equals(schema_for(entry["table"], entry["schema_version"]), check_metadata=False):
                raise ValueError(f"Parquet schema mismatch: {key}")
    finally:
        body.close()


def save_local_report(directory, report):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "ingestion_report.json").write_bytes(json_bytes(report))


def run_pipeline(s3, *, source_prefix, dataset_version, out,
                 source_bucket="source-department", bronze_bucket="bronze"):
    """Thực hiện một lần ingest. Lỗi được ghi lại và truyền lên để CLI exit != 0."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    local = Path(out) / run_id
    local.mkdir(parents=True, exist_ok=True)
    report = {"pipeline": "dp1_source_to_bronze", "pipeline_version": PIPELINE_VERSION,
              "run_id": run_id, "started_at": utc_now(), "passed": False,
              "source_bucket": source_bucket, "source_prefix": source_prefix,
              "bronze_bucket": bronze_bucket, "dataset_version": dataset_version,
              "copied_files": 0, "skipped_files": 0, "verified_files": 0,
              "verified_bytes": 0, "local_report": str(local / "ingestion_report.json")}
    try:
        validate_prefix(source_prefix, "source_prefix")
        validate_prefix(dataset_version, "dataset_version")
        if source_bucket == bronze_bucket:
            raise ValueError("Use separate source and Bronze buckets")
        entries, totals, snapshots, fingerprint, index_hash = load_source_contract(s3, source_bucket, source_prefix)
        report.update(source_fingerprint=fingerprint, file_index_sha256=index_hash,
                      tables=totals, expected_files=len(entries))

        # Kiểm tra đầy đủ nguồn trước khi ghi Bronze.
        source_heads = {}
        for entry in entries:
            key = f"{source_prefix}/{entry['path']}"
            head = head_optional(s3, source_bucket, key)
            assert_object_metadata(head, entry, fingerprint, key)
            source_heads[key] = head
        ensure_bucket(s3, bronze_bucket)
        identity = {"source_bucket": source_bucket, "source_prefix": source_prefix,
                    "source_fingerprint": fingerprint, "file_index_sha256": index_hash,
                    "source_manifest_sha256": sha256(snapshots["metadata/manifest.json"]),
                    "source_config_sha256": sha256(snapshots["metadata/config.json"])}
        bind_dataset(s3, bronze_bucket, dataset_version, identity)
        transfer = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                                  multipart_chunksize=64 * 1024 * 1024, max_concurrency=4)
        expected_keys = set()
        ledger_path = local / "ingest_ledger.jsonl"
        with ledger_path.open("w", encoding="utf-8") as ledger:
            for entry in entries:
                source_key = f"{source_prefix}/{entry['path']}"
                relative = entry["path"].removeprefix("source/offline/")
                target_key = f"{dataset_version}/source_files/{relative}"
                expected_keys.add(target_key)
                target_head = head_optional(s3, bronze_bucket, target_key)
                if target_head is None:
                    copied_at = utc_now()
                    s3.copy({"Bucket": source_bucket, "Key": source_key}, bronze_bucket, target_key,
                            ExtraArgs={"CopySourceIfMatch": source_heads[source_key]["ETag"],
                                       "MetadataDirective": "REPLACE", "Metadata": {
                                           "sha256": entry["sha256"], "source-fingerprint": fingerprint,
                                           "source-index-sha256": index_hash,
                                           "ingested-at": copied_at, "ingest-run-id": run_id,
                                           "pipeline-version": PIPELINE_VERSION}}, Config=transfer)
                    report["copied_files"] += 1
                    action = "copied"
                    target_head = head_optional(s3, bronze_bucket, target_key)
                else:
                    report["skipped_files"] += 1
                    action = "verified_skip"
                assert_object_metadata(target_head, entry, fingerprint, target_key, index_hash)
                verify_parquet(s3, bronze_bucket, target_key, entry, target_head)
                report["verified_files"] += 1
                report["verified_bytes"] += entry["bytes"]
                record = {**entry, "source_bucket": source_bucket, "source_key": source_key,
                          "bronze_bucket": bronze_bucket, "bronze_key": target_key,
                          "action": action, "verification_run_id": run_id, "verified_at": utc_now(),
                          "ingested_at": target_head["Metadata"].get("ingested-at"),
                          "ingest_run_id": target_head["Metadata"].get("ingest-run-id")}
                ledger.write(json.dumps(record, ensure_ascii=False) + "\n")
                ledger.flush()
                if report["verified_files"] % 100 == 0:
                    print(json.dumps({k: report[k] for k in ("verified_files", "copied_files", "skipped_files", "verified_bytes")}), flush=True)

        # Không chấp nhận file lạ khiến lần đọc bằng glob bị đếm thừa.
        actual_keys = set()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bronze_bucket, Prefix=f"{dataset_version}/source_files/"):
            actual_keys.update(item["Key"] for item in page.get("Contents", []))
        if actual_keys != expected_keys:
            raise ValueError("Bronze source_files contains missing or untracked objects")
        # Nguồn phải bất biến trong cả lần chạy, kể cả lần chỉ verify-skip.
        for key, head in source_heads.items():
            current = head_optional(s3, source_bucket, key)
            if current is None or any(current.get(k) != head.get(k) for k in ("ETag", "ContentLength", "Metadata")):
                raise ValueError(f"Source changed during ingestion: {key}")
        for suffix, data in snapshots.items():
            if read_metadata(s3, source_bucket, f"{source_prefix}/{suffix}") != data:
                raise ValueError(f"Source contract changed during ingestion: {suffix}")

        audit_prefix = f"{dataset_version}/_runs/{run_id}"
        for suffix, data in snapshots.items():
            name = "source_" + suffix.rsplit("/", 1)[-1]
            s3.put_object(Bucket=bronze_bucket, Key=f"{audit_prefix}/{name}", Body=data)
        ledger_key = f"{audit_prefix}/ingest_ledger.jsonl"
        s3.upload_file(str(ledger_path), bronze_bucket, ledger_key)
        report.update(passed=True, finished_at=utc_now(), ledger_key=ledger_key,
                      verification="Full destination-byte SHA256, Parquet row counts and physical schemas; unchanged source snapshot",
                      success_key=f"{dataset_version}/_SUCCESS.json")
        report_key = f"{audit_prefix}/ingestion_report.json"
        report_bytes = json_bytes(report)
        s3.put_object(Bucket=bronze_bucket, Key=report_key, Body=report_bytes, ContentType="application/json")
        # Marker xuất bản sau cùng. Consumer chỉ đọc phiên bản có marker này.
        marker = {**identity, "pipeline_version": PIPELINE_VERSION, "run_id": run_id,
                  "completed_at": report["finished_at"], "verified_files": report["verified_files"],
                  "verified_bytes": report["verified_bytes"], "report_key": report_key,
                  "report_sha256": sha256(report_bytes), "ledger_key": ledger_key}
        s3.put_object(Bucket=bronze_bucket, Key=report["success_key"],
                      Body=json_bytes(marker), ContentType="application/json")
        save_local_report(local, report)
        return report
    except Exception as exc:
        report.update(passed=False, finished_at=utc_now(), error=f"{type(exc).__name__}: {exc}")
        save_local_report(local, report)
        print(f"DP1 failed. Report: {local / 'ingestion_report.json'}", file=sys.stderr, flush=True)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-prefix", required=True, help="Example: synthetic/smoke")
    parser.add_argument("--dataset-version", required=True, help="Example: smoke_v1")
    parser.add_argument("--source-bucket", default="source-department")
    parser.add_argument("--bronze-bucket", default="bronze")
    parser.add_argument("--out", default="/data/bronze", help="Directory for local run reports")
    parser.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://localhost:9010"))
    args = parser.parse_args(argv)
    try:
        if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
            raise ValueError("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to your storage credentials")
        s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name="us-east-1", config=ClientConfig(
            s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
            request_checksum_calculation="when_required", response_checksum_validation="when_required"))
        report = run_pipeline(s3, source_prefix=args.source_prefix, dataset_version=args.dataset_version,
                              source_bucket=args.source_bucket, bronze_bucket=args.bronze_bucket, out=args.out)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
