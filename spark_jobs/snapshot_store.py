"""Control files cho snapshot nhiều bảng. Spark xử lý data; boto3 xử lý metadata."""

import hashlib
import json
import re

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


TABLES = {"customers", "products", "orders", "order_items", "payments", "customer_changes"}
NOT_FOUND = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encode_json(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def validate_version(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("Dataset version must contain only letters, digits, '_' and '-'")


class S3Store:
    def __init__(self, endpoint, bucket):
        self.bucket = bucket
        self.client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1", config=Config(
            s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
            request_checksum_calculation="when_required", response_checksum_validation="when_required"))

    def ensure(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if str(exc.response["Error"]["Code"]) not in NOT_FOUND:
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def read(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if str(exc.response["Error"]["Code"]) in NOT_FOUND:
                return None
            raise
        try:
            data = response["Body"].read(64 * 1024 * 1024 + 1)
        finally:
            response["Body"].close()
        if len(data) > 64 * 1024 * 1024:
            raise ValueError(f"Control file exceeds 64 MiB: {key}")
        return data

    def write(self, key, data, if_absent=False):
        kwargs = {"IfNoneMatch": "*"} if if_absent else {}
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="application/json", **kwargs)
            return True
        except ClientError as exc:
            if if_absent and str(exc.response["Error"]["Code"]) in {"412", "PreconditionFailed"}:
                return False
            raise

    def head(self, key):
        try:
            value = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if str(exc.response["Error"]["Code"]) in NOT_FOUND:
                return None
            raise
        return {"key": key, "bytes": value["ContentLength"], "etag": value["ETag"], "metadata": value.get("Metadata", {})}

    def listing(self, prefix):
        objects = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            objects.extend({"key": obj["Key"], "bytes": obj["Size"], "etag": obj["ETag"]} for obj in page.get("Contents", []))
        return sorted(objects, key=lambda obj: obj["key"])

    def uri(self, key):
        return f"s3a://{self.bucket}/{key}"


def required_read(store, key):
    data = store.read(key)
    if data is None:
        raise ValueError(f"Missing {store.bucket}/{key}; complete the previous pipeline first")
    return data


def bronze_identity(marker):
    return {key: marker[key] for key in ("source_fingerprint", "file_index_sha256",
                                       "source_manifest_sha256", "source_config_sha256")}


def inspect_bronze(store, version):
    """Chỉ đọc đúng file đã được DP1 công bố; không đọc ngược source-department."""
    validate_version(version)
    marker = json.loads(required_read(store, f"{version}/_SUCCESS.json"))
    run_id = marker["run_id"]
    validate_version(run_id)
    audit = f"{version}/_runs/{run_id}"
    if marker["report_key"] != f"{audit}/ingestion_report.json" or marker["ledger_key"] != f"{audit}/ingest_ledger.jsonl":
        raise ValueError("Bronze marker points outside its published run")
    report_bytes = required_read(store, marker["report_key"])
    if digest(report_bytes) != marker["report_sha256"]:
        raise ValueError("Bronze report checksum mismatch")
    report = json.loads(report_bytes)
    if report.get("passed") is not True or report.get("run_id") != run_id:
        raise ValueError("Bronze report is not a successful published run")
    index_bytes = required_read(store, f"{audit}/source_file_index.jsonl")
    if digest(index_bytes) != marker["file_index_sha256"]:
        raise ValueError("Bronze source inventory checksum mismatch")
    index = [json.loads(line) for line in index_bytes.decode().splitlines() if line.strip()]
    ledger = [json.loads(line) for line in required_read(store, marker["ledger_key"]).decode().splitlines() if line.strip()]
    by_path = {row["path"]: row for row in ledger}
    if len(index) != len(ledger) or len(by_path) != len(ledger) or len(index) != marker["verified_files"]:
        raise ValueError("Bronze inventory/ledger file counts disagree")
    paths = {t: [] for t in TABLES}
    counts = {t: 0 for t in TABLES}
    heads = {}
    seen = set()
    for row in index:
        table, path = row.get("table"), row.get("path", "")
        if table not in TABLES or not re.fullmatch(rf"source/offline/{re.escape(table)}/part-[0-9]{{12,}}\.parquet", path) or path in seen:
            raise ValueError(f"Invalid or duplicate inventory entry: {path}")
        seen.add(path)
        ledger_row = by_path.get(path, {})
        if any(ledger_row.get(k) != row[k] for k in ("table", "rows", "bytes", "sha256", "schema_version")):
            raise ValueError(f"Bronze ledger disagrees with inventory: {path}")
        key = f"{version}/source_files/" + path.removeprefix("source/offline/")
        if ledger_row.get("bronze_key") != key or ledger_row.get("bronze_bucket") != store.bucket:
            raise ValueError("Bronze ledger points to a different bucket or version")
        head = store.head(key)
        if head is None or head["bytes"] != row["bytes"]:
            raise ValueError(f"Bronze object missing or size changed: {key}")
        expected = {"sha256": row["sha256"], "source-fingerprint": marker["source_fingerprint"],
                    "source-index-sha256": marker["file_index_sha256"]}
        if any(head["metadata"].get(k) != v for k, v in expected.items()):
            raise ValueError(f"Bronze object metadata changed: {key}")
        if row["schema_version"] not in ((1, 2) if table == "payments" else (1,)):
            raise ValueError(f"Unsupported Bronze schema: {path}")
        paths[table].append(store.uri(key))
        counts[table] += row["rows"]
        heads[key] = head
    if any(not values for values in paths.values()):
        raise ValueError("Bronze requires all six source tables")
    if sum(head["bytes"] for head in heads.values()) != marker["verified_bytes"]:
        raise ValueError("Bronze verified byte total disagrees")
    if any(counts[t] != report["tables"][t]["rows"] for t in TABLES):
        raise ValueError("Bronze row totals disagree")
    return {"marker": marker, "identity": bronze_identity(marker), "paths": paths, "rows": counts, "heads": heads}


def assert_bronze_unchanged(store, version, contract):
    current = json.loads(required_read(store, f"{version}/_SUCCESS.json"))
    if bronze_identity(current) != contract["identity"]:
        raise ValueError("Bronze publication changed during Silver processing")
    for key, expected in contract["heads"].items():
        if store.head(key) != expected:
            raise ValueError(f"Bronze file changed during Silver processing: {key}")


def bind_version(store, version, identity):
    key = f"{version}/_DATASET.json"
    if not store.write(key, encode_json(identity), if_absent=True):
        if json.loads(required_read(store, key)) != identity:
            raise ValueError("Silver version belongs to different input/rules; choose a new --silver-version")


def published_report(store, version, identity):
    marker_data = store.read(f"{version}/_SUCCESS.json")
    if marker_data is None:
        return None
    marker = json.loads(marker_data)
    if marker.get("identity") != identity:
        raise ValueError("Published Silver version has different input/rules; choose a new --silver-version")
    run_id = marker["run_id"]
    validate_version(run_id)
    expected_key = f"{version}/_runs/{run_id}/silver_report.json"
    if marker["report_key"] != expected_key:
        raise ValueError("Silver marker points outside its run")
    data = required_read(store, expected_key)
    if digest(data) != marker["report_sha256"]:
        raise ValueError("Silver report checksum mismatch")
    report = json.loads(data)
    if report.get("passed") is not True or report.get("identity") != identity or report.get("run_id") != run_id:
        raise ValueError("Silver report is not a valid successful publication")
    actual = store.listing(f"{version}/_runs/{run_id}/tables/")
    if not actual or actual != report["output_inventory"]:
        raise ValueError("Published Silver files changed; verify the snapshot before continuing")
    return report
