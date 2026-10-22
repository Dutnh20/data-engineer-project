"""Publish generated source files to MinIO/S3 before a separate Bronze ingestion."""

import json
import os
from pathlib import Path

from .common import dump_json, sha256_file
from .offline import load_completed_run


def upload_run(root, endpoint, bucket="source-department", prefix=None):
    """Upload source shards with resumable skips and publish _READY.json last.

    Local SHA256 is checked against the generation index. Remote checks use
    ContentLength plus stored SHA256 metadata; this is not a full remote rehash.
    """
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config as ClientConfig
    from botocore.exceptions import ClientError

    root, cfg, manifest = load_completed_run(root)
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        raise ValueError("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to the MinIO credentials")
    prefix = (prefix or f"synthetic/{root.name}").strip("/")
    if not prefix or ".." in prefix.split("/"):
        raise ValueError("Choose a nonempty source prefix without '..'")
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1", config=ClientConfig(
        s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=30,
        retries={"max_attempts": 3, "mode": "standard"},
        request_checksum_calculation="when_required", response_checksum_validation="when_required",
    ))
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in ("404", "NoSuchBucket", "NotFound"):
            raise
        s3.create_bucket(Bucket=bucket)
    transfer = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                              multipart_chunksize=64 * 1024 * 1024, max_concurrency=4)
    inventory_hash = sha256_file(root / "file_index.jsonl")
    try:
        previous = json.loads(s3.get_object(Bucket=bucket, Key=f"{prefix}/_READY.json")["Body"].read())
        if previous.get("source_fingerprint") != cfg.fingerprint() or previous.get("file_index_sha256") != inventory_hash:
            raise ValueError("This MinIO prefix already contains a different completed dataset; choose --prefix with a new run name")
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in ("404", "NoSuchKey", "NotFound"):
            raise
    uploaded = skipped = verified_bytes = 0
    with (root / "file_index.jsonl").open(encoding="utf-8") as handle:
        for line in handle:
            entry = json.loads(line)
            path = (root / entry["path"]).resolve()
            if not path.is_relative_to(root / "source" / "offline"):
                raise ValueError("File inventory contains a path outside the source directory")
            if path.stat().st_size != entry["bytes"] or sha256_file(path) != entry["sha256"]:
                raise ValueError(f"Local source changed: {entry['path']}")
            key = f"{prefix}/{entry['path']}"
            remote = None
            try:
                remote = s3.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                if str(exc.response["Error"]["Code"]) not in ("404", "NoSuchKey", "NotFound"):
                    raise
            if remote and remote.get("Metadata", {}).get("source-fingerprint") != cfg.fingerprint():
                raise ValueError(f"Existing object belongs to another source; choose a new prefix: {key}")
            if remote and remote["ContentLength"] == entry["bytes"] and remote.get("Metadata", {}).get("sha256") == entry["sha256"]:
                skipped += 1
            else:
                s3.upload_file(str(path), bucket, key,
                               ExtraArgs={"Metadata": {"sha256": entry["sha256"], "source-fingerprint": cfg.fingerprint()}},
                               Config=transfer)
                uploaded += 1
            checked = s3.head_object(Bucket=bucket, Key=key)
            if checked["ContentLength"] != entry["bytes"] or checked.get("Metadata", {}).get("sha256") != entry["sha256"]:
                raise RuntimeError(f"Remote metadata verification failed: {key}")
            verified_bytes += entry["bytes"]
            if (uploaded + skipped) % 100 == 0:
                print(json.dumps({"uploaded": uploaded, "skipped": skipped,
                                  "verified_source_gb": verified_bytes / 1e9}), flush=True)
    if verified_bytes != manifest["physical_source_bytes"]:
        raise RuntimeError("Uploaded byte total does not match the completed source")
    for name in ("manifest.json", "config.json", "file_index.jsonl"):
        s3.upload_file(str(root / name), bucket, f"{prefix}/metadata/{name}")
    ready = {"source_fingerprint": cfg.fingerprint(), "file_index_sha256": inventory_hash,
             "physical_source_bytes": verified_bytes,
             "files": uploaded + skipped, "source_prefix": f"{prefix}/source/offline/",
             "manifest_key": f"{prefix}/metadata/manifest.json"}
    s3.put_object(Bucket=bucket, Key=f"{prefix}/_READY.json",
                  Body=json.dumps(ready).encode(), ContentType="application/json")
    report = {"passed": True, "endpoint": endpoint, "bucket": bucket, "prefix": prefix,
              "uploaded_files": uploaded, "skipped_files": skipped,
              "verified_source_bytes": verified_bytes, "ready_key": f"{prefix}/_READY.json",
              "verification": "Local SHA256; remote ContentLength and SHA256 metadata. No remote byte re-download."}
    dump_json(root / "evidence" / "upload_report.json", report)
    return report
