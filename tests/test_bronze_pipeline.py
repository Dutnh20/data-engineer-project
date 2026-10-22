"""DP1 contract/recovery tests. Fake S3 tests do not imply a live service test."""

import io
import json
from pathlib import Path
import tempfile
import unittest

from botocore.exceptions import ClientError

from edai_generator.config import Config
from edai_generator.offline import generate
from pipelines.dp1_source_to_bronze import run_pipeline, sha256


class FakeS3:
    def __init__(self):
        self.buckets = set()
        self.objects = {}
        self.writes = []
        self.copy_attempts = 0
        self.fail_copy_at = None
        self.before_copy = None

    @staticmethod
    def error(code):
        return ClientError({"Error": {"Code": code, "Message": "test error"}}, "TestS3")

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise self.error("404")
        return {}

    def create_bucket(self, Bucket):
        self.buckets.add(Bucket)
        return {}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self.error("404")
        body, metadata = self.objects[(Bucket, Key)]
        return {"ContentLength": len(body), "Metadata": dict(metadata), "ETag": '"' + sha256(body) + '"'}

    def get_object(self, Bucket, Key, IfMatch=None):
        head = self.head_object(Bucket, Key)
        if IfMatch is not None and IfMatch != head["ETag"]:
            raise self.error("PreconditionFailed")
        return {**head, "Body": io.BytesIO(self.objects[(Bucket, Key)][0])}

    def put_object(self, Bucket, Key, Body, Metadata=None, IfNoneMatch=None, **kwargs):
        self.head_bucket(Bucket)
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            raise self.error("PreconditionFailed")
        self.objects[(Bucket, Key)] = (Body, dict(Metadata or {}))
        self.writes.append((Bucket, Key))
        return {}

    def upload_file(self, Filename, Bucket, Key):
        self.put_object(Bucket=Bucket, Key=Key, Body=Path(Filename).read_bytes())

    def copy(self, CopySource, Bucket, Key, ExtraArgs, Config):
        self.copy_attempts += 1
        if self.copy_attempts == self.fail_copy_at:
            raise RuntimeError("simulated transfer interruption")
        if self.before_copy:
            callback, self.before_copy = self.before_copy, None
            callback(CopySource)
        source = self.head_object(**CopySource)
        if ExtraArgs["CopySourceIfMatch"] != source["ETag"]:
            raise self.error("PreconditionFailed")
        data = self.objects[(CopySource["Bucket"], CopySource["Key"])][0]
        self.put_object(Bucket=Bucket, Key=Key, Body=data, Metadata=ExtraArgs["Metadata"])

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": key} for bucket, key in self.objects if bucket == Bucket and key.startswith(Prefix)]}


class BronzePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="edai-bronze-tests-")
        cls.root = Path(cls.temp.name)
        cls.source = cls.root / "source"
        generate(Config(customers=20, products=20, orders=40, batch_orders=10,
                        schema_change_after_order=20, min_free_gb=0), cls.source)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.s3 = FakeS3()
        self.seed("synthetic/test")
        self.out = self.root / self._testMethodName

    def seed(self, prefix):
        bucket = "source-department"
        self.s3.create_bucket(Bucket=bucket)
        self.entries = [json.loads(line) for line in (self.source / "file_index.jsonl").read_text().splitlines()]
        manifest = json.loads((self.source / "manifest.json").read_bytes())
        fingerprint = manifest["config_fingerprint"]
        for entry in self.entries:
            self.s3.put_object(Bucket=bucket, Key=f"{prefix}/{entry['path']}",
                               Body=(self.source / entry["path"]).read_bytes(),
                               Metadata={"sha256": entry["sha256"], "source-fingerprint": fingerprint})
        for name in ("manifest.json", "config.json", "file_index.jsonl"):
            self.s3.put_object(Bucket=bucket, Key=f"{prefix}/metadata/{name}", Body=(self.source / name).read_bytes())
        ready = {"source_fingerprint": fingerprint, "file_index_sha256": sha256((self.source / "file_index.jsonl").read_bytes()),
                 "physical_source_bytes": manifest["physical_source_bytes"], "files": len(self.entries),
                 "source_prefix": f"{prefix}/source/offline/", "manifest_key": f"{prefix}/metadata/manifest.json"}
        self.s3.put_object(Bucket=bucket, Key=f"{prefix}/_READY.json", Body=json.dumps(ready).encode())

    def run_dp1(self, prefix="synthetic/test", version="test_v1"):
        return run_pipeline(self.s3, source_prefix=prefix, dataset_version=version, out=self.out)

    def rewrite_inventory(self, entries):
        key = ("source-department", "synthetic/test/metadata/file_index.jsonl")
        data = ("\n".join(json.dumps(e) for e in entries) + "\n").encode()
        self.s3.objects[key] = (data, {})
        ready_key = ("source-department", "synthetic/test/_READY.json")
        ready = json.loads(self.s3.objects[ready_key][0])
        ready["file_index_sha256"] = sha256(data)
        self.s3.objects[ready_key] = (json.dumps(ready).encode(), {})

    def assert_not_published(self):
        self.assertNotIn(("bronze", "test_v1/_SUCCESS.json"), self.s3.objects)
        reports = list(self.out.glob("*/ingestion_report.json"))
        self.assertTrue(reports)
        self.assertFalse(json.loads(reports[-1].read_bytes())["passed"])

    def test_exact_copy_keeps_duplicates_and_both_payment_schemas(self):
        report = self.run_dp1()
        self.assertTrue(report["passed"])
        self.assertEqual(report["copied_files"], len(self.entries))
        self.assertEqual(report["verified_bytes"], sum(e["bytes"] for e in self.entries))
        self.assertEqual({e["schema_version"] for e in self.entries if e["table"] == "payments"}, {1, 2})
        for entry in self.entries:
            target = "test_v1/source_files/" + entry["path"].removeprefix("source/offline/")
            self.assertEqual(self.s3.objects[("bronze", target)][0], (self.source / entry["path"]).read_bytes())
        self.assertEqual(self.s3.writes[-1], ("bronze", "test_v1/_SUCCESS.json"))
        manifest = json.loads((self.source / "manifest.json").read_bytes())
        self.assertGreater(manifest["offline_duplicates"]["extra_copies"], 0)

    def test_rerun_verifies_without_new_data_files(self):
        self.run_dp1()
        before = {k: v for k, v in self.s3.objects.items() if k[0] == "bronze" and "/source_files/" in k[1]}
        report = self.run_dp1()
        after = {k: v for k, v in self.s3.objects.items() if k[0] == "bronze" and "/source_files/" in k[1]}
        self.assertEqual(before, after)
        self.assertEqual(report["copied_files"], 0)
        self.assertEqual(report["skipped_files"], len(self.entries))
        self.assertEqual(report["verified_files"], len(self.entries))

    def test_missing_ready_does_not_write_bronze(self):
        del self.s3.objects[("source-department", "synthetic/test/_READY.json")]
        with self.assertRaisesRegex(ValueError, "Missing.*_READY"):
            self.run_dp1()
        self.assertNotIn("bronze", self.s3.buckets)
        self.assert_not_published()

    def test_source_missing_before_ingestion(self):
        del self.s3.objects[("source-department", f"synthetic/test/{self.entries[0]['path']}")]
        with self.assertRaisesRegex(ValueError, "Missing object"):
            self.run_dp1()
        self.assertNotIn("bronze", self.s3.buckets)
        self.assert_not_published()

    def test_modified_inventory_is_rejected(self):
        key = ("source-department", "synthetic/test/metadata/file_index.jsonl")
        data, meta = self.s3.objects[key]
        self.s3.objects[key] = (data + b"\n", meta)
        with self.assertRaisesRegex(ValueError, "file_index SHA256"):
            self.run_dp1()
        self.assert_not_published()

    def test_inventory_cannot_escape_data_prefix(self):
        self.entries[0]["path"] = "source/offline/../elsewhere.parquet"
        self.rewrite_inventory(self.entries)
        with self.assertRaisesRegex(ValueError, "Invalid source inventory path"):
            self.run_dp1()
        self.assert_not_published()

    def test_another_source_cannot_reuse_destination_version(self):
        self.run_dp1()
        before = dict(self.s3.objects)
        self.seed("synthetic/other")
        with self.assertRaisesRegex(ValueError, "different source"):
            self.run_dp1(prefix="synthetic/other")
        self.assertEqual({k: v for k, v in before.items() if k[0] == "bronze"},
                         {k: v for k, v in self.s3.objects.items() if k[0] == "bronze"})

    def test_partial_copy_can_resume(self):
        self.s3.fail_copy_at = 3
        with self.assertRaisesRegex(RuntimeError, "interruption"):
            self.run_dp1()
        self.assert_not_published()
        self.s3.fail_copy_at = None
        report = self.run_dp1()
        self.assertTrue(report["passed"])
        self.assertEqual(report["skipped_files"], 2)
        self.assertEqual(report["copied_files"], len(self.entries) - 2)

    def test_real_bytes_are_checked_even_when_metadata_matches(self):
        self.s3.fail_copy_at = 2
        with self.assertRaises(RuntimeError):
            self.run_dp1()
        key = next(k for k in self.s3.objects if k[0] == "bronze" and "/source_files/" in k[1])
        data, meta = self.s3.objects[key]
        self.s3.objects[key] = (bytes([data[0] ^ 1]) + data[1:], meta)
        self.s3.fail_copy_at = None
        with self.assertRaisesRegex(ValueError, "Actual byte SHA256"):
            self.run_dp1()
        self.assert_not_published()

    def test_copy_refuses_source_changed_after_preflight(self):
        def change(source):
            key = (source["Bucket"], source["Key"])
            data, metadata = self.s3.objects[key]
            self.s3.objects[key] = (data + b"changed", metadata)
        self.s3.before_copy = change
        with self.assertRaises(ClientError):
            self.run_dp1()
        self.assert_not_published()

    def test_footer_row_count_is_checked(self):
        entry = self.entries[0]
        entry["rows"] += 1
        self.rewrite_inventory(self.entries)
        key = ("source-department", "synthetic/test/metadata/manifest.json")
        manifest = json.loads(self.s3.objects[key][0])
        manifest["tables"][entry["table"]]["rows"] += 1
        self.s3.objects[key] = (json.dumps(manifest).encode(), {})
        with self.assertRaisesRegex(ValueError, "Parquet row count"):
            self.run_dp1()
        self.assert_not_published()

    def test_physical_schema_is_checked(self):
        entry = next(e for e in self.entries if e["table"] == "payments" and e["schema_version"] == 1)
        entry["schema_version"] = 2
        self.rewrite_inventory(self.entries)
        with self.assertRaisesRegex(ValueError, "Parquet schema"):
            self.run_dp1()
        self.assert_not_published()

    def test_untracked_file_blocks_publication(self):
        self.s3.create_bucket(Bucket="bronze")
        self.s3.put_object(Bucket="bronze", Key="test_v1/source_files/extra.parquet", Body=b"unexpected")
        with self.assertRaisesRegex(ValueError, "untracked"):
            self.run_dp1()
        self.assert_not_published()


if __name__ == "__main__":
    unittest.main()
