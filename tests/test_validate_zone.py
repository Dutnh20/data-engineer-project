"""Kiểm thử validator zone với store giả (không cần MinIO)."""
import json
import unittest
from unittest import mock

from spark_jobs import validate_zone as vz
from spark_jobs.snapshot_store import digest, encode_json


class FakeStore:
    objects = {}

    def __init__(self, endpoint, bucket):
        self.bucket = bucket

    def read(self, key):
        return self.objects.get((self.bucket, key))


def publish(bucket, marker_key, marker, report):
    data = encode_json(report)
    marker = dict(marker, report_key="r/report.json", report_sha256=digest(data))
    FakeStore.objects[(bucket, "r/report.json")] = data
    FakeStore.objects[(bucket, marker_key)] = encode_json(marker)


class ValidateZoneTests(unittest.TestCase):
    def setUp(self):
        FakeStore.objects = {}
        patcher = mock.patch.object(vz, "S3Store", FakeStore)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_zone(self, zone, version, bucket):
        results = vz.validate(zone, "http://x", version, bucket, 90)
        return all(r["ok"] for r in results), results

    def test_gold_passes_and_missing_table_fails(self):
        tables = {t: {"rows": 5, "path": "p"} for t in vz.GOLD_TABLES}
        publish("gold", "v1/_SUCCESS.json", {"run_id": "r1"}, {"passed": True, "run_id": "r1", "tables": tables})
        self.assertTrue(self.run_zone("gold", "v1", "gold")[0])
        del tables["fact_orders"]
        publish("gold", "v1/_SUCCESS.json", {"run_id": "r1"}, {"passed": True, "run_id": "r1", "tables": tables})
        ok, results = self.run_zone("gold", "v1", "gold")
        self.assertFalse(ok)
        self.assertTrue(any("fact_orders" in r["check"] and not r["ok"] for r in results))

    def test_bronze_file_count_mismatch_fails(self):
        publish("bronze", "b1/_SUCCESS.json", {"run_id": "r", "verified_files": 38},
                {"passed": True, "run_id": "r", "expected_files": 39})
        self.assertFalse(self.run_zone("bronze", "b1", "bronze")[0])

    def test_feat_requires_event_timestamp_and_created(self):
        report = {"passed": True, "run_id": "r", "rows": 10, "checks": [{"check": "x", "ok": True}],
                  "columns": ["customer_id", "event_timestamp", "created"]}
        publish("gold", "_features/f1/_SUCCESS.json", {"run_id": "r", "tables": {"feat_customer_orders_90d": "p"}}, report)
        self.assertTrue(self.run_zone("feat", "f1", "gold")[0])
        report["columns"] = ["customer_id"]
        publish("gold", "_features/f1/_SUCCESS.json", {"run_id": "r", "tables": {"feat_customer_orders_90d": "p"}}, report)
        self.assertFalse(self.run_zone("feat", "f1", "gold")[0])

    def test_missing_marker_reported_not_raised(self):
        ok, results = self.run_zone("silver", "none", "silver")
        self.assertFalse(ok)
        self.assertEqual(len(results), 1)


if __name__ == "__main__":
    unittest.main()
