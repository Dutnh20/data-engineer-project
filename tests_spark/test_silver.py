"""Real local Spark/Parquet tests; S3 transport is emulated, not a live storage test."""

import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_bronze_pipeline import FakeS3
from edai_generator.config import Config
from edai_generator.offline import generate
from pipelines.dp1_source_to_bronze import run_pipeline as dp1
from spark_jobs.dp2_bronze_to_silver import run_pipeline as dp2
from spark_jobs.silver_transforms import QualityError, build_customer_history
from spark_jobs.snapshot_store import digest, encode_json, published_report


class LocalStore:
    """Test adapter: real files for Spark, metadata in memory. Never used by CLI."""
    def __init__(self, root, bucket):
        self.root, self.bucket, self.metadata = Path(root), bucket, {}
        self.root.mkdir(parents=True, exist_ok=True)

    def ensure(self):
        self.root.mkdir(parents=True, exist_ok=True)

    def read(self, key):
        p = self.root / key
        return p.read_bytes() if p.is_file() else None

    def write(self, key, data, if_absent=False):
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            with p.open("xb" if if_absent else "wb") as f:
                f.write(data)
        except FileExistsError:
            return False
        return True

    def head(self, key):
        data = self.read(key)
        return None if data is None else {"key": key, "bytes": len(data), "etag": digest(data), "metadata": self.metadata.get(key, {})}

    def listing(self, prefix):
        result = []
        for p in (self.root / prefix).rglob("*"):
            if p.is_file() and not any(part.startswith(".") for part in p.relative_to(self.root).parts):
                h = self.head(p.relative_to(self.root).as_posix())
                result.append({k: h[k] for k in ("key", "bytes", "etag")})
        return sorted(result, key=lambda r: r["key"])

    def uri(self, key):
        return (self.root / key).resolve().as_uri()


def bronze_fixture(source, root, invalid_quantity=False):
    """Use the real DP1 code before testing DP2. Optional byte-valid bad business data."""
    fake = FakeS3()
    fake.buckets.add("source-department")
    entries = [json.loads(line) for line in (source / "file_index.jsonl").read_text().splitlines()]
    manifest = json.loads((source / "manifest.json").read_bytes())
    fingerprint = manifest["config_fingerprint"]
    replaced = False
    for entry in entries:
        data = (source / entry["path"]).read_bytes()
        if invalid_quantity and not replaced and entry["table"] == "order_items":
            table = pq.read_table(io.BytesIO(data))
            quantities = table["quantity"].to_pylist()
            quantities[0] = 0
            idx = table.schema.get_field_index("quantity")
            table = table.set_column(idx, table.schema.field(idx), pa.array(quantities, pa.int64()))
            sink = io.BytesIO()
            pq.write_table(table, sink)
            data = sink.getvalue()
            delta = len(data) - entry["bytes"]
            manifest["physical_source_bytes"] += delta
            manifest["tables"]["order_items"]["physical_bytes"] += delta
            entry.update(bytes=len(data), sha256=digest(data))
            replaced = True
        fake.objects[("source-department", "synthetic/test/" + entry["path"])] = (
            data, {"sha256": entry["sha256"], "source-fingerprint": fingerprint})
    index = ("\n".join(json.dumps(e) for e in entries) + "\n").encode()
    for name, data in {"manifest.json": encode_json(manifest), "config.json": (source / "config.json").read_bytes(), "file_index.jsonl": index}.items():
        fake.objects[("source-department", "synthetic/test/metadata/" + name)] = (data, {})
    ready = {"source_fingerprint": fingerprint, "file_index_sha256": digest(index), "files": len(entries),
             "physical_source_bytes": manifest["physical_source_bytes"], "source_prefix": "synthetic/test/source/offline/",
             "manifest_key": "synthetic/test/metadata/manifest.json"}
    fake.objects[("source-department", "synthetic/test/_READY.json")] = (encode_json(ready), {})
    dp1(fake, source_prefix="synthetic/test", dataset_version="bronze_test", out=root / "dp1_reports")
    store = LocalStore(root / "bronze", "bronze")
    for (bucket, key), (data, metadata) in fake.objects.items():
        if bucket == "bronze":
            store.write(key, data)
            store.metadata[key] = metadata
    return store


def new_spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (SparkSession.builder.master("local[2]").appName("EDAI-DP2-tests")
            .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


class SilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = None if os.getenv("EDAI_TEST_WORK") else tempfile.TemporaryDirectory(prefix="edai-silver-tests-")
        cls.work = Path(os.getenv("EDAI_TEST_WORK") or cls.temp.name)
        cls.work.mkdir(parents=True, exist_ok=True)
        if os.getenv("EDAI_TEST_SOURCE"):
            cls.source = Path(os.environ["EDAI_TEST_SOURCE"])
        else:
            cls.source = cls.work / "source"
            generate(Config(customers=100, products=100, orders=800, batch_orders=100,
                            schema_change_after_order=400, min_free_gb=0), cls.source)
        cls.manifest = json.loads((cls.source / "manifest.json").read_bytes())
        cls.bronze = bronze_fixture(cls.source, cls.work / "good")
        cls.silver = LocalStore(cls.work / "good/silver", "silver")
        cls.result = dp2(cls.bronze, cls.silver, bronze_version="bronze_test", silver_version="silver_test",
                         out=cls.work / "reports", spark_factory=new_spark)
        (cls.work / "successful_report.json").write_bytes(encode_json(cls.result))

    @classmethod
    def tearDownClass(cls):
        if cls.temp:
            cls.temp.cleanup()

    def read_table(self, name):
        key = f"silver_test/_runs/{self.result['run_id']}/tables/{name}"
        paths = sorted((self.silver.root / key).glob("*.parquet"))
        return pa.concat_tables([pq.read_table(p) for p in paths]).to_pylist()

    def test_counts_dedup_schema_and_exact_amount_reconciliation(self):
        self.assertTrue(self.result["passed"])
        self.assertTrue(all(check["passed"] for check in self.result["quality_checks"]))
        for table, counts in self.manifest["tables"].items():
            expected = self.manifest["offline_duplicates"]["clean_rows"] if table == "order_items" else counts["rows"]
            self.assertEqual(self.result["tables"]["stg_" + table]["rows"], expected)
        items = self.read_table("stg_order_items")
        orders = self.read_table("stg_orders")
        self.assertEqual(len(items), len({r["order_item_id"] for r in items}))
        self.assertEqual(sum(r["line_amount"] for r in items), sum(r["total_amount"] for r in orders))
        self.assertGreater(self.result["duplicate_amount_removed_vnd"], 0)
        payments = self.read_table("stg_payments")
        boundary = self.manifest["schema_evolution"]["last_v1_order_id"]
        self.assertTrue(all(p["failure_reason"] is None for p in payments if p["order_id"] <= boundary))
        self.assertTrue(any(p["failure_reason"] for p in payments if p["order_id"] > boundary and p["status"] == "failed"))
        newest = {}
        for path in (self.source / "source/offline/order_items").glob("*.parquet"):
            for row in pq.read_table(path).to_pylist():
                newest[row["order_item_id"]] = max(newest.get(row["order_item_id"], row["created_ts"]), row["created_ts"])
        self.assertTrue(all(row["created_ts"].replace(tzinfo=None) == newest[row["order_item_id"]].replace(tzinfo=None) for row in items))

    def test_history_half_open_boundary_and_current_uniqueness(self):
        history = self.read_table("stg_customer_history")
        groups = {}
        for row in history:
            groups.setdefault(row["customer_id"], []).append(row)
        self.assertEqual(len(groups), self.manifest["tables"]["customers"]["rows"])
        self.assertEqual(len({r["customer_sk"] for r in history}), len(history))
        changed = 0
        for rows in groups.values():
            rows.sort(key=lambda row: row["valid_from_ts"])
            self.assertEqual(sum(r["is_current"] for r in rows), 1)
            for before, after in zip(rows, rows[1:]):
                boundary = after["valid_from_ts"]
                self.assertEqual(before["valid_to_ts"], boundary)
                matches = [r for r in rows if r["valid_from_ts"] <= boundary and (r["valid_to_ts"] is None or boundary < r["valid_to_ts"])]
                self.assertEqual([r["customer_sk"] for r in matches], [after["customer_sk"]])
                changed += 1
        self.assertGreater(changed, 0)

    def test_rerun_does_not_recompute_or_append(self):
        before = self.silver.listing("silver_test/")
        def forbidden_factory():
            raise AssertionError("A completed rerun must reuse the published snapshot")
        result = dp2(self.bronze, self.silver, bronze_version="bronze_test", silver_version="silver_test",
                     out=self.work / "rerun_reports", spark_factory=forbidden_factory)
        self.assertEqual(result["status"], "already_published")
        self.assertEqual(result["reused_run_id"], self.result["run_id"])
        self.assertEqual(before, self.silver.listing("silver_test/"))

    def test_changed_output_is_detected_on_rerun(self):
        path = next((self.silver.root / f"silver_test/_runs/{self.result['run_id']}/tables/stg_products").glob("*.parquet"))
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"changed")
            with self.assertRaisesRegex(ValueError, "Silver files changed"):
                published_report(self.silver, "silver_test", self.result["identity"])
        finally:
            path.write_bytes(original)

    def test_missing_bronze_marker_blocks_before_spark(self):
        empty = LocalStore(self.work / "empty/bronze", "bronze")
        silver = LocalStore(self.work / "empty/silver", "silver")
        with self.assertRaisesRegex(ValueError, "Missing.*_SUCCESS"):
            dp2(empty, silver, bronze_version="absent", silver_version="blocked",
                out=self.work / "empty/reports", spark_factory=lambda: self.fail("Spark should not start"))
        self.assertIsNone(silver.read("blocked/_SUCCESS.json"))

    def test_bad_quantity_quarantined_and_not_published(self):
        bronze = bronze_fixture(self.source, self.work / "bad", invalid_quantity=True)
        silver = LocalStore(self.work / "bad/silver", "silver")
        with self.assertRaisesRegex(QualityError, "order_items_row_rules"):
            dp2(bronze, silver, bronze_version="bronze_test", silver_version="bad_test",
                out=self.work / "bad/reports", spark_factory=new_spark)
        self.assertIsNone(silver.read("bad_test/_SUCCESS.json"))
        quarantines = list(silver.root.glob("bad_test/_runs/*/quarantine/order_items_row_rules/*.parquet"))
        self.assertTrue(quarantines)
        bad = pa.concat_tables([pq.read_table(p) for p in quarantines]).to_pylist()
        self.assertTrue(any(r["quantity"] == 0 and "positive:quantity" in r["_dq_reason"] for r in bad))
        self.assertTrue(list(silver.root.glob("bad_test/_runs/*/failed_report.json")))

    def test_noop_history_update_is_removed_and_keys_are_stable(self):
        spark = new_spark()
        spark.sparkContext.setLogLevel("WARN")
        try:
            source = f"silver_test/_runs/{self.result['run_id']}/tables/stg_customer_changes"
            changes = spark.read.parquet(self.silver.uri(source))
            base = changes.filter("change_type = 'INSERT'").orderBy("customer_id").limit(1)
            noop = base.withColumn("effective_ts", F.expr("effective_ts + INTERVAL 1 HOUR")).withColumn("source_updated_ts", F.expr("effective_ts + INTERVAL 1 SECOND")).withColumn("change_id", F.lit(99999999).cast("long")).withColumn("change_type", F.lit("UPDATE"))
            combined = changes.unionByName(noop)
            first = build_customer_history(combined, "stable").orderBy("customer_sk").collect()
            second = build_customer_history(combined.repartition(3), "stable").orderBy("customer_sk").collect()
            self.assertEqual(first, second)
            self.assertEqual(len(first), self.result["tables"]["stg_customer_history"]["rows"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
