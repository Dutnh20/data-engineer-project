"""Meaningful small-run checks; mocked transports do not claim live Kafka/MinIO QA."""

from collections import Counter, defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError
import duckdb
import pyarrow.parquet as pq

from edai_generator.common import sha256_file
from edai_generator.config import Config, load_config
from edai_generator.model import make_order
from edai_generator.offline import generate
from edai_generator.storage import upload_run
from edai_generator.streaming import plan_stream, produce_stream, rebase_event
from edai_generator.validate import validate_run


class FakeS3:
    """In-memory S3 surface for uploader control-flow checks only."""

    def __init__(self):
        """Start with no bucket or objects."""
        self.exists = False
        self.objects = {}
        self.writes = []

    def missing(self):
        """Return a normal S3 not-found error."""
        return ClientError({"Error": {"Code": "404", "Message": "missing"}}, "HeadObject")

    def head_bucket(self, **kwargs):
        """Report bucket presence."""
        if not self.exists:
            raise self.missing()
        return {}

    def create_bucket(self, **kwargs):
        """Create the fake bucket."""
        self.exists = True
        return {}

    def head_object(self, Bucket, Key):
        """Expose object size and user metadata."""
        if Key not in self.objects:
            raise self.missing()
        body, meta = self.objects[Key]
        return {"ContentLength": len(body), "Metadata": meta}

    def get_object(self, Bucket, Key):
        """Read a fresh stream for an existing object."""
        if Key not in self.objects:
            raise self.missing()
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None, Config=None):
        """Store uploaded bytes and metadata."""
        self.objects[Key] = (Path(Filename).read_bytes(), (ExtraArgs or {}).get("Metadata", {}))
        self.writes.append(Key)

    def put_object(self, Bucket, Key, Body, **kwargs):
        """Store a ready marker after file uploads."""
        self.objects[Key] = (Body, {})
        self.writes.append(Key)


class FakeMessage:
    """Minimal delivery callback message."""

    def __init__(self, topic):
        """Retain the acknowledged topic."""
        self.name = topic

    def topic(self):
        """Return the acknowledged topic."""
        return self.name


class FakeProducer:
    """Exercise success/failure callbacks without claiming a Kafka connection."""

    def __init__(self, fail=False):
        """Configure a successful or failing fake delivery."""
        self.pending = []
        self.records = []
        self.fail = fail

    def produce(self, topic, key, value, on_delivery, headers):
        """Queue a delivery callback, as a real asynchronous producer does."""
        self.records.append((topic, key, value))
        self.pending.append((on_delivery, topic))

    def poll(self, timeout):
        """Serve queued callbacks."""
        while self.pending:
            callback, topic = self.pending.pop(0)
            callback("simulated delivery failure" if self.fail else None, FakeMessage(topic))

    def flush(self, timeout):
        """Complete queued fake deliveries."""
        self.poll(0)
        return 0


class GeneratorTests(unittest.TestCase):
    """Check business relationships, recovery, measured targets and stream faults."""

    @classmethod
    def setUpClass(cls):
        """Generate one small, fully synthetic source shared by read-only tests."""
        cls.temp = tempfile.TemporaryDirectory(prefix="edai-tests-")
        cls.base = Path(cls.temp.name)
        cls.cfg = Config(customers=200, products=100, orders=800,
                         batch_orders=100, dimension_batch=100,
                         schema_change_after_order=400, min_free_gb=0)
        cls.root = cls.base / "source"
        generate(cls.cfg, cls.root)
        cls.plan = plan_stream(cls.root, n_orders=600)

    @classmethod
    def tearDownClass(cls):
        """Remove only the temporary test data."""
        cls.temp.cleanup()

    def test_independent_business_validation(self):
        """Check FK domains, amounts, key cardinality and physical schema evolution."""
        report = validate_run(self.root, checksums=True)
        self.assertTrue(report["passed"], report["failures"])
        self.assertGreater(report["duplicate_amount_effect_vnd"]["overcount"], 0)
        self.assertLess(abs(report["duplicate_fraction_of_final_rows"] - 0.02), 1 / report["rows"]["order_items"])
        first = pq.ParquetFile(self.root / "source/offline/payments/part-000000000001.parquet")
        second = pq.ParquetFile(self.root / "source/offline/payments/part-000000000401.parquet")
        self.assertNotIn("failure_reason", first.schema_arrow.names)
        self.assertIn("failure_reason", second.schema_arrow.names)

    def test_customer_history_supports_temporal_join(self):
        """A point-in-time lookup returns the old segment before a change, new at it."""
        db = duckdb.connect()
        try:
            glob = (self.root / "source/offline/customer_changes/*.parquet").as_posix()
            db.execute("""CREATE VIEW history AS SELECT *,
                lead(effective_ts) OVER(PARTITION BY customer_id ORDER BY effective_ts) valid_to_ts
                FROM read_parquet('""" + glob.replace("'", "''") + "')")
            histories = db.execute("""SELECT customer_id FROM history
                GROUP BY customer_id HAVING count(*)=3 ORDER BY customer_id""").fetchall()
            self.assertTrue(histories)
            customer_id = histories[0][0]
            rows = db.execute("""SELECT effective_ts,segment FROM history
                WHERE customer_id=? ORDER BY effective_ts""", [customer_id]).fetchall()
            snapshots = [(rows[1][0] - timedelta(microseconds=1), rows[0][1]),
                         (rows[1][0], rows[1][1]), (rows[2][0], rows[2][1])]
            for timestamp, expected_segment in snapshots:
                found = db.execute("""SELECT segment FROM history WHERE customer_id=?
                    AND effective_ts<=? AND (valid_to_ts IS NULL OR ?<valid_to_ts)""",
                    [customer_id, timestamp, timestamp]).fetchall()
                self.assertEqual(found, [(expected_segment,)])
            self.assertNotEqual(rows[0][1], rows[1][1])
            self.assertEqual(rows[0][1], rows[2][1])
            current = db.execute("SELECT count(*) FROM history WHERE valid_to_ts IS NULL").fetchone()[0]
            self.assertEqual(current, self.cfg.customers)
        finally:
            db.close()

    def test_customer_history_can_disable_updates(self):
        """The zero-rate option still supplies exactly one initial row per customer."""
        cfg = replace(self.cfg, customer_change_fraction=0)
        root = self.base / "no_customer_updates"
        generate(cfg, root)
        report = validate_run(root)
        self.assertTrue(report["passed"], report["failures"])
        self.assertEqual(report["rows"]["customer_changes"], cfg.customers)
        self.assertEqual(report["customer_history"]["update_rows"], 0)

    def test_ml_profile_has_long_history_without_wrapping(self):
        """The ML profile spans >340 days, rather than only claiming a long horizon."""
        cfg = load_config(Path(__file__).parents[1] / "configs/ml_history.json")
        first = make_order(1, cfg)[0]["order_ts"]
        last = make_order(cfg.orders, cfg)[0]["order_ts"]
        span = (last - first).total_seconds()
        self.assertGreater(span, 340 * 86400)
        self.assertLess(span, cfg.history_days * 86400)
        self.assertEqual(span, cfg.minimum_span_seconds)

    def test_resume_and_orphan_recovery(self):
        """A resumed run regenerates an uncommitted file and matches uninterrupted bytes."""
        root = self.base / "resumed"
        report = generate(self.cfg, root, stop_after_chunks=2)
        self.assertEqual(report["status"], "paused")
        (root / "source/offline/orders/part-000000000201.parquet").write_bytes(b"interrupted write")
        generate(self.cfg, root, resume=True)
        for file in (self.root / "source/offline").rglob("*.parquet"):
            self.assertEqual(sha256_file(file), sha256_file(root / file.relative_to(self.root)))
        self.assertTrue(validate_run(root)["passed"])

    def test_config_change_refused_on_resume(self):
        """Prevent mixing data from different seeds inside one source run."""
        with self.assertRaisesRegex(ValueError, "differs"):
            generate(replace(self.cfg, seed=999), self.root, resume=True)

    def test_existing_output_not_overwritten(self):
        """A fresh run cannot silently replace an existing output."""
        with self.assertRaisesRegex(ValueError, "not empty"):
            generate(self.cfg, self.root)

    def test_actual_byte_target(self):
        """The byte target is checked on files; more records are made when needed."""
        root = self.base / "byte_target"
        cfg = replace(self.cfg, target_gb=0.0003)
        report = generate(cfg, root)
        actual = sum(file.stat().st_size for file in (root / "source/offline").rglob("*.parquet"))
        self.assertEqual(actual, report["physical_source_bytes"])
        self.assertGreaterEqual(actual, 300000)
        self.assertGreater(report["orders_generated"], cfg.orders)
        self.assertTrue(report["target_reached"])
        self.assertFalse(report["rubric_100gb_met"])
        self.assertTrue(validate_run(root)["passed"])

    def test_detect_changed_file(self):
        """Independent integrity validation rejects a damaged source file."""
        root = self.base / "tampered"
        shutil.copytree(self.root, root)
        file = next((root / "source/offline/orders").glob("*.parquet"))
        with file.open("ab") as handle:
            handle.write(b"damage")
        report = validate_run(root, checksums=True)
        self.assertFalse(report["passed"])
        self.assertTrue(any("Size differs" in message for message in report["failures"]))

    def test_stream_relations_delays_duplicates(self):
        """Read payloads independently and compare callback IDs with stored payments."""
        plan_path = self.root / "stream/events.jsonl"
        records = [json.loads(line) for line in plan_path.read_text().splitlines()]
        arrivals = [datetime.fromisoformat(row["simulated_arrival_ts"]) for row in records]
        self.assertEqual(arrivals, sorted(arrivals))
        callbacks = defaultdict(list)
        late_count = 0
        versions = set()
        for row in records:
            event = row["event"]
            self.assertNotIn("injected_late", event)
            self.assertNotIn("injected_copy", event)
            delay = (datetime.fromisoformat(row["simulated_arrival_ts"]) - datetime.fromisoformat(event["event_ts"])).total_seconds()
            self.assertGreaterEqual(delay, 0)
            if row["topic"].endswith("clickstream"):
                self.assertTrue(1 <= event["customer_id"] <= self.cfg.customers)
                self.assertTrue(1 <= event["product_id"] <= self.cfg.products)
                if delay >= 300:
                    late_count += 1
                    self.assertLessEqual(delay, 600)
                    self.assertEqual(event["device_type"], "mobile")
                else:
                    self.assertLessEqual(delay, 2)
            else:
                callbacks[event["event_id"]].append(event)
                versions.add(event["schema_version"])
        self.assertEqual(late_count, self.plan["late_click_events"])
        self.assertGreater(late_count, 0)
        self.assertEqual(versions, {1, 2})
        self.assertGreater(self.plan["out_of_order_relative_to_topic_max_event_ts"]["edai.clickstream"], 0)
        copies = sum(len(events) - 1 for events in callbacks.values())
        self.assertEqual(copies, self.plan["extra_callback_copies"])
        db = duckdb.connect()
        try:
            glob = (self.root / "source/offline/payments/*.parquet").as_posix()
            payment_truth = {row[0]: row[1:] for row in db.execute(
                "SELECT payment_id,order_id,amount,status FROM read_parquet(?, union_by_name=true)", [glob]).fetchall()}
        finally:
            db.close()
        result_codes = {"succeeded": "SUCCESS", "pending": "PENDING", "failed": "FAILED"}
        for events in callbacks.values():
            for event in events:
                self.assertEqual(event, events[0])
                order_id, amount, status = payment_truth[event["payment_id"]]
                self.assertEqual((event["order_id"], event["amount"], event["result_code"]),
                                 (order_id, amount, result_codes[status]))

    def test_stream_plan_reproducible(self):
        """Planning the same source again yields identical scheduled messages."""
        first = sha256_file(self.root / "stream/events.jsonl")
        plan_stream(self.root, n_orders=600)
        self.assertEqual(first, sha256_file(self.root / "stream/events.jsonl"))

    def test_wall_clock_keeps_source_timestamp(self):
        """Clock rebasing preserves original business time and adds the exact offset."""
        original = {"event_id": "one", "event_ts": "2025-01-01T00:00:00+00:00",
                    "callback_ts": "2025-01-01T00:00:00+00:00"}
        shifted = rebase_event(original, 300000)
        self.assertEqual(shifted["source_event_ts"], original["event_ts"])
        self.assertEqual(shifted["event_ts"], "2025-01-01T00:05:00+00:00")
        self.assertEqual(shifted["callback_ts"], shifted["event_ts"])
        with self.assertRaisesRegex(ValueError, "speed 1"):
            produce_stream(self.root, clock="wall", speed=60)

    def test_kafka_delivery_callbacks(self):
        """A delivery report counts acknowledgements and preserves intentional repeats."""
        producer = FakeProducer()
        with patch("edai_generator.streaming.ensure_topics"), patch("confluent_kafka.Producer", return_value=producer):
            report = produce_stream(self.root, bootstrap="mock:9092", speed=0, max_rate=1e12)
        self.assertTrue(report["passed"])
        self.assertEqual(sum(report["acknowledged_by_topic"].values()), self.plan["delivery_count"])
        callback_ids = Counter(json.loads(value)["event_id"] for topic, key, value in producer.records
                               if topic.endswith("payment_callbacks"))
        self.assertEqual(sum(n - 1 for n in callback_ids.values()), self.plan["extra_callback_copies"])

    def test_kafka_delivery_error_not_silent(self):
        """An asynchronous broker failure must fail the command and evidence report."""
        with patch("edai_generator.streaming.ensure_topics"), patch("confluent_kafka.Producer", return_value=FakeProducer(fail=True)):
            with self.assertRaisesRegex(RuntimeError, "delivery failure"):
                produce_stream(self.root, bootstrap="mock:9092", speed=0, max_rate=1e12)
        self.assertFalse(json.loads((self.root / "stream/delivery_report.json").read_text())["passed"])

    def test_upload_verification_idempotency_and_ready_marker(self):
        """An upload publishes its marker last and skips verified files on repeat."""
        fake = FakeS3()
        credentials = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}
        with patch.dict(os.environ, credentials), patch("boto3.client", return_value=fake):
            first = upload_run(self.root, "http://mock", prefix="test/run")
            self.assertEqual(fake.writes[-1], "test/run/_READY.json")
            second = upload_run(self.root, "http://mock", prefix="test/run")
        self.assertTrue(first["passed"])
        self.assertGreater(first["uploaded_files"], 0)
        self.assertEqual(second["uploaded_files"], 0)
        self.assertEqual(second["skipped_files"], first["uploaded_files"])
        self.assertEqual(first["verified_source_bytes"], second["verified_source_bytes"])

    def test_upload_prefix_conflict(self):
        """Another source's completed MinIO prefix is not overwritten."""
        fake = FakeS3()
        fake.exists = True
        fake.objects["taken/_READY.json"] = (b'{"source_fingerprint":"someone_else"}', {})
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}), patch("boto3.client", return_value=fake):
            with self.assertRaisesRegex(ValueError, "different completed dataset"):
                upload_run(self.root, "http://mock", prefix="taken")


if __name__ == "__main__":
    unittest.main(verbosity=2)
