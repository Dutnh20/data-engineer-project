"""Kiểm thử logic feature offline (point-in-time, cửa sổ 90 ngày) bằng Spark local, không cần MinIO."""
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402

from spark_jobs.feat_customer_orders import (build_feat_customer_orders, default_as_of,  # noqa: E402
                                              feature_names, quality_checks)


class FeatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("feat-test")
                     .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        d = datetime
        cls.orders = cls.spark.createDataFrame([
            (1, d(2026, 1, 1), True, 100),     # ngoài cửa sổ 90 ngày của as_of 2026-06-01
            (1, d(2026, 4, 10), True, 200),
            (1, d(2026, 5, 20), False, 999),   # chưa hoàn tất: tính đơn, không tính tiền
            (2, d(2026, 6, 5), True, 50),      # SAU as_of: không được lọt vào feature (point-in-time)
        ], "customer_id long, order_ts timestamp, is_completed boolean, total_amount long")
        cls.customers = cls.spark.createDataFrame([(1,), (2,), (3,)], "customer_id long")
        cls.as_of = [d(2026, 6, 1)]
        cls.feat = build_feat_customer_orders(cls.spark, cls.orders, cls.customers, cls.as_of, 90,
                                              "gold_v2", "run1", d(2026, 6, 1, 1))
        cls.rows = {r["customer_id"]: r.asDict() for r in cls.feat.collect()}
        cls.n = feature_names(90)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_window_and_completed_logic(self):
        r = self.rows[1]
        self.assertEqual(r[self.n["orders"]], 2)
        self.assertEqual(r[self.n["completed"]], 1)
        self.assertEqual(r[self.n["spend"]], 200)
        self.assertEqual(r[self.n["aov"]], 200.0)
        self.assertEqual(r[self.n["recency"]], 12)   # 2026-05-20 -> 2026-06-01

    def test_point_in_time_no_future_leak(self):
        r = self.rows[2]
        self.assertEqual(r[self.n["orders"]], 0)
        self.assertIsNone(r[self.n["recency"]])

    def test_customer_without_orders_present(self):
        r = self.rows[3]
        self.assertEqual((r[self.n["orders"]], r[self.n["spend"]]), (0, 0))
        self.assertIsNone(r[self.n["aov"]])

    def test_event_timestamp_and_created(self):
        for r in self.rows.values():
            self.assertEqual(r["event_timestamp"], datetime(2026, 6, 1))
            self.assertIsNotNone(r["created"])

    def test_multiple_snapshots_and_quality(self):
        d = datetime
        two = build_feat_customer_orders(self.spark, self.orders, self.customers,
                                         [d(2026, 5, 31), d(2026, 6, 1)], 90, "gold_v2", "run1", d(2026, 6, 1))
        checks = quality_checks(two, 3, 2, 90)
        self.assertTrue(all(c["ok"] for c in checks), checks)

    def test_default_as_of(self):
        self.assertEqual(default_as_of(datetime(2026, 6, 5, 13, 0)), datetime(2026, 6, 6))


if __name__ == "__main__":
    unittest.main()
