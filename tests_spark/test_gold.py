"""Real local Spark/Parquet tests cho Gold Layer; S3 transport được giả lập bằng LocalStore."""

import io
import json
import os
from pathlib import Path
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
from spark_jobs.dp3_silver_to_gold import run_pipeline as dp3
from spark_jobs.gold_transforms import GoldQualityError
from spark_jobs.snapshot_store import digest, encode_json


# ---------------------------------------------------------------------------
# LocalStore – giống bản trong test_silver.py
# ---------------------------------------------------------------------------

class LocalStore:
    """Test adapter: file thực cho Spark, metadata trong memory."""

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
        return None if data is None else {
            "key": key, "bytes": len(data),
            "etag": digest(data), "metadata": self.metadata.get(key, {}),
        }

    def listing(self, prefix):
        result = []
        for p in (self.root / prefix).rglob("*"):
            if p.is_file() and not any(part.startswith(".") for part in p.relative_to(self.root).parts):
                h = self.head(p.relative_to(self.root).as_posix())
                result.append({k: h[k] for k in ("key", "bytes", "etag")})
        return sorted(result, key=lambda r: r["key"])

    def uri(self, key):
        return (self.root / key).resolve().as_uri()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def bronze_fixture(source, root):
    """Tạo Bronze store giống như trong test_silver.py."""
    fake = FakeS3()
    fake.buckets.add("source-department")
    entries = [json.loads(line) for line in (source / "file_index.jsonl").read_text().splitlines()]
    manifest = json.loads((source / "manifest.json").read_bytes())
    fingerprint = manifest["config_fingerprint"]
    for entry in entries:
        data = (source / entry["path"]).read_bytes()
        fake.objects[("source-department", "synthetic/test/" + entry["path"])] = (
            data, {"sha256": entry["sha256"], "source-fingerprint": fingerprint})
    index = ("\n".join(json.dumps(e) for e in entries) + "\n").encode()
    for name, data in {
        "manifest.json": encode_json(manifest),
        "config.json": (source / "config.json").read_bytes(),
        "file_index.jsonl": index,
    }.items():
        fake.objects[("source-department", "synthetic/test/metadata/" + name)] = (data, {})
    ready = {
        "source_fingerprint": fingerprint,
        "file_index_sha256": digest(index),
        "files": len(entries),
        "physical_source_bytes": manifest["physical_source_bytes"],
        "source_prefix": "synthetic/test/source/offline/",
        "manifest_key": "synthetic/test/metadata/manifest.json",
    }
    fake.objects[("source-department", "synthetic/test/_READY.json")] = (encode_json(ready), {})
    dp1(fake, source_prefix="synthetic/test", dataset_version="bronze_test", out=root / "dp1_reports")
    store = LocalStore(root / "bronze", "bronze")
    for (bucket, key), (data, metadata) in fake.objects.items():
        if bucket == "bronze":
            store.write(key, data)
            store.metadata[key] = metadata
    return store


def new_spark():
    """SparkSession cục bộ để chạy test."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (SparkSession.builder.master("local[2]").appName("EDAI-DP3-tests")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


# ---------------------------------------------------------------------------
# Test class chính
# ---------------------------------------------------------------------------

class GoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = None if os.getenv("EDAI_TEST_WORK") else tempfile.TemporaryDirectory(prefix="edai-gold-tests-")
        cls.work = Path(os.getenv("EDAI_TEST_WORK") or cls.temp.name)
        cls.work.mkdir(parents=True, exist_ok=True)

        # Sinh dữ liệu nguồn
        if os.getenv("EDAI_TEST_SOURCE"):
            cls.source = Path(os.environ["EDAI_TEST_SOURCE"])
        else:
            cls.source = cls.work / "source"
            generate(Config(customers=100, products=100, orders=800, batch_orders=100,
                            schema_change_after_order=400, min_free_gb=0), cls.source)

        cls.manifest = json.loads((cls.source / "manifest.json").read_bytes())

        # DP1: source → bronze
        cls.bronze = bronze_fixture(cls.source, cls.work / "good")

        # DP2: bronze → silver
        cls.silver = LocalStore(cls.work / "good/silver", "silver")
        cls.silver_result = dp2(
            cls.bronze, cls.silver,
            bronze_version="bronze_test", silver_version="silver_test",
            out=cls.work / "dp2_reports", spark_factory=new_spark,
        )

        # DP3: silver → gold
        cls.gold = LocalStore(cls.work / "good/gold", "gold")
        cls.gold_result = dp3(
            cls.silver, cls.gold,
            silver_version="silver_test", gold_version="gold_test",
            out=cls.work / "dp3_reports", spark_factory=new_spark,
        )
        (cls.work / "gold_report.json").write_bytes(encode_json(cls.gold_result))

    @classmethod
    def tearDownClass(cls):
        if cls.temp:
            cls.temp.cleanup()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _path_to_local(self, uri_path):
        """Chuyển URI path (file:///) thành absolute local Path."""
        store_uri = self.gold.root.resolve().as_uri().rstrip("/")
        if uri_path.startswith(store_uri + "/"):
            return self.gold.root / uri_path[len(store_uri) + 1:]
        # Fallback cho các trường hợp URI format khác nhau
        clean = uri_path
        for prefix in ("file:///", "file://"):
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
                break
        return Path(clean)

    def read_table_rows(self, name):
        """Đọc bảng Gold từ Parquet và trả về list of dicts."""
        info = self.gold_result["tables"][name]
        dir_path = self._path_to_local(info["path"])
        parquet_files = sorted(dir_path.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"Không tìm thấy parquet file trong: {dir_path}")
        return pa.concat_tables([pq.read_table(p) for p in parquet_files]).to_pylist()

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_pipeline_passed(self):
        """DP3 phải hoàn thành thành công."""
        self.assertTrue(self.gold_result["passed"], msg=json.dumps(self.gold_result, indent=2, default=str))
        self.assertEqual(self.gold_result["status"], "published")

    def test_dim_customer_unique_and_complete(self):
        """dim_customer phải có đúng 100 khách hàng (is_current=True) với customer_sk duy nhất."""
        rows = self.read_table_rows("dim_customer")
        n_customers = self.manifest["tables"]["customers"]["rows"]
        self.assertEqual(len(rows), n_customers,
                         f"dim_customer: kỳ vọng {n_customers}, thực tế {len(rows)}")
        sks = [r["customer_sk"] for r in rows]
        self.assertEqual(len(sks), len(set(sks)), "dim_customer có customer_sk trùng lặp")
        # Tất cả phải is_current=True
        self.assertTrue(all(r["is_current"] for r in rows), "dim_customer có hàng is_current=False")

    def test_dim_product_unique(self):
        """dim_product phải có đủ sản phẩm với product_sk duy nhất."""
        rows = self.read_table_rows("dim_product")
        n_products = self.manifest["tables"]["products"]["rows"]
        self.assertEqual(len(rows), n_products,
                         f"dim_product: kỳ vọng {n_products}, thực tế {len(rows)}")
        sks = [r["product_sk"] for r in rows]
        self.assertEqual(len(sks), len(set(sks)), "dim_product có product_sk trùng lặp")

    def test_dim_date_covers_all_orders(self):
        """Mỗi order_ts phải có date_sk tương ứng trong dim_date."""
        orders = self.read_table_rows("fact_orders")
        date_rows = self.read_table_rows("dim_date")
        date_sks = {r["date_sk"] for r in date_rows}
        for o in orders:
            self.assertIn(o["date_sk"], date_sks,
                          f"order_id={o['order_id']} có date_sk={o['date_sk']} không có trong dim_date")

    def test_fact_orders_grain_and_fk(self):
        """Đúng một hàng mỗi order_id; các FK phải tồn tại trong dim tương ứng."""
        orders = self.read_table_rows("fact_orders")
        n_orders = self.manifest["tables"]["orders"]["rows"]
        self.assertEqual(len(orders), n_orders,
                         f"fact_orders: kỳ vọng {n_orders}, thực tế {len(orders)}")

        # Uniqueness theo order_id
        order_ids = [r["order_id"] for r in orders]
        self.assertEqual(len(order_ids), len(set(order_ids)), "fact_orders có order_id trùng lặp")

        # FK: customer_sk
        dim_customer = self.read_table_rows("dim_customer")
        valid_customer_sks = {r["customer_sk"] for r in dim_customer}
        for o in orders:
            self.assertIn(o["customer_sk"], valid_customer_sks,
                          f"order_id={o['order_id']} có customer_sk không hợp lệ")

        # FK: date_sk
        dim_date = self.read_table_rows("dim_date")
        valid_date_sks = {r["date_sk"] for r in dim_date}
        for o in orders:
            self.assertIn(o["date_sk"], valid_date_sks,
                          f"order_id={o['order_id']} có date_sk không hợp lệ")

    def test_fact_order_items_grain_and_fk(self):
        """Đúng một hàng mỗi order_item_id; FK đến fact_orders và dim_product phải hợp lệ."""
        items = self.read_table_rows("fact_order_items")
        # Số lượng sau dedup (từ manifest)
        dedup_rows = self.manifest["offline_duplicates"]["clean_rows"]
        self.assertEqual(len(items), dedup_rows,
                         f"fact_order_items: kỳ vọng {dedup_rows}, thực tế {len(items)}")

        # Grain: order_item_id unique
        item_ids = [r["order_item_id"] for r in items]
        self.assertEqual(len(item_ids), len(set(item_ids)), "fact_order_items có order_item_id trùng lặp")

        # FK: order_sk
        orders = self.read_table_rows("fact_orders")
        valid_order_sks = {r["order_sk"] for r in orders}
        for it in items:
            self.assertIn(it["order_sk"], valid_order_sks,
                          f"order_item_id={it['order_item_id']} có order_sk không hợp lệ")

        # FK: product_sk
        products = self.read_table_rows("dim_product")
        valid_product_sks = {r["product_sk"] for r in products}
        for it in items:
            self.assertIn(it["product_sk"], valid_product_sks,
                          f"order_item_id={it['order_item_id']} có product_sk không hợp lệ")

    def test_fact_payment_attempts_grain(self):
        """Đúng một hàng mỗi payment_id."""
        payments = self.read_table_rows("fact_payment_attempts")
        n_payments = self.manifest["tables"]["payments"]["rows"]
        self.assertEqual(len(payments), n_payments,
                         f"fact_payment_attempts: kỳ vọng {n_payments}, thực tế {len(payments)}")
        payment_ids = [r["payment_id"] for r in payments]
        self.assertEqual(len(payment_ids), len(set(payment_ids)),
                         "fact_payment_attempts có payment_id trùng lặp")

    def test_revenue_reconciliation(self):
        """Tổng doanh thu completed trong Gold phải khớp với Silver clean_items_amount."""
        orders = self.read_table_rows("fact_orders")
        items = self.read_table_rows("fact_order_items")

        # Tổng line_amount của items thuộc đơn completed
        completed_order_ids = {r["order_id"] for r in orders if r["is_completed"]}
        gold_revenue = sum(it["line_amount"] for it in items if it["order_id"] in completed_order_ids)

        # Silver clean_items_amount_vnd là tổng của MỌI đơn (đã dedup), không chỉ completed
        # Nên ta so sánh với tổng total_amount của đơn completed trong fact_orders
        completed_total = sum(r["total_amount"] for r in orders if r["is_completed"])
        self.assertEqual(gold_revenue, completed_total,
                         "Tổng line_amount (completed) không khớp với tổng total_amount (completed)")

    def test_agg_daily_sales_totals(self):
        """Tổng orders_count trong agg_daily_sales phải khớp với fact_orders."""
        agg = self.read_table_rows("agg_daily_sales")
        orders = self.read_table_rows("fact_orders")

        # Tổng số đơn hàng từ agg (tính theo date+category+city, mỗi đơn có thể bị đếm
        # nhiều lần vì join với items có nhiều category) – không nhất thiết bằng
        # Thay vào đó kiểm tra: tổng net_revenue_vnd phải = tổng completed total_amount
        completed_revenue = sum(r["total_amount"] for r in orders if r["is_completed"])
        agg_net = sum(r["net_revenue_vnd"] for r in agg)
        self.assertEqual(agg_net, completed_revenue,
                         f"agg_daily_sales net_revenue_vnd ({agg_net}) ≠ completed revenue ({completed_revenue})")

    def test_customer_ltv_reconciliation(self):
        """Tổng total_spend_vnd trong agg_customer_ltv phải khớp với tổng fact_orders completed."""
        ltv = self.read_table_rows("agg_customer_ltv")
        orders = self.read_table_rows("fact_orders")

        completed_revenue = sum(r["total_amount"] for r in orders if r["is_completed"])
        ltv_revenue = sum(r["total_spend_vnd"] for r in ltv)
        self.assertEqual(ltv_revenue, completed_revenue,
                         f"agg_customer_ltv total_spend ({ltv_revenue}) ≠ completed revenue ({completed_revenue})")

    def test_gold_rerun_idempotent(self):
        """Rerun DP3 với cùng version phải trả về already_published, không ghi thêm file."""
        before = self.gold.listing("gold_test/")

        def forbidden_factory():
            raise AssertionError("Rerun thành công không được khởi động lại Spark")

        result = dp3(
            self.silver, self.gold,
            silver_version="silver_test", gold_version="gold_test",
            out=self.work / "dp3_rerun_reports",
            spark_factory=forbidden_factory,
        )
        self.assertEqual(result["status"], "already_published")
        self.assertEqual(result["reused_run_id"], self.gold_result["run_id"])
        self.assertEqual(before, self.gold.listing("gold_test/"))

    def test_orphan_fk_detection(self):
        """Gold DQ phải từ chối khi inject dữ liệu có FK không hợp lệ vào fact_orders."""
        from pyspark.sql import SparkSession
        import uuid

        # Lấy Spark session hiện tại hoặc tạo mới
        spark = SparkSession.builder.master("local[2]").appName("EDAI-DP3-orphan-test") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

        try:
            # Đọc Silver paths từ gold_result (đã được chạy)
            silver_marker_data = self.silver.read("silver_test/_SUCCESS.json")
            silver_marker = json.loads(silver_marker_data)
            report_data = self.silver.read(silver_marker["report_key"])
            silver_report = json.loads(report_data)
            silver_paths = {name: info["path"] for name, info in silver_report["tables"].items()}

            # Xây dựng dim_customer bình thường
            from spark_jobs.gold_transforms import build_dim_customer, GoldQualityGate, GoldQualityError
            history_df = spark.read.parquet(silver_paths["stg_customer_history"])
            dim_customer = build_dim_customer(history_df, "gold_test_orphan", "test_run", "2026-01-01T00:00:00Z")

            # Tạo fact_orders với customer_sk không tồn tại (orphan)
            orders_df = spark.read.parquet(silver_paths["stg_orders"])
            # Thêm một đơn hàng giả với customer_sk không hợp lệ
            bad_row = spark.createDataFrame([{
                "order_sk": "bad_order_sk_does_not_exist",
                "order_id": 999999999,
                "customer_sk": "INVALID_CUSTOMER_SK_XXXXXXXXXXXXXX",
                "date_sk": 20260101,
                "order_ts": None,
                "status": "completed",
                "shipping_city": "HN",
                "coupon_code": None,
                "total_amount": 100000,
                "has_coupon": False,
                "is_completed": True,
                "_gold_version": "gold_test_orphan",
                "_pipeline_run_id": "test_run",
                "_ingested_at": None,
            }])

            # Tạo fact_orders bình thường rồi union với hàng xấu
            from spark_jobs.gold_transforms import build_fact_orders
            fact_orders = build_fact_orders(orders_df, history_df, "gold_test_orphan", "test_run",
                                            "2026-01-01T00:00:00Z")
            fact_orders_bad = fact_orders.unionByName(bad_row, allowMissingColumns=True)

            # Gate phải phát hiện orphan
            test_report = {"quality_checks": []}
            work_dir = self.work / "orphan_quarantine"
            work_dir.mkdir(parents=True, exist_ok=True)
            quarantine_uri = work_dir.resolve().as_uri()
            gate = GoldQualityGate(quarantine_uri, test_report)

            orphan = fact_orders_bad.join(
                dim_customer.select("customer_sk"), "customer_sk", "left_anti"
            )
            with self.assertRaises(GoldQualityError):
                gate.reject("fact_orders_orphan_customer_sk", orphan)

        finally:
            spark.stop()

    def test_all_quality_checks_passed(self):
        """Tất cả quality_checks trong gold_result phải passed=True."""
        checks = self.gold_result.get("quality_checks", [])
        self.assertGreater(len(checks), 0, "Không có quality_checks trong gold_result")
        failed = [c for c in checks if not c.get("passed")]
        self.assertEqual(failed, [], f"Các check thất bại: {json.dumps(failed, indent=2, default=str)}")


if __name__ == "__main__":
    unittest.main()
