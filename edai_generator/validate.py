"""Independent SQL checks against the generated Parquet files, not only counters."""

from fractions import Fraction
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .common import dump_json, sha256_file
from .offline import load_completed_run
from .schemas import schema_for


def sql_string(value):
    """Quote a filesystem path as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def validate_run(root, checksums=False, memory_limit="512MB"):
    """Check physical files, FK ranges, arithmetic, duplicates and schema versions.

    DuckDB can spill to disk; the full validation of 100GB still takes time and
    additional temporary disk space. This command never repairs the dirty data.
    """
    root, cfg, manifest = load_completed_run(root)
    inventory = [json.loads(line) for line in (root / "file_index.jsonl").read_text().splitlines()]
    failures = []
    observed_paths = {str(path.relative_to(root)).replace("\\", "/")
                      for path in (root / "source" / "offline").rglob("*.parquet")}
    expected_paths = {entry["path"] for entry in inventory}
    if observed_paths != expected_paths:
        failures.append("Parquet inventory differs from committed source files")
    actual_bytes = 0
    versions = set()
    for entry in inventory:
        path = root / entry["path"]
        if not path.is_file():
            failures.append(f"Missing file: {entry['path']}")
            continue
        actual_bytes += path.stat().st_size
        if path.stat().st_size != entry["bytes"]:
            failures.append(f"Size differs: {entry['path']}")
            continue
        try:
            parquet = pq.ParquetFile(path)
        except Exception as exc:
            failures.append(f"Invalid Parquet: {entry['path']}: {type(exc).__name__}")
            continue
        if parquet.metadata.num_rows != entry["rows"]:
            failures.append(f"Row count differs: {entry['path']}")
        if not parquet.schema_arrow.equals(schema_for(entry["table"], entry["schema_version"]), check_metadata=False):
            failures.append(f"Schema differs: {entry['path']}")
        if entry["table"] == "payments":
            versions.add(entry["schema_version"])
        if checksums and sha256_file(path) != entry["sha256"]:
            failures.append(f"SHA256 differs: {entry['path']}")
    if versions != {1, 2}:
        failures.append("Both physical payment schemas must be present")
    if actual_bytes != manifest["physical_source_bytes"]:
        failures.append("Physical byte total differs from manifest")
    if cfg.target_bytes is not None and actual_bytes < cfg.target_bytes:
        failures.append("The configured physical-byte target has not been reached")
    if failures:
        report = {"passed": False, "failures": failures, "scope": "file_integrity",
                  "physical_source_bytes": actual_bytes}
        dump_json(root / "evidence" / "validation.json", report)
        return report

    temporary = root / "_validation_tmp"
    temporary.mkdir(exist_ok=True)
    db = duckdb.connect()
    checks = {}
    counts = {}
    cardinality = {}
    try:
        db.execute(f"SET memory_limit={sql_string(memory_limit)}")
        db.execute("SET threads=2")
        db.execute("SET preserve_insertion_order=false")
        db.execute(f"SET temp_directory={sql_string(temporary.as_posix())}")
        for table in ("customers", "customer_changes", "products", "orders", "order_items", "payments"):
            glob = (root / "source" / "offline" / table / "*.parquet").as_posix()
            db.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet({sql_string(glob)}, union_by_name=true)")
        for table, key in (("customers", "customer_id"), ("customer_changes", "change_id"), ("products", "product_id"),
                           ("orders", "order_id"), ("order_items", "order_item_id"), ("payments", "payment_id")):
            count, distinct, low, high = db.execute(
                f"SELECT count(*),count(DISTINCT {key}),min({key}),max({key}) FROM {table}"
            ).fetchone()
            counts[table] = count
            cardinality[f"{table}.{key}"] = {"unique_values": distinct, "rows": count, "ratio": distinct / count}
            checks[f"{table}_row_count_matches_manifest"] = count == manifest["tables"][table]["rows"]
            if table != "order_items":
                checks[f"{table}_unique_primary_key"] = count == distinct
            if table in ("customers", "products", "orders"):
                expected = cfg.customers if table == "customers" else cfg.products if table == "products" else manifest["orders_generated"]
                checks[f"{table}_complete_id_domain"] = low == 1 and high == expected and count == distinct == expected

        total_orders = manifest["orders_generated"]
        zero_queries = {
            "orders_customer_fk": f"SELECT count(*) FROM orders WHERE customer_id IS NULL OR customer_id NOT BETWEEN 1 AND {cfg.customers}",
            "changes_customer_fk": f"SELECT count(*) FROM customer_changes WHERE customer_id IS NULL OR customer_id NOT BETWEEN 1 AND {cfg.customers}",
            "changes_valid_time": "SELECT count(*) FROM customer_changes h JOIN customers c USING(customer_id) WHERE h.effective_ts < c.signup_ts OR h.source_updated_ts < h.effective_ts",
            "changes_known_values": "SELECT count(*) FROM customer_changes WHERE change_type NOT IN ('INSERT','UPDATE') OR segment NOT IN ('loyal','standard')",
            "changes_unique_effective_time": "SELECT count(*) FROM (SELECT customer_id,effective_ts FROM customer_changes GROUP BY customer_id,effective_ts HAVING count(*)<>1)",
            "changes_initial_snapshot": "SELECT count(*) FROM customers c LEFT JOIN customer_changes h ON c.customer_id=h.customer_id AND h.change_type='INSERT' WHERE h.change_id IS NULL OR h.effective_ts<>c.signup_ts OR (h.customer_name,h.city,h.segment,h.device_type) IS DISTINCT FROM (c.customer_name,c.city,c.segment,c.device_type)",
            "changes_one_initial_per_customer": "SELECT count(*) FROM (SELECT customer_id FROM customer_changes GROUP BY customer_id HAVING count(*) FILTER (WHERE change_type='INSERT')<>1)",
            "changes_actual_segment_update": "SELECT count(*) FROM (SELECT change_type,segment,lag(segment) OVER(PARTITION BY customer_id ORDER BY effective_ts) previous_segment FROM customer_changes) WHERE change_type='UPDATE' AND (previous_segment IS NULL OR segment=previous_segment)",
            "items_order_fk": f"SELECT count(*) FROM order_items WHERE order_id IS NULL OR order_id NOT BETWEEN 1 AND {total_orders}",
            "items_product_fk": f"SELECT count(*) FROM order_items WHERE product_id IS NULL OR product_id NOT BETWEEN 1 AND {cfg.products}",
            "payments_order_fk": f"SELECT count(*) FROM payments WHERE order_id IS NULL OR order_id NOT BETWEEN 1 AND {total_orders}",
            "item_arithmetic": "SELECT count(*) FROM order_items WHERE quantity <= 0 OR unit_price <= 0 OR discount_amount < 0 OR discount_amount > quantity*unit_price OR line_amount IS NULL OR line_amount <> quantity*unit_price-discount_amount",
            "customer_before_order": "SELECT count(*) FROM orders o JOIN customers c USING(customer_id) WHERE o.order_ts < c.signup_ts OR o.order_ts IS NULL",
            "payment_amount": "SELECT count(*) FROM payments p JOIN orders o USING(order_id) WHERE p.amount <> o.total_amount OR p.amount IS NULL OR p.payment_ts < o.order_ts",
            "duplicate_business_fields_unchanged": "SELECT count(*) FROM (SELECT order_item_id FROM order_items GROUP BY order_item_id HAVING count(DISTINCT (order_id,product_id,quantity,unit_price,discount_amount,line_amount))<>1 OR count(*)>2)",
            "duplicate_timestamps": "SELECT count(*) FROM (SELECT order_item_id FROM order_items GROUP BY order_item_id HAVING count(*)=2 AND max(created_ts)-min(created_ts)<>INTERVAL '1 second')",
            "payment_schema_boundary": f"SELECT count(*) FROM payments WHERE order_id<={cfg.schema_change_after_order} AND failure_reason IS NOT NULL",
            "payment_failure_reason_v2": f"SELECT count(*) FROM payments WHERE order_id>{cfg.schema_change_after_order} AND ((status='failed' AND failure_reason IS NULL) OR (status<>'failed' AND failure_reason IS NOT NULL))",
            "shipping_city_matches_customer": "SELECT count(*) FROM orders o JOIN customers c USING(customer_id) WHERE o.shipping_city<>c.city",
        }
        for label, query in zero_queries.items():
            checks[label] = db.execute(query).fetchone()[0] == 0
        change_rate = Fraction(str(cfg.customer_change_fraction))
        expected_customers = cfg.customers * change_rate.numerator // change_rate.denominator
        changed_customers, updates = db.execute("""
            SELECT count(DISTINCT customer_id),count(*) FROM customer_changes
            WHERE change_type='UPDATE'
        """).fetchone()
        checks["changes_count_matches_rate"] = (changed_customers == expected_customers
                                                   and updates == expected_customers + expected_customers // 2)
        history_start, history_end = db.execute("SELECT min(order_ts),max(order_ts) FROM orders").fetchone()
        checks["changes_within_observed_order_history"] = db.execute("""
            SELECT count(*) FROM customer_changes
            WHERE change_type='UPDATE' AND (effective_ts < ? OR effective_ts > ?)
        """, [history_start, history_end]).fetchone()[0] == 0
        db.execute("""CREATE VIEW clean_items AS SELECT * FROM order_items
                      QUALIFY row_number() OVER(PARTITION BY order_item_id ORDER BY created_ts DESC)=1""")
        mismatches = db.execute("""
            SELECT count(*) FROM orders o
            LEFT JOIN (SELECT order_id,sum(line_amount) amount FROM clean_items GROUP BY order_id) i USING(order_id)
            WHERE i.amount IS NULL OR o.total_amount <> i.amount
        """).fetchone()[0]
        checks["order_totals_after_deduplication"] = mismatches == 0
        checks["payment_status_consistency"] = db.execute("""
            SELECT count(*) FROM orders o LEFT JOIN (
                SELECT order_id,count(*) FILTER(WHERE status='succeeded') successes,
                       count(*) FILTER(WHERE status='pending') pending
                FROM payments GROUP BY order_id
            ) p USING(order_id)
            WHERE p.order_id IS NULL OR (o.status='completed' AND p.successes<>1)
              OR (o.status<>'completed' AND p.successes<>0)
              OR (o.status='pending' AND p.pending<>1)
        """).fetchone()[0] == 0
        for table, column in (("orders", "customer_id"), ("order_items", "product_id"), ("payments", "order_id")):
            unique = db.execute(f"SELECT count(DISTINCT {column}) FROM {table}").fetchone()[0]
            cardinality[f"{table}.{column}"] = {"unique_values": unique, "rows": counts[table], "ratio": unique / counts[table]}
        city_counts = dict(db.execute("SELECT shipping_city,count(*) FROM orders GROUP BY shipping_city").fetchall())
        category_counts = dict(db.execute("SELECT category,count(*) FROM products GROUP BY category").fetchall())
        checks["shipping_distribution_matches_manifest"] = city_counts == manifest["shipping_city_counts"]
        checks["category_skew"] = category_counts.get("electronics", 0) == cfg.products - cfg.products // 4
        unique_items = cardinality["order_items.order_item_id"]["unique_values"]
        copies = counts["order_items"] - unique_items
        rate = Fraction(str(cfg.offline_duplicate_fraction))
        expected_copies = unique_items * rate.numerator // (rate.denominator - rate.numerator)
        checks["duplicate_count_matches_rate"] = copies == expected_copies
        checks["duplicate_count_matches_manifest"] = copies == manifest["offline_duplicates"]["extra_copies"]
        raw_amount = db.execute("SELECT sum(line_amount) FROM order_items").fetchone()[0]
        clean_amount = db.execute("SELECT sum(total_amount) FROM orders").fetchone()[0]
        report = {
            "passed": all(checks.values()), "scope": "full_parquet_scan",
            "config_fingerprint": cfg.fingerprint(),
            "checks": checks, "failures": [name for name, good in checks.items() if not good],
            "checksums_verified": checksums, "physical_source_bytes": actual_bytes,
            "physical_source_gb": actual_bytes / 1e9, "rubric_100gb_met": actual_bytes >= 100_000_000_000,
            "rows": counts, "cardinality": cardinality,
            "order_time_range": {"start": history_start.isoformat(), "end": history_end.isoformat(),
                                 "span_days": (history_end - history_start).total_seconds() / 86400},
            "customer_history": {"customers_with_updates": changed_customers, "update_rows": updates,
                                 "initial_rows": counts["customer_changes"] - updates},
            "shipping_city_counts": city_counts, "category_counts": category_counts,
            "duplicate_extra_rows": copies, "duplicate_fraction_of_final_rows": copies / counts["order_items"],
            "schema_versions": sorted(versions),
            "duplicate_amount_effect_vnd": {"raw_item_total": int(raw_amount),
                "deduplicated_order_total": int(clean_amount), "overcount": int(raw_amount - clean_amount),
                "note": "Order value, not recognized revenue; cancelled/pending orders are included."},
        }
        dump_json(root / "evidence" / "validation.json", report)
        return report
    finally:
        db.close()
