"""Bounded-batch Parquet generation, transactional checkpoints and measured size."""

from collections import Counter
from datetime import timedelta
import json
from pathlib import Path
import shutil
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq

from .common import dump_json, sha256_file
from . import __version__
from .config import Config
from .model import customer, customer_changes, product, make_order, duplicate_copy_due
from .schemas import SCHEMAS, schema_for, describe_schema


class RunStore:
    """Commit file metadata only after every Parquet file of a chunk is complete.

    A stopped process may leave uncommitted files. A resume regenerates those
    exact paths. Only files listed in the SQLite journal count toward the target.
    One generator process may write a run directory at a time.
    """

    def __init__(self, root, cfg, resume=False):
        """Create a run, or verify its configuration before resuming."""
        self.root = Path(root).resolve()
        existing = self.root.exists() and any(self.root.iterdir())
        if existing and not resume:
            raise ValueError("Output is not empty; use a new directory or --resume with the same config")
        if existing and not (self.root / "journal.sqlite").exists():
            raise ValueError("Cannot resume: journal.sqlite is missing")
        self.root.mkdir(parents=True, exist_ok=True)
        self.cfg = cfg
        self.db = sqlite3.connect(self.root / "journal.sqlite")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS chunks (
              phase TEXT, first_id INTEGER, last_id INTEGER, stats TEXT NOT NULL,
              PRIMARY KEY (phase, first_id));
            CREATE TABLE IF NOT EXISTS files (
              path TEXT PRIMARY KEY, table_name TEXT, rows INTEGER, bytes INTEGER,
              arrow_bytes INTEGER, sha256 TEXT, schema_version INTEGER);
        """)
        prior = self.db.execute("SELECT value FROM settings WHERE key='fingerprint'").fetchone()
        if prior and prior[0] != cfg.fingerprint():
            self.db.close()
            raise ValueError("Configuration/version differs from checkpoint; choose a new output directory")
        if not prior:
            with self.db:
                self.db.execute("INSERT INTO settings VALUES ('fingerprint', ?)", (cfg.fingerprint(),))
            dump_json(self.root / "config.json", cfg.to_dict())
        if resume:
            for path, size in self.db.execute("SELECT path, bytes FROM files"):
                file = self.root / path
                if not file.is_file() or file.stat().st_size != size:
                    raise ValueError(f"Committed file missing or size changed: {file}")

    def close(self):
        """Close the checkpoint database."""
        self.db.close()

    def last_id(self, phase):
        """Return the final committed ID of a phase, zero if none exists."""
        return self.db.execute("SELECT COALESCE(MAX(last_id),0) FROM chunks WHERE phase=?", (phase,)).fetchone()[0]

    def physical_bytes(self):
        """Sum only committed Parquet source bytes, never copies or reports."""
        return self.db.execute("SELECT COALESCE(SUM(bytes),0) FROM files").fetchone()[0]

    def free_space_check(self):
        """Stop at a checkpoint when free disk space falls below the configured margin."""
        if shutil.disk_usage(self.root).free < self.cfg.min_free_gb * 1_000_000_000:
            raise OSError("Low disk space; free space and resume with the same configuration")

    def write_table(self, table_name, first_id, rows, version=1):
        """Write a closed Parquet shard atomically and return its actual metadata."""
        schema = schema_for(table_name, version)
        arrow = pa.Table.from_pylist(rows, schema=schema)
        arrow.validate(full=True)
        relative = f"source/offline/{table_name}/part-{first_id:012d}.parquet"
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".parquet.tmp")
        pq.write_table(
            arrow, temporary, compression=self.cfg.compression,
            row_group_size=100000, use_dictionary=True, write_statistics=True,
        )
        temporary.replace(path)
        return (relative, table_name, len(rows), path.stat().st_size,
                arrow.nbytes, sha256_file(path), version)

    def commit(self, phase, first_id, last_id, files, stats):
        """Publish one logical chunk of files in a single SQLite transaction."""
        with self.db:
            self.db.executemany("INSERT INTO files VALUES (?,?,?,?,?,?,?)", files)
            self.db.execute("INSERT INTO chunks VALUES (?,?,?,?)",
                            (phase, first_id, last_id, json.dumps(stats, sort_keys=True)))

    def write_report(self, status):
        """Export an auditable manifest and file inventory from committed metadata."""
        table_stats = {}
        for table, rows, size, logical, count in self.db.execute(
            "SELECT table_name,SUM(rows),SUM(bytes),SUM(arrow_bytes),COUNT(*) FROM files GROUP BY table_name"
        ):
            table_stats[table] = {"rows": rows, "physical_bytes": size,
                                  "arrow_logical_bytes": logical, "files": count}
        city_counts = Counter()
        clean_items = copies = 0
        for (body,) in self.db.execute("SELECT stats FROM chunks WHERE phase='orders'"):
            stats = json.loads(body)
            clean_items += stats["clean_items"]
            copies += stats["duplicate_copies"]
            city_counts.update(stats["shipping_city_counts"])
        order_count = self.last_id("orders")
        total_bytes = self.physical_bytes()
        report = {
            "status": status, "data_origin": "100_percent_synthetic",
            "generator_version": __version__, "config_fingerprint": self.cfg.fingerprint(),
            "tables": table_stats, "orders_generated": order_count,
            "physical_source_bytes": total_bytes, "physical_source_gb": total_bytes / 1e9,
            "target_bytes": self.cfg.target_bytes,
            "target_reached": self.cfg.target_bytes is not None and total_bytes >= self.cfg.target_bytes,
            "rubric_100gb_met": total_bytes >= 100_000_000_000,
            "size_definition": "Sum of actual committed Parquet bytes in five core tables plus customer_changes; excludes reports, streams, copies in MinIO/bronze, and padding.",
            "customer_history": {
                "source_table": "customer_changes",
                "baseline": "customers is the signup snapshot, not the latest state",
                "changed_customer_fraction": self.cfg.customer_change_fraction,
                "minimum_order_history_seconds": self.cfg.minimum_span_seconds,
                "first_change_offset_seconds": self.cfg.minimum_span_seconds * 0.25,
                "second_change_offset_seconds": self.cfg.minimum_span_seconds * 0.60,
                "note": "Initial image per customer; selected customers change segment once, every second selected customer changes twice. Build SCD2 downstream using effective_ts.",
            },
            "shipping_city_counts": dict(city_counts),
            "hcm_order_fraction": city_counts["Ho Chi Minh"] / order_count if order_count else None,
            "product_electronics_fraction": (self.cfg.products - self.cfg.products // 4) / self.cfg.products,
            "offline_duplicates": {
                "clean_rows": clean_items, "extra_copies": copies,
                "total_rows": clean_items + copies,
                "fraction_of_final_rows": copies / (clean_items + copies) if clean_items else 0,
                "configured_fraction_of_final_rows": self.cfg.offline_duplicate_fraction,
                "key": "order_item_id", "changed_column": "created_ts (+1 second)",
            },
            "schema_evolution": {
                "table": "payments", "last_v1_order_id": self.cfg.schema_change_after_order,
                "first_v2_order_id": self.cfg.schema_change_after_order + 1,
                "boundary_semantics": "source generation order, not historical event time",
                "before": describe_schema(schema_for("payments", 1)),
                "after": describe_schema(schema_for("payments", 2)),
                "observed_versions": [row[0] for row in self.db.execute(
                    "SELECT DISTINCT schema_version FROM files WHERE table_name='payments' ORDER BY 1")],
            },
            "declared_primary_key_cardinality": {
                "customers.customer_id": 1.0, "customer_changes.change_id": 1.0, "products.product_id": 1.0,
                "orders.order_id": 1.0, "payments.payment_id": 1.0,
                "order_items.order_item_id": clean_items / (clean_items + copies) if clean_items else None,
            },
            "validation_note": "These are generator counters; run validate for independent file-based checks and measured cardinality.",
        }
        dump_json(self.root / "manifest.json", report)
        index = self.root / "file_index.jsonl"
        temporary = index.with_suffix(".jsonl.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            for row in self.db.execute("SELECT path,table_name,rows,bytes,sha256,schema_version FROM files ORDER BY path"):
                handle.write(json.dumps(dict(zip(
                    ["path", "table", "rows", "bytes", "sha256", "schema_version"], row
                ))) + "\n")
        temporary.replace(index)
        return report


def generate(cfg, output, resume=False, stop_after_chunks=None):
    """Generate dimensions and orders in batches until row count or physical target.

    In byte-target mode, cfg.orders is a minimum, not a maximum. The final batch
    can exceed the target. No random padding or repeated entire datasets is used.
    """
    cfg.validate()
    store = RunStore(output, cfg, resume)
    try:
        for table_name, total, factory in (("customers", cfg.customers, customer), ("products", cfg.products, product)):
            first = store.last_id(table_name) + 1
            while first <= total:
                store.free_space_check()
                last = min(total, first + cfg.dimension_batch - 1)
                rows = [factory(entity_id, cfg) for entity_id in range(first, last + 1)]
                info = store.write_table(table_name, first, rows)
                store.commit(table_name, first, last, [info], {})
                first = last + 1
        # Lịch sử nguồn được checkpoint và upload cùng các bảng còn lại.
        first = store.last_id("customer_changes") + 1
        while first <= cfg.customers:
            store.free_space_check()
            last = min(cfg.customers, first + cfg.dimension_batch - 1)
            changes = [row for entity_id in range(first, last + 1)
                       for row in customer_changes(entity_id, cfg)]
            info = store.write_table("customer_changes", first, changes)
            store.commit("customer_changes", first, last, [info], {})
            first = last + 1
        clean_ordinal = sum(json.loads(body)["clean_items"] for (body,) in store.db.execute(
            "SELECT stats FROM chunks WHERE phase='orders'"))
        committed_this_call = 0
        while True:
            previous = store.last_id("orders")
            enough_rows = previous >= cfg.orders
            enough_bytes = cfg.target_bytes is None or store.physical_bytes() >= cfg.target_bytes
            if enough_rows and enough_bytes:
                report = store.write_report("complete")
                print(json.dumps({"status": "complete", "orders": previous,
                                  "physical_gb": report["physical_source_gb"],
                                  "rubric_100gb_met": report["rubric_100gb_met"]}), flush=True)
                return report
            if stop_after_chunks is not None and committed_this_call >= stop_after_chunks:
                return store.write_report("paused")
            store.free_space_check()
            first = previous + 1
            last = first + cfg.batch_orders - 1
            if cfg.target_bytes is None:
                last = min(last, cfg.orders)
            if first <= cfg.schema_change_after_order:
                last = min(last, cfg.schema_change_after_order)
            version = 1 if first <= cfg.schema_change_after_order else 2
            batches = {table: [] for table in ("orders", "order_items", "payments")}
            cities = Counter()
            clean_count = copy_count = 0
            for order_id in range(first, last + 1):
                order, items, payments = make_order(order_id, cfg)
                batches["orders"].append(order)
                cities[order["shipping_city"]] += 1
                for item in items:
                    batches["order_items"].append(item)
                    clean_ordinal += 1
                    clean_count += 1
                    if duplicate_copy_due(clean_ordinal, cfg.offline_duplicate_fraction):
                        batches["order_items"].append({**item, "created_ts": item["created_ts"] + timedelta(seconds=1)})
                        copy_count += 1
                batches["payments"].extend(payments)
            files = [store.write_table(table, first, rows, version if table == "payments" else 1)
                     for table, rows in batches.items()]
            store.commit("orders", first, last, files, {
                "clean_items": clean_count, "duplicate_copies": copy_count,
                "shipping_city_counts": dict(cities),
            })
            committed_this_call += 1
            if committed_this_call % 10 == 0 or committed_this_call == 1:
                print(json.dumps({"committed_order_id": last,
                                  "physical_gb": round(store.physical_bytes() / 1e9, 6)}), flush=True)
    except BaseException:
        store.write_report("interrupted")
        raise
    finally:
        store.close()


def load_completed_run(root):
    """Read a completed source configuration and its measured manifest."""
    root = Path(root).resolve()
    report = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    cfg = Config(**json.loads((root / "config.json").read_text(encoding="utf-8"))).validate()
    if report["status"] != "complete" or report["config_fingerprint"] != cfg.fingerprint():
        raise ValueError("Run is incomplete or configuration changed; resume generation first")
    return root, cfg, report
