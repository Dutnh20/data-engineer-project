"""Logic Spark: làm sạch, đối soát và SCD2. Không phụ thuộc MinIO hay Docker."""

from pyspark.sql import Window, functions as F

from .silver_contracts import ATTRIBUTES, KEYS, SPECS, fields, normalize, schema_for, with_row_errors


class QualityError(ValueError):
    pass


class QualityGate:
    """Nếu có lỗi, lưu toàn bộ hàng lỗi rồi chặn công bố snapshot."""
    def __init__(self, quarantine_root, report):
        self.root = quarantine_root.rstrip("/")
        self.report = report
        report.setdefault("quality_checks", [])

    def reject(self, check, bad):
        count = bad.count()
        entry = {"check": check, "bad_rows": count, "passed": count == 0}
        self.report["quality_checks"].append(entry)
        if count:
            path = f"{self.root}/{check}"
            if "_dq_reason" not in bad.columns:
                bad = bad.withColumn("_dq_reason", F.lit(check))
            bad.write.mode("errorifexists").parquet(path)
            entry["quarantine_path"] = path
            raise QualityError(f"{check}: {count} invalid rows; quarantine: {path}")

    def require(self, check, condition, detail):
        self.report["quality_checks"].append({"check": check, "passed": bool(condition), "detail": detail})
        if not condition:
            raise QualityError(f"{check}: {detail}")


def duplicate_keys(df, key):
    bad = df.groupBy(key).count().filter(F.col("count") > 1).select(key)
    return df.join(bad, key, "left_semi")


def deduplicate_items(df):
    """Lấy bản retry có created_ts mới nhất; tên file là tie-break ổn định."""
    order = Window.partitionBy("_dataset_version", "order_item_id").orderBy(
        F.col("created_ts").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(order)).filter(F.col("_rn") == 1).drop("_rn")


def build_customer_history(changes, silver_version):
    """Tạo khoảng [valid_from_ts, valid_to_ts), bỏ các update không đổi thuộc tính."""
    same_time = Window.partitionBy("customer_id", "effective_ts").orderBy(
        F.col("source_updated_ts").desc(), F.col("change_id").desc())
    changes = changes.withColumn("_rn", F.row_number().over(same_time)).filter("_rn = 1").drop("_rn")
    sequence = Window.partitionBy("customer_id").orderBy("effective_ts")
    state = F.to_json(F.struct(*ATTRIBUTES))
    changes = changes.withColumn("_state", state).withColumn("_previous_state", F.lag("_state").over(sequence))
    changes = changes.filter(F.col("_previous_state").isNull() | (F.col("_state") != F.col("_previous_state")))
    history = (changes.withColumn("valid_from_ts", F.col("effective_ts"))
               .withColumn("valid_to_ts", F.lead("effective_ts").over(sequence))
               .withColumn("is_current", F.col("valid_to_ts").isNull()))
    # Microseconds giữ nguyên grain thời gian; không dùng ID phụ thuộc partition.
    history = history.withColumn("customer_sk", F.sha2(F.concat_ws(":", F.lit(silver_version),
                                 F.col("customer_id").cast("string"),
                                 F.unix_micros("valid_from_ts").cast("string")), 256))
    return history.select("customer_sk", "customer_id", *ATTRIBUTES, "valid_from_ts", "valid_to_ts", "is_current",
                          "change_id", "_dataset_version", "_source_file", "_pipeline_run_id", "_ingested_at")


def read_bronze(spark, table, paths, silver_version, run_id, ingested_at):
    # Schema payments chứa failure_reason. Parquet v1 thiếu cột sẽ được đọc NULL.
    return (spark.read.schema(schema_for(table)).parquet(*paths)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_dataset_version", F.lit(silver_version))
            .withColumn("_pipeline_run_id", F.lit(run_id))
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp")))


def monetary_sum(df, column):
    value = df.agg(F.sum(F.col(column).cast("decimal(38,0)")).alias("amount")).first()["amount"]
    return int(value or 0)


def transform_snapshot(spark, inputs, expected_rows, tables_root, quarantine_root,
                       silver_version, run_id, ingested_at, report):
    """Đọc Bronze, ghi bảy bảng Silver vào prefix riêng của lần chạy, validate lại."""
    gate = QualityGate(quarantine_root, report)
    report["tables"] = {}
    frames = {}
    for table in SPECS:
        print(f"[DP2] Processing {table}", flush=True)
        df = normalize(read_bronze(spark, table, inputs[table], silver_version, run_id, ingested_at), table)
        tagged = with_row_errors(df, table)
        counts = tagged.agg(F.count("*").alias("rows"),
                            F.sum(F.when(F.length("_dq_reason") > 0, 1).otherwise(0)).alias("bad")).first()
        n_raw = int(counts["rows"])
        gate.require(f"{table}_input_rows", n_raw == expected_rows[table],
                     {"expected": expected_rows[table], "actual": n_raw})
        if counts["bad"]:
            gate.reject(f"{table}_row_rules", tagged.filter(F.length("_dq_reason") > 0))
        else:
            report["quality_checks"].append({"check": f"{table}_row_rules", "bad_rows": 0, "passed": True})
        if table == "order_items":
            business = [name for name, _, _ in fields(table) if name != "created_ts"]
            conflicts = (df.groupBy("order_item_id").agg(F.countDistinct(F.struct(*business)).alias("variants"))
                         .filter("variants > 1").select("order_item_id"))
            gate.reject("order_items_conflicting_retry", df.join(conflicts, "order_item_id", "left_semi"))
            report["raw_items_amount_vnd"] = monetary_sum(df, "line_amount")
            unique_count = df.select("order_item_id").distinct().count()
            df = deduplicate_items(df)
        else:
            gate.reject(f"{table}_duplicate_primary_key", duplicate_keys(df, KEYS[table]))
            unique_count = n_raw
        path = f"{tables_root}/stg_{table}"
        df.write.mode("errorifexists").option("compression", "snappy").parquet(path)
        written = spark.read.parquet(path)
        n_out = written.count()
        gate.require(f"{table}_output_rows", n_out == unique_count,
                     {"expected": unique_count, "actual": n_out})
        gate.reject(f"{table}_output_unique", duplicate_keys(written, KEYS[table]))
        report["tables"][f"stg_{table}"] = {"path": path, "input_rows": n_raw, "rows": n_out,
                                                 "removed_duplicates": n_raw - n_out,
                                                 "columns": written.columns, "schema": written.schema.jsonValue()}
        frames[table] = written

    print("[DP2] Checking foreign keys, amounts and timestamps", flush=True)
    customers, products, orders, items, payments, changes = [frames[t] for t in SPECS]
    for name, child, key, parent in [
        ("orders_customer_fk", orders, "customer_id", customers),
        ("items_order_fk", items, "order_id", orders),
        ("items_product_fk", items, "product_id", products),
        ("payments_order_fk", payments, "order_id", orders),
        ("changes_customer_fk", changes, "customer_id", customers),
    ]:
        gate.reject(name, child.join(parent.select(key), key, "left_anti"))
    gate.reject("orders_before_signup", orders.join(customers.select("customer_id", "signup_ts"), "customer_id")
                .filter(F.col("order_ts") < F.col("signup_ts")))
    order_totals = items.groupBy("order_id").agg(F.sum(F.col("line_amount").cast("decimal(38,0)")).alias("items_total"))
    gate.reject("orders_items_amount", orders.join(order_totals, "order_id", "left")
                .filter(F.col("items_total").isNull() | (F.col("items_total") != F.col("total_amount"))))
    payment_orders = payments.join(orders.select("order_id", "total_amount", "order_ts"), "order_id")
    gate.reject("payments_amount_or_time", payment_orders.filter(
        (F.col("amount") != F.col("total_amount")) | (F.col("payment_ts") < F.col("order_ts"))))
    item_orders = items.join(orders.select("order_id", "order_ts"), "order_id")
    gate.reject("items_before_order", item_orders.filter(F.col("created_ts") < F.col("order_ts")))
    item_products = items.join(products.select("product_id", "base_price", F.col("created_ts").alias("product_created_ts")), "product_id")
    gate.reject("items_product_price_or_time", item_products.filter(
        (F.col("unit_price") != F.col("base_price")) | (F.col("created_ts") < F.col("product_created_ts"))))
    clean_amount = monetary_sum(items, "line_amount")
    report.update(clean_items_amount_vnd=clean_amount,
                  duplicate_amount_removed_vnd=report["raw_items_amount_vnd"] - clean_amount)
    # Đây là giá trị line items của mọi trạng thái, chưa gọi là doanh thu được ghi nhận.

    print("[DP2] Building and checking customer history (SCD2)", flush=True)
    gate.reject("changes_before_signup", changes.join(customers.select("customer_id", "signup_ts"), "customer_id")
                .filter(F.col("effective_ts") < F.col("signup_ts")))
    ambiguity = (changes.groupBy("customer_id", "effective_ts")
                 .agg(F.countDistinct(F.struct(*ATTRIBUTES)).alias("states"))
                 .filter("states > 1").select("customer_id", "effective_ts"))
    gate.reject("changes_ambiguous_timestamp", changes.join(ambiguity, ["customer_id", "effective_ts"], "left_semi"))
    first_window = Window.partitionBy("customer_id").orderBy("effective_ts", "source_updated_ts", "change_id")
    first_changes = changes.withColumn("_rn", F.row_number().over(first_window)).filter("_rn = 1").drop("_rn")
    baseline = customers.alias("c").join(first_changes.alias("h"), F.col("c.customer_id") == F.col("h.customer_id"), "left")
    mismatch = F.col("h.customer_id").isNull() | (F.col("h.change_type") != "INSERT") | ~F.col("c.signup_ts").eqNullSafe(F.col("h.effective_ts"))
    for attr in ATTRIBUTES:
        mismatch = mismatch | ~F.col("c." + attr).eqNullSafe(F.col("h." + attr))
    gate.reject("customer_history_baseline", baseline.filter(mismatch).select("c.*"))
    history = build_customer_history(changes, silver_version)
    history_path = f"{tables_root}/stg_customer_history"
    history.write.mode("errorifexists").option("compression", "snappy").parquet(history_path)
    history = spark.read.parquet(history_path)
    gate.reject("history_unique_sk", duplicate_keys(history, "customer_sk"))
    gate.reject("history_valid_intervals", history.filter(
        F.col("valid_to_ts").isNotNull() & (F.col("valid_from_ts") >= F.col("valid_to_ts"))))
    current_counts = history.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("current_count"))
    gate.reject("history_one_current", customers.select("customer_id").join(current_counts, "customer_id", "left")
                .filter(F.col("current_count").isNull() | (F.col("current_count") != 1)))
    history_sequence = Window.partitionBy("customer_id").orderBy("valid_from_ts")
    intervals = history.withColumn("_next_from", F.lead("valid_from_ts").over(history_sequence))
    gate.reject("history_no_gaps_or_overlap", intervals.filter(~F.col("valid_to_ts").eqNullSafe(F.col("_next_from"))))
    matching = orders.alias("o").join(history.alias("h"),
        (F.col("o.customer_id") == F.col("h.customer_id")) &
        (F.col("o.order_ts") >= F.col("h.valid_from_ts")) &
        (F.col("h.valid_to_ts").isNull() | (F.col("o.order_ts") < F.col("h.valid_to_ts"))), "left")
    matches = matching.groupBy(F.col("o.order_id").alias("order_id")).agg(F.count("h.customer_sk").alias("versions"))
    gate.reject("orders_exactly_one_history_version", matches.filter("versions != 1"))
    report["tables"]["stg_customer_history"] = {"path": history_path, "rows": history.count(),
                                                "columns": history.columns, "schema": history.schema.jsonValue()}
    return report
