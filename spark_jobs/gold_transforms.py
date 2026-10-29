"""Logic Spark: xây dựng Gold Layer (Star Schema) từ Silver.

Bao gồm các hàm build_dim_*, build_fact_*, build_agg_*,
class GoldQualityGate và hàm orchestrator transform_gold().
"""

import logging

from pyspark.sql import Window, functions as F
from pyspark.sql.types import IntegerType

from .gold_contracts import (
    DAY_NAME_VI,
    MONTH_NAME_VI,
    gold_dq_rules_dim_customer,
    gold_dq_rules_fact_order_items,
    gold_dq_rules_fact_orders,
    gold_dq_rules_fact_payment_attempts,
    sk_expr_order,
    sk_expr_order_item,
    sk_expr_payment,
    sk_expr_product,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GoldQualityGate – cùng pattern với QualityGate ở silver_transforms.py
# ---------------------------------------------------------------------------

class GoldQualityError(ValueError):
    """Raise khi có hàng vi phạm quy tắc DQ ở Gold."""


class GoldQualityGate:
    """Nếu có lỗi, lưu toàn bộ hàng lỗi vào quarantine rồi chặn công bố."""

    def __init__(self, quarantine_root, report):
        self.root = quarantine_root.rstrip("/")
        self.report = report
        report.setdefault("quality_checks", [])

    def reject(self, check, bad):
        """Đếm hàng lỗi; nếu > 0 thì ghi quarantine và raise GoldQualityError."""
        count = bad.count()
        entry = {"check": check, "bad_rows": count, "passed": count == 0}
        self.report["quality_checks"].append(entry)
        if count:
            path = f"{self.root}/{check}"
            if "_dq_reason" not in bad.columns:
                bad = bad.withColumn("_dq_reason", F.lit(check))
            bad.write.mode("errorifexists").parquet(path)
            entry["quarantine_path"] = path
            raise GoldQualityError(f"{check}: {count} invalid rows; quarantine: {path}")

    def require(self, check, condition, detail):
        """Kiểm tra điều kiện boolean; không raise nhưng ghi kết quả vào report."""
        self.report["quality_checks"].append({"check": check, "passed": bool(condition), "detail": detail})
        if not condition:
            raise GoldQualityError(f"{check}: {detail}")

    def reject_sql(self, check, df, sql_condition):
        """Reject các hàng KHÔNG thỏa mãn sql_condition (filter ngược)."""
        bad = df.filter(f"NOT ({sql_condition})")
        self.reject(check, bad)


# ---------------------------------------------------------------------------
# Dimension: dim_customer
# ---------------------------------------------------------------------------

def build_dim_customer(history_df, gold_version, run_id, ingested_at):
    """Giữ TOÀN BỘ phiên bản SCD2 của khách hàng (đúng nghĩa SCD Type 2).

    fact_orders gắn customer_sk của phiên bản có hiệu lực tại order_ts, nên dim phải
    chứa cả phiên bản cũ; nếu chỉ giữ is_current=True thì đơn cũ thành orphan FK.
    Mỗi customer_id có đúng một dòng is_current=True.
    Surrogate key customer_sk được giữ nguyên từ Silver.
    """
    dim = (history_df
           .select(
               "customer_sk",
               "customer_id",
               "customer_name",
               "city",
               "segment",
               "device_type",
               "valid_from_ts",
               "valid_to_ts",
               "is_current",
               F.lit(gold_version).alias("_gold_version"),
               F.lit(run_id).alias("_pipeline_run_id"),
               F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
           ))
    return dim


# ---------------------------------------------------------------------------
# Dimension: dim_product
# ---------------------------------------------------------------------------

def build_dim_product(products_df, gold_version, run_id, ingested_at):
    """Xây dựng bảng chiều sản phẩm với surrogate key sha256."""
    dim = (products_df
           .withColumn("product_sk", F.expr(sk_expr_product(gold_version)))
           .select(
               "product_sk",
               "product_id",
               "product_name",
               "category",
               "brand",
               "base_price",
               "is_active",
               "created_ts",
               F.lit(gold_version).alias("_gold_version"),
               F.lit(run_id).alias("_pipeline_run_id"),
               F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
           ))
    return dim


# ---------------------------------------------------------------------------
# Dimension: dim_date
# ---------------------------------------------------------------------------

def build_dim_date(spark, min_date, max_date, gold_version):
    """Sinh bảng chiều thời gian từ min_date đến max_date + 30 ngày buffer.

    Sử dụng spark.range() + date arithmetic để tránh collect trên driver.
    min_date, max_date: datetime.date
    """
    from datetime import timedelta

    start = min_date
    end = max_date + timedelta(days=30)
    n_days = (end - start).days + 1

    # Ánh xạ tên tháng và ngày tiếng Việt bằng Spark SQL CASE WHEN
    month_case = "CASE month(date_actual) " + " ".join(
        f"WHEN {k} THEN '{v}'" for k, v in MONTH_NAME_VI.items()
    ) + " END"

    # day_of_week: dayofweek trả về 1=Sunday ... 7=Saturday (Spark convention)
    # Chuyển sang ISO: Mon=1 ... Sun=7
    # Spark: 1=Sun → ISO 7, 2=Mon → ISO 1, ..., 7=Sat → ISO 6
    iso_dow = "CASE dayofweek(date_actual) WHEN 1 THEN 7 ELSE dayofweek(date_actual) - 1 END"
    day_case = (
        "CASE (" + iso_dow + ") "
        + " ".join(f"WHEN {k} THEN '{v}'" for k, v in DAY_NAME_VI.items())
        + " END"
    )

    start_epoch = int(start.strftime("%Y%m%d"))

    dim = (spark.range(n_days)
           .selectExpr(f"date_add(to_date('{start.isoformat()}'), cast(id as int)) as date_actual")
           .selectExpr(
               "cast(date_format(date_actual, 'yyyyMMdd') as int) as date_sk",
               "date_actual",
               "year(date_actual) as year",
               "quarter(date_actual) as quarter",
               "month(date_actual) as month",
               f"{month_case} as month_name_vi",
               "dayofmonth(date_actual) as day_of_month",
               f"({iso_dow}) as day_of_week",
               f"{day_case} as day_name_vi",
               f"CASE WHEN ({iso_dow}) >= 6 THEN true ELSE false END as is_weekend",
               "weekofyear(date_actual) as week_of_year",
           ))
    return dim


# ---------------------------------------------------------------------------
# Fact: fact_orders
# ---------------------------------------------------------------------------

def build_fact_orders(orders_df, customer_history_df, gold_version, run_id, ingested_at):
    """Một hàng mỗi đơn hàng; join SCD2 để tìm customer_sk tại thời điểm đặt hàng."""
    # Range join: tìm phiên bản lịch sử khách hàng tại thời điểm order_ts
    fact = (orders_df.alias("o")
            .join(
                customer_history_df.alias("h"),
                (F.col("o.customer_id") == F.col("h.customer_id")) &
                (F.col("o.order_ts") >= F.col("h.valid_from_ts")) &
                (F.col("h.valid_to_ts").isNull() | (F.col("o.order_ts") < F.col("h.valid_to_ts"))),
                "left",
            )
            .withColumn("order_sk", F.expr(sk_expr_order(gold_version)))
            .withColumn("date_sk", F.col("o.order_ts").cast("date"))
            .withColumn("date_sk", F.expr("cast(date_format(cast(o.order_ts as date), 'yyyyMMdd') as int)"))
            .withColumn("has_coupon", F.col("o.coupon_code").isNotNull())
            .withColumn("is_completed", F.col("o.status") == "completed")
            .select(
                "order_sk",
                F.col("o.order_id").alias("order_id"),
                F.col("h.customer_sk").alias("customer_sk"),
                "date_sk",
                F.col("o.order_ts").alias("order_ts"),
                F.col("o.status").alias("status"),
                F.col("o.shipping_city").alias("shipping_city"),
                F.col("o.coupon_code").alias("coupon_code"),
                F.col("o.total_amount").alias("total_amount"),
                "has_coupon",
                "is_completed",
                F.lit(gold_version).alias("_gold_version"),
                F.lit(run_id).alias("_pipeline_run_id"),
                F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
            ))
    return fact


# ---------------------------------------------------------------------------
# Fact: fact_order_items
# ---------------------------------------------------------------------------

def build_fact_order_items(items_df, fact_orders_df, products_df, gold_version, run_id, ingested_at):
    """Một hàng mỗi dòng đơn hàng; FK đến fact_orders và dim_product."""
    # Lấy order_sk từ fact_orders
    order_keys = fact_orders_df.select("order_id", "order_sk")
    # Lấy product_sk từ dim_product
    product_keys = products_df.withColumn(
        "product_sk", F.expr(sk_expr_product(gold_version))
    ).select("product_id", "product_sk")

    fact = (items_df
            .join(order_keys, "order_id", "left")
            .join(product_keys, "product_id", "left")
            .withColumn("order_item_sk", F.expr(sk_expr_order_item(gold_version)))
            .select(
                "order_item_sk",
                "order_item_id",
                F.col("order_sk"),
                "order_id",
                F.col("product_sk"),
                "product_id",
                "quantity",
                "unit_price",
                "discount_amount",
                "line_amount",
                "created_ts",
                F.lit(gold_version).alias("_gold_version"),
                F.lit(run_id).alias("_pipeline_run_id"),
                F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
            ))
    return fact


# ---------------------------------------------------------------------------
# Fact: fact_payment_attempts
# ---------------------------------------------------------------------------

def build_fact_payment_attempts(payments_df, fact_orders_df, gold_version, run_id, ingested_at):
    """Một hàng mỗi lần thanh toán; is_final_success = lần succeeded cuối cùng."""
    # Window để tìm attempt_no cao nhất có status='succeeded' trong mỗi đơn hàng
    w = Window.partitionBy("order_id")
    payments_enriched = (payments_df
                         .withColumn(
                             "max_success_attempt",
                             F.max(F.when(F.col("status") == "succeeded", F.col("attempt_no"))).over(w),
                         )
                         .withColumn(
                             "is_final_success",
                             (F.col("status") == "succeeded") & (F.col("attempt_no") == F.col("max_success_attempt")),
                         ))

    # Lấy order_sk từ fact_orders
    order_keys = fact_orders_df.select("order_id", "order_sk")

    fact = (payments_enriched
            .join(order_keys, "order_id", "left")
            .withColumn("payment_sk", F.expr(sk_expr_payment(gold_version)))
            .select(
                "payment_sk",
                "payment_id",
                F.col("order_sk"),
                "order_id",
                "attempt_no",
                "payment_ts",
                "method",
                "provider",
                "amount",
                "status",
                "failure_reason",
                "is_final_success",
                F.lit(gold_version).alias("_gold_version"),
                F.lit(run_id).alias("_pipeline_run_id"),
                F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
            ))
    return fact


# ---------------------------------------------------------------------------
# Aggregation: agg_daily_sales
# ---------------------------------------------------------------------------

def build_agg_daily_sales(fact_orders_df, fact_items_df, dim_date_df, dim_product_df,
                           gold_version, run_id, ingested_at):
    """Tổng hợp doanh thu theo ngày, danh mục sản phẩm và thành phố giao hàng."""
    # Gắn thêm category vào items qua product_id → dim_product
    items_with_cat = fact_items_df.join(
        dim_product_df.select("product_id", "category"), "product_id", "left"
    )

    # Tổng số lượng và gross revenue mỗi order+category+shipping_city
    items_agg = (items_with_cat
                 .groupBy("order_id", "category")
                 .agg(
                     F.sum("quantity").alias("total_items_sold"),
                     F.sum(F.col("line_amount").cast("decimal(38,0)")).alias("gross_revenue_vnd"),
                 ))

    # Kết hợp với fact_orders để lấy date_sk, shipping_city, status, customer_sk
    orders_with_date = fact_orders_df.select(
        "order_id", "date_sk", "shipping_city", "status", "total_amount",
        "customer_sk", "is_completed",
    )

    combined = (orders_with_date
                .join(items_agg, "order_id", "left")
                .join(dim_date_df.select("date_sk", "date_actual"), "date_sk", "left"))

    agg = (combined
           .groupBy("date_sk", "date_actual", "category", "shipping_city")
           .agg(
               F.countDistinct("order_id").alias("orders_count"),
               # countDistinct các order_id có is_completed=True trong nhóm này
               F.countDistinct(
                   F.when(F.col("is_completed"), F.col("order_id"))
               ).alias("completed_orders_count"),
               F.sum("total_items_sold").alias("total_items_sold"),
               F.sum(F.col("gross_revenue_vnd").cast("decimal(38,0)")).alias("gross_revenue_vnd"),
               # Net revenue: chỉ tính line_amount của đơn completed
               F.sum(
                   F.when(F.col("is_completed"), F.col("gross_revenue_vnd").cast("decimal(38,0)")).otherwise(F.lit(0))
               ).alias("net_revenue_vnd"),
               F.avg(F.col("total_amount").cast("double")).alias("avg_order_value_vnd"),
               F.countDistinct("customer_sk").alias("unique_customers"),
           )
           .select(
               F.col("date_sk").alias("agg_date_sk"),
               F.col("date_actual").alias("agg_date"),
               "category",
               "shipping_city",
               F.col("orders_count").cast("long"),
               F.col("completed_orders_count").cast("long"),
               F.col("total_items_sold").cast("long"),
               F.col("gross_revenue_vnd").cast("long"),
               F.col("net_revenue_vnd").cast("long"),
               F.col("avg_order_value_vnd").cast("double"),
               F.col("unique_customers").cast("long"),
               F.lit(gold_version).alias("_gold_version"),
               F.lit(run_id).alias("_pipeline_run_id"),
               F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
           ))
    return agg


# ---------------------------------------------------------------------------
# Aggregation: agg_customer_ltv
# ---------------------------------------------------------------------------

def build_agg_customer_ltv(fact_orders_df, dim_customer_df, gold_version, run_id, ingested_at):
    """Customer Lifetime Value: một dòng mỗi khách hàng (customer_id), gộp mọi phiên bản SCD2."""
    # fact_orders giữ customer_sk theo phiên bản tại thời điểm đặt; map về customer_id để gộp.
    orders_by_customer = fact_orders_df.join(
        dim_customer_df.select("customer_sk", "customer_id"), "customer_sk", "left"
    )
    orders_agg = (orders_by_customer
                  .groupBy("customer_id")
                  .agg(
                      F.min("order_ts").alias("first_order_ts"),
                      F.max("order_ts").alias("last_order_ts"),
                      F.count("order_id").alias("total_orders"),
                      F.sum(F.col("is_completed").cast("long")).alias("completed_orders"),
                      F.sum(
                          F.when(F.col("is_completed"), F.col("total_amount").cast("decimal(38,0)")).otherwise(F.lit(0))
                      ).alias("total_spend_vnd"),
                      F.avg(F.when(F.col("is_completed"), F.col("total_amount").cast("double"))).alias("avg_order_value_vnd"),
                  ))

    # Thông tin khách lấy từ phiên bản hiện tại
    current = dim_customer_df.filter(F.col("is_current") == True).select(  # noqa: E712
        "customer_sk", "customer_id", "city", "segment")
    ltv = (orders_agg
           .join(current, "customer_id", "left")
           .select(
               "customer_sk",
               "customer_id",
               "city",
               "segment",
               "first_order_ts",
               "last_order_ts",
               F.col("total_orders").cast("long"),
               F.col("completed_orders").cast("long"),
               F.col("total_spend_vnd").cast("long"),
               F.col("avg_order_value_vnd").cast("double"),
               F.lit(gold_version).alias("_gold_version"),
               F.lit(run_id).alias("_pipeline_run_id"),
               F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
           ))
    return ltv


# ---------------------------------------------------------------------------
# Helper: ghi DataFrame, đọc lại, trả về thông tin bảng
# ---------------------------------------------------------------------------

def _write_and_read(spark, df, path, table_name, report):
    """Ghi Parquet với compression snappy, đọc lại, ghi thông tin vào report."""
    df.write.mode("errorifexists").option("compression", "snappy").parquet(path)
    written = spark.read.parquet(path)
    n = written.count()
    report["tables"][table_name] = {
        "path": path,
        "rows": n,
        "columns": written.columns,
        "schema": written.schema.jsonValue(),
    }
    print(f"[DP3]   {table_name}: {n} rows → {path}", flush=True)
    return written


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def transform_gold(spark, silver_paths, gold_tables_root, quarantine_root,
                   gold_version, run_id, ingested_at, report):
    """Đọc Silver, xây dựng tám bảng Gold, validate và ghi kết quả.

    Args:
        spark: SparkSession đã cấu hình.
        silver_paths: dict tên bảng Silver → đường dẫn Parquet (s3a:// hoặc file://).
        gold_tables_root: prefix để ghi Gold (ví dụ s3a://gold/v1/_runs/run_id/tables).
        quarantine_root: prefix quarantine (ví dụ s3a://gold/v1/_runs/run_id/quarantine).
        gold_version: phiên bản Gold (string).
        run_id: mã run hiện tại.
        ingested_at: ISO timestamp UTC.
        report: dict report để ghi thêm thông tin.
    """
    gate = GoldQualityGate(quarantine_root, report)
    report["tables"] = {}
    root = gold_tables_root.rstrip("/")

    # ------------------------------------------------------------------
    # Đọc Silver
    # ------------------------------------------------------------------
    print("[DP3] Đọc Silver tables", flush=True)
    customers_df = spark.read.parquet(silver_paths["stg_customers"])
    products_df = spark.read.parquet(silver_paths["stg_products"])
    orders_df = spark.read.parquet(silver_paths["stg_orders"])
    items_df = spark.read.parquet(silver_paths["stg_order_items"])
    payments_df = spark.read.parquet(silver_paths["stg_payments"])
    history_df = spark.read.parquet(silver_paths["stg_customer_history"])

    # ------------------------------------------------------------------
    # dim_customer
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng dim_customer", flush=True)
    dim_customer = build_dim_customer(history_df, gold_version, run_id, ingested_at)
    # Thêm signup_ts từ stg_customers
    dim_customer = dim_customer.join(
        customers_df.select("customer_id", "signup_ts"), "customer_id", "left"
    )
    dim_customer = _write_and_read(spark, dim_customer, f"{root}/dim_customer", "dim_customer", report)

    for check, sql_cond in gold_dq_rules_dim_customer():
        gate.reject_sql(check, dim_customer, sql_cond)

    gate.reject("dim_customer_duplicate_sk",
                dim_customer.groupBy("customer_sk").count().filter("count > 1").select("customer_sk"))
    # SCD2: mỗi khách hàng phải có đúng một phiên bản hiện tại
    gate.reject("dim_customer_one_current_per_customer",
                dim_customer.groupBy("customer_id")
                .agg(F.sum(F.col("is_current").cast("int")).alias("n_current"))
                .filter("n_current != 1").select("customer_id"))

    # ------------------------------------------------------------------
    # dim_product
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng dim_product", flush=True)
    dim_product = build_dim_product(products_df, gold_version, run_id, ingested_at)
    dim_product = _write_and_read(spark, dim_product, f"{root}/dim_product", "dim_product", report)

    gate.reject("dim_product_duplicate_sk",
                dim_product.groupBy("product_sk").count().filter("count > 1").select("product_sk"))

    # ------------------------------------------------------------------
    # dim_date
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng dim_date", flush=True)
    from datetime import date, timedelta

    date_bounds = orders_df.agg(
        F.min(F.col("order_ts").cast("date")).alias("min_d"),
        F.max(F.col("order_ts").cast("date")).alias("max_d"),
    ).first()
    min_date = date_bounds["min_d"] or date.today()
    max_date = date_bounds["max_d"] or date.today()

    dim_date = build_dim_date(spark, min_date, max_date, gold_version)
    dim_date = _write_and_read(spark, dim_date, f"{root}/dim_date", "dim_date", report)

    # ------------------------------------------------------------------
    # fact_orders
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng fact_orders", flush=True)
    fact_orders = build_fact_orders(orders_df, history_df, gold_version, run_id, ingested_at)
    fact_orders = _write_and_read(spark, fact_orders, f"{root}/fact_orders", "fact_orders", report)

    for check, sql_cond in gold_dq_rules_fact_orders():
        gate.reject_sql(check, fact_orders, sql_cond)

    gate.reject("fact_orders_duplicate_sk",
                fact_orders.groupBy("order_sk").count().filter("count > 1").select("order_sk"))

    # Kiểm tra FK: customer_sk phải có trong dim_customer
    orphan_customer = fact_orders.join(
        dim_customer.select("customer_sk"), "customer_sk", "left_anti"
    )
    gate.reject("fact_orders_orphan_customer_sk", orphan_customer)

    # Kiểm tra FK: date_sk phải có trong dim_date
    orphan_date = fact_orders.join(
        dim_date.select("date_sk"), "date_sk", "left_anti"
    )
    gate.reject("fact_orders_orphan_date_sk", orphan_date)

    # ------------------------------------------------------------------
    # fact_order_items
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng fact_order_items", flush=True)
    fact_items = build_fact_order_items(items_df, fact_orders, products_df, gold_version, run_id, ingested_at)
    fact_items = _write_and_read(spark, fact_items, f"{root}/fact_order_items", "fact_order_items", report)

    for check, sql_cond in gold_dq_rules_fact_order_items():
        gate.reject_sql(check, fact_items, sql_cond)

    gate.reject("fact_items_duplicate_sk",
                fact_items.groupBy("order_item_sk").count().filter("count > 1").select("order_item_sk"))

    # Kiểm tra FK: order_sk phải có trong fact_orders
    orphan_order = fact_items.join(
        fact_orders.select("order_sk"), "order_sk", "left_anti"
    )
    gate.reject("fact_items_orphan_order_sk", orphan_order)

    # ------------------------------------------------------------------
    # fact_payment_attempts
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng fact_payment_attempts", flush=True)
    fact_payments = build_fact_payment_attempts(payments_df, fact_orders, gold_version, run_id, ingested_at)
    fact_payments = _write_and_read(spark, fact_payments, f"{root}/fact_payment_attempts",
                                    "fact_payment_attempts", report)

    for check, sql_cond in gold_dq_rules_fact_payment_attempts():
        gate.reject_sql(check, fact_payments, sql_cond)

    gate.reject("fact_payments_duplicate_sk",
                fact_payments.groupBy("payment_sk").count().filter("count > 1").select("payment_sk"))

    # ------------------------------------------------------------------
    # agg_daily_sales
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng agg_daily_sales", flush=True)
    agg_daily = build_agg_daily_sales(fact_orders, fact_items, dim_date, dim_product,
                                       gold_version, run_id, ingested_at)
    agg_daily = _write_and_read(spark, agg_daily, f"{root}/agg_daily_sales", "agg_daily_sales", report)

    # ------------------------------------------------------------------
    # agg_customer_ltv
    # ------------------------------------------------------------------
    print("[DP3] Xây dựng agg_customer_ltv", flush=True)
    agg_ltv = build_agg_customer_ltv(fact_orders, dim_customer, gold_version, run_id, ingested_at)
    agg_ltv = _write_and_read(spark, agg_ltv, f"{root}/agg_customer_ltv", "agg_customer_ltv", report)

    print("[DP3] Hoàn thành transform_gold", flush=True)
    return report
