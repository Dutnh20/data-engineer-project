"""Schema và quy tắc nghiệp vụ của nguồn generator 1.1.x."""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, BooleanType


SPECS = {
    "customers": "customer_id:long customer_name:string signup_ts:timestamp city:string segment:string device_type:string",
    "products": "product_id:long product_name:string category:string brand:string base_price:long is_active:boolean created_ts:timestamp description:string",
    "orders": "order_id:long customer_id:long order_ts:timestamp status:string shipping_city:string coupon_code:string? total_amount:long",
    "order_items": "order_item_id:long order_id:long product_id:long quantity:long unit_price:long discount_amount:long line_amount:long created_ts:timestamp",
    "payments": "payment_id:long order_id:long attempt_no:long payment_ts:timestamp method:string provider:string amount:long status:string failure_reason:string?",
    "customer_changes": "change_id:long customer_id:long customer_name:string city:string segment:string device_type:string change_type:string effective_ts:timestamp source_updated_ts:timestamp",
}
KEYS = {"customers": "customer_id", "products": "product_id", "orders": "order_id",
        "order_items": "order_item_id", "payments": "payment_id", "customer_changes": "change_id"}
ATTRIBUTES = ["customer_name", "city", "segment", "device_type"]
TYPES = {"long": LongType, "string": StringType, "timestamp": TimestampType, "boolean": BooleanType}


def fields(table):
    return [(token.split(":")[0], token.split(":")[1].rstrip("?"), token.endswith("?"))
            for token in SPECS[table].split()]


def schema_for(table):
    # Đọc nullable để kiểm tra NULL bằng DQ thay vì mặc định tin schema nguồn.
    return StructType([StructField(name, TYPES[dtype](), True) for name, dtype, _ in fields(table)])


def normalize(df, table):
    """Trim chuỗi; chuẩn hóa mã phân loại, không điền tiền/ID thiếu bằng 0."""
    lower = {"segment", "device_type", "category", "status", "method", "provider", "failure_reason"}
    for name, dtype, _ in fields(table):
        if dtype == "string":
            value = F.trim(F.col(name))
            if name in lower:
                value = F.lower(value)
            if name == "change_type":
                value = F.upper(value)
            df = df.withColumn(name, value)
    for name in ("coupon_code", "failure_reason"):
        if name in df.columns:
            df = df.withColumn(name, F.when(F.length(F.col(name)) == 0, F.lit(None).cast("string")).otherwise(F.col(name)))
    return df


def with_row_errors(df, table):
    """Một bản ghi có thể có nhiều lỗi; giữ lý do để đọc trong quarantine."""
    rules = []
    for name, dtype, nullable in fields(table):
        if not nullable:
            rules.append((f"required:{name}", F.col(name).isNotNull()))
            if dtype == "string":
                rules.append((f"not_blank:{name}", F.length(F.col(name)) > 0))
        if name.endswith("_id"):
            rules.append((f"positive_id:{name}", F.col(name) > 0))
    domains = {
        "customers": {"segment": ["loyal", "standard"], "device_type": ["mobile", "desktop"]},
        "products": {"category": ["electronics", "fashion", "home"]},
        "orders": {"status": ["completed", "pending", "cancelled"]},
        "payments": {"status": ["succeeded", "pending", "failed"], "method": ["card", "bank_transfer", "e_wallet"], "provider": ["gateway_a", "gateway_b"]},
        "customer_changes": {"segment": ["loyal", "standard"], "device_type": ["mobile", "desktop"], "change_type": ["INSERT", "UPDATE"]},
    }
    for name, allowed in domains.get(table, {}).items():
        rules.append((f"domain:{name}", F.col(name).isin(allowed)))
    for name in {"products": ["base_price"], "orders": ["total_amount"],
                 "order_items": ["quantity", "unit_price", "line_amount"],
                 "payments": ["amount", "attempt_no"]}.get(table, []):
        rules.append((f"positive:{name}", F.col(name) > 0))
    if table == "order_items":
        gross = F.col("quantity").cast("decimal(38,0)") * F.col("unit_price").cast("decimal(38,0)")
        rules.extend([
            ("nonnegative:discount_amount", F.col("discount_amount") >= 0),
            ("discount_lte_gross", F.col("discount_amount") <= gross),
            ("line_amount_equation", F.col("line_amount") == gross - F.col("discount_amount")),
        ])
    if table == "customer_changes":
        rules.append(("source_updated_not_before_effective", F.col("source_updated_ts") >= F.col("effective_ts")))
    if table == "payments":
        # V1 failed payments có thể chưa có reason: đây là schema evolution hợp lệ.
        rules.append(("reason_only_for_failed", F.col("failure_reason").isNull() | (F.col("status") == "failed")))
    errors = [F.when(~F.coalesce(condition, F.lit(False)), F.lit(name)) for name, condition in rules]
    return df.withColumn("_dq_reason", F.concat_ws("|", *errors))
