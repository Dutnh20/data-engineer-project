"""Explicit Arrow schemas; payment v1 truly omits failure_reason."""

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")
I = pa.int64()
S = pa.string()


def fields(specification):
    """Build fields with explicit nullability from (name, type, nullable) tuples."""
    return pa.schema([pa.field(name, dtype, nullable=nullable) for name, dtype, nullable in specification])


SCHEMAS = {
    "customer_changes": fields([
        ("change_id", I, False), ("customer_id", I, False),
        ("customer_name", S, False), ("city", S, False),
        ("segment", S, False), ("device_type", S, False),
        ("change_type", S, False), ("effective_ts", TS, False),
        ("source_updated_ts", TS, False),
    ]),
    "customers": fields([
        ("customer_id", I, False), ("customer_name", S, False), ("signup_ts", TS, False),
        ("city", S, False), ("segment", S, False), ("device_type", S, False),
    ]),
    "products": fields([
        ("product_id", I, False), ("product_name", S, False), ("category", S, False),
        ("brand", S, False), ("base_price", I, False), ("is_active", pa.bool_(), False),
        ("created_ts", TS, False), ("description", S, False),
    ]),
    "orders": fields([
        ("order_id", I, False), ("customer_id", I, False), ("order_ts", TS, False),
        ("status", S, False), ("shipping_city", S, False), ("coupon_code", S, True),
        ("total_amount", I, False),
    ]),
    "order_items": fields([
        ("order_item_id", I, False), ("order_id", I, False), ("product_id", I, False),
        ("quantity", I, False), ("unit_price", I, False), ("discount_amount", I, False),
        ("line_amount", I, False), ("created_ts", TS, False),
    ]),
    "payments": fields([
        ("payment_id", I, False), ("order_id", I, False), ("attempt_no", I, False),
        ("payment_ts", TS, False), ("method", S, False), ("provider", S, False),
        ("amount", I, False), ("status", S, False),
    ]),
}
PAYMENTS_V2 = SCHEMAS["payments"].append(pa.field("failure_reason", S, nullable=True))


def schema_for(table, version=1):
    """Select the physical payment-file schema at the configured boundary."""
    return PAYMENTS_V2 if table == "payments" and version == 2 else SCHEMAS[table]


def describe_schema(schema):
    """Return a portable schema description for the evidence report."""
    return [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema]
