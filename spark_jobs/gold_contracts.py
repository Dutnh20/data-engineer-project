"""Schema và quy tắc nghiệp vụ của Gold Layer (Star Schema).

Không import PySpark ở module level để có thể dùng trong test setup thuần Python.
"""

# ---------------------------------------------------------------------------
# Tên bảng Gold
# ---------------------------------------------------------------------------

GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_orders",
    "fact_order_items",
    "fact_payment_attempts",
    "agg_daily_sales",
    "agg_customer_ltv",
]

# ---------------------------------------------------------------------------
# Tên tháng và ngày theo tiếng Việt
# ---------------------------------------------------------------------------

MONTH_NAME_VI = {
    1: "Tháng 1", 2: "Tháng 2", 3: "Tháng 3",
    4: "Tháng 4", 5: "Tháng 5", 6: "Tháng 6",
    7: "Tháng 7", 8: "Tháng 8", 9: "Tháng 9",
    10: "Tháng 10", 11: "Tháng 11", 12: "Tháng 12",
}

# ISO weekday: 1=Monday ... 7=Sunday
DAY_NAME_VI = {
    1: "Thứ Hai",
    2: "Thứ Ba",
    3: "Thứ Tư",
    4: "Thứ Năm",
    5: "Thứ Sáu",
    6: "Thứ Bảy",
    7: "Chủ Nhật",
}

# ---------------------------------------------------------------------------
# Quy tắc DQ cho Gold – trả về list[(tên_check, cột_condition)]
# Caller sẽ import PySpark khi cần.
# ---------------------------------------------------------------------------

def gold_dq_rules_dim_customer():
    """Trả về danh sách (tên_check, biểu_thức_string) cho dim_customer.

    Dùng cú pháp SQL expression để tránh import PySpark ở đây.
    """
    return [
        ("dim_customer_sk_not_null", "customer_sk IS NOT NULL"),
        ("dim_customer_id_positive", "customer_id > 0"),
        ("dim_customer_is_current_not_null", "is_current IS NOT NULL"),
    ]


def gold_dq_rules_fact_orders():
    """Trả về danh sách (tên_check, biểu_thức_string) cho fact_orders."""
    return [
        ("fact_orders_sk_not_null", "order_sk IS NOT NULL"),
        ("fact_orders_id_positive", "order_id > 0"),
        ("fact_orders_amount_positive", "total_amount > 0"),
        ("fact_orders_customer_sk_not_null", "customer_sk IS NOT NULL"),
        ("fact_orders_date_sk_not_null", "date_sk IS NOT NULL"),
    ]


def gold_dq_rules_fact_order_items():
    """Trả về danh sách (tên_check, biểu_thức_string) cho fact_order_items."""
    return [
        ("fact_items_sk_not_null", "order_item_sk IS NOT NULL"),
        ("fact_items_quantity_positive", "quantity > 0"),
        ("fact_items_line_amount_positive", "line_amount > 0"),
        ("fact_items_order_sk_not_null", "order_sk IS NOT NULL"),
        ("fact_items_product_sk_not_null", "product_sk IS NOT NULL"),
    ]


def gold_dq_rules_fact_payment_attempts():
    """Trả về danh sách (tên_check, biểu_thức_string) cho fact_payment_attempts."""
    return [
        ("fact_payments_sk_not_null", "payment_sk IS NOT NULL"),
        ("fact_payments_amount_positive", "amount > 0"),
        ("fact_payments_order_sk_not_null", "order_sk IS NOT NULL"),
    ]


# ---------------------------------------------------------------------------
# Surrogate key builder helpers (dùng trong gold_transforms.py)
# ---------------------------------------------------------------------------

def sk_expr_order(gold_version):
    """SQL expression tạo surrogate key cho fact_orders."""
    return f"sha2(concat_ws(':', '{gold_version}', cast(order_id as string)), 256)"


def sk_expr_order_item(gold_version):
    """SQL expression tạo surrogate key cho fact_order_items."""
    return f"sha2(concat_ws(':', '{gold_version}', cast(order_item_id as string)), 256)"


def sk_expr_payment(gold_version):
    """SQL expression tạo surrogate key cho fact_payment_attempts."""
    return f"sha2(concat_ws(':', '{gold_version}', cast(payment_id as string)), 256)"


def sk_expr_product(gold_version):
    """SQL expression tạo surrogate key cho dim_product."""
    return f"sha2(concat_ws(':', '{gold_version}', cast(product_id as string)), 256)"

