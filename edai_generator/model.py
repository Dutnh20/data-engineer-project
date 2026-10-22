"""Business records with stable IDs, shared dimensions and consistent amounts."""

from datetime import datetime, timedelta, timezone
from fractions import Fraction
import random


def mix64(value):
    """Stable integer mixer; unlike hash(), results do not depend on PYTHONHASHSEED."""
    mask = (1 << 64) - 1
    value = (value + 0x9E3779B97F4A7C15) & mask
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    return value ^ (value >> 31)


def rng_for(seed, entity_id, namespace):
    """Give each entity an independent reproducible random sequence."""
    return random.Random(mix64(seed ^ (entity_id * 1315423911) ^ namespace))


def duplicate_copy_due(clean_ordinal, final_fraction):
    """Schedule copies so copies/(originals+copies) approaches the configured rate.

    Fractions avoid float rounding drift for very large generated datasets.
    Exactly floor(N*r/(1-r)) extra copies exist after N originals.
    """
    rate = Fraction(str(final_fraction))
    numerator, denominator = rate.numerator, rate.denominator - rate.numerator
    return (clean_ordinal * numerator // denominator
            > (clean_ordinal - 1) * numerator // denominator)


def customer(customer_id, cfg):
    """Create a customer by ID without keeping the dimension in RAM."""
    value = mix64(customer_id + cfg.seed)
    city = "Ho Chi Minh" if value % 100 < 80 else ("Ha Noi" if value % 100 < 90 else "Da Nang")
    start = datetime.fromisoformat(cfg.start_ts).astimezone(timezone.utc)
    return {
        "customer_id": customer_id,
        "customer_name": f"Khach hang mo phong {customer_id}",
        "signup_ts": start - timedelta(days=1 + value % 365),
        "city": city,
        "segment": "loyal" if customer_id <= max(1, cfg.customers // 5) else "standard",
        "device_type": "mobile" if value % 10 < 7 else "desktop",
    }


def customer_changes(customer_id, cfg):
    """Sinh ảnh chụp ban đầu và các lần đổi segment để downstream học SCD2.

    Chọn đều theo ID, đúng floor(N * rate) khách thay đổi một lần. Một nửa
    trong số đó đổi lần hai. Đây là lịch sử nguồn, chưa phải dimension SCD2.
    customers vẫn là ảnh chụp lúc đăng ký; không ghi đè bằng trạng thái cuối.
    """
    original = customer(customer_id, cfg)
    attributes = {key: original[key] for key in
                  ("customer_id", "customer_name", "city", "segment", "device_type")}
    rows = [{**attributes, "change_id": customer_id * 10,
             "change_type": "INSERT", "effective_ts": original["signup_ts"],
             "source_updated_ts": original["signup_ts"]}]
    rate = Fraction(str(cfg.customer_change_fraction))
    ordinal = customer_id * rate.numerator // rate.denominator
    previous = (customer_id - 1) * rate.numerator // rate.denominator
    if ordinal == previous:
        return rows
    start = datetime.fromisoformat(cfg.start_ts).astimezone(timezone.utc)
    # 25% và 60% khoảng lịch sử tối thiểu. Smoke cũng có thay đổi để thực hành.
    first_ts = start + timedelta(seconds=cfg.minimum_span_seconds * 0.25)
    new_segment = "standard" if original["segment"] == "loyal" else "loyal"
    rows.append({**attributes, "segment": new_segment,
                 "change_id": customer_id * 10 + 1, "change_type": "UPDATE",
                 "effective_ts": first_ts, "source_updated_ts": first_ts + timedelta(seconds=1)})
    if ordinal % 2 == 0:
        second_ts = start + timedelta(seconds=cfg.minimum_span_seconds * 0.60)
        rows.append({**attributes, "change_id": customer_id * 10 + 2,
                     "change_type": "UPDATE", "effective_ts": second_ts,
                     "source_updated_ts": second_ts + timedelta(seconds=1)})
    return rows


def product(product_id, cfg):
    """Create a product; 3 of every 4 catalog IDs belong to electronics."""
    value = mix64(product_id + cfg.seed * 7)
    category = "electronics" if product_id % 4 else ("fashion" if product_id % 8 else "home")
    bounds = {"electronics": (100, 2000), "fashion": (10, 150), "home": (20, 300)}
    low, high = bounds[category]
    price = (low + value % (high - low + 1)) * 10000
    start = datetime.fromisoformat(cfg.start_ts).astimezone(timezone.utc)
    return {
        "product_id": product_id,
        "product_name": f"{category} sample {product_id}",
        "category": category,
        "brand": f"SyntheticBrand-{value % 30:02d}",
        "base_price": price,
        "is_active": True,
        "created_ts": start - timedelta(days=30 + value % 300),
        "description": f"Synthetic {category} item for an educational store; model {product_id}.",
    }


def make_order(order_id, cfg):
    """Return one order, 1-4 items and payment attempts sharing the same truth.

    Loyal customers receive 60% of order draws. Dates are distributed over a
    configured cyclic history horizon; IDs remain globally unique across cycles.
    Prices are integer VND. This demo does not include tax or shipping fees.
    """
    rng = rng_for(cfg.seed, order_id, 100)
    loyal_end = max(1, cfg.customers // 5)
    customer_id = rng.randint(1, loyal_end) if rng.random() < 0.6 else rng.randint(loyal_end + 1, cfg.customers)
    buyer = customer(customer_id, cfg)
    start = datetime.fromisoformat(cfg.start_ts).astimezone(timezone.utc)
    offset = ((order_id - 1) * cfg.order_interval_seconds) % (cfg.history_days * 86400)
    order_ts = start + timedelta(seconds=offset)
    coupon = "DEMO10" if rng.random() < 0.2 else None
    status = rng.choices(["completed", "pending", "cancelled"], [85, 5, 10])[0]
    items = []
    for position, product_id in enumerate(rng.sample(range(1, cfg.products + 1), rng.randint(1, 4)), 1):
        item_product = product(product_id, cfg)
        quantity = rng.randint(1, 3)
        gross = quantity * item_product["base_price"]
        discount = gross // 10 if coupon else 0
        items.append({
            "order_item_id": order_id * 10 + position,
            "order_id": order_id, "product_id": product_id,
            "quantity": quantity, "unit_price": item_product["base_price"],
            "discount_amount": discount, "line_amount": gross - discount,
            "created_ts": order_ts,
        })
    amount = sum(item["line_amount"] for item in items)
    order = {
        "order_id": order_id, "customer_id": customer_id, "order_ts": order_ts,
        "status": status, "shipping_city": buyer["city"], "coupon_code": coupon,
        "total_amount": amount,
    }
    final_status = {"completed": "succeeded", "pending": "pending", "cancelled": "failed"}[status]
    statuses = ["failed", final_status] if status == "completed" and rng.random() < 0.15 else [final_status]
    payments = []
    method = rng.choice(["card", "bank_transfer", "e_wallet"])
    provider = rng.choice(["gateway_a", "gateway_b"])
    for attempt, payment_status in enumerate(statuses, 1):
        payments.append({
            "payment_id": order_id * 10 + attempt,
            "order_id": order_id, "attempt_no": attempt,
            "payment_ts": order_ts + timedelta(seconds=attempt * 30),
            "method": method, "provider": provider,
            "amount": amount, "status": payment_status,
            "failure_reason": rng.choice(["insufficient_funds", "bank_timeout", "declined"])
            if payment_status == "failed" else None,
        })
    return order, items, payments
