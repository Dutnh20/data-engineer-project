"""Load, validate and fingerprint a reproducible generator configuration."""

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path

from . import __version__


@dataclass(frozen=True)
class Config:
    """Counts, distributions and a physical-byte stopping condition for one run."""

    seed: int = 42
    customers: int = 2000
    products: int = 1000
    orders: int = 12000
    target_gb: float | None = None
    batch_orders: int = 1000
    dimension_batch: int = 20000
    start_ts: str = "2025-01-01T00:00:00+00:00"
    history_days: int = 180
    order_interval_seconds: float = 1.0
    schema_change_after_order: int = 6000
    offline_duplicate_fraction: float = 0.02
    stream_duplicate_fraction: float = 0.015
    late_mobile_fraction: float = 0.15
    late_min_seconds: int = 300
    late_max_seconds: int = 600
    customer_change_fraction: float = 0.05
    compression: str = "zstd"
    min_free_gb: float = 0.25

    def validate(self):
        """Reject impossible counts/rates before writing any files."""
        for name in ("customers", "products", "orders", "batch_orders", "dimension_batch",
                     "history_days", "schema_change_after_order"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.customers < 10 or self.products < 4:
            raise ValueError("Use at least 10 customers and 4 products")
        if self.orders <= self.schema_change_after_order:
            raise ValueError("orders must exceed schema_change_after_order to create both schemas")
        for name in ("offline_duplicate_fraction", "stream_duplicate_fraction"):
            if not 0 < getattr(self, name) < 0.5:
                raise ValueError(f"{name} must be between 0 and 0.5")
        if not 0 < self.late_mobile_fraction < 1:
            raise ValueError("late_mobile_fraction must be between 0 and 1")
        if not 0 < self.late_min_seconds <= self.late_max_seconds:
            raise ValueError("Invalid late-arrival bounds")
        if self.target_gb is not None and self.target_gb <= 0:
            raise ValueError("target_gb must be positive, or null for a row-count run")
        if self.order_interval_seconds <= 0 or self.min_free_gb < 0:
            raise ValueError("Invalid time interval or free-space setting")
        if not math.isfinite(self.order_interval_seconds) or self.order_interval_seconds >= self.history_days * 86400:
            raise ValueError("order_interval_seconds must be finite and shorter than history_days")
        if not 0 <= self.customer_change_fraction <= 1:
            raise ValueError("customer_change_fraction must be between 0 and 1")
        if self.compression not in ("zstd", "snappy", "NONE"):
            raise ValueError("compression must be zstd, snappy or NONE")
        if datetime.fromisoformat(self.start_ts).utcoffset() is None:
            raise ValueError("start_ts must have a timezone")
        return self

    def to_dict(self):
        """Return a JSON-compatible configuration."""
        return asdict(self)

    def fingerprint(self):
        """Bind checkpoints to configuration and generator version."""
        body = json.dumps({"generator_version": __version__, **self.to_dict()}, sort_keys=True)
        return hashlib.sha256(body.encode()).hexdigest()

    @property
    def target_bytes(self):
        """Use decimal GB: 100 GB = 100,000,000,000 actual Parquet bytes."""
        return None if self.target_gb is None else int(self.target_gb * 1_000_000_000)

    @property
    def minimum_span_seconds(self):
        """Time covered before the first wrap by the minimum configured orders.

        Lịch thay đổi khách hàng cố định theo config, không phụ thuộc số batch
        đã chạy. Vì vậy resume không làm thay đổi lịch sử đã sinh.
        """
        first_cycle_last = math.ceil(self.history_days * 86400 / self.order_interval_seconds) - 1
        return min(self.orders - 1, first_cycle_last) * self.order_interval_seconds


def load_config(path):
    """Read JSON; unknown keys intentionally fail rather than being ignored."""
    return Config(**json.loads(Path(path).read_text(encoding="utf-8-sig"))).validate()
