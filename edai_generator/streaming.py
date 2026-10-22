"""Plan deterministic delayed events and replay them to real Kafka topics."""

from collections import Counter
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .common import dump_json, epoch_ms, iso_ms, json_default, sha256_file
from .model import customer, make_order, duplicate_copy_due, rng_for
from .offline import load_completed_run


def plan_stream(root, n_orders=2000, first_order=1, topic_prefix="edai"):
    """Build an arrival-sorted JSONL source using disk-backed sorting.

    Events reference actually generated customer/product/order/payment IDs.
    The replay preserves historical event times and offline/online joinability.
    Late means simulated source-arrival time minus event time, not wall-clock age.
    """
    root, cfg, manifest = load_completed_run(root)
    if n_orders <= 0 or first_order < 1 or first_order + n_orders - 1 > manifest["orders_generated"]:
        raise ValueError("Requested order range does not exist in the completed offline source")
    if not topic_prefix or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in topic_prefix):
        raise ValueError("Invalid Kafka topic prefix")
    folder = root / "stream"
    folder.mkdir(exist_ok=True)
    staging = folder / "planning.sqlite"
    if staging.exists():
        staging.unlink()  # Only this generator-owned, disposable staging database.
    db = sqlite3.connect(staging)
    db.execute("PRAGMA temp_store=FILE")
    db.execute("""CREATE TABLE events (
        ordinal INTEGER PRIMARY KEY, topic TEXT, key TEXT, event_ms INTEGER,
        arrival_ms INTEGER, payload TEXT, injected_copy INTEGER, injected_late INTEGER)""")
    original_counts = Counter()
    late_originals = Counter()
    copies = 0
    eligible_mobile = 0
    ordinal = 0
    callback_originals = 0

    def insert(topic, key, event, delay_ms, copied=False, late=False):
        """Keep injection labels in the staging audit, never in the business payload."""
        nonlocal ordinal
        ordinal += 1
        timestamp = epoch_ms(event["event_ts"])
        db.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)", (
            ordinal, topic, str(key), timestamp, timestamp + delay_ms,
            json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=json_default),
            int(copied), int(late),
        ))

    try:
        for order_id in range(first_order, first_order + n_orders):
            rng = rng_for(cfg.seed, order_id, 900)
            order, items, payments = make_order(order_id, cfg)
            buyer = customer(order["customer_id"], cfg)
            click_events = []
            for position, item in enumerate(items, 1):
                for action, seconds_before in (("view", 120 - position * 5), ("cart", 30 - position)):
                    click_events.append({
                        "event_id": f"click-{order_id}-{position}-{action}",
                        "schema_version": 1, "session_id": f"session-order-{order_id}",
                        "customer_id": order["customer_id"], "product_id": item["product_id"],
                        "order_id": None, "event_type": action,
                        "page_type": "product" if action == "view" else "cart",
                        "event_ts": order["order_ts"] - timedelta(seconds=seconds_before),
                        "device_type": buyer["device_type"],
                    })
            click_events.append({
                "event_id": f"click-{order_id}-order", "schema_version": 1,
                "session_id": f"session-order-{order_id}", "customer_id": order["customer_id"],
                "product_id": items[0]["product_id"], "order_id": order_id,
                "event_type": "order_placed", "page_type": "checkout",
                "event_ts": order["order_ts"], "device_type": buyer["device_type"],
            })
            # A separate non-converting browsing session; no fabricated order FK.
            visitor_id = rng.randint(1, cfg.customers)
            visitor = customer(visitor_id, cfg)
            click_events.append({
                "event_id": f"browse-{order_id}", "schema_version": 1,
                "session_id": f"session-browse-{order_id}", "customer_id": visitor_id,
                "product_id": rng.randint(1, cfg.products), "order_id": None,
                "event_type": "view", "page_type": "product",
                "event_ts": order["order_ts"] + timedelta(seconds=10),
                "device_type": visitor["device_type"],
            })
            click_topic = topic_prefix + ".clickstream"
            for event in click_events:
                mobile = event["device_type"] == "mobile"
                eligible_mobile += int(mobile)
                late = mobile and rng.random() < cfg.late_mobile_fraction
                delay = rng.randint(cfg.late_min_seconds * 1000, cfg.late_max_seconds * 1000) if late else rng.randint(0, 2000)
                insert(click_topic, event["customer_id"], event, delay, late=late)
                original_counts[click_topic] += 1
                late_originals[click_topic] += int(late)
            callback_topic = topic_prefix + ".payment_callbacks"
            for payment in payments:
                callback_originals += 1
                version = 1 if order_id <= cfg.schema_change_after_order else 2
                timestamp = payment["payment_ts"] + timedelta(seconds=1)
                event = {
                    "event_id": f"callback-{payment['payment_id']}-{payment['status']}",
                    "schema_version": version, "payment_id": payment["payment_id"],
                    "order_id": order_id, "event_ts": timestamp, "callback_ts": timestamp,
                    "result_code": {"succeeded": "SUCCESS", "failed": "FAILED", "pending": "PENDING"}[payment["status"]],
                    "provider": payment["provider"], "amount": payment["amount"],
                }
                if version == 2:
                    event["failure_reason"] = payment["failure_reason"]
                delay = rng.randint(100, 2000)
                insert(callback_topic, payment["payment_id"], event, delay)
                original_counts[callback_topic] += 1
                if duplicate_copy_due(callback_originals, cfg.stream_duplicate_fraction):
                    insert(callback_topic, payment["payment_id"], event,
                           delay + rng.randint(2000, 10000), copied=True)
                    copies += 1
            if order_id % 1000 == 0:
                db.commit()
        db.commit()
        db.execute("CREATE INDEX arrival_order ON events(arrival_ms,ordinal)")
        db.commit()
        plan = folder / "events.jsonl"
        temporary = plan.with_suffix(".jsonl.tmp")
        max_event = {}
        out_of_order = Counter()
        with temporary.open("w", encoding="utf-8") as handle:
            for topic, key, event_ms, arrival_ms, payload in db.execute(
                "SELECT topic,key,event_ms,arrival_ms,payload FROM events ORDER BY arrival_ms,ordinal"
            ):
                if event_ms < max_event.get(topic, event_ms):
                    out_of_order[topic] += 1
                max_event[topic] = max(event_ms, max_event.get(topic, event_ms))
                envelope = {"topic": topic, "key": key,
                            "simulated_arrival_ts": iso_ms(arrival_ms), "event": json.loads(payload)}
                handle.write(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n")
        temporary.replace(plan)
        first_arrival, last_arrival = db.execute("SELECT min(arrival_ms),max(arrival_ms) FROM events").fetchone()
        bounds = db.execute("SELECT min(arrival_ms-event_ms),max(arrival_ms-event_ms) FROM events WHERE injected_late=1").fetchone()
        report = {
            "config_fingerprint": cfg.fingerprint(), "source_orders": [first_order, first_order + n_orders - 1],
            "plan_sha256": sha256_file(plan), "delivery_count": ordinal,
            "original_events_by_topic": dict(original_counts),
            "extra_callback_copies": copies,
            "callback_duplicate_fraction_of_final_rows": copies / (callback_originals + copies),
            "configured_callback_duplicate_fraction": cfg.stream_duplicate_fraction,
            "mobile_click_events": eligible_mobile, "late_click_events": sum(late_originals.values()),
            "late_fraction_of_mobile_clicks": sum(late_originals.values()) / eligible_mobile if eligible_mobile else 0,
            "late_delay_ms_min_max": list(bounds),
            "out_of_order_relative_to_topic_max_event_ts": dict(out_of_order),
            "first_simulated_arrival_ts": iso_ms(first_arrival),
            "last_simulated_arrival_ts": iso_ms(last_arrival),
            "logical_duration_seconds": (last_arrival - first_arrival) / 1000,
            "kafka_published": False,
            "late_semantics": f"Injected source delay {cfg.late_min_seconds}-{cfg.late_max_seconds}s; replay retains historical event timestamps. This report does not claim a Flink watermark result.",
        }
        dump_json(folder / "plan_report.json", report)
        return report
    finally:
        db.close()
        if staging.exists():
            staging.unlink()


def ensure_topics(bootstrap, topics, partitions=3):
    """Create missing single-broker lab topics and wait for broker acknowledgement."""
    from confluent_kafka import KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=20).topics
    new = [NewTopic(topic, num_partitions=partitions, replication_factor=1)
           for topic in topics if topic not in existing]
    if new:
        for topic, future in admin.create_topics(new, request_timeout=20).items():
            try:
                future.result(timeout=30)
            except Exception as exc:
                error = exc.args[0] if exc.args else None
                if not hasattr(error, "code") or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise RuntimeError(f"Cannot create topic {topic}: {exc}") from exc


def rebase_event(event, shift_ms):
    """Shift replay event time to wall time while preserving original source times."""
    shifted = dict(event)
    for field in ("event_ts", "callback_ts"):
        if field in event:
            shifted["source_" + field] = event[field]
            shifted[field] = iso_ms(epoch_ms(datetime.fromisoformat(event[field])) + shift_ms)
    shifted["replay_time_shift_ms"] = shift_ms
    return shifted


def produce_stream(root, bootstrap="localhost:19093", speed=60.0, max_rate=500.0, clock="historical"):
    """Replay in planned arrival order, checking delivery callbacks and flush.

    speed=1 preserves source-time delay in wall time; speed=60 accelerates it.
    speed=0 sends immediately, still in the correct arrival order. Message
    timestamps in JSON stay historical by default, agreeing with offline data.
    clock='wall' requires speed=1, shifts event_ts to today and retains
    source_event_ts. This allows actual 5-10 minute wall-clock delay demonstrations.
    """
    from confluent_kafka import Producer
    root, cfg, _ = load_completed_run(root)
    if speed < 0 or max_rate <= 0:
        raise ValueError("speed must be nonnegative and max_rate positive")
    if clock not in ("historical", "wall") or (clock == "wall" and speed != 1):
        raise ValueError("clock must be historical or wall; wall clock requires --speed 1")
    folder = root / "stream"
    plan = folder / "events.jsonl"
    info = json.loads((folder / "plan_report.json").read_text())
    if info["config_fingerprint"] != cfg.fingerprint() or sha256_file(plan) != info["plan_sha256"]:
        raise ValueError("Stream plan changed or belongs to another configuration")
    ensure_topics(bootstrap, list(info["original_events_by_topic"]))
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                         "acks": "all", "compression.type": "lz4", "linger.ms": 10,
                         "delivery.timeout.ms": 30000})
    delivered = Counter()
    errors = []

    def ack(error, message):
        """Account for broker-acknowledged messages, not only local enqueue calls."""
        if error:
            if len(errors) < 10:
                errors.append(str(error))
        else:
            delivered[message.topic()] += 1

    started = time.monotonic()
    started_wall_ms = int(time.time() * 1000)
    first_ms = None
    sent = 0
    try:
        with plan.open(encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                arrival_ms = epoch_ms(datetime.fromisoformat(record["simulated_arrival_ts"]))
                if first_ms is None:
                    first_ms = arrival_ms
                logical_due = (arrival_ms - first_ms) / (1000 * speed) if speed else 0
                due = started + max(logical_due, sent / max_rate)
                while time.monotonic() < due:
                    producer.poll(0)
                    if errors:
                        raise RuntimeError(errors[0])
                    time.sleep(min(0.1, max(0, due - time.monotonic())))
                event = rebase_event(record["event"], started_wall_ms - first_ms) if clock == "wall" else record["event"]
                value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode()
                while True:
                    try:
                        producer.produce(record["topic"], key=record["key"].encode(), value=value,
                                         on_delivery=ack, headers={"source_fingerprint": cfg.fingerprint().encode()})
                        break
                    except BufferError:
                        producer.poll(0.2)
                        if errors:
                            raise RuntimeError(errors[0])
                sent += 1
                producer.poll(0)
                if sent % 5000 == 0:
                    print(json.dumps({"enqueued": sent, "acknowledged": sum(delivered.values())}), flush=True)
                if errors:
                    raise RuntimeError(errors[0])
        remaining = producer.flush(35)
        if errors or remaining or sum(delivered.values()) != sent:
            raise RuntimeError(f"Kafka delivery incomplete: sent={sent}, ack={sum(delivered.values())}, remaining={remaining}, errors={errors}")
    except BaseException as exc:
        dump_json(folder / "delivery_report.json", {
            "passed": False, "enqueued": sent, "acknowledged_by_topic": dict(delivered),
            "error": str(exc), "note": "Retrying a partial replay can add business duplicates; deduplicate event_id downstream.",
        })
        raise
    report = {"passed": True, "bootstrap": bootstrap, "enqueued": sent,
              "acknowledged_by_topic": dict(delivered), "speed": speed, "clock": clock,
              "replay_time_shift_ms": started_wall_ms - first_ms if clock == "wall" else 0,
              "elapsed_seconds": time.monotonic() - started,
              "plan_sha256": info["plan_sha256"],
              "note": "Producer idempotence covers transport retries; deliberate repeated callback records remain in Kafka."}
    dump_json(folder / "delivery_report.json", report)
    return report


def consume_sample(root, bootstrap="localhost:19093", limit=100, timeout=30):
    """Read a bounded real Kafka sample with a new group, without auto commits."""
    from confluent_kafka import Consumer
    root, _, _ = load_completed_run(root)
    info = json.loads((root / "stream" / "plan_report.json").read_text())
    if limit <= 0 or timeout <= 0:
        raise ValueError("limit and timeout must be positive")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "edai-check-" + uuid.uuid4().hex,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(list(info["original_events_by_topic"]))
    count = 0
    deadline = time.monotonic() + timeout
    path = root / "evidence" / "kafka_sample.jsonl"
    path.parent.mkdir(exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as handle:
            while count < limit and time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                row = {"topic": message.topic(), "partition": message.partition(),
                       "offset": message.offset(), "event": json.loads(message.value())}
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                count += 1
    finally:
        consumer.close()
    if count == 0:
        raise RuntimeError("No Kafka records received before the timeout")
    return {"received": count, "sample_file": str(path),
            "note": "Earliest offsets also include earlier replays in these topics; this is a sample, not a full reconciliation."}
