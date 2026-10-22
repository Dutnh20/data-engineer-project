"""Command-line interface: offline, validate, plan-stream, produce, consume, upload."""

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import sys

from .common import json_default


def build_parser():
    """Define explicit, inspectable commands for each stage of the source lifecycle."""
    parser = argparse.ArgumentParser(prog="python -m edai_generator")
    subs = parser.add_subparsers(dest="command", required=True)
    offline = subs.add_parser("offline", help="Generate source Parquet files")
    offline.add_argument("--config", type=Path, default=Path("configs/smoke.json"))
    offline.add_argument("--out", type=Path, required=True)
    offline.add_argument("--resume", action="store_true")
    offline.add_argument("--target-gb", type=float, help="Override the physical Parquet byte target")
    offline.add_argument("--stop-after-chunks", type=int, help="Pause after N order chunks to exercise resume")

    validate = subs.add_parser("validate", help="Independently validate complete Parquet source")
    validate.add_argument("--out", type=Path, required=True)
    validate.add_argument("--checksums", action="store_true")
    validate.add_argument("--memory-limit", default="512MB")

    plan = subs.add_parser("plan-stream", help="Generate a deterministic source arrival schedule")
    plan.add_argument("--out", type=Path, required=True)
    plan.add_argument("--orders", type=int, default=2000)
    plan.add_argument("--first-order", type=int, default=1)
    plan.add_argument("--topic-prefix", default="edai")

    produce = subs.add_parser("produce", help="Replay the schedule into Kafka")
    produce.add_argument("--out", type=Path, required=True)
    produce.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19093"))
    produce.add_argument("--speed", type=float, default=60)
    produce.add_argument("--max-rate", type=float, default=500)
    produce.add_argument("--clock", choices=["historical", "wall"], default="historical")

    consume = subs.add_parser("consume", help="Read a bounded Kafka sample")
    consume.add_argument("--out", type=Path, required=True)
    consume.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19093"))
    consume.add_argument("--limit", type=int, default=100)
    consume.add_argument("--timeout", type=int, default=30)

    upload = subs.add_parser("upload", help="Publish the source to MinIO, separately from Bronze")
    upload.add_argument("--out", type=Path, required=True)
    upload.add_argument("--endpoint", default=os.getenv("S3_ENDPOINT_URL", "http://localhost:9010"))
    upload.add_argument("--bucket", default="source-department")
    upload.add_argument("--prefix")

    preview = subs.add_parser("preview", help="Print a few rows and the physical schema")
    preview.add_argument("file", type=Path)
    preview.add_argument("--rows", type=int, default=5)
    return parser


def main():
    """Run a command and return a nonzero exit code on failed verification."""
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "offline":
            from .config import load_config
            from .offline import generate
            cfg = load_config(args.config)
            if args.target_gb is not None:
                cfg = replace(cfg, target_gb=args.target_gb).validate()
            if args.stop_after_chunks is not None and args.stop_after_chunks < 1:
                raise ValueError("--stop-after-chunks must be positive")
            result = generate(cfg, args.out, args.resume, args.stop_after_chunks)
            print(f"Manifest: {args.out / 'manifest.json'}")
            return 0
        if args.command == "validate":
            from .validate import validate_run
            result = validate_run(args.out, args.checksums, args.memory_limit)
        elif args.command == "plan-stream":
            from .streaming import plan_stream
            result = plan_stream(args.out, args.orders, args.first_order, args.topic_prefix)
        elif args.command == "produce":
            from .streaming import produce_stream
            result = produce_stream(args.out, args.bootstrap, args.speed, args.max_rate, args.clock)
        elif args.command == "consume":
            from .streaming import consume_sample
            result = consume_sample(args.out, args.bootstrap, args.limit, args.timeout)
        elif args.command == "upload":
            from .storage import upload_run
            result = upload_run(args.out, args.endpoint, args.bucket, args.prefix)
        elif args.command == "preview":
            import pyarrow.parquet as pq
            if args.rows < 1:
                raise ValueError("--rows must be positive")
            file = pq.ParquetFile(args.file)
            batch = next(file.iter_batches(batch_size=args.rows), None)
            result = {"schema": str(file.schema_arrow), "rows": batch.to_pylist() if batch else []}
        print(json.dumps(result, indent=2, ensure_ascii=False, default=json_default))
        return 1 if result.get("passed") is False else 0
    except KeyboardInterrupt:
        print("Stopped. For offline generation, repeat the same command with --resume.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
