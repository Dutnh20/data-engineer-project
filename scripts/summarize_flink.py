"""So sánh kết quả Flink baseline và tối ưu với số event thực tế trong Kafka.

Đọc CSV Flink ghi ở output/flink_out/<job>/<sink>/ (không có header) và tính: tổng event trong các cửa sổ, số event bị mất do
đến muộn, số bản trùng còn sót. Kỳ vọng lấy từ stream_report.json của DP4 (kafka_rows, distinct_event_ids_in_kafka).
Cách dùng: python scripts/summarize_flink.py --report output/stream/stream_v1/<run>/stream_report.json
"""
import argparse
import csv
import json
from pathlib import Path


def window_totals(folder, count_col):
    """Tổng cột đếm trong mọi file CSV của một sink; trả (số cửa sổ, tổng)."""
    windows = total = 0
    for path in Path(folder).rglob("*"):
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.reader(fh):
                if row:
                    windows += 1
                    total += int(row[count_col])
    return windows, total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, help="stream_report.json của DP4 (nguồn số event thật)")
    parser.add_argument("--flink-out", default="output/flink_out")
    args = parser.parse_args()
    topics = json.loads(Path(args.report).read_text(encoding="utf-8"))["topics"]
    expected = {"click_windows": (topics["clickstream"], 4), "callback_windows": (topics["payment_callbacks"], 3)}
    out = {}
    for sink, (info, col) in expected.items():
        row = {"kafka_rows": info["kafka_rows"], "distinct_events": info["distinct_event_ids_in_kafka"]}
        for job in ("baseline", "optimized"):
            folder = Path(args.flink_out) / job / sink
            if not folder.exists():
                row[job] = "chưa chạy"
                continue
            windows, total = window_totals(folder, col)
            row[job] = {"windows": windows, "events_counted": total,
                        "lost_vs_distinct": row["distinct_events"] - total,
                        "extra_vs_distinct": total - row["distinct_events"]}
        out[sink] = row
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
