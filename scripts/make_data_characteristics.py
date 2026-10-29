"""Sinh docs/data_characteristics.md từ manifest.json và validation.json của một lần chạy generator.

Mọi số trong tài liệu lấy từ hai file này (không nhập tay): cột skew, cardinality, schema trước/sau, tỉ lệ duplicate, dung lượng.
Cách dùng: python scripts/make_data_characteristics.py --run-dir output/ml_history [--config configs/ml_history.json] [--out docs/data_characteristics.md]
"""
import argparse
import json
from pathlib import Path


def pct(x):
    return f"{100 * x:.2f}%"


def schema_table(fields):
    return "\n".join(f"| `{f['name']}` | {f['type']} | {'có' if f['nullable'] else 'không'} |" for f in fields)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, help="Thư mục chứa manifest.json và validation.json")
    parser.add_argument("--config", help="Config đã dùng (lấy tham số late/duplicate của stream)")
    parser.add_argument("--out", default="docs/data_characteristics.md")
    args = parser.parse_args()
    run = Path(args.run_dir)
    m = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    v = json.loads((run / "validation.json").read_text(encoding="utf-8"))
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8")) if args.config else json.loads(
        (run / "config.json").read_text(encoding="utf-8")) if (run / "config.json").exists() else {}
    cities, cats = v["shipping_city_counts"], v["category_counts"]
    city_total, cat_total = sum(cities.values()), sum(cats.values())
    dup, evo = m["offline_duplicates"], m["schema_evolution"]
    amt = v["duplicate_amount_effect_vnd"]
    gb = m["physical_source_gb"]
    lines = [
        "# Đặc tính dữ liệu sau khi sinh", "",
        f"Nguồn: `{run.as_posix()}` (config `{m.get('config_fingerprint', '')[:12]}…`, generator {m.get('generator_version')}). "
        "Tài liệu này được sinh tự động bằng `scripts/make_data_characteristics.py` từ `manifest.json` và `validation.json`, "
        f"validator độc lập báo `passed = {v['passed']}`.", "",
        "## Dung lượng", "",
        f"- Dung lượng Parquet thực tế trên đĩa: **{m['physical_source_bytes']:,} byte ({gb:.4f} GB)**.",
        f"- Mốc rubric 100 GB: **{'đạt' if m.get('rubric_100gb_met') else 'chưa đạt'}** ở lần chạy này"
        f" (target_gb = {cfg.get('target_gb')}). Để đạt phải dùng `configs/full_100gb.json` (xem `docs/large_data.md`).", "",
        "| Bảng | Số dòng |", "|---|---:|", *[f"| `{t}` | {n:,} |" for t, n in v["rows"].items()], "",
        f"Khoảng thời gian đơn hàng: {v['order_time_range']['start']} → {v['order_time_range']['end']} "
        f"({v['order_time_range']['span_days']:.1f} ngày).", "",
        "## Cột bị skew", "",
        "| Cột | Giá trị | Số dòng | Tỉ lệ |", "|---|---|---:|---:|",
        *[f"| `orders.shipping_city` | {k} | {n:,} | {pct(n / city_total)} |" for k, n in cities.items()],
        *[f"| `products.category` | {k} | {n:,} | {pct(n / cat_total)} |" for k, n in cats.items()], "",
        "Skew thành phố làm các phép group/join theo `shipping_city` lệch; skew `order_items.product_id` (một số sản phẩm rất hot) "
        "được thấy ở benchmark Spark (`docs/spark_optimization.md`).", "",
        "## Cardinality (số giá trị khác nhau / tổng số dòng)", "",
        "| Cột | Giá trị khác nhau | Số dòng | Tỉ lệ |", "|---|---:|---:|---:|",
        *[f"| `{c}` | {i['unique_values']:,} | {i['rows']:,} | {i['ratio']:.4f} |" for c, i in v["cardinality"].items()], "",
        "Tỉ lệ gần 1 là cột high-cardinality (khóa chính). `order_items.order_item_id` < 1 vì có duplicate; "
        "`order_items.product_id` và `orders.customer_id` thấp vì mỗi giá trị lặp nhiều lần.", "",
        "## Duplicate offline", "",
        f"- Khóa trùng: `{dup['key']}`, bản sao thay đổi `{dup['changed_column']}`.",
        f"- Dòng sạch {dup['clean_rows']:,}, bản sao thừa {dup['extra_copies']:,}, tổng {dup['total_rows']:,}; "
        f"tỉ lệ **{pct(dup['fraction_of_final_rows'])}** (cấu hình {pct(dup['configured_fraction_of_final_rows'])}).",
        f"- Nếu không khử trùng, tổng tiền thô {amt['raw_item_total']:,} VND so với {amt['deduplicated_order_total']:,} VND sau khử trùng: "
        f"**phình thêm {amt['overcount']:,} VND**. {amt.get('note', '')}", "",
        "## Schema evolution", "",
        f"Bảng `{evo['table']}`: đơn có `order_id` ≤ {evo['last_v1_order_id']:,} dùng schema v1, từ {evo['first_v2_order_id']:,} dùng v2 "
        f"({evo['boundary_semantics']}). Phiên bản quan sát được: {v['schema_versions']}.", "",
        "Schema trước (v1):", "", "| Cột | Kiểu | Cho phép NULL |", "|---|---|---|", schema_table(evo["before"]), "",
        "Schema sau (v2):", "", "| Cột | Kiểu | Cho phép NULL |", "|---|---|---|", schema_table(evo["after"]), "",
        "## Vấn đề streaming (Kafka)", "",
        f"- Clickstream mobile đến muộn: {pct(cfg['late_mobile_fraction'])} event mobile, trễ {cfg['late_min_seconds']}–{cfg['late_max_seconds']} giây."
        if cfg.get("late_mobile_fraction") is not None else "- Late arrival: xem `stream/plan_report.json`.",
        f"- Callback thanh toán gửi lại: {pct(cfg['stream_duplicate_fraction'])}." if cfg.get("stream_duplicate_fraction") is not None
        else "- Duplicate callback: xem `stream/plan_report.json`.",
        "- Số event thật sự phát vào Kafka và số bản trùng: `output/<run>/stream/plan_report.json` và `delivery_report.json`; "
        "kết quả xử lý ở `docs/pipeline_05_stream.md`.", "",
    ]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Đã ghi {out}")


if __name__ == "__main__":
    main()
