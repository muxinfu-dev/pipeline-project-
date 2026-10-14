"""把 results.csv 汇总成 markdown 表（每个变体取非 warmup 运行的中位数），粘贴进 experiments.md。

用法：python experiments/summarize.py [--experiment exp1_storage]
"""
from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

RESULTS_CSV = Path(__file__).resolve().parent / "results.csv"
COLS = ["wall_s", "n_tasks", "shuffle_read_tasks", "input_bytes", "shuffle_read_bytes",
        "shuffle_write_bytes", "task_ms_max_over_p50", "read_records_max_over_p50"]


def _num(x):
    if x in (None, "", "None"):
        return None
    return float(x)


def _fmt(col, v):
    if v is None:
        return "–"
    if col.endswith("_bytes"):
        return f"{v / 1e6:,.1f} MB"
    if col == "wall_s":
        return f"{v:.2f}"
    if col.endswith("over_p50"):
        return f"{v:.2f}"
    return f"{v:,.0f}"


def summarize(rows, experiment=None):
    groups = defaultdict(list)
    order = []
    for r in rows:
        if r["is_warmup"] == "True" or (experiment and r["experiment"] != experiment):
            continue
        key = (r["experiment"], r["variant"])
        if key not in groups:
            order.append(key)
        groups[key].append(r)

    out = []
    for exp in dict.fromkeys(k[0] for k in order):
        out.append(f"\n### {exp}\n")
        out.append("| variant | n | " + " | ".join(COLS) + " | wall_s 全部 |")
        out.append("|---|---|" + "---|" * len(COLS) + "---|")
        for key in [k for k in order if k[0] == exp]:
            runs = groups[key]
            cells = []
            for c in COLS:
                vals = [v for v in (_num(r[c]) for r in runs) if v is not None]
                cells.append(_fmt(c, statistics.median(vals) if vals else None))
            walls = ", ".join(f"{_num(r['wall_s']):.2f}" for r in runs)
            out.append(f"| {key[1]} | {len(runs)} | " + " | ".join(cells) + f" | {walls} |")
    return "\n".join(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment")
    args = ap.parse_args()
    with RESULTS_CSV.open() as f:
        text = summarize(list(csv.DictReader(f)), args.experiment)
    print(text)
    (RESULTS_CSV.parent / "results_summary.md").write_text(text + "\n")
