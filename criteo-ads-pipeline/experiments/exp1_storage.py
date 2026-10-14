"""实验 1：存储格式与分区裁剪。

对比
  A. 格式：gz 文本 vs Parquet，同一个查询
  B. 分区裁剪：Parquet 全表 vs 只查一天（filter day = N）
     对照组：gz 上做同样的“只查一天”，它没有分区，只能全读再过滤

三种查询（每种都在两个格式上跑）
  full_scan  读全部 22 列并物化（noop 写出）→ 纯读取成本
  agg        按 campaign 求 sum(click)、sum(cost) → 典型分析查询，Parquet 只需读 3 列
  one_day    agg 加 day = N 的过滤

另外记录：文件大小、文件个数、每个变体实际读入的字节数（input_bytes，来自 Spark UI）。
gz 文件不可切分，整个文件只能由 1 个 task 读，n_tasks 会体现这一点。

用法：python experiments/exp1_storage.py [--day 5] [--reps 3]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from experiments.bench import EXP_DIR, Bench, noop_write, save_plan
from jobs.bronze import read_raw
from jobs.common import BRONZE_PATH, ORIGINAL_COLS, RAW_PATH, SECONDS_PER_DAY, get_logger, get_spark, p

log = get_logger("exp1")


def dir_stats(path: Path, suffix: str) -> dict:
    files = [f for f in path.rglob(f"*{suffix}") if f.is_file()]
    return {"files": len(files), "bytes": sum(f.stat().st_size for f in files)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", type=int, default=5)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()

    spark = get_spark("exp1-storage")
    bench = Bench(spark, "exp1_storage", reps=args.reps, warmup=args.warmup)

    sizes = {"gz_tsv": {"files": 1, "bytes": RAW_PATH.stat().st_size},
             "parquet": dir_stats(BRONZE_PATH, ".parquet")}
    (EXP_DIR / "exp1_sizes.json").write_text(json.dumps(sizes, indent=2))
    log.info("sizes: %s", sizes)

    def gz():
        return read_raw(spark).withColumn("day", (F.col("timestamp") / SECONDS_PER_DAY).cast("int"))

    def pq():
        return spark.read.parquet(p(BRONZE_PATH))

    def agg(df):
        return df.groupBy("campaign").agg(F.sum("click").alias("clicks"), F.sum("cost").alias("cost"))

    queries = {
        "full_scan": lambda src: src().select(*ORIGINAL_COLS),
        "agg": lambda src: agg(src()),
        "one_day": lambda src: agg(src().filter(F.col("day") == args.day)),
    }
    # 两个格式都用同样的 shuffle 设置，只让“格式”这一个变量变化
    conf = {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "16"}

    for qname, build in queries.items():
        for fmt, src in [("gz_tsv", gz), ("parquet", pq)]:
            df = build(src)
            plan = save_plan(df, f"exp1_{fmt}_{qname}")
            if fmt == "parquet" and qname == "one_day":
                # 证据：Parquet 扫描节点里有 PartitionFilters，说明只读了那一天的目录
                ok = "PartitionFilters: [" in plan and "PartitionFilters: []" not in plan
                log.info("partition pruning present in plan: %s (see plans/exp1_parquet_one_day.txt)", ok)
            bench.run(f"{fmt}/{qname}", lambda df=df: noop_write(df), conf=conf,
                      notes=f"day={args.day}" if qname == "one_day" else "")

    spark.stop()


if __name__ == "__main__":
    main()
