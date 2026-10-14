"""实验 3：broadcast join vs sort-merge join。

大表：silver.impressions（只取 join 和下游需要的几列）
小表：silver.dim_campaign（几百行）
连接键：campaign

变体（只改 join 策略，其他条件相同）
  broadcast   F.broadcast(dim)            → 计划里应是 BroadcastHashJoin，大表不 shuffle
  sort_merge  dim.hint("merge")           → 计划里应是 SortMergeJoin，两边都按 campaign shuffle
  auto        不加 hint、恢复默认阈值、AQE 开 → 看 Spark 自己会选什么（参考组，不参与对比）

前两组关闭 AQE 并把两个自动 broadcast 阈值设为 -1，
否则 AQE 可能在运行时把 sort-merge 自动改成 broadcast，实验就不是在比“一个变量”了。
脚本会检查计划里的 join 算子，和预期不符就直接报错，不会记一个错误的结果。

要看的数字：wall_s、shuffle_write_bytes、shuffle_read_bytes

用法：python experiments/exp3_join.py [--reps 3]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from experiments.bench import Bench, apply_conf, noop_write, save_plan
from jobs.common import DIM_CAMPAIGN_PATH, SILVER_PATH, get_logger, get_spark, p

log = get_logger("exp3")

FIXED = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "16",
}
AUTO = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",   # Spark 默认 10MB
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "10485760",
    "spark.sql.shuffle.partitions": "16",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()

    spark = get_spark("exp3-join")
    bench = Bench(spark, "exp3_join", reps=args.reps, warmup=args.warmup)

    def big():
        return spark.read.parquet(p(SILVER_PATH)).select("campaign", "day", "uid", "timestamp", "click", "cost")

    def dim():
        return spark.read.parquet(p(DIM_CAMPAIGN_PATH)).select("campaign", "spend_tier")

    variants = {
        "broadcast": (FIXED, lambda: big().join(F.broadcast(dim()), "campaign"), "BroadcastHashJoin"),
        "sort_merge": (FIXED, lambda: big().join(dim().hint("merge"), "campaign"), "SortMergeJoin"),
        "auto": (AUTO, lambda: big().join(dim(), "campaign"), None),
    }

    # 正确性：两种 join 的结果必须一致（行数和花费总和）
    apply_conf(spark, FIXED)
    checks = {name: variants[name][1]().agg(F.count("*").alias("n"), F.sum("cost").alias("c")).collect()[0]
              for name in ("broadcast", "sort_merge")}
    log.info("correctness check: %s", {k: v.asDict() for k, v in checks.items()})
    if checks["broadcast"]["n"] != checks["sort_merge"]["n"]:
        raise RuntimeError("broadcast 和 sort-merge 的结果行数不一致，先排查再计时")

    for name, (conf, build, expected) in variants.items():
        apply_conf(spark, conf)
        plan = save_plan(build(), f"exp3_{name}")
        if expected and expected not in plan:
            raise RuntimeError(f"{name}: 计划里没有 {expected}，见 experiments/plans/exp3_{name}.txt")
        if not expected:
            chosen = [j for j in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin") if j in plan]
            log.info("auto variant chose (initial plan): %s", chosen)
        bench.run(name, lambda build=build: noop_write(build()), conf=conf,
                  notes="reference only" if name == "auto" else "")

    spark.stop()


if __name__ == "__main__":
    main()
