"""实验 4：数据倾斜与加盐。

第 0 步（不计时，纯统计）：倾斜到底有多严重
  - 每个 campaign 的曝光数分布、前 k 个活动的占比
  - 预测 shuffle 后每个分区分到多少行：Spark 的 hash 分区规则是
    pmod(murmur3_hash(key, seed=42), N)，和 F.hash 完全一致，所以能在跑之前算出来。
    注意：分区数越少，每个分区混进的活动越多，倾斜会被“稀释”。
    本实验默认用 200 个分区，并关闭 AQE 的分区合并，让倾斜能被看到。

计时变体（AQE 关闭，broadcast 关闭，shuffle 分区数相同）
  A groupby          groupBy(campaign).sum(cost)
                     Spark 会先在 map 端做部分聚合，每个 task 每个活动只发 1 行，
                     所以倾斜可能根本不出现。是不是这样，以实测的 read_records 比值为准
  B groupby_salted   两阶段加盐聚合，用来对比 A
  C join             大表 sort-merge join 维表，键 = campaign。join 没有 map 端合并，
                     头部活动的所有行都会进同一个 task
  D join_salted      大表加 salt 列（0..S-1），维表复制 S 份，按 (campaign, salt) join
                     salt 用 xxhash64(uid, timestamp) 取模而不是 rand()，
                     这样 task 重试时结果不变（rand 在重算时可能得到不同的值）
  E join_aqe_skew    不加盐，打开 AQE 的 skew join 优化（Spark 内置方案）
                     只有分区大于 skewedPartitionThresholdInBytes 且大于中位数 × factor 时才会触发，
                     阈值见 --aqe-skew-threshold-mb。是否真的触发，要在 Spark UI 的 SQL 页面确认

要看的数字：重 stage 里 task 耗时 max/中位数、读取行数 max/中位数，以及 wall_s

用法：python experiments/exp4_skew.py [--partitions 200] [--salt 16] [--reps 3]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from experiments.bench import EXP_DIR, Bench, apply_conf, noop_write, save_plan
from jobs.common import DIM_CAMPAIGN_PATH, SILVER_PATH, get_logger, get_spark, p

log = get_logger("exp4")


def partition_load(df, n: int) -> dict:
    """按 Spark 的 hash 规则，预测 n 个分区各分到多少行。"""
    loads = (df.groupBy(F.pmod(F.hash("campaign"), F.lit(n)).alias("pid")).count()
             .agg(F.max("count").alias("max"),
                  F.expr("percentile_approx(count, 0.5)").alias("p50"),
                  F.count("*").alias("non_empty_partitions"))
             .collect()[0].asDict())
    loads["max_over_p50"] = round(loads["max"] / loads["p50"], 3) if loads["p50"] else None
    return loads


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partitions", type=int, default=200)
    ap.add_argument("--salt", type=int, default=16)
    ap.add_argument("--aqe-skew-threshold-mb", type=int, default=8)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()
    S, N = args.salt, args.partitions

    spark = get_spark("exp4-skew")
    bench = Bench(spark, "exp4_skew", reps=args.reps, warmup=args.warmup)

    def big():
        return spark.read.parquet(p(SILVER_PATH)).select("campaign", "day", "uid", "timestamp", "click", "cost")

    def dim():
        return spark.read.parquet(p(DIM_CAMPAIGN_PATH)).select("campaign", "spend_tier")

    # ---------- 第 0 步：量化倾斜 ----------
    by_c = big().groupBy("campaign").count()
    total = big().count()
    top = [r["count"] for r in by_c.orderBy(F.col("count").desc()).limit(10).collect()]
    profile = {
        "rows": total,
        "campaigns": by_c.count(),
        "top1_share": top[0] / total, "top10_share": sum(top) / total,
        "campaign_rows_p50": by_c.agg(F.expr("percentile_approx(count, 0.5)")).collect()[0][0],
        "campaign_rows_max": top[0],
        "predicted_partition_load": {n: partition_load(big(), n) for n in sorted({16, N})},
    }
    (EXP_DIR / "exp4_skew_profile.json").write_text(json.dumps(profile, indent=2, default=str))
    log.info("skew profile: %s", profile)

    # ---------- 计时变体 ----------
    base = {
        "spark.sql.shuffle.partitions": N,
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    }
    aqe_skew = {
        **base,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",   # 不合并，便于和 C 比较
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": f"{args.aqe_skew_threshold_mb}m",
    }

    def salt_col():
        return F.pmod(F.xxhash64("uid", "timestamp"), F.lit(S)).cast("int")

    def groupby():
        return big().groupBy("campaign").agg(F.sum("cost").alias("cost"))

    def groupby_salted():
        partial = (big().withColumn("salt", salt_col())
                   .groupBy("campaign", "salt").agg(F.sum("cost").alias("cost")))
        return partial.groupBy("campaign").agg(F.sum("cost").alias("cost"))

    def join():
        return big().join(dim().hint("merge"), "campaign")

    def join_salted():
        dim_s = dim().crossJoin(spark.range(S).select(F.col("id").cast("int").alias("salt")))
        return (big().withColumn("salt", salt_col())
                .join(dim_s.hint("merge"), ["campaign", "salt"]).drop("salt"))

    variants = [
        ("groupby", base, groupby),
        ("groupby_salted", base, groupby_salted),
        ("join", base, join),
        ("join_salted", base, join_salted),
        ("join_aqe_skew", aqe_skew, join),
    ]

    # 正确性：加盐前后结果必须一致
    apply_conf(spark, base)
    j = join().agg(F.count("*"), F.sum("cost")).collect()[0]
    js = join_salted().agg(F.count("*"), F.sum("cost")).collect()[0]
    log.info("correctness join=%s join_salted=%s", tuple(j), tuple(js))
    if j[0] != js[0]:
        raise RuntimeError("加盐 join 行数和原 join 不一致")

    for name, conf, build in variants:
        apply_conf(spark, conf)
        save_plan(build(), f"exp4_{name}")
        bench.run(name, lambda build=build: noop_write(build()), conf=conf,
                  notes=f"salt={S}" if "salted" in name else "")

    spark.stop()


if __name__ == "__main__":
    main()
