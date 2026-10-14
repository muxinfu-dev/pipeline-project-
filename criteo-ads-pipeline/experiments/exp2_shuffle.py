"""实验 2：shuffle 分区数 × AQE。

2×2 网格：spark.sql.shuffle.partitions ∈ {200, 16} × spark.sql.adaptive.enabled ∈ {false, true}
负载：Gold 作业本身（campaign_daily + conversion_delay），从 Silver 读，noop 写出
      （不写盘，避免磁盘写入时间盖过要比较的 shuffle 开销）。

要看的数字
  wall_s              作业耗时
  shuffle_read_tasks  读 shuffle 的 task 数：AQE 关时应该等于分区数 × shuffle 次数；
                      AQE 开时会被合并（coalesce），具体合并成多少以实测为准
解释时注意：local 模式下 task 是线程，调度开销比集群小，差距可能没有集群上明显。

用法：python experiments/exp2_shuffle.py [--reps 3]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.bench import Bench, noop_write, save_plan
from jobs.common import SILVER_PATH, get_spark, p
from jobs.gold import build_campaign_daily, build_conversion_delay


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--partitions", type=int, nargs="+", default=[200, 16])
    args = ap.parse_args()

    spark = get_spark("exp2-shuffle")
    bench = Bench(spark, "exp2_shuffle", reps=args.reps, warmup=args.warmup)

    def gold_job():
        silver = spark.read.parquet(p(SILVER_PATH))
        noop_write(build_campaign_daily(silver))
        noop_write(build_conversion_delay(silver))

    for aqe in ("false", "true"):
        for n in args.partitions:
            conf = {"spark.sql.adaptive.enabled": aqe, "spark.sql.shuffle.partitions": n}
            for k, v in conf.items():
                spark.conf.set(k, str(v))
            save_plan(build_campaign_daily(spark.read.parquet(p(SILVER_PATH))),
                      f"exp2_campaign_daily_aqe-{aqe}_p{n}")
            bench.run(f"aqe={aqe}/partitions={n}", gold_job, conf=conf)

    spark.stop()


if __name__ == "__main__":
    main()
