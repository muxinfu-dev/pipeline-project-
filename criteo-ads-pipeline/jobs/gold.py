"""Gold：分析师直接用的两张表。

- gold/campaign_daily    一个广告活动 × 一天
- gold/conversion_delay  一个广告活动 × 一个延迟区间（另加 scope='all' 的全局行）

指标口径（写进 README，面试会被问）
- impressions                 曝光数 = 行数
- clicks                      sum(click)
- impressions_with_conversion sum(conversion)：这次曝光之后发生了转化。
                              同一个转化可能挂在同一用户的多次曝光上，所以这个数
                              ≥ 真实转化数，不能直接当“转化数”用
- attributed_conversions      sum(attribution)：被归因的转化，作为 CVR、CPA 的分子/分母
- relative_cost               sum(cost)。cost 被 Criteo 变换过，只能比相对大小
- ctr = clicks / impressions
- cvr = attributed_conversions / clicks
- cpa = relative_cost / attributed_conversions
Spark 4 默认开启 ANSI 模式，除以 0 会直接报错，所以分母为 0 时显式返回 null。

用法：python jobs/gold.py [--cohort-max-day N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.common import (GOLD_CAMPAIGN_DAILY_PATH, GOLD_CONVERSION_DELAY_PATH,
                         SECONDS_PER_DAY, SILVER_PATH, get_logger, get_spark, p,
                         timed)

log = get_logger("gold")

# (标签, 下界天数含, 上界天数不含)；最后一档兜底，避免超过 30 天的数据被悄悄丢掉
DELAY_BUCKETS = [("0-1d", 0, 1), ("1-3d", 1, 3), ("3-7d", 3, 7), ("7-30d", 7, 30)]
OVERFLOW_BUCKET = ">=30d"


def safe_div(num, den):
    return F.when(den > 0, num / den)


def build_campaign_daily(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("campaign", "day").agg(
                F.count("*").alias("impressions"),
                F.sum("click").alias("clicks"),
                F.sum("conversion").alias("impressions_with_conversion"),
                F.sum("attribution").alias("attributed_conversions"),
                F.sum("cost").alias("relative_cost"))
            .withColumn("ctr", safe_div(F.col("clicks"), F.col("impressions")))
            .withColumn("cvr", safe_div(F.col("attributed_conversions"), F.col("clicks")))
            .withColumn("cpa", safe_div(F.col("relative_cost"), F.col("attributed_conversions"))))


def delay_bucket_cols():
    days = F.col("conversion_delay_s") / SECONDS_PER_DAY
    label = F.lit(OVERFLOW_BUCKET)
    order = F.lit(len(DELAY_BUCKETS))
    for i, (name, lo, hi) in reversed(list(enumerate(DELAY_BUCKETS))):
        cond = (days >= lo) & (days < hi)
        label = F.when(cond, F.lit(name)).otherwise(label)
        order = F.when(cond, F.lit(i)).otherwise(order)
    return label, order


def build_conversion_delay(silver: DataFrame, cohort_max_day: int | None = None) -> DataFrame:
    """以“带转化的曝光”为单位，统计曝光到转化的延迟分布。

    右删失：越靠后的曝光，能观察到的最长延迟越短（数据只有 30 天），
    所以用全部曝光算出来的分布会偏向“短延迟”。--cohort-max-day 可以只取前 N 天的曝光，
    结果里会记录用了哪个 cohort。
    """
    conv = silver.filter(F.col("conversion_timestamp").isNotNull())
    if cohort_max_day is not None:
        conv = conv.filter(F.col("day") <= cohort_max_day)
    label, order = delay_bucket_cols()
    conv = conv.withColumn("delay_bucket", label).withColumn("bucket_order", order)

    per_campaign = (conv.groupBy("campaign", "delay_bucket", "bucket_order")
                    .agg(F.count("*").alias("impressions_with_conversion"))
                    .withColumn("scope", F.lit("campaign")))
    overall = (conv.groupBy("delay_bucket", "bucket_order")
               .agg(F.count("*").alias("impressions_with_conversion"))
               .withColumn("campaign", F.lit(None).cast("long"))
               .withColumn("scope", F.lit("all")))
    both = per_campaign.unionByName(overall)

    total_w = Window.partitionBy("scope", "campaign")
    cum_w = total_w.orderBy("bucket_order").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    total = F.sum("impressions_with_conversion").over(total_w)
    return (both
            .withColumn("share", F.col("impressions_with_conversion") / total)
            .withColumn("cumulative_share", F.sum("impressions_with_conversion").over(cum_w) / total)
            .withColumn("cohort_max_day", F.lit(cohort_max_day).cast("int"))
            .select("scope", "campaign", "delay_bucket", "bucket_order",
                    "impressions_with_conversion", "share", "cumulative_share", "cohort_max_day"))


def run(spark: SparkSession, cohort_max_day: int | None = None) -> None:
    silver = spark.read.parquet(p(SILVER_PATH))

    with timed(log, "gold: wrote campaign_daily"):
        (build_campaign_daily(silver)
         .repartition("day")
         .write.mode("overwrite").partitionBy("day").parquet(p(GOLD_CAMPAIGN_DAILY_PATH)))

    with timed(log, "gold: wrote conversion_delay"):
        (build_conversion_delay(silver, cohort_max_day)
         .coalesce(1)
         .write.mode("overwrite").parquet(p(GOLD_CONVERSION_DELAY_PATH)))

    overall = (spark.read.parquet(p(GOLD_CONVERSION_DELAY_PATH))
               .filter("scope = 'all'").orderBy("bucket_order").collect())
    log.info("overall conversion delay (cohort_max_day=%s): %s", cohort_max_day,
             [(r["delay_bucket"], r["impressions_with_conversion"], round(r["cumulative_share"], 4))
              for r in overall])


def parse_args(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort-max-day", type=int, default=None)
    return ap.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark("criteo-gold")
    run(spark, args.cohort_max_day)
    spark.stop()
