"""在写 Silver/Gold 之前，先用数据回答几个“口径”问题。所有结论都以这里跑出来的数字为准。

1. 为什么 days=31？day=30 有多少行、是不是不完整的一天
2. (timestamp, uid, campaign) 能不能当主键？有没有完全重复的行
3. conversion / attribution / conversion_id 的关系：
   sum(conversion) 是不是会多算同一个转化？
4. 转化延迟的范围，有没有转化早于曝光的行
5. 广告活动的集中度（业务问题 5，也是实验 4 的前提）
6. 计划里的“用 Spark 做几个熟悉的 pandas 操作”（注释里写了 pandas 等价写法）

用法：python analysis/sanity_checks.py   → 结果写到 logs/sanity_report.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs.common import BRONZE_PATH, LOG_DIR, ORIGINAL_COLS, SECONDS_PER_DAY, get_logger, get_spark, p

log = get_logger("sanity")


def rows(df):
    return [r.asDict() for r in df.collect()]


def main():
    spark = get_spark("criteo-sanity")
    df = spark.read.parquet(p(BRONZE_PATH))
    report = {}

    # 1) 边界天
    report["edge_days"] = rows(
        df.filter((F.col("day") <= 0) | (F.col("day") >= 29))
          .groupBy("day").agg(F.count("*").alias("rows"),
                              F.min("timestamp").alias("min_ts"),
                              F.max("timestamp").alias("max_ts"))
          .orderBy("day"))
    report["timestamp_range"] = rows(df.agg(F.min("timestamp").alias("min"),
                                            F.max("timestamp").alias("max")))[0]

    # 2) 主键与重复
    n = df.count()
    report["rows"] = n
    report["distinct_rows_original_cols"] = df.select(*ORIGINAL_COLS).distinct().count()
    report["pk_duplicate_keys"] = (df.groupBy("timestamp", "uid", "campaign").count()
                                   .filter("count > 1").count())

    # 3) 转化口径
    conv = df.filter(F.col("conversion_id") != -1)
    per_conv = conv.groupBy("conversion_id").agg(
        F.count("*").alias("impressions"),
        F.sum("attribution").alias("attributed_impressions"),
        F.countDistinct("campaign").alias("campaigns"))
    report["conversion_semantics"] = {
        "sum_conversion": rows(df.agg(F.sum("conversion").alias("x")))[0]["x"],
        "sum_attribution": rows(df.agg(F.sum("attribution").alias("x")))[0]["x"],
        "distinct_conversion_id": conv.select("conversion_id").distinct().count(),
        "impressions_per_conversion_id": rows(per_conv.agg(
            F.min("impressions").alias("min"),
            F.expr("percentile_approx(impressions, 0.5)").alias("p50"),
            F.expr("percentile_approx(impressions, 0.99)").alias("p99"),
            F.max("impressions").alias("max")))[0],
        "attributed_impressions_per_conversion_id": rows(
            per_conv.groupBy("attributed_impressions").count().orderBy("attributed_impressions")),
        "conversion_ids_spanning_multiple_campaigns": per_conv.filter("campaigns > 1").count(),
        # 下面三个应该是 0；不是 0 就说明对字段的理解有偏差，要回去看数据说明
        "attribution1_but_conversion0": df.filter("attribution = 1 AND conversion = 0").count(),
        "attribution1_but_click0": df.filter("attribution = 1 AND click = 0").count(),
        "conversion1_but_no_conversion_ts": df.filter("conversion = 1 AND conversion_timestamp = -1").count(),
    }

    # 4) 转化延迟
    delay = conv.withColumn("delay_days", (F.col("conversion_timestamp") - F.col("timestamp")) / SECONDS_PER_DAY)
    report["delay_days"] = rows(delay.agg(
        F.min("delay_days").alias("min"),
        F.expr("percentile_approx(delay_days, array(0.5, 0.9, 0.99))").alias("p50_p90_p99"),
        F.max("delay_days").alias("max"),
        F.sum(F.when(F.col("delay_days") < 0, 1).otherwise(0)).alias("negative_rows")))[0]

    # 5) 集中度：前 k 个活动占多少曝光、多少花费
    by_c = df.groupBy("campaign").agg(F.count("*").alias("impressions"), F.sum("cost").alias("cost"))
    tot = rows(by_c.agg(F.sum("impressions").alias("i"), F.sum("cost").alias("c")))[0]
    conc = {}
    for metric, total in [("impressions", tot["i"]), ("cost", tot["c"])]:
        ranked = by_c.withColumn("rk", F.row_number().over(Window.orderBy(F.col(metric).desc())))
        conc[metric] = {f"top{k}_share": rows(ranked.filter(F.col("rk") <= k)
                                             .agg(F.sum(metric).alias("s")))[0]["s"] / total
                        for k in (1, 5, 10, 50)}
    conc["campaign_impressions_quantiles"] = rows(by_c.agg(
        F.expr("percentile_approx(impressions, array(0.5, 0.9, 0.99))").alias("p50_p90_p99"),
        F.max("impressions").alias("max")))[0]
    report["concentration"] = conc

    # 6) pandas → Spark 对照
    #   pandas: df[df.click == 1]
    clicked = df.filter(F.col("click") == 1)
    #   pandas: df.groupby("campaign").agg(impr=("uid","size"), clicks=("click","sum"))
    #             .assign(ctr=lambda x: x.clicks / x.impr).nlargest(10, "impr")
    top_ctr = (df.groupBy("campaign")
                 .agg(F.count("*").alias("impressions"), F.sum("click").alias("clicks"))
                 .withColumn("ctr", F.col("clicks") / F.col("impressions"))
                 .orderBy(F.col("impressions").desc())
                 .limit(10))
    report["pandas_style"] = {"clicked_rows": clicked.count(), "top10_campaigns_by_impressions": rows(top_ctr)}

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out = LOG_DIR / "sanity_report.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    log.info("sanity report -> %s", out)
    print(json.dumps({k: v for k, v in report.items() if k != "pandas_style"}, indent=2, default=str))
    spark.stop()
    




if __name__ == "__main__":
    main()
