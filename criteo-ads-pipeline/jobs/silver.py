"""Silver：清洗 + 数据质量检查 + 广告活动维表。

产出
- silver/impressions    清洗后的曝光，按天分区
- silver/dq_quarantine  没通过行级检查的行，带 dq_failed_rules 列说明原因
- silver/dim_campaign   一行一个广告活动
- logs/dq_report.json   本次运行的数据质量报告（所有数字都来自真实计算）

五条数据质量规则
  行级（不通过 → 隔离）：
    R2 转化时间不早于曝光时间
    R3 click、conversion 只能是 0 或 1
    R4 花费不为负（也不能为空）
  表级（不通过 → 报告里记 WARN，可选择让作业失败）：
    R1 主键 (timestamp, uid, campaign) 不重复
    R5 每天行数与前 7 天均值的偏差不超过阈值

用法：python jobs/silver.py [--rowcount-tolerance 0.3] [--fail-on-warn]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.common import (BRONZE_PATH, DIM_CAMPAIGN_PATH, LOG_DIR, ORIGINAL_COLS,
                         QUARANTINE_PATH, SILVER_PATH, get_logger, get_spark, p,
                         timed)

log = get_logger("silver")

# 原始数据用 -1 表示“没有”，Silver 里统一改成 null，避免 -1 被误算进平均值
SENTINEL_COLS = ["conversion_timestamp", "conversion_id", "click_pos",
                 "click_nb", "time_since_last_click"]
PK_COLS = ["timestamp", "uid", "campaign"]

# 行级规则：条件为 True 表示“这一行违反规则”
ROW_RULES = {
    "R2_conversion_before_impression": F.col("conversion_timestamp") < F.col("timestamp"),
    "R3_click_not_binary": F.col("click").isNull() | ~F.col("click").isin(0, 1),
    "R3_conversion_not_binary": F.col("conversion").isNull() | ~F.col("conversion").isin(0, 1),
    "R4_cost_negative_or_null": F.col("cost").isNull() | (F.col("cost") < 0),
}


def clean(bronze: DataFrame) -> DataFrame:
    df = bronze
    for c in SENTINEL_COLS:
        df = df.withColumn(c, F.when(F.col(c) == -1, F.lit(None)).otherwise(F.col(c)))
    # 转化延迟（秒）。没有转化的行为 null
    df = df.withColumn("conversion_delay_s", F.col("conversion_timestamp") - F.col("timestamp"))
    return df


def flag_rows(df: DataFrame) -> DataFrame:
    """加一列 dq_failed_rules：这一行违反了哪些规则（空数组表示全部通过）。"""
    checks = [F.when(cond, F.lit(name)) for name, cond in ROW_RULES.items()]
    return df.withColumn("dq_failed_rules", F.array_compact(F.array(*checks)))


def rowcount_check(daily_counts: list[tuple[int, int]], lookback: int, tolerance: float) -> list[dict]:
    """R5：每天的行数和前 `lookback` 天的均值比较。纯 Python，方便单元测试。

    day 0 没有历史，跳过；前几天历史不满 7 天时，用能拿到的天数。
    """
    counts = dict(daily_counts)
    days = sorted(counts)
    out = []
    for d in days:
        history = [counts[x] for x in days if d - lookback <= x < d]
        if not history:
            continue
        mean = sum(history) / len(history)
        deviation = (counts[d] - mean) / mean
        out.append({
            "day": d, "rows": counts[d], "trailing_mean": round(mean, 1),
            "history_days": len(history), "deviation": round(deviation, 4),
            "status": "PASS" if abs(deviation) <= tolerance else "WARN",
        })
    return out


def build_dim_campaign(silver: DataFrame) -> DataFrame:
    agg = silver.groupBy("campaign").agg(
        F.min("timestamp").alias("first_ts"),
        F.max("timestamp").alias("last_ts"),
        F.min("day").alias("first_day"),
        F.max("day").alias("last_day"),
        F.count("*").alias("impressions"),
        F.sum("click").alias("clicks"),
        F.sum("cost").alias("relative_cost"),
    )
    # 只有几百行，不分区的窗口函数没问题（Spark 会提示 No Partition Defined，可忽略）
    w = Window.orderBy(F.col("relative_cost").desc())
    return (agg
            .withColumn("cost_rank_pct", F.percent_rank().over(w))
            .withColumn("spend_tier",
                        F.when(F.col("cost_rank_pct") < 0.10, "high")   # 花费前 10%
                         .when(F.col("cost_rank_pct") < 0.50, "mid")
                         .otherwise("low")))


def run(spark: SparkSession, rowcount_tolerance: float = 0.30, lookback: int = 7,
        fail_on_warn: bool = False) -> dict:
    bronze = spark.read.parquet(p(BRONZE_PATH))
    n_bronze = bronze.count()

    # 1) 去完全相同的重复行（只比较原始 22 列，不比较 ingested_at）
    deduped = clean(bronze.dropDuplicates(ORIGINAL_COLS))
    flagged = flag_rows(deduped).persist(StorageLevel.MEMORY_AND_DISK)

    with timed(log, "silver: wrote quarantine"):
        (flagged.filter(F.size("dq_failed_rules") > 0)
         .withColumn("quarantined_at", F.current_timestamp())
         .write.mode("overwrite").parquet(p(QUARANTINE_PATH)))

    with timed(log, "silver: wrote impressions"):
        (flagged.filter(F.size("dq_failed_rules") == 0)
         .drop("dq_failed_rules")
         .repartition("day")
         .write.mode("overwrite").partitionBy("day").parquet(p(SILVER_PATH)))
    flagged.unpersist()

    # 2) 以下检查都在写好的文件上做，读回来很便宜
    silver = spark.read.parquet(p(SILVER_PATH))
    quarantine = spark.read.parquet(p(QUARANTINE_PATH))
    n_silver, n_quarantine = silver.count(), quarantine.count()
    n_exact_dups = n_bronze - n_silver - n_quarantine

    rule_counts = {r["rule"]: r["count"] for r in
                   quarantine.select(F.explode("dq_failed_rules").alias("rule"))
                             .groupBy("rule").count().collect()}

    # R1 主键唯一性（在去掉完全重复行之后检查）
    pk_dups = (silver.groupBy(*PK_COLS).count().filter("count > 1")
               .agg(F.count("*").alias("dup_keys"),
                    F.coalesce(F.sum("count"), F.lit(0)).alias("rows_in_dup_keys"))
               .collect()[0])

    # R5 每天行数
    daily = sorted((r["day"], r["count"]) for r in silver.groupBy("day").count().collect())
    r5 = rowcount_check(daily, lookback, rowcount_tolerance)
    r5_warn = [x for x in r5 if x["status"] == "WARN"]

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {"bronze": n_bronze, "exact_duplicates_removed": n_exact_dups,
                 "quarantined": n_quarantine, "silver": n_silver},
        "R1_primary_key": {
            "key": PK_COLS,
            "duplicate_keys": pk_dups["dup_keys"],
            "rows_in_duplicate_keys": pk_dups["rows_in_dup_keys"],
            "status": "PASS" if pk_dups["dup_keys"] == 0 else "WARN",
        },
        "row_rules_failed_counts": {name: rule_counts.get(name, 0) for name in ROW_RULES},
        "R5_daily_rowcount": {"lookback_days": lookback, "tolerance": rowcount_tolerance,
                              "warn_days": r5_warn, "all_days": r5},
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    report_path = LOG_DIR / "dq_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    log.info("silver rows: %s", report["rows"])
    log.info("R1 pk: %s", {k: v for k, v in report["R1_primary_key"].items() if k != "key"})
    log.info("row rules failed: %s", report["row_rules_failed_counts"])
    log.info("R5 warn days: %s", [(x["day"], x["rows"], x["deviation"]) for x in r5_warn])
    log.info("dq report -> %s", report_path)

    # 3) 广告活动维表
    with timed(log, "silver: wrote dim_campaign"):
        build_dim_campaign(silver).coalesce(1).write.mode("overwrite").parquet(p(DIM_CAMPAIGN_PATH))

    warns = report["R1_primary_key"]["status"] == "WARN" or bool(r5_warn)
    if warns and fail_on_warn:
        raise RuntimeError(f"data quality warnings, see {report_path}")
    return report


def parse_args(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--rowcount-tolerance", type=float, default=0.30)
    ap.add_argument("--lookback", type=int, default=7)
    ap.add_argument("--fail-on-warn", action="store_true")
    return ap.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark("criteo-silver")
    run(spark, args.rowcount_tolerance, args.lookback, args.fail_on_warn)
    spark.stop()
