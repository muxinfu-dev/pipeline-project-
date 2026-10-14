"""一条命令从原始文件生成全部 Gold 表（阶段 1 的完成标准）。

用法：
  python run_pipeline.py                  # bronze → silver → gold
  python run_pipeline.py --skip-bronze    # bronze 已经有了，只重跑后两层
"""
import argparse
import time

from jobs import bronze, gold, silver
from jobs.common import get_logger, get_spark

log = get_logger("pipeline")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-bronze", action="store_true")
    ap.add_argument("--cohort-max-day", type=int, default=None)
    ap.add_argument("--fail-on-warn", action="store_true")
    args = ap.parse_args()

    spark = get_spark("criteo-ads-pipeline")
    t0 = time.time()
    if not args.skip_bronze:
        bronze.run(spark)
    silver.run(spark, fail_on_warn=args.fail_on_warn)
    gold.run(spark, cohort_max_day=args.cohort_max_day)
    log.info("pipeline finished in %.1fs", time.time() - t0)
    spark.stop()
