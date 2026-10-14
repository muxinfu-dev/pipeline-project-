from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs.common import BRONZE_PATH, LOG_DIR, ORIGINAL_COLS, SECONDS_PER_DAY, get_logger, get_spark, p

def main():
    spark = get_spark("criteo-sanity")
    df = spark.read.parquet(p(BRONZE_PATH))
    conv = df.filter("conversion_id != -1")

# 查询 1：attribution 是不是转化层面的标签，被复制到了每一次点击曝光上？
    per = conv.groupBy("conversion_id").agg(F.sum("attribution").alias("attr"),
                                         F.sum("click").alias("clicks"))
    per.filter("attr > 0").agg(
    F.sum(F.when(F.col("attr") == F.col("clicks"), 1).otherwise(0)).alias("attr_eq_clicks"),
    F.count("*").alias("n")).show()

# 查询 2：转化时间会不会超过曝光的结束时间？（决定是否有右删失）
    df.agg(F.max("timestamp"), F.max("conversion_timestamp")).show()
    conv.withColumn("d", (F.col("conversion_timestamp") - F.col("timestamp")) / 86400) \
    .groupBy("day").agg(F.max("d").alias("max_delay")).orderBy("day").show(31)




if __name__ == "__main__":
    main()