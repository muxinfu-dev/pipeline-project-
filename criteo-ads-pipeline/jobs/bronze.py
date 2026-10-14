"""Bronze：原始 TSV.gz → 按天分区的 Parquet，数据原样保留。

用法：python jobs/bronze.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

from jobs.common import (BRONZE_PATH, RAW_PATH, SECONDS_PER_DAY, get_logger,
                         get_spark, p, timed)

log = get_logger("bronze")

RAW_SCHEMA = StructType(
    [
        StructField("timestamp", LongType()),             # 曝光时间（秒，从 0 开始）
        StructField("uid", LongType()),
        StructField("campaign", LongType()),
        StructField("conversion", IntegerType()),
        StructField("conversion_timestamp", LongType()),  # -1 表示没有转化
        StructField("conversion_id", LongType()),
        StructField("attribution", IntegerType()),
        StructField("click", IntegerType()),
        StructField("click_pos", IntegerType()),
        StructField("click_nb", IntegerType()),
        StructField("cost", DoubleType()),
        StructField("cpo", DoubleType()),
        StructField("time_since_last_click", LongType()),
    ]
    + [StructField(f"cat{i}", StringType()) for i in range(1, 10)]
)


def download_raw() -> Path:
    """已经下载过就跳过。"""
    if RAW_PATH.exists():
        log.info("raw file exists: %s (%.0f MB)", RAW_PATH, RAW_PATH.stat().st_size / 1e6)
        return RAW_PATH
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(
        repo_id="criteo/criteo-attribution-dataset",
        filename="criteo_attribution_dataset.tsv.gz",
        repo_type="dataset",
        local_dir=RAW_PATH.parent,
    )
    log.info("downloaded: %s (%.0f MB)", path, os.path.getsize(path) / 1e6)
    return Path(path)


def read_raw(spark: SparkSession):
    return (spark.read
            .option("sep", "\t")
            .option("header", True)
            .option("mode", "FAILFAST")      # 列数或类型不对就直接报错，不悄悄变成 null
            .schema(RAW_SCHEMA)
            .csv(p(RAW_PATH)))


def run(spark: SparkSession) -> None:
    download_raw()
    with timed(log, "bronze: wrote parquet"):
        bronze = (read_raw(spark)
                  .withColumn("day", (F.col("timestamp") / SECONDS_PER_DAY).cast("int"))
                  .withColumn("ingested_at", F.current_timestamp()))
        (bronze
         .repartition("day")              # 每天的数据放到一起，每天一个文件
         .write
         .mode("overwrite")
         .partitionBy("day")
         .parquet(p(BRONZE_PATH)))
    # count() 在 Parquet 上只读文件 footer 里的行数统计，所以很快；它不代表全表扫描耗时
    n = spark.read.parquet(p(BRONZE_PATH)).count()
    log.info("bronze: %d rows -> %s", n, BRONZE_PATH)


if __name__ == "__main__":
    spark = get_spark("criteo-bronze")
    run(spark)
    spark.stop()
