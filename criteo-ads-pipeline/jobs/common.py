"""公共工具：路径、日志、SparkSession、计时。

所有 job 和实验脚本都从这里拿路径和 SparkSession，保证配置一致。
第 2 周搬到 Docker 集群时，只需设置环境变量 DATA_DIR=/opt/data，代码不用改。
"""
from __future__ import annotations

import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

# ---------------- 路径 ----------------
PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[1]))
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_DIR / "data"))
LOG_DIR = Path(os.environ.get("LOG_DIR", PROJECT_DIR / "logs"))

RAW_PATH = DATA_DIR / "raw" / "criteo_attribution_dataset.tsv.gz"
BRONZE_PATH = DATA_DIR / "bronze" / "impressions"
SILVER_PATH = DATA_DIR / "silver" / "impressions"
QUARANTINE_PATH = DATA_DIR / "silver" / "dq_quarantine"
DIM_CAMPAIGN_PATH = DATA_DIR / "silver" / "dim_campaign"
GOLD_CAMPAIGN_DAILY_PATH = DATA_DIR / "gold" / "campaign_daily"
GOLD_CONVERSION_DELAY_PATH = DATA_DIR / "gold" / "conversion_delay"

# 原始文件里的 22 列（bronze 额外加了 day、ingested_at）
ORIGINAL_COLS = [
    "timestamp", "uid", "campaign", "conversion", "conversion_timestamp",
    "conversion_id", "attribution", "click", "click_pos", "click_nb",
    "cost", "cpo", "time_since_last_click",
] + [f"cat{i}" for i in range(1, 10)]

SECONDS_PER_DAY = 86400


# ---------------- 日志 ----------------
def get_logger(name: str = "pipeline") -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    if not getattr(root, "_criteo_configured", False):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
            force=True,
        )
        root._criteo_configured = True  # type: ignore[attr-defined]
    return logging.getLogger(name)


# ---------------- Spark ----------------
DEFAULT_CONF = {
    "spark.driver.memory": "6g",              # 约为电脑内存的一半
    "spark.sql.shuffle.partitions": "16",     # 默认 200，本地设为核数 2 倍左右
    "spark.sql.session.timeZone": "UTC",
}


def get_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """创建 SparkSession。

    - python 直接运行：用 SPARK_MASTER 环境变量，默认 local[*]
    - spark-submit 运行（第 2 周）：master 由 --master 决定，这里不覆盖
    """
    builder = SparkSession.builder.appName(app_name)
    launched_by_spark_submit = "PYSPARK_GATEWAY_PORT" in os.environ
    if not launched_by_spark_submit:
        builder = builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
    for k, v in {**DEFAULT_CONF, **(extra_conf or {})}.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log = get_logger("spark")
    log.info("Python %s | Spark %s | master=%s | defaultParallelism=%s | JAVA_HOME=%s",
             sys.version.split()[0], spark.version, spark.sparkContext.master,
             spark.sparkContext.defaultParallelism, os.environ.get("JAVA_HOME"))
    log.info("Spark UI: %s", spark.sparkContext.uiWebUrl)
    return spark


@contextmanager
def timed(log: logging.Logger, label: str):
    t0 = time.time()
    yield
    log.info("%s: %.1fs", label, time.time() - t0)


def p(path: Path) -> str:
    """Spark 读写要字符串路径。"""
    return str(path)
