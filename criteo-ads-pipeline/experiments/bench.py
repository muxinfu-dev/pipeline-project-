"""实验计时与指标采集。

每个实验变体：先跑 warmup 次（不计入），再跑 reps 次，取中位数。
每次运行都打上 job group 标签，结束后从 Spark 自带的 REST API
（就是 Spark UI 背后的接口，http://localhost:4040/api/v1）把这次运行涉及的所有 stage 拿出来：
  - n_tasks / shuffle_read_tasks：task 总数 / 读 shuffle 的 task 数（实验 2）
  - input_bytes / shuffle_read_bytes / shuffle_write_bytes（实验 1、3）
  - 读 shuffle 最多的那个 stage 里，task 耗时和读取行数的 max / 中位数（实验 4）
每一行原始结果都追加进 experiments/results.csv，之后用 summarize.py 汇总。
"""
from __future__ import annotations

import csv
import json
import platform
import statistics
import sys
import time
import urllib.request
import uuid
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession

from jobs.common import get_logger

EXP_DIR = Path(__file__).resolve().parent
RESULTS_CSV = EXP_DIR / "results.csv"
PLANS_DIR = EXP_DIR / "plans"

FIELDS = [
    "run_at_utc", "experiment", "variant", "rep", "is_warmup", "wall_s",
    "n_jobs", "n_stages", "n_tasks", "shuffle_read_tasks",
    "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
    "heavy_stage_id", "heavy_stage_tasks",
    "task_ms_p50", "task_ms_max", "task_ms_max_over_p50",
    "read_records_p50", "read_records_max", "read_records_max_over_p50",
    "conf", "notes",
]

log = get_logger("bench")


def noop_write(df: DataFrame) -> None:
    """强制把整个 DataFrame 算完，但不写盘。

    不用 count()：在 Parquet 上 count() 只读文件 footer 里的统计信息，几乎不读数据，
    你 bronze 那一步 'read+count in 0.7s' 就是这个原因，不能当成扫描耗时。
    """
    df.write.format("noop").mode("overwrite").save()


def apply_conf(spark: SparkSession, conf: dict) -> None:
    for k, v in conf.items():
        spark.conf.set(k, str(v))


def save_plan(df: DataFrame, name: str) -> str:
    """保存物理计划，用来证明“确实发生了分区裁剪 / 用的确实是 broadcast join”。"""
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    buf = StringIO()
    with redirect_stdout(buf):
        df.explain(mode="formatted")
    text = buf.getvalue()
    (PLANS_DIR / f"{name}.txt").write_text(text)
    return text


def write_env(spark: SparkSession) -> None:
    sc = spark.sparkContext
    env = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "master": sc.master,
        "default_parallelism": sc.defaultParallelism,
        "driver_memory": sc.getConf().get("spark.driver.memory", "default"),
    }
    (EXP_DIR / "env.json").write_text(json.dumps(env, indent=2))


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


def _ratio(a, b):
    return round(a / b, 3) if a is not None and b else None


class Bench:
    def __init__(self, spark: SparkSession, experiment: str, reps: int = 3, warmup: int = 1):
        self.spark = spark
        self.sc = spark.sparkContext
        self.experiment = experiment
        self.reps = reps
        self.warmup = warmup
        if not self.sc.uiWebUrl:
            raise RuntimeError("需要 Spark UI（spark.ui.enabled=true）才能采集 stage 指标")
        self.api = f"{self.sc.uiWebUrl}/api/v1/applications/{self.sc.applicationId}"
        write_env(spark)

    # ---------- 对外接口 ----------
    def run(self, variant: str, fn, conf: dict | None = None, notes: str = "") -> dict:
        conf = conf or {}
        apply_conf(self.spark, conf)
        log.info("[%s] %s  conf=%s", self.experiment, variant, conf)
        results = []
        for i in range(self.warmup + self.reps):
            is_warmup = i < self.warmup
            row = self._one(variant, fn, rep=i - self.warmup, is_warmup=is_warmup)
            row["conf"] = json.dumps(conf, sort_keys=True)
            row["notes"] = notes
            self._append(row)
            if not is_warmup:
                results.append(row)
            log.info("  %s rep=%s wall=%.2fs tasks=%s shuffleR=%s shuffleW=%s skew(t)=%s skew(rec)=%s",
                     "warmup" if is_warmup else "timed", row["rep"], row["wall_s"], row["n_tasks"],
                     row["shuffle_read_bytes"], row["shuffle_write_bytes"],
                     row["task_ms_max_over_p50"], row["read_records_max_over_p50"])
        med = statistics.median(r["wall_s"] for r in results)
        log.info("[%s] %s  median wall = %.2fs over %d reps", self.experiment, variant, med, len(results))
        return {"variant": variant, "median_wall_s": med, "runs": results}

    # ---------- 内部 ----------
    def _one(self, variant: str, fn, rep: int, is_warmup: bool) -> dict:
        tag = f"{self.experiment}|{variant}|{rep}|{uuid.uuid4().hex[:8]}"
        self.spark.catalog.clearCache()
        self.sc.setJobGroup(tag, tag)
        t0 = time.perf_counter()
        fn()
        wall = time.perf_counter() - t0
        self.sc.setLocalProperty("spark.jobGroup.id", None)
        row = {
            "run_at_utc": datetime.now(timezone.utc).isoformat(),
            "experiment": self.experiment, "variant": variant, "rep": rep,
            "is_warmup": is_warmup, "wall_s": round(wall, 3),
        }
        row.update(self._stage_metrics(tag))
        return row

    def _stage_metrics(self, tag: str) -> dict:
        tracker = self.sc.statusTracker()
        job_ids = tracker.getJobIdsForGroup(tag)
        stage_ids = sorted({s for j in job_ids if (info := tracker.getJobInfo(j)) for s in info.stageIds})

        stages = []
        deadline = time.time() + 15    # 事件总线是异步的，stage 信息可能晚一点才进 UI
        for sid in stage_ids:
            while True:
                try:
                    attempts = _get_json(f"{self.api}/stages/{sid}?details=false")
                    latest = max(attempts, key=lambda a: a["attemptId"])
                    if latest["status"] in ("COMPLETE", "SKIPPED", "FAILED") or time.time() > deadline:
                        stages.append(latest)
                        break
                except Exception:
                    if time.time() > deadline:
                        break
                time.sleep(0.3)

        ran = [s for s in stages if s["status"] != "SKIPPED"]
        out = {
            "n_jobs": len(job_ids), "n_stages": len(ran),
            "n_tasks": sum(s.get("numCompleteTasks", 0) for s in ran),
            "shuffle_read_tasks": sum(s.get("numCompleteTasks", 0) for s in ran if s.get("shuffleReadBytes", 0) > 0),
            "input_bytes": sum(s.get("inputBytes", 0) for s in ran),
            "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in ran),
            "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in ran),
        }
        heavy = max((s for s in ran if s.get("shuffleReadBytes", 0) > 0 and s.get("numCompleteTasks", 0) > 1),
                    key=lambda s: s["shuffleReadBytes"], default=None)
        out.update(self._task_skew(heavy))
        return out

    def _task_skew(self, stage: dict | None) -> dict:
        empty = {"heavy_stage_id": None, "heavy_stage_tasks": None, "task_ms_p50": None,
                 "task_ms_max": None, "task_ms_max_over_p50": None, "read_records_p50": None,
                 "read_records_max": None, "read_records_max_over_p50": None}
        if stage is None:
            return empty
        try:
            s = _get_json(f"{self.api}/stages/{stage['stageId']}/{stage['attemptId']}"
                          f"/taskSummary?quantiles=0.5,1.0")
        except Exception as e:  # 指标拿不到不影响计时结果
            log.warning("taskSummary failed for stage %s: %s", stage["stageId"], e)
            return {**empty, "heavy_stage_id": stage["stageId"]}
        t50, tmax = s["executorRunTime"]
        r50, rmax = s["shuffleReadMetrics"]["readRecords"]
        return {"heavy_stage_id": stage["stageId"], "heavy_stage_tasks": stage.get("numCompleteTasks"),
                "task_ms_p50": t50, "task_ms_max": tmax, "task_ms_max_over_p50": _ratio(tmax, t50),
                "read_records_p50": r50, "read_records_max": rmax,
                "read_records_max_over_p50": _ratio(rmax, r50)}

    @staticmethod
    def _append(row: dict) -> None:
        new = not RESULTS_CSV.exists()
        with RESULTS_CSV.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            if new:
                w.writeheader()
            w.writerow({k: row.get(k) for k in FIELDS})
