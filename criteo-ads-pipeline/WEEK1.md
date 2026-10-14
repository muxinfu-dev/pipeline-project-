# Week 1: How to run

Run everything from the project root, using the uv environment.

```bash
# 0. Dependencies (if not installed yet)
uv add pyspark huggingface_hub

# 1. Measure the open questions first (before writing Silver)
uv run python jobs/bronze.py                 # already have bronze, can skip
uv run python analysis/sanity_checks.py      # -> logs/sanity_report.json

# 2. Batch pipeline, one command
uv run python run_pipeline.py --skip-bronze  # silver + gold
#    -> logs/dq_report.json, data/silver/*, data/gold/*

# 3. Experiments 1–4 (each one alone, other heavy programs closed)
uv run python experiments/exp1_storage.py --day 5
uv run python experiments/exp2_shuffle.py
uv run python experiments/exp3_join.py
uv run python experiments/exp4_skew.py --partitions 200 --salt 16
uv run python experiments/summarize.py       # -> experiments/results_summary.md

# 4. Tests that don't need Spark
uv run python tests/test_pure_python.py
```

While an experiment runs you can watch it at http://localhost:4040; the address is printed in the log.

## Layout

```
jobs/common.py            paths, logging, SparkSession (week 2 only needs DATA_DIR changed)
jobs/bronze.py            raw gz -> Parquet partitioned by day
jobs/silver.py            cleaning, deduplication, 5 data-quality checks, quarantine table, dim_campaign
jobs/gold.py              campaign_daily, conversion_delay
run_pipeline.py           one command for all three layers
analysis/sanity_checks.py metric-definition and data-boundary checks
experiments/bench.py      timing + Spark REST API metrics -> results.csv
experiments/exp1-4_*.py   the four experiments
experiments/summarize.py  median summary table
experiments.md            experiment protocols + places to fill in results
```
