# Experiments

All numbers are filled in only after the experiments are actually run, using `python experiments/summarize.py`.
Report "no speedup" results too, together with an explanation of why.

## Common protocol (all experiments)

- **One variable per experiment**: every other config is written explicitly into the script, and each run's `conf` is recorded in `results.csv`
- **1 warm-up + 3 timed runs, median reported**: the warm-up run absorbs JVM JIT warm-up and OS file-cache effects and is not counted
- **Force full computation with `noop` writes**: no `count()` (on Parquet it only reads the row counts in the file footers), and no real writes (they would mix disk-write time into the result)
- **Every number traces back to the Spark UI**: task counts, shuffle bytes and task-time distributions come from the Spark REST API (`/api/v1`) and can be cross-checked in the UI
- **Physical plans saved as evidence**: `experiments/plans/*.txt` proves partition pruning happened and shows which join strategy was used
- **Machine**: `experiments/env.json` (written automatically: Spark version, number of cores, driver memory)
- Close other heavy programs while experiments run; don't run two experiments at once

Environment (copy from env.json):

| Field | Value |
|---|---|
| Machine / chip | |
| Cores (defaultParallelism) | |
| Spark version | |
| driver memory | |

---

## Experiment 1: storage format and partition pruning

**Question**: Why use Parquet? How should partitions be designed?

**How to run**: `python experiments/exp1_storage.py --day 5`

| Variant | Meaning |
|---|---|
| `gz_tsv/full_scan` vs `parquet/full_scan` | Read all 22 columns: pure read cost |
| `gz_tsv/agg` vs `parquet/agg` | Sum clicks and cost by campaign: Parquet only needs to read 3 columns (column pruning) |
| `gz_tsv/one_day` vs `parquet/one_day` | Same as above plus `day = 5`: Parquet skips the other days' directories (partition pruning) |

**Numbers to record**: `wall_s`, `input_bytes` (bytes actually read), `n_tasks`, file sizes (`exp1_sizes.json`)

**Evidence to check**:
- `plans/exp1_parquet_one_day.txt` should contain a non-empty `PartitionFilters`
- `input_bytes` for `parquet/one_day` should be far smaller than for `parquet/agg` (whether that holds is up to the measurement)
- `n_tasks` for the gz variants: gzip can't be split, so the whole file is read by a single task

Results (paste summarize output):

File sizes:

**Explanation** (written after the results are in): what the time difference comes from: decompression and text parsing, column pruning, partition pruning, single-task reads. Which one contributes most?

---

## Experiment 2: shuffle partitions × AQE

**Question**: Why is shuffle slow? What does AQE do?

**How to run**: `python experiments/exp2_shuffle.py`

**Workload**: the full Gold job (campaign_daily + conversion_delay), reading Silver

**Grid**: `shuffle.partitions` ∈ {200, 16} × `adaptive.enabled` ∈ {false, true}

**Numbers to record**: `wall_s`, `shuffle_read_tasks`

**Evidence to check**:
- With AQE off, does `shuffle_read_tasks` match the configured partition count?
- With AQE on, how many partitions were merged? The SQL tab in the Spark UI shows `AQEShuffleRead` and the partition count after coalescing
- Use `plans/exp2_*` and the UI to confirm that the Gold job's shuffle volume is small (it is post-aggregation data, so 200 partitions means many tiny tasks)

Results:

**Explanation**:
- How much time does 200 vs 16 make with AQE off? Is it task scheduling overhead?
- With AQE on, does the gap between the two settings shrink? Why?
- In local mode tasks are threads, so scheduling overhead is smaller than on a cluster. Will this experiment be re-run on the cluster in week 2?

---

## Experiment 3: join strategy

**Question**: When should you use a broadcast join?

**How to run**: `python experiments/exp3_join.py`

**Workload**: silver.impressions (about 16.5M rows) join dim_campaign (one row per campaign) on `campaign`

| Variant | Setup | Operator that must appear in the plan |
|---|---|---|
| `broadcast` | `F.broadcast(dim)` | BroadcastHashJoin |
| `sort_merge` | `dim.hint("merge")` | SortMergeJoin |
| `auto` (reference) | No hint, default thresholds, AQE on | Whatever Spark picks |

Both main variants run with AQE off and the auto-broadcast thresholds set to -1; otherwise AQE could switch sort-merge to broadcast at runtime. If the plan doesn't match, the script raises an error.
A correctness check runs before timing: both join methods must produce the same number of rows.

**Numbers to record**: `wall_s`, `shuffle_write_bytes`, `shuffle_read_bytes`

Results:

**Explanation**: where the shuffle volume difference comes from (does the large table get shuffled?), which strategy `auto` chose and why, and when broadcast is not appropriate (the small table is too big for driver/executor memory).

---

## Experiment 4: data skew

**Question**: How do you detect skew? How do you handle it?

**How to run**: `python experiments/exp4_skew.py --partitions 200 --salt 16`

**Step 0 (untimed)**: `exp4_skew_profile.json`
- Top-1 / top-10 campaign share, max / median rows per campaign
- **Predicted** rows per shuffle partition (computed with the same hash rule Spark uses), comparing 16 and 200 partitions
  With few partitions, each one holds many campaigns and skew gets diluted, so this experiment uses 200 partitions

**Timed variants** (AQE off, broadcast off, partition count fixed):

| Variant | Description |
|---|---|
| `groupby` | Plain aggregation by campaign. Spark aggregates partially on the map side first, so skew may not show up at all |
| `groupby_salted` | Two-stage salted aggregation, as a comparison |
| `join` | Sort-merge join by campaign. No map-side merging, so every row of a top campaign lands in one task |
| `join_salted` | Large table gets salt = xxhash64(uid, timestamp) % S; the dimension table is replicated S times |
| `join_aqe_skew` | No salting; AQE skew-join optimization turned on (Spark's built-in approach) |

**Numbers to record**: `task_ms_max_over_p50` (slowest task / median), `read_records_max_over_p50` (rows read by the largest task / median), `wall_s`
Task time in local mode has a lot of noise; the row-count ratio is deterministic, so put more weight on it.

**Evidence to check**:
- Before timing, the script checks that the salted and unsalted versions return the same row count
- Whether AQE skew join actually triggered: the join node in the Spark UI SQL tab shows skew-related metrics. If it didn't trigger, write "did not trigger" and lower `--aqe-skew-threshold-mb` before re-running

Skew profile (paste key fields from exp4_skew_profile.json):

Results:

**Explanation**:
- Is the skew in `groupby` obvious? If not, explain it with map-side partial aggregation
- How much does salting the join reduce the row-count ratio and the time? What does salting cost (the dimension table grows S times, plus one extra column)?
- Compare salting with AQE skew join: which is better as a first choice?
- Why use a hash for the salt instead of `rand()` (results must be the same when a task is retried)
