# Criteo 广告数据平台：项目方案

Oct 14, 2026 · @aurora

## 结论：可行，但它是"模拟"的分布式

可以。用 Docker Compose 在一台 Mac 上启动 1 个 Spark master 和 2 个 worker，组成 Spark Standalone 集群。PySpark 作业提交上去后，任务会真正被拆给两个 worker 容器里的 executor 执行。但所有容器共用同一台电脑的 CPU 和内存，所以**不会比 local 模式更快**。它的价值在于展示你理解集群是怎么运行的，而不是提升性能。

| 维度 | 能模拟 | 不能模拟 |
| --- | --- | --- |
| 角色分工 | driver、master、worker、executor 各自独立运行，和公司集群的结构一致 | 真实的多机器网络延迟和带宽限制 |
| 任务调度 | 任务分发到不同 worker；在 Spark UI 里能看到每个 executor 处理了哪些 task | 真正的横向扩展：加 worker 不会加算力，因为 CPU 是同一块 |
| 容错 | 运行中关掉一个 worker 容器，Spark 会把丢失的 task 重新分给剩下的 worker | 硬盘损坏、整台机器宕机等真实故障 |
| 资源配置 | executor 的核数和内存设置、shuffle 分区数调优 | TB 级数据：本项目约 1650 万行、2.4 GB，单机就能装下 |

面试时要主动说清楚这一点："这是单机上模拟的集群，用来验证调度、shuffle 和容错行为，不是为了提升性能。"能讲清楚边界，比夸大成"分布式系统"更加分。

## 项目要回答的业务问题

项目的定位是：**为广告变现团队搭建一套数据底座，让分析师每天能拿到可信的广告活动指标。** 数据来自 Criteo 公开的真实广告流量：约 1650 万次曝光、30 天、约 700 个广告活动。每个技术环节都对应下面某个问题，这样面试时讲的是"解决了什么问题"，而不是"用了哪些工具"。

| 业务问题 | 对应的产出 | 对应 TikTok JD 的哪一条 |
| --- | --- | --- |
| 每个广告活动每天的表现如何？ | 日级指标表：曝光、点击、CTR、转化、CVR、花费、CPA | data models, datasets, metrics |
| 一天的数据要等多久才算"准"？ | 转化延迟分布：多少比例的转化在曝光后 1 天、7 天、30 天内发生，据此定出报表补数窗口 | measurement, data quality, latency |
| 此刻哪些广告活动异常？ | 每 5 分钟更新一次的实时指标，带简单告警阈值 | real-time pipelines, Kafka |
| 转化归功给哪一次点击？ | 归因分析：归因转化占比、转化前点击次数和位置的分布 | ads measurement |
| 花费是否集中在少数活动？ | 花费集中度分析，同时是数据倾斜实验的素材 | business analytics, performance |

注意：数据里的 cost 和 cpo 是 Criteo 做过变换的，不是真实价格。报表和简历里都要写明"相对花费"。

## 架构

&#91;embedded content: 项目架构 · 7 个容器与一个共享数据卷\]

master 只负责登记 worker 和分配资源；分配好之后，driver 把 task 直接发给两个 worker 上的 executor。所有容器通过同一个数据卷读写文件。Kafka 和 Airflow 分别在阶段 3 和阶段 4 才加入。

## 技术选型与理由

核心原则：**所有 Spark 进程（driver 和 executor）都用同一个官方镜像，版本写死。** 分布式 PySpark 最常见的报错，就是 driver 和 executor 的 Spark 版本或 Python 版本不一致。

| 组件 | 选择 | 理由 |
| --- | --- | --- |
| Spark 镜像 | 官方 `apache/spark:4.1.3-python3`（写死完整版本号） | Apache 官方维护。不要用网上教程常用的 Bitnami 镜像：2025 年 8 月起它已移到不再更新的 legacy 仓库 |
| 集群模式 | Spark Standalone：1 master + 2 worker | 最简单的真实集群模式，不需要 YARN 或 Kubernetes |
| Driver | 单独一个 Jupyter 容器，从同一个 Spark 镜像构建 | 保证和 executor 版本完全一致。不建议从 Mac 本机直接连容器里的集群：网络配置麻烦，而且你本机是 PySpark 4.2.0，和镜像版本不同 |
| 共享存储 | 一个挂载到所有容器、路径相同的数据卷（如 `/opt/data`） | 每个 executor 都要能读写同一份文件，这是集群模式下最容易忽略的一点 |
| 存储格式 | Parquet，按天分区；最后一周可选升级为 Delta Lake 或 Iceberg | 先把基础做扎实。表格式的版本必须和 Spark 版本匹配，用之前先查兼容表 |
| 实时流 | 官方 `apache/kafka` 镜像（KRaft 模式，不需要 ZooKeeper）+ Spark Structured Streaming | 对应 JD 里的 real-time pipelines 和 Kafka |
| 编排 | Airflow 调度每日批处理 | JD 点名的工具。但官方 Compose 配置要占用好几 GB 内存，所以和 Kafka 分开启动 |
| 建模 | Gold 层用 Spark SQL；可选用 dbt-duckdb 在 Gold Parquet 上加测试 | dbt 直接连 Spark 需要额外部署 Thrift Server，对这个项目来说投入产出比不高 |
| 诊断 | Spark History Server + 每个作业自己的日志 | 实验结束后还能回看各个 stage 的耗时，简历上的数字从这里来 |

开工前先运行 `docker run --rm apache/spark:4.1.3-python3 python3 --version`，确认镜像里的 Python 版本。Driver 容器从同一个镜像构建，Python 版本自然就一致。版本信息查自 [Docker Hub apache/spark](https://hub.docker.com/r/apache/spark/tags) 和 [Northflank 关于 Bitnami 的说明](https://northflank.com/blog/bitnami-deprecates-free-images-migration-steps-and-alternatives)。

## 数据模型

三层、六张表。Bronze 保留原始数据，Silver 负责清洗，Gold 是分析师直接使用的表。每一层都可以从上一层重新生成，任何一步的规则改了，不用从头下载。

| 层 | 表 | 粒度（一行代表什么） | 主要处理 |
| --- | --- | --- | --- |
| Bronze | `bronze.impressions` | 一次曝光，原样保留 | 显式 schema 读入，加 `day`、`ingested_at`，按天分区存 Parquet |
| Silver | `silver.impressions` | 一次曝光，已清洗 | 去重；`-1` 转为 null；算出转化延迟（转化时间 − 曝光时间）；数据质量检查不通过的行单独存入隔离表 |
| Silver | `silver.dim_campaign` | 一个广告活动 | 首次和最后一次出现的时间、总曝光量、花费等级；用于 broadcast join 实验 |
| Gold | `gold.campaign_daily` | 一个活动 × 一天 | 曝光、点击、转化、归因转化、相对花费、CTR、CVR、CPA |
| Gold | `gold.conversion_delay` | 一个活动 × 一个延迟区间 | 曝光后 0–1、1–3、3–7、7–30 天内发生的转化占比 |
| Gold（实时） | `gold.campaign_5min` | 一个活动 × 5 分钟窗口 | 由 Structured Streaming 从 Kafka 写入，带 watermark 处理迟到数据 |

数据质量规则先定这五条：主键不重复；转化时间不早于曝光时间；`click` 和 `conversion` 只能是 0 或 1；花费不为负；每天行数与前 7 天均值的偏差不超过阈值。

待确认：曝光的主键。数据里没有曝光 ID，需要先检查 (timestamp, uid, campaign) 是否唯一。

## 实验设计：简历上的数字从这里来

这是这个项目和网上教程项目拉开差距的地方：**每个实验只改一个变量，每组跑 3 次取中位数，结果记录在 `experiments.md` 里。** 结果是什么就写什么，包括"没有变快"这类结果，只要能解释原因，在面试里一样是加分项。

| # | 实验 | 对比什么 | 要记录的数字 | 能应对的面试题 |
| --- | --- | --- | --- | --- |
| 1 | 存储格式 | gz 文本 vs Parquet；全表查询 vs 只查一天（分区裁剪） | 读取耗时、文件大小 | 为什么用 Parquet？分区怎么设计？ |
| 2 | Shuffle 分区数 | 200 vs 16；AQE 开 vs 关 | Gold 作业耗时、task 数量 | shuffle 为什么慢？AQE 做了什么？ |
| 3 | Join 策略 | 广告活动维表用 broadcast join vs sort-merge join | 耗时、shuffle 读写量（在 Spark UI 里看） | 什么时候用 broadcast join？ |
| 4 | 数据倾斜 | 按广告活动聚合；头部活动的数据量远大于其他活动；加盐前 vs 后 | 最慢 task 耗时与中位数的比值 | 倾斜怎么发现？怎么处理？ |
| 5 | Executor 配置 | 1 worker vs 2 worker；每个 executor 1 核 vs 2 核 | 耗时。预期提升很小，因为 CPU 是同一块，要解释原因 | executor 怎么定核数和内存？ |
| 6 | 容错 | 作业运行中 `docker stop` 掉一个 worker | 作业是否完成、重试了多少个 task、多花了多少时间 | Spark 怎么容错？什么是 lineage？ |
| 7 | 实时延迟 | Kafka 回放速度（每秒事件数）vs 端到端延迟；人为制造迟到数据 | 端到端延迟、被 watermark 丢弃的行数 | watermark 解决什么问题？exactly-once 怎么保证？ |
| 8 | 数据质量 | 在一天的数据里注入重复行和时间倒置的行 | 被拦下、隔离的行数 | 你怎么保证数据质量？ |

实验 1–4 在 local 模式下就能做，实验 5–6 必须在 Docker 集群上做，这也是搭集群最直接的理由。

## 分阶段计划（约 4 周）

先在 local 模式下把批处理跑通，再搬到 Docker 集群上。这样出了问题，能分清是代码的问题还是集群配置的问题。每个阶段结束时都要有一个能运行、能提交到 GitHub 的状态。

**阶段 1（第 1 周）：local 模式跑通批处理** — 完成标准：一条命令从原始文件生成全部 Gold 表

- [ ] Bronze：显式 schema 读入，存成 Parquet（先用最简单的版本）
- [ ] 用 Spark 做几个熟悉的 pandas 操作：筛选、分组求和、算 CTR
- [ ] Silver：清洗、去重、转化延迟字段、五条数据质量检查
- [ ] Gold：`campaign_daily` 和 `conversion_delay`
- [ ] 把 notebook 里的代码整理成 `jobs/bronze.py`、`jobs/silver.py`、`jobs/gold.py` 三个脚本
- [ ] 完成实验 1–4，结果记入 `experiments.md`

**阶段 2（第 2 周）：Docker Compose 集群** — 完成标准：`docker compose up` 后，用 `spark-submit` 在集群上跑通同样三个脚本

- [ ] 编写 Compose 文件：master、两个 worker、driver（Jupyter）、History Server，以及共享数据卷
- [ ] 在 master 的网页界面（端口 8080）确认两个 worker 都已注册
- [ ] 三个脚本通过 `spark-submit --master spark://spark-master:7077` 跑通
- [ ] 完成实验 5–6

**阶段 3（第 3 周）：实时流** — 完成标准：回放脚本运行时，`campaign_5min` 表持续更新

- [ ] 在 Compose 里加入 Kafka
- [ ] 编写 producer：按 timestamp 顺序把曝光事件写入 Kafka，支持加速回放
- [ ] Structured Streaming：5 分钟窗口聚合、watermark、checkpoint
- [ ] 完成实验 7

**阶段 4（第 4 周）：编排和交付** — 完成标准：面试官 clone 仓库后，按 README 能复现全部结果

- [ ] Airflow DAG：bronze → silver → 数据质量检查 → gold，支持按天补跑
- [ ] 完成实验 8
- [ ] 指标看板：每天各广告活动的表现、转化延迟曲线
- [ ] README：架构图、一键启动步骤、实验结果表、已知局限
- [ ] 更新简历上的项目描述

时间不够时，优先保证阶段 1 和 2 做扎实，阶段 4 的看板可以省掉。阶段 1 做完就能写进简历、开始投递，不必等全部完成。

## 简历 bullet 草稿

方括号里的数字必须等实验测出来再填。没做完的部分就不写，不要提前写上。

**Ads Monetization Data Platform (Criteo, 16.5M impressions)** · PySpark, Kafka, Airflow, Docker

- Built a medallion (bronze/silver/gold) batch pipeline in PySpark over 16.5M ad impressions, producing campaign-day CTR/CVR/CPA and conversion-delay tables; day-partitioned Parquet cut full-scan time from \[X\] s to \[Y\] s.
- Ran the pipeline on a Dockerized Spark standalone cluster (1 master, 2 workers) and benchmarked shuffle partitions, broadcast joins and salting for skewed campaigns, reducing the gold job from \[X\] s to \[Y\] s.
- Verified fault tolerance by killing a worker mid-job; Spark recomputed \[N\] lost tasks and the job completed with \[X\]% extra runtime.
- Streamed replayed impressions through Kafka into Spark Structured Streaming 5-minute campaign metrics with watermarking, reaching \[X\] s end-to-end latency at \[N\] events/s.
- Orchestrated daily runs and backfills in Airflow with 5 data-quality checks that quarantined \[N\] injected bad rows.

简历上只放 3–4 条：挑数字最有说服力的那几条。剩下的留着在面试里讲。

## 风险与对策

| 风险 | 对策 |
| --- | --- |
| Mac 内存不够：Spark 集群、Kafka、Airflow 同时运行会占用大量内存 | 用 Compose 的 profiles 分组启动（批处理 / 实时 / 编排），不同时全开。在 Docker Desktop 设置里确认分配给 Docker 的内存上限 |
| Driver 与 executor 版本不一致，报 Python 版本不匹配或序列化错误 | driver 容器和 worker 容器从同一个镜像构建，版本号写死 |
| Executor 找不到文件 | 数据卷在所有容器里挂载到同一路径；代码里只用这个路径，不用 Mac 本机路径 |
| 网上教程大多用 Bitnami 镜像，照着做会拉不到镜像或拿到过时版本 | 只用 `apache/spark` 和 `apache/kafka` 官方镜像 |
| 范围越做越大，迷失在工具配置里 | 严格按阶段推进；每个新工具只在它对应的阶段引入；卡住超过半天就先跳过 |
| 面试时被问住"这不就是单机吗" | 主动说明模拟的边界（见第一节），把重点放在实验结论和你对原理的理解上 |
