# 共享单车离线湖仓与增量分析

这是面向数据开发、数仓开发和大数据开发实习岗位的第二个独立作品集项目。它使用 PySpark 本地模式和 Parquet 演练批处理数仓，重点展示显式数据契约、Bronze/Silver/Gold 分层、分区增量处理、维度历史、数据质量与 SQL，而不是重复旗舰项目中的 Kafka/Flink 实时链路。

本项目是根据学习目标重新实现的新项目，不是对旧课程源码的恢复，也不宣称生产集群经验。

## 当前进度

- [x] 在 Windows 11 + WSL、Python 3.14、JDK 17 环境真实启动 Spark 4.2.0 `local[2]`，完成 DataFrame 聚合与 Parquet 往返读写。
- [x] 定义脱敏共享单车行程与站点快照契约。
- [x] 实现固定随机种子的确定性测试数据生成器。
- [x] 使用显式 Spark Schema 读取 NDJSON，不依赖自动推断业务类型。
- [x] 将 Bronze 行程写为按 `ingestion_date` 分区的 Parquet。
- [x] 使用动态分区覆盖实现同批次安全重跑，并保留未触及日期分区。
- [x] 实现 Silver 合法性校验、最小披露拒绝数据、精确/冲突去重与派生字段。
- [x] 实现站点快照校验、冲突隔离与 SCD2 历史维度。
- [x] 实现 Gold 日指标、SCD2 时态关联和分区 Top 路线。
- [x] 为 Bronze 摄取加入确定性批次 ID、文件指纹和原子状态清单。
- [x] 实现单日期预检、分区替换和 Silver/Gold 重建的受控回填。
- [x] 实现跨层行数、SCD2 区间、Gold 汇总和路线排名质量报告。
- [x] 完成固定输入、预热加三轮测量的本机端到端性能证据。
- [x] 使用纯合成匿名骑行者键实现 cohort 留存分析。

## 为什么单独建仓

电商项目证明实时事件时间、状态、异常侧流和批流对账；本项目专注批处理与离线数仓：

| 项目 | 主要证据 |
| --- | --- |
| 电商实时数据仓库 | Kafka、Flink、ClickHouse、Watermark、状态去重、至少一次写入 |
| 共享单车离线湖仓 | Spark DataFrame/SQL、Parquet 分区、增量覆盖、SCD2、离线质量与回填 |

## 实现数据流

```mermaid
flowchart LR
    A[确定性行程与站点快照] --> B[Bronze 原始 Parquet]
    B --> C[Silver 校验 去重 标准化]
    A --> D[Silver 站点 SCD2]
    C --> E[Gold 日指标 热门路线 Cohort留存]
    D --> E
    E --> F[跨层数据质量报告]
    B --> G[批次清单与受控回填]
    G --> F
```

## 当前目录

```text
bike-trip-lakehouse/
├─ .github/workflows/        持续集成
├─ data/sample/              固定脱敏正常样例
├─ data/quality/             固定质量问题演示批次
├─ docs/                     需求、架构和学习材料
├─ evidence/                 经核验的原始性能证据
├─ scripts/                  环境探测、生成和测试脚本
├─ src/bike_lakehouse/       分层作业、控制流程、质量与基准代码
├─ tests/                    Python 与本地 Spark 测试
├─ pyproject.toml
└─ README.md
```

## 环境

- Python 3.10 或更高版本；
- JDK 17 或更高版本；
- PySpark 4.2.0。

安装依赖：

```powershell
python -m pip install -e .
```

Windows 本机的 Hadoop 文件系统实现写 Parquet 需要额外的 `winutils.exe`。本项目不下载来源不明的二进制文件，而是在 WSL 中运行 Spark；项目脚本使用微软官方免安装 JDK 17，只写入被 Git 忽略的 `build/tools/`，不修改系统 Java。

首次准备：

```powershell
python -m pip install -e .
./scripts/setup-wsl-tools.ps1
```

默认 WSL 发行版名为 `Ubuntu`；如名称不同，可通过 `BIKE_LAKEHOUSE_WSL_DISTRO` 环境变量指定。第一次准备会下载约 183 MB 的官方 JDK，之后测试和构建会复用本地文件。

## 生成固定样例

```powershell
./scripts/generate-sample.ps1
```

默认生成 20 条 `2026-10-01` 行程和一份站点快照。相同日期、数量和随机种子产生相同内容；`rider_key` 只由固定种子和 UUID5 生成，不来源于或映射任何真人，数据不包含姓名、电话、定位轨迹或真实卡号。

## Bronze 装载

```powershell
./scripts/build-bronze.ps1
```

输出位于被 Git 忽略的 `build/lakehouse/bronze/trips/`，按 `ingestion_date=YYYY-MM-DD` 分区。脚本使用动态分区覆盖：重跑某日只替换该日分区，不删除其他日期。

每次摄取还会在 `build/lakehouse/control/bronze_batches/` 写入原子 JSON 批次清单。批次 ID 由摄取日期与来源文件 SHA-256 确定；同批重跑更新同一记录并增加 `attempt_count`，不会制造重复清单。记录包含输入字节数、输入/损坏/输出行数、UTC 起止时间，以及 `RUNNING`、`SUCCEEDED` 或 `FAILED` 状态。

## Silver 质量与去重

```powershell
./scripts/build-silver.ps1
```

Silver 将完整重建三个数据集：

- `trips_valid`：通过规则、去重后的行程，并派生业务日期和骑行分钟数；
- `trips_rejected`：业务或结构失败记录，损坏原文只保留 SHA-256 指纹和字节数；
- `trips_duplicates`：区分完全相同的重复投递与同 ID 不同内容的主键冲突。

质量问题演示：

```powershell
./scripts/silver-quality-demo.ps1
```

固定 12 行输入会产生 1 条合法记录、8 条拒绝记录、1 条精确重复和 2 条冲突重复。

## 站点 SCD2 维度

```powershell
./scripts/build-station-dimension.ps1
```

作业读取多日全量站点快照，先校验业务字段，再区分完全重复与同站点同日期的冲突记录。站点名称、行政区、容量或启用状态变化时才生成新版本；有效期采用左闭右开区间 `[valid_from, valid_to)`，当前版本的 `valid_to` 为 `null`。

固定两日样例共 16 条合法快照，生成 11 个维度版本和 8 个当前版本。`ST-003` 容量、`ST-005` 名称和 `ST-008` 启用状态发生变化，可用于手工核对版本切换。输出位于 `build/lakehouse/dim/`。

## Gold 日指标与热门路线

```powershell
./scripts/build-gold.ps1
```

Gold 先按行程业务日期分别关联起点、终点在当日有效的 SCD2 站点版本，再生成三个数据集：

- `daily_metrics`：按日期、起点行政区、骑行者类型和车辆类型统计行程数、总/平均时长与总距离；
- `popular_routes`：按日期和起点行政区对路线计数，使用确定性排序保留 Top 3。
- `cohort_retention`：按匿名骑行者首次骑行日分 cohort，统计每个活动日的 cohort 规模、留存人数和四位小数留存率。

若任一起终站点无法匹配当日有效版本，作业直接失败而不是静默丢弃行程。固定 20 条正常样例会生成 10 行日指标、9 行热门路线和 1 行第 0 天 cohort 留存；5 条跨日测试样例的聚合与留存值可手工核对，并验证站点跨日变更后使用正确历史属性。

## 受控日期回填

```powershell
./scripts/backfill-date.ps1 `
    -InputPath data/sample/trips.ndjson `
    -TargetDate 2026-10-01
```

回填先确认输入中所有可解析的业务日期都等于目标日期，再只动态覆盖对应 Bronze 分区；当前 Silver 与 Gold 仍采用完整重建，因此控制记录会明确写出 `FULL_DATASET`，不把它包装成尚未实现的下游分区增量。成功报告包含各层行数，失败也会保留错误状态。

## 跨层数据质量报告

```powershell
./scripts/build-quality-report.ps1
```

报告写入 `build/reports/data-quality.json`，检查 Bronze 行数能否由 Silver 合法、拒绝和重复数据完整对账，每个站点是否只有一个当前版本、SCD2 区间是否连续，Gold 聚合行程数是否等于合法 Silver 行程数，Top 路线排名是否满足范围和唯一性约束，以及 cohort 留存率、人数和第 0 天基线是否合法。当前正式样例 6 项检查全部为 `PASS`。

## 本机性能证据

```powershell
./scripts/run-benchmark.ps1
```

固定 10,000 行、2,957,025 字节 NDJSON，在 WSL2、Spark 4.2.0、JDK 17、Python 3.14.4、`local[2]`、约 7.35 GiB 可用内存环境中，复用单个 Spark 会话，先预热 1 轮再测量 3 轮。端到端完整执行 Bronze 覆盖、Silver 全量重建，以及包含日指标、路线和 cohort 留存的 Gold 全量重建，中位耗时 19.995 秒，即 500.13 输入行/秒；Spark/JVM 启动和测试数据生成不计入。

三轮端到端原始耗时为 21.782、19.995、19.906 秒；阶段中位数为 Bronze 1.883 秒、Silver 5.931 秒、Gold 12.508 秒。每轮都输出 10,000 行合法 Silver、108 行日指标、81 行热门路线和 45 行 cohort 留存。完整环境、输入 SHA-256、方法和逐轮结果见 [`evidence/benchmark-10000-local.json`](evidence/benchmark-10000-local.json)。这些数字只代表该本机基准，不外推为生产集群能力。

## 自动化验证

```powershell
./scripts/test-all.ps1
```

当前测试覆盖确定性、契约、金额/时间边界、匿名键格式与隐私字段、显式 Spark Schema、Parquet 类型、同分区幂等重跑、跨日期分区保留、确定性批次清单、状态与失败记录、单日期回填预检及下游重建、质量失败、精确/冲突重复、派生字段、空输出 Schema、Silver 完整重建幂等、站点 SCD2 连续有效期与冲突处理，以及 Gold 时态关联、手工指标核对、路线排名、cohort 留存和维度缺失保护。

## 一键作品集验收

```powershell
./scripts/run-portfolio-demo.ps1
```

脚本使用隔离的 `build/portfolio-demo/` 输出目录，依次重建所有数据层、执行 6 项跨层质量门禁并运行全部测试，最终输出机器可读摘要。架构、数据粒度和约束见 [`docs/architecture.md`](docs/architecture.md)，面试讲解与追问准备见 [`docs/interview-guide.md`](docs/interview-guide.md)。

## 当前边界

- Spark 当前只在单机 `local[2]` 模式运行，不能表述为生产集群经验。
- Windows 原生 Spark 仅做过 DataFrame 聚合验证；涉及 Hadoop 文件系统的 Parquet 测试和构建统一在 WSL/Linux 执行，CI 也使用 Linux。
- Bronze、批次清单、受控回填、行程 Silver、站点 SCD2、Gold 日指标、热门路线、cohort 留存、跨层质量报告和性能证据均已完成。当前保留全量 Silver/Gold 重建边界，不把本机演示包装成生产集群或增量湖仓经验。
- 没有脚本和原始报告前，不写吞吐、延迟或节省比例。
