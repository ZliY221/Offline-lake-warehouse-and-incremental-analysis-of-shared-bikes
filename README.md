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
- [ ] 实现 Gold 日指标、热门路线和留存分析。
- [ ] 加入数据质量报告、增量回填与性能证据。

## 为什么单独建仓

电商项目证明实时事件时间、状态、异常侧流和批流对账；本项目专注批处理与离线数仓：

| 项目 | 主要证据 |
| --- | --- |
| 电商实时数据仓库 | Kafka、Flink、ClickHouse、Watermark、状态去重、至少一次写入 |
| 共享单车离线湖仓 | Spark DataFrame/SQL、Parquet 分区、增量覆盖、SCD2、离线质量与回填 |

## 计划数据流

```mermaid
flowchart LR
    A[确定性行程与站点快照] --> B[Bronze 原始 Parquet]
    B --> C[Silver 校验 去重 标准化]
    A --> D[Silver 站点 SCD2]
    C --> E[Gold 日指标与热门路线]
    D --> E
    E --> F[SQL 验收与数据质量报告]
```

## 当前目录

```text
bike-trip-lakehouse/
├─ .github/workflows/        持续集成
├─ data/sample/              固定脱敏正常样例
├─ data/quality/             固定质量问题演示批次
├─ docs/                     需求、架构和学习材料
├─ scripts/                  环境探测、生成和测试脚本
├─ src/bike_lakehouse/       生成器、契约与 Spark Bronze 装载
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

默认生成 20 条 `2026-10-01` 行程和一份站点快照。相同日期、数量和随机种子产生相同内容；数据不包含姓名、电话、定位轨迹或真实卡号。

## Bronze 装载

```powershell
./scripts/build-bronze.ps1
```

输出位于被 Git 忽略的 `build/lakehouse/bronze/trips/`，按 `ingestion_date=YYYY-MM-DD` 分区。脚本使用动态分区覆盖：重跑某日只替换该日分区，不删除其他日期。

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

## 自动化验证

```powershell
./scripts/test-all.ps1
```

当前测试覆盖确定性、契约、金额/时间边界、隐私字段、显式 Spark Schema、Parquet 类型、同分区幂等重跑、跨日期分区保留、8 类质量失败、精确/冲突重复、派生字段、空输出 Schema、Silver 完整重建幂等，以及站点 SCD2 连续有效期、当前版本、重复与冲突快照处理。

## 当前边界

- Spark 当前只在单机 `local[2]` 模式运行，不能表述为生产集群经验。
- Windows 原生 Spark 仅做过 DataFrame 聚合验证；涉及 Hadoop 文件系统的 Parquet 测试和构建统一在 WSL/Linux 执行，CI 也使用 Linux。
- Bronze、行程 Silver 与站点 SCD2 已完成；Gold 指标、批次清单和性能证据尚未完成。
- 没有脚本和原始报告前，不写吞吐、延迟或节省比例。
