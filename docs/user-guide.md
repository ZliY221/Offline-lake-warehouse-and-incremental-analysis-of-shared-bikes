# 用户使用指南

适用版本：`v0.11.2`

本指南随项目版本同步更新，覆盖当前版本的启动、增量处理、质量治理和恢复操作。

## 1. 环境要求

- Python 3.10 或更高版本
- JDK 17
- Windows 完整流水线需要 WSL；Linux 和 macOS 可直接运行 PySpark

## 2. 最快启动

仅运行可移植数据源流程：

```powershell
python scripts/bootstrap.py -- --mode source
```

运行完整 Bronze、Silver、Gold 湖仓流水线：

```powershell
python scripts/bootstrap.py -- --mode pipeline
```

主要结果位于 `build/quickstart/`，包括分层 Parquet 数据、质量报告和执行摘要。

## 3. 增量与回填

按日期执行受控回填：

```powershell
./scripts/backfill-date.ps1 -IngestionDate 2026-10-01
```

流水线使用分区覆盖、跨分区主键保护和源文件指纹，重复执行相同输入不会累积重复数据。批次清单与修复日志保存在 `build/` 对应目录。

## 4. 数据质量与修复

```powershell
./scripts/build-quality-report.ps1
./scripts/build-repair-source-catalog.ps1
```

质量报告检查分区连续性、新鲜度和分层数据约束。修复计划在执行前核对来源指纹，执行后验证目标分区并保存审计记录。

## 5. 小文件压实

先生成计划：

```powershell
./scripts/compact-lakehouse.ps1
```

确认计划后执行，并保留 `build/compaction/journal.json` 作为续跑依据。查看日志状态：

```powershell
inspect-compaction-journal build/compaction/journal.json
```

输出包含计划动作总数、完成与待处理动作数、完成百分比、已完成分区、失败动作标识、恢复状态和自动化就绪状态。再次执行相同计划时，已经成功的动作会直接复用。

需要供调度系统判定是否完成时使用：

```powershell
inspect-compaction-journal build/compaction/journal.json --require-complete
```

日志未完全成功时命令返回状态码 `2`；需要人工阅读时可附加 `--pretty` 输出缩进 JSON。

## 6. 调度与性能观察

- `scripts/run-managed-pipeline.ps1`：运行依赖图、重试和断点恢复。
- `scripts/run-airflow-dag.ps1`：运行 Airflow 编排入口。
- `scripts/explain-spark-plans.ps1`：保存物理计划对比。
- `scripts/explain-spark-stage-metrics.ps1`：采集 Job、Stage 和 Task 指标。

## 7. 常见问题

- 找不到 Java：将 `JAVA_HOME` 指向 JDK 17。
- Windows 中文路径出现 Spark UI 警告：数据计算不受影响；需要 UI 时可将仓库检出到纯英文短路径。
- WSL 流程无法启动：先执行 `./scripts/setup-wsl-tools.ps1`。
- 压实拒绝执行：分区在计划生成后发生变化，应重新生成计划，不要覆盖日志中的指纹校验。

## 8. 数据边界

样例行程和骑行者标识均为合成数据。运行产物写入 `build/`，分区替换前会进行行数与指纹核对。
