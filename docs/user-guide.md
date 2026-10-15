# 用户使用指南

适用版本：`v1.3.0`

本指南随项目版本同步更新，覆盖当前版本的启动、增量处理、质量治理和恢复操作。首次使用建议先阅读仓库首页的项目简介、数据架构和常用入口，再按本指南选择运行模式。

## 1. 环境要求

- Python 3.10 或更高版本
- JDK 17
- Windows 完整流水线需要 WSL；Linux 和 macOS 可直接运行 PySpark

Source、Pipeline 和 Airflow 环境相互独立，按当前工作内容安装即可。选择方法与检查命令见[运行环境分层](runtime-environments.md)，启动器使用 UTF-8 子进程输出适配中文路径。

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

对比已保存的基线报告与本次候选报告：

```powershell
python -m bike_lakehouse.quality_compare_cli --baseline build/quality/baseline.json --candidate build/quality/report.json --output build/quality/comparison.json --fail-on-regression
```

对比结果列出每项检查的原状态、当前状态和变化类型；候选报告将原本正常的检查变为失败或缺失时，`--fail-on-regression` 返回状态码 `1`。恢复项单独记录，不会触发回退门禁。

将质量报告转换为按优先级排列的修复建议：

```powershell
build-quality-actions --report build/reports/data-quality.json --output build/reports/quality-actions.json
```

输出会保留失败检查的证据字段，并给出 `P0` 至 `P2` 的处置顺序。发布流程可附加 `--require-ready`；仍有待处理动作时命令返回状态码 `2`。

生成可离线打开的运行控制中心：

```powershell
build-lakehouse-operations --quality-report build/reports/data-quality.json --orchestration-state build/control/orchestration-run.json --output-json build/operations/snapshot.json --output-html build/operations/index.html
```

控制中心将任务状态、依赖、尝试次数、质量检查、分层数据规模和修复队列汇总到一个 JSON 快照与独立 HTML 页面。无需启动服务即可查看；加入 `--require-ready` 后可直接作为离线发布门禁。

将多个运行快照汇总为历史看板：

```shell
build-lakehouse-history build/operations/run-*/snapshot.json --recent-window 5 --readiness-threshold 0.8 --output-json build/history/history.json --output-html build/history/index.html
```

历史页面按批次展示发布状态、流水线状态、质量状态、失败检查和待处置动作，并同时计算整体就绪率、近期就绪率及当前状态连续批次数。`--recent-window` 可设置 1 至 50 次观察窗口，`--readiness-threshold` 可设置 0 至 1 的健康阈值；趋势状态会输出为 `HEALTHY`、`AT_RISK` 或 `NO_DATA`。它只读取快照文件，适合在离线环境中归档或交付。

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

## 7. 工程验证

```powershell
./scripts/run-project-acceptance.ps1
```

验收入口依次构建 Bronze、Silver、维表和 Gold 数据，检查质量报告与 Spark 物理计划，再执行 Python、Spark 和仓库发布内容检查。仅需重新执行自动化检查时可以运行 `./scripts/test-all.ps1`。

## 8. 常见问题

- 找不到 Java：将 `JAVA_HOME` 指向 JDK 17。
- Windows 中文路径出现 Spark UI 警告：数据计算不受影响；需要 UI 时可将仓库检出到纯英文短路径。
- WSL 流程无法启动：先执行 `./scripts/setup-wsl-tools.ps1`。
- 压实拒绝执行：分区在计划生成后发生变化，应重新生成计划，不要覆盖日志中的指纹校验。

## 9. 数据边界

样例行程和骑行者标识均为合成数据。运行产物写入 `build/`，分区替换前会进行行数与指纹核对。
