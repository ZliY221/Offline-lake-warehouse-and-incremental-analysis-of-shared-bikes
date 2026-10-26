# 用户使用指南

适用版本：`v4.5.0`

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

`assess_lineage_risk` 会把影响预演转换为 Bronze、Silver 与三类 Gold 产出的血缘范围，并按待处理行数估算完成时间。超过配置 SLA 时返回 `HIGH`，便于在审批前调整窗口或容量。

正式执行还可使用 `create_approval` 生成与计划编号和来源指纹绑定的复核记录，并在执行前通过 `validate_approval` 检查变更与有效期。计划或来源发生变化后必须重新复核。

回填来源接入前可调用 `evaluate_trip_contract` 对字段名与类型执行兼容性检查。缺少必填字段或改变核心字段类型会阻止接入；新增字段会被记录但不阻塞现有流水线，便于先评估再升级数据契约。

正式回填前先执行只读影响预演：

```powershell
plan-trip-backfill --input data/trips.ndjson --target-date 2026-10-01 --bronze build/lakehouse/bronze/trips --silver-valid build/lakehouse/silver/trips_valid --output build/backfill/impact-plan.json
```

预演报告列出来源行数、去重后的行程与骑行者数量、目标 Bronze/Silver 现有行数、扫描文件数量以及潜在受影响的 cohort 日期。预演会执行日期和跨分区主键检查，但不会创建运行清单或改写任何湖仓分区；输出文件仅用于人工复核。

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

控制中心将任务状态、依赖、尝试次数、质量检查、分层数据规模和修复队列汇总到一个 JSON 快照与独立 HTML 页面。无需启动服务即可查看；加入 `--require-ready` 后可直接作为离线发布门禁。v2.9.0 页面采用离线批次工作台布局：顶部先给出发布结论和关键指标，任务表与分层路线并列展示，数据规模和质量处置放在后续区域；桌面端使用侧栏定位，移动端自动收起次要入口。

将多个运行快照汇总为历史看板：

```shell
build-lakehouse-history build/operations/run-*/snapshot.json --recent-window 5 --readiness-threshold 0.8 --output-json build/history/history.json --output-html build/history/index.html
```

历史摘要会将“失败检查数 × 2 + 待处置动作数”计算为质量债务分，并展示最新值、近期平均值、历史峰值和相邻批次环比。该指标用于快速比较修复压力，不替代明细检查；环比为正时应继续查看失败检查和处置动作来源。建议动作同时标记 P0 至 P3 优先级：P0 应在 1 小时内复核，P1 应在 4 小时内处理，P2 在 24 小时内跟进，P3 在 72 小时内复核；P0/P1 还会标记为需要升级值守负责人。`recovery_work_order` 会把最新批次、建议动作、优先级和复核要求整理成稳定编号的离线恢复工作单，便于跨班次交接；`review_due_at_utc` 是按批次生成时间自动计算的复核截止时间，`review_status` 和 `overdue_by_hours` 用于判断是否逾期以及逾期时长。`owner_route` 会按优先级分配平台负责人、质量值守或批处理操作员，`handoff_summary` 可直接用于换班交接。`age_hours` 与 `age_status` 用于识别已经开启超过 24 小时的老化工作单，`sla_status` 进一步区分 `ON_TRACK`、`DUE_SOON`、`BREACHED` 和 `NOT_APPLICABLE`。`handoff_checklist` 给出证据复核、负责人通知和结果登记步骤；运行历史命令可重复使用 `--completed-handoff-step` 登记完成项，`handoff_progress` 会返回完成率和剩余步骤。`handoff_readiness` 综合数据、SLA 和清单进度返回 `AWAITING_DATA`、`ACTION_REQUIRED`、`IN_PROGRESS` 或 `READY`，具体阻塞项位于 `handoff_blockers`。

分数为 0 的批次归为 `CLEAN`，1 至 4 分归为 `MANAGEABLE`，5 分及以上归为 `HIGH`。看板会单独统计近期与历史高债务批次数，便于判断风险是否集中在最近窗口。

“建议动作”按固定优先级输出：高债务先检查质量问题，未发布就绪时先清除门禁，低债务时完成剩余动作，近期就绪率不足时继续观察；其余情况可以继续发布。无数据时输出 `NO_DATA`。

历史页面按批次展示发布状态、流水线状态、质量状态、失败检查和待处置动作，并同时计算整体就绪率、近期就绪率及当前状态连续批次数。稳定性卡片会统计历史最长连续就绪批次数和最长连续需关注批次数，用于区分偶发失败与持续性风险；没有运行记录时两项均为零。最新批次还会与上一批次比较失败检查、处置动作和分区数量；正数表示增加，负数表示减少，只有一个批次时显示“暂无上一批次”。系统结合发布就绪变化、失败检查和处置动作增减，给出 `REGRESSED`、`IMPROVED`、`MIXED`、`STABLE` 或 `NO_BASELINE` 判定，并列出标准化退化原因。输入快照中的就绪标识必须是布尔值，三类数量必须为非负整数，非法快照会在汇总前直接拒绝。`--recent-window` 可设置 1 至 50 次观察窗口，`--readiness-threshold` 可设置 0 至 1 的健康阈值；趋势状态会输出为 `HEALTHY`、`AT_RISK` 或 `NO_DATA`。它只读取快照文件，适合在离线环境中归档或交付。

## 5. 小文件压实

生成计划后可调用 `advise_compaction` 查看当前与目标文件数、可减少文件数和预计节省的文件打开开销。没有可合并文件时返回 `KEEP`，避免为无收益的分区启动压实作业。

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

## 10. 数据产品发布

使用 `build_release` 将数据集名称、交付分区、行数、Schema 版本和来源指纹固化为稳定发布编号。只有质量门禁通过的清单状态为 `READY`；`BLOCKED` 清单不得交付下游。相同内容重复生成会得到相同编号，可用于核对离线交付物。

使用 `build_restore_point` 为已发布数据生成恢复点。恢复点保留原发布编号、分区与来源指纹；只有 `READY` 发布生成的恢复点可执行，避免从未通过质量门禁的候选数据回滚。

使用 `retention_decision` 评估历史发布。达到保留天数且高于最低恢复版本数时返回 `ARCHIVE`；最低数量内的旧版本仍返回 `KEEP_MINIMUM`，防止清理后失去基本恢复能力。

使用 `release_audit` 汇总发布组合。结果包含就绪与阻断数量、覆盖数据集和重复发布编号；存在阻断版本或编号重复时状态为 `REVIEW`，交付前应先核对来源和质量证据。

使用 `compare_releases` 比较相邻发布。结果列出新增和移除分区、行数变化及 Schema 是否变化；移除分区或修改 Schema 时必须复核后再交付。

使用 `recovery_drill` 对恢复点做只读演练。只有恢复点可执行、分区完全一致且来源指纹核对通过时结果才为 `PASS`，否则列出缺失或意外分区供复核。

使用 `delivery_sla` 记录发布就绪、约定截止和实际交付时间。结果区分待交付、按时交付和延迟交付，并给出延迟秒数，便于离线批次复盘。

使用 `release_handoff` 生成数据负责人、消费方、证据链接和固定交接步骤。质量未就绪或没有交付证据时状态为 `BLOCKED`，补齐阻塞项后再确认交付。

v3.8.0 可使用 `ConsumerContract` 为报表、财务或分析应用分别登记必需字段、最低历史天数和最大允许延迟，再调用 `consumer_contract_readiness` 校验候选发布。结果按消费方列出缺失字段与阻断原因；只有所有已登记消费方均满足要求时，组合状态才为 `READY`。该检查关注下游能否真实使用数据，不替代发布质量门禁。

v3.9.0 可在提交 Spark 作业前调用 `plan_compute_budget`，传入输入数据量、执行器数量、预计运行分钟数、执行器小时成本、预算上限和 Shuffle 放大倍数。结果给出预计费用、预算利用率、单位数据成本和预算内最大运行时间。`OVER_BUDGET` 时应先缩小批次、减少执行器或优化 Shuffle；该计划只用于执行前预算，不代替实际集群计费数据。

v3.10.0 可使用 `plan_partition_retention` 对离线分区生成保留计划。输入分区年龄、个人数据标记和法务保留状态后，结果会区分 `KEEP_ONLINE`、`ARCHIVE`、`ANONYMIZE_REVIEW` 与 `HOLD`。最近分区始终按最低在线数量保护，法务保留优先于归档；该能力只生成审核建议，不直接删除、移动或匿名化数据。

## 11. v4.0.0 多数据集发布编排

使用 `DatasetReleaseCandidate` 描述每个数据集的上游依赖、质量状态、消费方契约状态、业务优先级和预计计算成本，再调用 `plan_portfolio_release`。系统会先满足依赖拓扑，再按优先级和成本生成发布顺序；门禁未通过、依赖不可用或超出组合预算的数据集会进入 `blocked` 并给出原因。`READY` 表示整个组合可发布，`PARTIAL` 表示只能发布安全子集，`BLOCKED` 表示没有可执行数据集。该流程只生成计划，不会直接发布或移动数据。

## 12. v4.1.0 Schema 变更演练

使用 `DatasetSchemaChange` 登记每个数据集当前字段、拟议字段和数据集依赖，再用 `SchemaConsumerContract` 登记各消费方的必需字段，最后调用 `rehearse_schema_changes`。结果逐数据集返回新增字段、删除字段、依赖数据集和受影响消费方：删除消费方必需字段时为 `BLOCKED`；删除字段且存在下游数据集但缺少明确契约证据时为 `REVIEW`；纯新增且契约仍满足时为 `READY`。`schema_changes_applied=false` 表示演练不会修改目录、表结构或历史分区。

## 13. v4.2.0 质量取证抽样

使用 `PartitionEvidenceCandidate` 登记分区名称、行数、质量风险分数和新鲜度小时数，再调用 `plan_quality_evidence_samples` 并设置总样本预算与每个非空分区的最低样本数。计划先保证分区覆盖，再按规模、风险和新鲜度分配剩余预算，逐分区返回 `sample_count` 与稳定哈希选择规则。`source_rows_read=false`、`sampling_started=false` 表示该步骤仅规划审计证据，不会读取或修改源数据；审核通过后可由下游抽样作业按同一稳定主键规则执行。

## 14. v4.3.0 质量规则覆盖地图

使用 `DatasetQualityCoverage` 登记数据集的关键字段、已有校验字段、新鲜度 SLA 和血缘登记状态，再调用 `assess_quality_coverage`。结果逐数据集返回覆盖率、缺少校验的关键字段和治理缺口：关键字段未覆盖时为 `BLOCKED`，字段完整但 SLA 或血缘缺失时为 `REVIEW`，三类要求均满足时为 `READY`。`quality_rules_changed=false` 表示报告只用于安排治理工作，不会自动新增规则或修改数据。

## 15. v4.4.0 临时质量豁免治理

使用 `QualityException` 登记豁免编号、数据集、检查项、负责人、原因、剩余有效天数和影响分区数，再调用 `review_quality_exceptions`。已过期豁免返回 `EXPIRED`，进入临期窗口或超过允许分区范围时返回 `REVIEW`，其余为 `ACTIVE`。组合状态 `ACTION_REQUIRED` 表示至少一项需要处理；`quality_enforcement_changed=false` 表示复核不会自动续期、关闭豁免或更改质量门禁。

## 16. v4.5.0 运维旅程演练

在仓库根目录执行：

```powershell
rehearse-operator-journeys --output build/usability/operator-journeys.json
```

命令固定模拟 100 次常见使用过程，覆盖首次运行环境缺失、来源分区问题、流水线失败恢复、质量门禁失败、发布证据不完整和正常交付。报告中每条旅程均包含 `primary_action`、三步 `steps`、中文 `guidance` 和 `requires_confirmation`，用户可直接按首要动作进入正确入口。汇总字段 `action_counts` 用于发现最常见的操作阻塞，`unresolved_count` 应为 0。该演练不会自动恢复任务、修改数据或执行发布。
