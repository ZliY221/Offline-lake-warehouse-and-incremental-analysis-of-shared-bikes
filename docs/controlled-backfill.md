# 受控日期回填与影响范围

## 回填不是重新跑一遍脚本

历史数据修正需要先回答三个问题：输入属于哪一天、会替换哪些存储分区、哪些下游结果必须重算。缺少这些边界时，回填可能误删其他日期、混入错误业务日期，或只修 Bronze 却留下过期指标。

## 预检

回填读取显式行程 Schema，提取所有可解析的 `started_at` 业务日期。只要发现目标日期之外的值，就在写入 Bronze 前终止。损坏或缺字段记录仍可进入 Bronze，之后由 Silver 质量规则隔离。

第二项预检处理去重依赖：局部重算只在合法 `trip_id` 不跨摄取日期时成立。作业先检查现有 Bronze 是否已违反这一约束，再检查新输入是否与其他日期的合法主键相交；任一检查失败都在覆盖 Bronze 前停止。这样不会把原本需要跨分区判定的精确重复或主键冲突错误地当成独立记录。

## 当前影响范围

当前实现的处理顺序是：

```text
单个目标 Bronze 分区动态覆盖
  -> Silver 目标 ingestion_date 分区替换
  -> Gold 日指标与热门路线目标 business_date 分区替换
  -> 合并受影响骑行者的旧/新 cohort 日期
  -> 只替换受影响 cohort_date 分区
```

日指标与路线只依赖目标日合法行程及其当日有效的 SCD2 站点版本，因此可以安全替换单分区。Cohort 依赖骑行者的全局首次活动日：历史日新增或删除行程可能让骑行者从旧 cohort 移到新 cohort，所以两边都必须更新。作业在 Silver 改写前物化“旧目标日骑行者 ∪ 新目标日合法骑行者”，先求旧 cohort 日期，改写后再求新 cohort 日期，最终只替换日期并集。清单记录 `AFFECTED_COHORT_DATES` 写入范围和 `FULL_DATASET_SCAN` 计算范围；这不等同于增量计算或事务湖仓。

## 成功与失败记录

回填 ID 由目标日期和来源文件 SHA-256 构成。控制记录在开始时写 `RUNNING`，成功后保存 Bronze、Silver、Gold 的结构化结果，失败则写 `FAILED`、异常类型和错误摘要。目标 Bronze 批次仍有自己独立的摄取清单，两层记录分别回答“这次回填做了什么”和“这个文件如何落入 Bronze”。

## 可复现实验

```powershell
./scripts/backfill-date.ps1 `
    -InputPath data/sample/trips.ndjson `
    -TargetDate 2026-10-01
```

固定样例会替换 `2026-10-01` Bronze 分区为 20 行，并更新同日 Silver、Gold 日指标和路线。测试先建立两日完整数据，再让一个第二天 cohort 的骑行者通过回填前移到第一天：清单必须列出两个 cohort 日期，同时第二天 Silver、日指标和路线 Parquet 文件名称与 SHA-256 保持不变。另一个测试制造跨日期主键冲突，验证作业在 Bronze 写入前失败且目标分区内容保持不变。

## 缺失分区修复计划

`bike_lakehouse.repair_cli plan` 扫描 Bronze 物理分区，并在最长 366 天的显式日期范围内生成缺口清单。来源目录按日期提供文件名和 SHA-256；存在可信来源的动作标记为 `READY`，没有来源的日期标记为 `BLOCKED_SOURCE_MISSING`。计划只保存文件名，不接受目录穿越路径，也不会自动执行回填。

来源清单可直接从受控目录生成：

```powershell
./scripts/build-repair-source-catalog.ps1 `
  -SourceRoot data/repair `
  -Output build/repair/source-catalog.json
```

目录中的 NDJSON 文件必须使用 `trips-YYYY-MM-DD.ndjson` 命名。生成器按日期排序并保存文件名、字节数和 SHA-256，不读取或复制业务行内容；不符合约定的文件会阻止清单生成，避免文件被静默遗漏。

完成计划中的单日回填后，`bike_lakehouse.repair_cli audit` 同时验证目标分区已经存在，并且控制目录内具有目标日期与来源 SHA-256 完全匹配的 `SUCCEEDED` 回填清单。两项证据缺一不可，审计状态才为 `COMPLETE`。

## 计划执行与恢复

`repair_cli execute` 在启动 Spark 前验证计划版本、动作编号、来源路径和 SHA-256。任一动作缺少来源或指纹不匹配时，整批保持 `BLOCKED`，不会先执行部分动作。`--dry-run` 输出 `WOULD_EXECUTE` 清单，不改写湖仓。

正式执行按计划顺序调用受控单日期回填。目标分区与同来源成功清单同时存在时标记为 `SKIPPED_ALREADY_COMPLETE`，因此失败后的同计划重跑只处理未完成动作。每个回填返回后，执行器会重新扫描 Bronze 分区和回填清单；只有目标分区存在且 `SUCCEEDED` 清单中的来源 SHA-256 完全匹配，动作才标记为 `VERIFIED`。任一后验条件缺失时动作标记为 `POSTCONDITION_FAILED` 并停止后续动作，避免仅凭函数返回值报告完成。独立的 `repair_cli audit` 仍可用于批次结束后的再次核对。

执行模式默认把追加式审计事件写入 `build/repair/execution-journal.ndjson`，也可通过 `--journal` 指定路径。每次执行使用独立 `execution_id`，依次记录批次开始、动作开始、跳过、验证成功、执行失败、后验失败与批次结束。每条记录写入后立即刷新到磁盘；若进程中断，可通过只有 `ACTION_STARTED` 而没有终态事件的动作定位不确定执行边界。日志只保存动作编号、日期、来源 SHA-256、状态和异常类型，不保存来源绝对路径或数据行。
