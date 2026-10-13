# Parquet 分区小文件治理

## 设计范围

该功能治理本地湖仓分区中由高频增量写入产生的小 Parquet 文件。它只处理形如 `partition_column=value` 的单层分区，不改变业务 Schema、分区键或记录内容。

## 计划阶段

计划器扫描每个分区的 Parquet 文件，记录文件数、总字节数、小文件数和内容指纹。只有同时满足以下条件的分区才生成动作：

- 文件数达到 `min_files`；
- 小于目标文件大小的文件数达到 `min_files`；
- 按总字节数计算出的目标文件数少于当前文件数。

动作编号由分区名、来源指纹和目标文件数确定，相同数据与参数会生成相同编号。计划默认写入 `build/compaction/plan.json`。

## 执行阶段

执行器在写入前重新计算来源指纹，分区变化时立即拒绝旧计划。Spark 读取单个分区并使用 `coalesce` 控制目标文件数，写入独立临时目录后重新读取并核对输入、输出行数。

只有行数一致且输出文件数不超过计划时才进行目录交换。原分区先移动到备份位置；若新目录替换失败，执行器恢复原分区。成功后删除备份并写入包含文件数、字节数、行数和新旧指纹的审计报告。

## 运行

只生成计划：

```powershell
./scripts/compact-lakehouse.ps1 -Dataset build/lakehouse/silver/trips_valid -PartitionColumn business_date
```

执行计划中的动作：

```powershell
./scripts/compact-lakehouse.ps1 -Dataset build/lakehouse/silver/trips_valid -PartitionColumn business_date -Execute
```

## 功能边界

当前执行器面向单进程维护窗口，不提供分布式锁。生产环境应在调度器中暂停同一分区的并发写入，或使用支持事务提交的湖表格式。压缩结果只说明文件组织变化，不代表查询性能必然提升；性能结论仍需使用相同查询与环境做前后对比。
