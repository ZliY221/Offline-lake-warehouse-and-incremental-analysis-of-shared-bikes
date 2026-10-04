# 学习单元 08：可复现性能证据方法

## 性能数字必须带上下文

单独写“每秒处理多少条”没有意义。输入规模、文件大小、硬件、Spark 模式、是否包含启动时间、预热次数、测量轮数和统计方法都会改变结果。本项目把这些信息与原始逐轮耗时一起写入 JSON。

## 基准范围

每轮依次执行：

```text
Bronze 动态分区覆盖
  -> Silver 全量质量校验与重建
  -> Gold SCD2 时态关联与全量重建
```

生成输入和站点维度构建不计入轮次耗时；Parquet 写入、Spark action、输出行数验证均计入。所有轮次复用一个 `local[2]` Spark 会话，因此报告明确标注 Spark/JVM 启动时间不在范围内。

## 预热与统计

默认先运行 1 轮预热，再测量 3 轮。报告保留每轮 Bronze、Silver、Gold 和端到端 wall time，摘要使用中位数降低单次抖动影响。吞吐量定义为“输入行数 / 端到端中位耗时”，不是集群能力或生产 SLA。

## 执行

```powershell
./scripts/run-benchmark.ps1
```

默认使用固定种子生成 10,000 行 NDJSON。运行时输出保存在被 Git 忽略的 `build/reports/`；本次核验后的原始报告已保存为 `evidence/benchmark-10000-local.json`，README 引用了具体环境、范围和结果。
