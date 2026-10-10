# 从 Spark UI REST 指标定位 Stage 与 Task

## 为什么执行计划还不够

执行计划说明 Spark 准备如何执行，运行指标才说明各个 Stage 和 Task 实际处理了多少数据、花了多少执行时间，以及是否发生 GC 或 Spill。本单元使用 Spark UI 的 `/api/v1` JSON 接口保存结构化证据，避免只截一张无法复核的网页图。

## 受控工作负载

固定生成 50,000 行、32 个 Key，并按 Key 重分为 8 个 Shuffle 分区，再执行分组计数和载荷字符求和。脚本通过 Job Group 精确筛选本次作业，在 Spark 停止前读取：

- Job 状态、Stage ID 与完成任务数；
- Stage 的 Shuffle 读写字节和记录数；
- Executor 执行时间、CPU 时间和 JVM GC 时间；
- 内存与磁盘 Spill；
- 最繁忙 Shuffle Stage 的逐任务耗时和 Shuffle 字节。

```powershell
./scripts/explain-spark-stage-metrics.ps1
```

## 如何阅读本机证据

本机一次实际报告包含 1 个 Job、2 个完成 Stage 和 10 个 Task。第一个 Stage 写出 Shuffle，第二个 Stage 读取相同的 3,302,403 字节；总输入记录和 Shuffle 记录均为 50,000，最终 32 个分组重新汇总为 50,000 行，因此数据守恒。内存和磁盘 Spill 都是 0。

最繁忙 Stage 的 8 个 Task 保留了最大、中位数和最小执行时间。最大/中位数差异可以作为继续排查的信号，但不能单独证明数据倾斜，因为 JVM 预热、本地线程调度、GC 和分区数据量都可能造成差异。应把它与逐任务 Shuffle 字节、最终计划和多轮运行放在一起解释。

## 证据和边界

- [`evidence/spark-stage-metrics-local.json`](../evidence/spark-stage-metrics-local.json) 保存机器可读原始指标；
- [`evidence/spark-stage-metrics-local.md`](../evidence/spark-stage-metrics-local.md) 保存便于人工复核的摘要；
- 数值只适用于 WSL2 `local[2]` 和这次合成工作负载，不是集群容量或 SLA；
- 当前仓库路径包含中文，Spark 4.2 Jetty 静态资源会打印 `Bad escape` 告警，但 `/api/v1` 已实际返回完整指标，所以这里只宣称 REST 采集成功，不宣称本机静态网页展示已验证。
