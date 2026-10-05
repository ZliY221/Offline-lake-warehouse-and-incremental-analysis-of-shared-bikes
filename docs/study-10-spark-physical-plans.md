# 学习单元 10：Spark 物理计划、广播 Join 与 AQE

## 为什么要保存执行计划证据

会写 DataFrame 代码不等于能解释 Spark 如何执行。数据开发面试常追问 Join 是否发生 Shuffle、为什么广播小表、AQE 能做什么，以及 `spark.sql.shuffle.partitions` 如何影响任务数。本实验保存真实格式化物理计划，同时核对不同 Join 策略的聚合结果完全一致。

## 三组受控计划

固定输入为 1,000 行事实数据和 8 行维度数据：

1. 禁用 AQE，并把自动广播阈值设为 `-1`：计划出现 `SortMergeJoin`、两侧排序与 `Exchange`；
2. 对同一维度显式使用 `broadcast`：计划改为 `BroadcastHashJoin` 和 `BroadcastExchange`，聚合结果与基线逐行一致；
3. 开启 AQE、分区合并并把初始 Shuffle 分区设为 16：计划出现 `AdaptiveSparkPlan`。

本次小数据计划没有观察到 `AQEShuffleRead coalesced`，因此只能证明 AQE 包装计划已启用，不能宣称发生了动态分区合并或倾斜优化。

## 运行与证据

```powershell
./scripts/explain-spark-plans.ps1
```

输出默认写入被忽略的 `build/reports/`。仓库保存一次经核验的本机原始证据：

- [`evidence/spark-plan-analysis-local.json`](../evidence/spark-plan-analysis-local.json)：环境、设置、结果、算子计数和格式化计划；
- [`evidence/spark-plan-analysis-local.md`](../evidence/spark-plan-analysis-local.md)：便于面试前阅读的计划展开。

计划中的表达式 ID 和 `plan_id` 已归一化，避免把一次 JVM 会话的递增编号误认为业务证据。

## 不能扩大表述的边界

- 这是 WSL2 `local[2]`、合成小数据上的计划选择实验，不是速度或吞吐基准；
- 广播 Join 避免大表侧 Shuffle 的结论依赖维度确实足够小，生产中需要结合统计信息和内存评估；
- AQE 的 `AdaptiveSparkPlan` 只说明功能启用，是否合并分区、转换 Join 或处理倾斜必须以最终运行计划和真实指标为准；
- 当前没有 Spark History Server、生产集群或大规模数据倾斜证据。

## 面试回答结构

先说明业务输入规模与 Join 两侧角色，再展示基线 `SortMergeJoin` 计划；然后说明显式广播将维度复制到执行器，用 `BroadcastHashJoin` 避免事实表按 Join Key 重新分区；最后强调本实验验证了计划变化和结果等价，但没有把小样本计划当作性能提升数字。
