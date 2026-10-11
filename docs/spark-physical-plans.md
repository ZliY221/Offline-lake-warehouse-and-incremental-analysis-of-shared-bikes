# Spark 物理计划、广播 Join 与 AQE

## 执行计划采集

执行计划用于核对 Join 类型、Shuffle、广播策略、AQE 行为以及 `spark.sql.shuffle.partitions` 对任务数的影响。测试保存格式化物理计划，并比较不同 Join 策略的聚合结果。

## 三组受控计划

固定输入为 1,000 行事实数据和 8 行维度数据：

1. 禁用 AQE，并把自动广播阈值设为 `-1`：计划出现 `SortMergeJoin`、两侧排序与 `Exchange`；
2. 对同一维度显式使用 `broadcast`：计划改为 `BroadcastHashJoin` 和 `BroadcastExchange`，聚合结果与基线逐行一致；
3. 开启 AQE、分区合并并把初始 Shuffle 分区设为 16：action 后的最终计划出现 `AdaptiveSparkPlan isFinalPlan=true` 与 `AQEShuffleRead coalesced`；
4. 生成 20,000 行事实数据，其中 90% 使用热点键 `0`，关闭 AQE 保存基线，再开启倾斜 Join：最终计划出现 `SortMergeJoin(skew=true)` 与 `AQEShuffleRead ... skewed`。

普通 AQE 与倾斜对照都直接在被解释的 DataFrame 上执行 action，避免把仍为 `isFinalPlan=false` 的初始包装计划当作运行时证据。倾斜优化前后还会核对 8 个分组的计数和载荷字符总数完全一致。

## 运行与证据

```powershell
./scripts/explain-spark-plans.ps1
```

输出默认写入被忽略的 `build/reports/`。仓库保存一次经核验的本机原始证据：

- [`evidence/spark-plan-analysis-local.json`](../evidence/spark-plan-analysis-local.json)：环境、设置、结果、算子计数和格式化计划；
- [`evidence/spark-plan-analysis-local.md`](../evidence/spark-plan-analysis-local.md)：便于发布前阅读的计划展开。

计划中的表达式 ID 和 `plan_id` 已归一化，避免把一次 JVM 会话的递增编号误认为业务证据。

## 验证边界

- 这是 WSL2 `local[2]`、合成小数据上的计划选择实验，不是速度或吞吐基准；
- 广播 Join 避免大表侧 Shuffle 的结论依赖维度确实足够小，生产中需要结合统计信息和内存评估；
- 本实验观察到了最终计划中的分区合并和倾斜拆分，但阈值是为 20,000 行合成数据刻意调低，不能直接复用为生产参数；
- 当前没有 Spark History Server、生产集群或大规模数据倾斜证据。

## 结果说明

基线使用 `SortMergeJoin`。显式广播将小维表复制到执行器，并使用 `BroadcastHashJoin` 避免事实表按 Join Key 重新分区。实验仅验证计划变化和结果等价，不根据小样本执行计划推导性能提升。
