# Spark 环境、数据契约与 Bronze 分区

## 本单元目标

1. 区分 Spark 本地模式与生产集群。
2. 理解为什么生产数据读取要使用显式 Schema。
3. 理解 Bronze 层的职责和边界。
4. 理解静态覆盖和动态分区覆盖的差异。

## 本地模式边界

`local[2]` 表示一个 JVM 进程使用两个本地执行线程。它能够验证 DataFrame 变换、Shuffle、Parquet 和 SQL 语义，但不证明掌握集群资源调度、高可用、网络 Shuffle 或生产运维。

本机已在 Windows 原生环境真实运行 Spark 4.2.0 DataFrame 分组聚合；但 Windows Hadoop 文件系统实现写 Parquet 时仍需要 `winutils.exe`。项目不下载来源不明的二进制文件，而是把 Parquet 测试与构建放到 WSL/Linux，并使用项目目录内的微软官方 JDK 17。这样既保留了可复现性，也明确说明了本地模式与运行平台边界。

## 为什么使用显式 Schema

自动推断会扫描数据并根据当前样本猜测类型。今天全是整数的字段可能在明天出现小数或空值，导致类型变化；坏时间也可能被静默解释为字符串。

本项目明确声明：

- 时间为 `TimestampType`；
- 距离为 `DecimalType(8, 2)`；
- 站点和行程 ID 为不可为空的字符串；
- 枚举值的业务合法性在 Silver 层检查。

Schema 只解决结构解析，不等于业务质量校验。

## Bronze 的职责

Bronze 保存“本批次收到并按结构解析的数据”，不提前混入复杂业务聚合。当前增加：

- `ingestion_date`：本次摄取日期，也是物理分区键；
- `source_file`：Spark 的输入文件名，用于追踪来源；
- `_corrupt_record`：无法按 JSON 语法解析的原文，仅用于后续最小披露拒绝处理。

后续 Silver 才执行时间顺序、站点关系、枚举、距离、去重等规则。

## 动态分区覆盖

普通 `mode("overwrite")` 可能删除整个输出目录。当前写入同时设置：

```python
.option("partitionOverwriteMode", "dynamic")
.mode("overwrite")
.partitionBy("ingestion_date")
```

这样只覆盖本次 DataFrame 实际包含的摄取日期。测试先写两个日期，再重写其中一天，验证另一日期仍然存在。

## 下一步

Silver 层会把 `_corrupt_record` 和业务失败记录写入独立拒绝数据集，同时只在主表保留合法、去重后的行程。每条规则都要有可触发的固定样例和测试。
