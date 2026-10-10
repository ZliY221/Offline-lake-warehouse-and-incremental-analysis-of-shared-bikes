# 依赖编排、失败重试与断点恢复

## 为什么不能只写顺序脚本

顺序调用五个命令可以跑通演示，但无法回答任务失败后重试几次、下游是否误跑、进程中断后从哪里恢复，以及每次失败日志能否审计。编排核心把这些语义显式写进状态清单，而不是依赖终端输出或人的记忆。

## 任务图

```text
Bronze ──> Silver ──┐
                    ├──> Gold ──> Quality gate
Station SCD2 ───────┘
```

启动前会验证任务名唯一、依赖存在且图中没有环。Gold 只有在 Silver 与站点 SCD2 都成功后才能运行，质量门禁只在 Gold 成功后运行。

## 状态与审计

每次状态变化都使用原子替换写入 JSON。每个任务保留：

- 累计尝试次数和每次调用允许的最大尝试数；
- 每次尝试的开始、结束时间、状态和退出码；
- 相对日志路径与日志 SHA-256；
- 任务依赖、最终状态以及完整事件序列；
- 任务图指纹，防止修改依赖或命令后错误恢复旧状态。

## 真实失败实验

```powershell
./scripts/run-managed-pipeline.ps1 `
  -RunRoot build/managed-pipeline-evidence `
  -RunId retry-evidence-20261005 `
  -InjectFailureOnce silver
```

实际运行中 Silver 第一次尝试以受控退出码 75 失败并产生独立日志，第二次尝试成功；其余四个任务各执行一次，最终6项质量检查通过。随后使用 `-Resume` 再次运行，同一任务图中的5个成功任务全部记录为 `TASK_RESUME_SKIPPED`，没有增加尝试次数。

证据保存在 [`evidence/orchestration-retry-local.json`](../evidence/orchestration-retry-local.json) 与 [`evidence/orchestration-retry-local.md`](../evidence/orchestration-retry-local.md)。

## 诚实边界

这是单机进程级编排器，重点是可测试的依赖、重试、恢复与审计语义。它还没有 Airflow/Dagster 的调度器、元数据库、Worker、高可用、告警和权限治理，因此文档应写“实现可恢复的本地编排核心”，不能写“具备生产 Airflow 平台经验”。
