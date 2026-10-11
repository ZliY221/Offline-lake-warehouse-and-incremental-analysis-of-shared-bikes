# 依赖编排、失败重试与断点恢复

## 编排状态模型

编排核心将任务依赖、尝试次数、下游阻塞、断点恢复和失败日志写入状态清单，不依赖终端输出保存运行状态。

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

## 故障注入验证

```powershell
./scripts/run-managed-pipeline.ps1 `
  -RunRoot build/managed-pipeline-evidence `
  -RunId retry-evidence-20261005 `
  -InjectFailureOnce silver
```

实际运行中 Silver 第一次尝试以受控退出码 75 失败并产生独立日志，第二次尝试成功；其余四个任务各执行一次，最终6项质量检查通过。随后使用 `-Resume` 再次运行，同一任务图中的5个成功任务全部记录为 `TASK_RESUME_SKIPPED`，没有增加尝试次数。

证据保存在 [`evidence/orchestration-retry-local.json`](../evidence/orchestration-retry-local.json) 与 [`evidence/orchestration-retry-local.md`](../evidence/orchestration-retry-local.md)。

## 功能边界

该实现是单机进程级编排器，覆盖依赖、重试、恢复与审计语义，不包含 Airflow/Dagster 的调度器、元数据库、Worker、高可用、告警和权限治理。
