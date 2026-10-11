# Airflow 批处理任务图

## 设计范围

Airflow 适配层复用项目任务定义，将 Bronze、Silver、站点维表、Gold 和质量门禁映射为 TaskFlow DAG，并配置依赖与重试策略。

## 共享任务定义

如果本地脚本和 Airflow DAG 各自维护五套命令，参数很容易漂移。本项目将任务名、上游依赖、Python 模块和参数集中在 `pipeline_definition.py`：

- 本地 `PipelineOrchestrator` 将其转换为 `TaskSpec`；
- Airflow DAG 将其转换为 5 个 TaskFlow 任务；
- 单元测试比较两种适配结果，保证业务命令一致。

## DAG 结构

```text
bronze ─→ silver ─┐
                  ├─→ gold ─→ quality_gate
station_dimension ┘
```

每个 Airflow 任务允许一次重试。质量报告 CLI 使用 `--fail-on-error`，因此质量门禁失败会让 DAG 失败，而不是只生成一份失败报告后继续显示成功。

## 本地验证

```powershell
./scripts/setup-airflow.ps1
./scripts/run-airflow-dag.ps1
```

Airflow 安装在 Git 忽略的项目目录中，使用独立 Python 3.12，不污染 PySpark 环境。受控运行让 Silver 第一次抛出异常，Airflow 将任务置为 `up_for_retry`，一秒后执行第二次尝试；Gold 只在 Silver 与站点维表成功后运行。

## 已验证结果

- Airflow 版本：3.1.6；
- DAG Run：`SUCCESS`；
- 任务数：5；
- Silver 尝试次数：2；
- 其他任务尝试次数：1；
- 跨层质量检查：6 项全部 `PASS`；
- Bronze 与合法 Silver：均为 20 行。

原始结果保存在 `evidence/airflow-dag-test-local.json`，可读摘要保存在同名 Markdown 文件。

## 远程 CI 与证据边界

GitHub Actions 使用官方约束文件安装 Airflow 3.1.6，检查 DAG 能导入、依赖正确且每个任务配置一次重试。为控制 CI 时间，远程 Job 不再次执行完整五阶段 Spark DAG；完整运行证据来自本机 WSL。

当前验证不覆盖常驻 Scheduler、Web UI、分布式 Executor、高可用元数据库、告警、SLA 或补数权限治理。
