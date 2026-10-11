# Airflow 参数化补数与失败审计

## 独立补数 DAG

完整构建 DAG 处理五阶段依赖、重试和质量门禁；独立补数 DAG 负责按指定日期重算。补数请求需要校验日期、输入路径和源数据所属日期，并为失败任务保留审计记录。

本项目新增 `bike_lakehouse_date_backfill`，任务链为：

```text
validate_request -> backfill -> quality_gate
```

## 参数与边界

- `target_date` 使用 Airflow `Param` 声明日期字符串约束；任务内再通过 `date.fromisoformat` 校验。
- `input_path` 只允许仓库 `data/` 下已存在的相对文件，拒绝绝对路径和 `..` 路径穿越。
- 回填核心继续执行已有 `backfill_cli`，不在 DAG 中复制 Spark 业务逻辑。
- 质量报告使用 `--fail-on-error`，未通过时 DAG 不能显示成功。

## 重试与回调

回填任务允许一次重试，并分别设置 `on_retry_callback` 与 `on_failure_callback`。回调写入 JSONL，每条只包含事件、DAG、任务、Run ID、尝试次数和上下文可提供的异常类型；不保存异常正文、命令参数或业务数据。回调捕获自身异常并写 Airflow 日志，避免审计失败掩盖原任务状态。

这只是本地失败审计，不是邮件、短信或企业告警平台。生产环境还需要集中日志、告警路由、去重、升级策略、权限与密钥管理。

## 可复现实验

```powershell
./scripts/setup-airflow.ps1
./scripts/run-airflow-backfill.ps1
```

脚本先用完整构建 DAG 生成基线，再执行两个补数 Run：

1. `2026-10-01` 配合对应源文件：首轮注入受控故障，Airflow 将任务置为 `up_for_retry`，第二次成功，质量门禁通过。
2. `2026-10-02` 配合仅含 `2026-10-01` 数据的文件：两次尝试都被源日期校验拒绝，最终 DAG 失败，质量任务为 `UPSTREAM_FAILED`。

失败场景执行前后会递归计算 Bronze 文件 SHA-256；只有指纹完全一致才生成 `PASS` 结果。结构化记录保存在 `evidence/airflow-backfill-local.json`，包含参数、任务状态和尝试次数。

## 功能边界

当前结果覆盖 Airflow 3.1.6 参数化补数 DAG、本机 SQLite 元数据库、原生重试、失败回调、阻塞式质量门禁和写前保护，不覆盖生产 Scheduler、分布式 Executor、高可用、并发补数锁、审批授权、企业告警或 SLA。
