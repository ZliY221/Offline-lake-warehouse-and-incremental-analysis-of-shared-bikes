# Airflow 参数化回填与失败审计证据

- Airflow：`3.1.6`
- Python：`3.12.15`
- 回填 DAG：`bike_lakehouse_date_backfill`
- 成功参数：`{"target_date": "2026-10-01", "input_path": "data/sample/trips.ndjson"}`
- 失败参数：`{"target_date": "2026-10-02", "input_path": "data/sample/trips.ndjson"}`

## 成功回填

| 任务 | 状态 | 尝试次数 |
| --- | --- | ---: |
| backfill | SUCCESS | 2 |
| quality_gate | SUCCESS | 1 |
| validate_request | SUCCESS | 1 |

成功 DAG Run：`SUCCESS`；质量门禁：`PASS`。

## 失败回填

| 任务 | 状态 | 尝试次数 |
| --- | --- | ---: |
| backfill | FAILED | 2 |
| quality_gate | UPSTREAM_FAILED | 0 |
| validate_request | SUCCESS | 1 |

失败 DAG Run：`FAILED`；写前保护后 Bronze 未变化：`True`。

## 回调审计

- 重试事件：`2`
- 最终失败事件：`1`
- 审计只保存 DAG、任务、Run ID、尝试次数和上下文可提供的异常类型；未提供时为 `null`，不复制异常正文或数据内容。

## 证据边界

该实验使用 Airflow `DAG.test()`、SQLite 和 Spark `local[2]`。回调写入本地 JSONL，不是邮件、短信或企业告警平台，也未验证并发补数、权限审批或生产 SLA。
