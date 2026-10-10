# Airflow DAG 本地执行证据

- DAG：`bike_lakehouse_full_build`
- Airflow：`3.1.6`
- Python：`3.12.15`
- DAG Run 状态：`SUCCESS`
- 受控首次失败任务：`silver`
- 执行方式：Airflow `DAG.test()`，本地 SQLite 元数据库

## 任务结果

| 任务 | 上游依赖 | 状态 | 尝试次数 |
| --- | --- | --- | ---: |
| bronze | - | SUCCESS | 1 |
| gold | silver, station_dimension | SUCCESS | 1 |
| quality_gate | gold | SUCCESS | 1 |
| silver | bronze | SUCCESS | 2 |
| station_dimension | - | SUCCESS | 1 |

## 数据验收

- 质量门禁：`PASS`
- 质量检查数：`6`
- Bronze 行数：`20`
- Silver 合法行数：`20`

## 证据边界

该证据证明 Airflow 3.1.6 能解析并执行完整任务图、保存任务状态并完成原生重试。
它使用单机 `DAG.test()`、SQLite 和 Spark `local[2]`，不代表生产 Scheduler、分布式 Executor、SLA 或集群容错经验。
