# Managed pipeline retry and resume evidence

Pipeline: `bike-trip-lakehouse-full-build`
Run ID: `retry-resume-evidence-20261005`
Status: `SUCCEEDED`
Resume count: `1`

This manifest proves local dependency, retry, log hashing and resume behavior. It is not evidence of a production scheduler or distributed control plane.

## Task states

| Task | Dependencies | Status | Attempts | Last exit | Log SHA-256 |
| --- | --- | --- | ---: | ---: | --- |
| bronze | - | SUCCEEDED | 1 | 0 | `2afa2ae80f29067ab3e9b7b7e92606cec69cf3b5be6a164c57a70849704aa8b7` |
| gold | silver, station_dimension | SUCCEEDED | 1 | 0 | `af1702827324c0c5320cc6ae37c9493b72db60f88cf5f990c4b1fc4237455363` |
| quality_gate | gold | SUCCEEDED | 1 | 0 | `09cce1275f2c0a4dbe080b607d6ada5b5b683fc42e96fe99f7bacf22a96bdc52` |
| silver | bronze | SUCCEEDED | 2 | 0 | `aeff290f8afa9c9a815a8394074ad86e02face133e354905c30dcc7cb8c101df` |
| station_dimension | - | SUCCEEDED | 1 | 0 | `44cd485f1e0c30b2e25490308d4cfaef72cd7b86f2863b35258876f3578fac1c` |

## Attempt history

| Task | Attempt | Status | Exit | Log | Log SHA-256 |
| --- | ---: | --- | ---: | --- | --- |
| bronze | 1 | SUCCEEDED | 0 | `build/managed-pipeline-evidence-v2/logs/bronze-attempt-1.log` | `2afa2ae80f29067ab3e9b7b7e92606cec69cf3b5be6a164c57a70849704aa8b7` |
| gold | 1 | SUCCEEDED | 0 | `build/managed-pipeline-evidence-v2/logs/gold-attempt-1.log` | `af1702827324c0c5320cc6ae37c9493b72db60f88cf5f990c4b1fc4237455363` |
| quality_gate | 1 | SUCCEEDED | 0 | `build/managed-pipeline-evidence-v2/logs/quality_gate-attempt-1.log` | `09cce1275f2c0a4dbe080b607d6ada5b5b683fc42e96fe99f7bacf22a96bdc52` |
| silver | 1 | FAILED | 75 | `build/managed-pipeline-evidence-v2/logs/silver-attempt-1.log` | `0918f6e2da4bb301dad1a9b1b874cbed88234d6b15912c8a800fedd7af99e913` |
| silver | 2 | SUCCEEDED | 0 | `build/managed-pipeline-evidence-v2/logs/silver-attempt-2.log` | `aeff290f8afa9c9a815a8394074ad86e02face133e354905c30dcc7cb8c101df` |
| station_dimension | 1 | SUCCEEDED | 0 | `build/managed-pipeline-evidence-v2/logs/station_dimension-attempt-1.log` | `44cd485f1e0c30b2e25490308d4cfaef72cd7b86f2863b35258876f3578fac1c` |

## State transitions

| Seq | Task | Event | Attempt |
| ---: | --- | --- | ---: |
| 1 | - | PIPELINE_STARTED | - |
| 2 | bronze | TASK_STARTED | 1 |
| 3 | bronze | TASK_SUCCEEDED | 1 |
| 4 | silver | TASK_STARTED | 1 |
| 5 | silver | TASK_RETRY_SCHEDULED | 1 |
| 6 | silver | TASK_STARTED | 2 |
| 7 | silver | TASK_SUCCEEDED | 2 |
| 8 | station_dimension | TASK_STARTED | 1 |
| 9 | station_dimension | TASK_SUCCEEDED | 1 |
| 10 | gold | TASK_STARTED | 1 |
| 11 | gold | TASK_SUCCEEDED | 1 |
| 12 | quality_gate | TASK_STARTED | 1 |
| 13 | quality_gate | TASK_SUCCEEDED | 1 |
| 14 | - | PIPELINE_SUCCEEDED | - |
| 15 | - | PIPELINE_STARTED | - |
| 16 | bronze | TASK_RESUME_SKIPPED | - |
| 17 | silver | TASK_RESUME_SKIPPED | - |
| 18 | station_dimension | TASK_RESUME_SKIPPED | - |
| 19 | gold | TASK_RESUME_SKIPPED | - |
| 20 | quality_gate | TASK_RESUME_SKIPPED | - |
| 21 | - | PIPELINE_SUCCEEDED | - |
