# Spark UI stage and task metrics

Generated: `2026-10-05T02:36:45Z`

These are local Spark UI REST metrics for one synthetic workload. They characterize stages and tasks; they are not production capacity or SLA evidence.

## Workload result

```json
{
  "group_count": 32,
  "total_payload_characters": 3200000,
  "total_rows": 50000
}
```

## Stage totals

```json
{
  "disk_bytes_spilled": 0,
  "executor_cpu_time_ns": 632867640,
  "executor_run_time_ms": 1043,
  "input_bytes": 0,
  "jvm_gc_time_ms": 26,
  "memory_bytes_spilled": 0,
  "num_tasks": 10,
  "output_bytes": 0,
  "shuffle_read_bytes": 3302403,
  "shuffle_read_records": 50000,
  "shuffle_write_bytes": 3302403,
  "shuffle_write_records": 50000
}
```

## Completed stages

| Stage | Tasks | Run time (ms) | Shuffle read | Shuffle write | Spill |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 2 | 538 | 0 | 3302403 | 0 |
| 1 | 8 | 505 | 3302403 | 0 | 0 |

## Busiest shuffle stage task distribution

```json
{
  "attempt_id": 0,
  "stage_id": 1,
  "task_summary": {
    "executor_run_time_ms": {
      "max": 197,
      "max_to_median_ratio": 8.955,
      "median": 22.0,
      "min": 12
    },
    "task_count": 8,
    "tasks": [
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 136705224,
        "executor_run_time_ms": 197,
        "index": 0,
        "jvm_gc_time_ms": 8,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 620219,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 2
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 79211781,
        "executor_run_time_ms": 197,
        "index": 1,
        "jvm_gc_time_ms": 8,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 308707,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 3
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 19881587,
        "executor_run_time_ms": 26,
        "index": 2,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 308587,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 4
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 17908909,
        "executor_run_time_ms": 24,
        "index": 3,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 204670,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 5
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 19006322,
        "executor_run_time_ms": 20,
        "index": 4,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 827883,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 6
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 14864684,
        "executor_run_time_ms": 16,
        "index": 5,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 415639,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 7
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 12770160,
        "executor_run_time_ms": 13,
        "index": 6,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 308507,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 8
      },
      {
        "attempt": 0,
        "disk_bytes_spilled": 0,
        "executor_cpu_time_ns": 11294718,
        "executor_run_time_ms": 12,
        "index": 7,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "shuffle_read_bytes": 308191,
        "shuffle_write_bytes": 0,
        "status": "SUCCESS",
        "task_id": 9
      }
    ]
  }
}
```
