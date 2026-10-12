# 跨平台环境自检

`scripts/project_doctor.py` 在启动 Spark 或 Airflow 前检查实际运行条件，并可输出 JSON 与 Markdown 报告。

## 检查范围

| Profile | 检查内容 | 适用入口 |
|---|---|---|
| `source` | Python、样例、任务定义与输出目录 | 数据生成、契约校验和首次安装检查 |
| `pipeline` | Python、样例与任务定义、PySpark、JDK 17、输出目录 | Bronze/Silver/Gold、回填与质量报告 |
| `airflow` | `pipeline` 加 Airflow 运行时 | Airflow 完整构建与日期回填 DAG |

Windows 使用仓库提供的 WSL 执行路径：自检会确认 WSL、`build/tools/` 下的隔离 JDK/PySpark，以及 Airflow 模式所需的隔离虚拟环境。缺少隔离运行时时，报告会指向 `scripts/setup-wsl-tools.ps1` 或 `scripts/setup-airflow.ps1`。Linux 直接检查当前 Python 模块和 JDK。

```shell
python scripts/project_doctor.py --profile pipeline --json-output build/doctor/report.json --markdown-output build/doctor/report.md
```

状态 `READY` 对应退出码 `0`；状态 `BLOCKED` 对应退出码 `1`。报告不写入业务数据行、来源路径或环境变量值。

## 平台验证范围

`portable-smoke.yml` 在 Windows、Ubuntu 和 macOS 上从全新检出执行 `scripts/quickstart.py --mode source`。Ubuntu 与 macOS 同时执行 `--mode pipeline`，构建 Bronze、Silver、站点 SCD2、Gold 和跨层质量报告；Windows 的 Parquet 与完整数仓流程统一通过 WSL，避免依赖非官方 `winutils.exe`。
