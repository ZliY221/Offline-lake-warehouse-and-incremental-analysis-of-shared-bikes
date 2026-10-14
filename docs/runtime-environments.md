# 运行环境分层

项目将数据生成、Spark 湖仓和 Airflow 调度拆成独立环境，避免调度依赖影响核心数据链路。

| 环境 | 适用场景 | 主要依赖 | 检查命令 |
| --- | --- | --- | --- |
| Source | 契约校验与确定性样例数据 | Python 3.10+ | `python scripts/project_doctor.py --profile source` |
| Pipeline | Bronze、Silver、Gold 与增量处理 | Python、PySpark、JDK 17+ | `python scripts/project_doctor.py --profile pipeline` |
| Airflow | DAG 编排、重试与回填 | Pipeline 环境、Airflow | `python scripts/project_doctor.py --profile airflow` |

Windows 的完整 Spark 流程使用 WSL 用户缓存目录中的受管 JDK 与 PySpark，避免仓库中文路径影响 Spark UI 的资源 URI；Linux 与 macOS 使用本机 JDK。公开启动器为每个检出目录维护隔离运行时，并固定 UTF-8 子进程输出。自动化环境覆盖 Linux Python 3.12/3.13、Windows Python 3.12 和 macOS Python 3.12。

仅查看数据契约时使用 Source；开发湖仓逻辑时使用 Pipeline；验证编排行为时再启用 Airflow。
