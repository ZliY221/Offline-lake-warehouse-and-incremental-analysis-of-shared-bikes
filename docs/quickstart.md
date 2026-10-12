# 一键本地运行

`scripts/quickstart.py` 统一提供跨平台数据源入口和完整 Spark 湖仓入口。脚本从自身位置解析仓库根目录，所有子进程使用当前 Python 环境，输出保存在 Git 忽略的 `build/` 目录。

## 跨平台数据源模式

```shell
python -m pip install -e .
python scripts/quickstart.py --mode source
```

该模式可在 Windows、Ubuntu 和 macOS 直接运行，执行 `source` 环境自检，并生成已通过业务契约校验的行程与站点 NDJSON。`source-summary.json` 记录行数、SHA-256 和产物位置。

## 完整湖仓模式

```shell
python scripts/quickstart.py --mode pipeline
```

Linux 和 macOS 使用当前 JDK、Python 与 PySpark 环境运行。Windows 自动调用仓库的 PowerShell/WSL 入口；缺少项目隔离运行时时，现有安装脚本会准备官方 JDK 17 和锁定版本的 PySpark。完整模式依次运行 Bronze、Silver、站点 SCD2、Gold 和跨层质量门禁。

完整模式默认写入 `build/quickstart/pipeline/`，并生成：

- `control/orchestration-run.json`：任务依赖、状态、重试次数和日志指纹；
- `reports/data-quality.json`：跨层不变量和质量门禁结果；
- `pipeline-evidence.json` 与 `pipeline-evidence.md`：编排摘要；
- `pipeline-summary.json`：对外运行摘要。

相同输出目录再次执行时会读取已存在的编排状态并进入安全续跑；已经完成的阶段不会重复计算。完整模式要求输出目录位于仓库内，以保证 Windows 与 WSL 对同一运行状态和 Parquet 文件进行读写。

GitHub Actions 在 Windows、Ubuntu 和 macOS 执行数据源模式，并在 Ubuntu 与 macOS 额外执行完整湖仓模式。Windows 完整 Spark 流程保留 WSL 边界，不引入来源不明的 Hadoop 本机二进制文件。
