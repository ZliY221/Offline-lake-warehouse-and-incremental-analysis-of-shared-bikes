# 隔离运行时启动器

`scripts/bootstrap.py` 面向全新检出或 ZIP 解压后的首次运行。入口只要求 Python 3.10 或更高版本，在项目的 `build/portable-runtime/` 中创建虚拟环境，然后执行公开 Quickstart。`source` 模式只安装当前项目，不下载 PySpark；`pipeline` 模式安装锁定的 PySpark 依赖。

跨平台数据源模式：

```bash
python scripts/bootstrap.py -- --mode source
```

完整湖仓模式：

```bash
python scripts/bootstrap.py -- --mode pipeline
```

运行时标记记录项目版本、Python 版本、安装范围和 `pyproject.toml` 的 SHA-256。标记与当前配置一致时复用已有环境；从 `source` 切换到 `pipeline`、版本变化或依赖配置变化时自动重建。需要主动重建时使用：

```bash
python scripts/bootstrap.py --refresh-runtime -- --mode source
```

运行时目录必须位于项目 `build/` 目录内，并由 `.gitignore` 排除。启动器不会修改系统 Python，也不会写入仓库源码目录。首次安装依赖需要能够访问 Python 软件包索引。完整 Spark 模式在 Linux 与 macOS 需要 JDK 17；Windows 继续使用项目的 WSL 隔离执行路径。
