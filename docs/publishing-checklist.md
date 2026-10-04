# 远程仓库发布清单

当前本地仓库已完成代码、测试、质量报告、基准和面试材料，但尚未配置 GitHub/Gitee 远程地址。以下清单用于首次公开发布，避免误传个人信息或声称未经验证的状态。

## 推荐仓库信息

- 仓库名：`bike-trip-lakehouse`
- 简介：`Reproducible PySpark batch lakehouse with SCD2, data quality, backfill, cohort retention and benchmark evidence.`
- 建议 Topics：`pyspark`、`data-engineering`、`data-warehouse`、`parquet`、`scd2`、`data-quality`、`cohort-analysis`
- 默认分支：`main`
- 可见性：准备求职作品集时使用 Public；发布前再次检查个人信息。

## 发布前本地验收

```powershell
git status --short
./scripts/run-portfolio-demo.ps1
git log --oneline -10
```

必须满足：

- `git status --short` 无输出；
- 一键演示最后输出 `"status": "PASS"`、`"automated_tests": "PASS"`；
- 质量检查数为 6，正式样例行数与 README 一致；
- `evidence/benchmark-10000-local.json` 能被 JSON 解析；
- 仓库内没有简历 PDF、证书原图、学籍验证码、手机号、邮箱密钥或 `.env`。

## 需要本人决定

- GitHub 还是 Gitee，以及账号名；
- 仓库是否立即公开；
- 采用哪一种开源许可证。未确认前不自动添加许可证；
- 是否同时发布英文 README。当前中文 README 更适合国内实习投递。

## 创建远程后执行

将占位地址替换为本人创建的真实仓库地址：

```powershell
git remote add origin <REMOTE_REPOSITORY_URL>
git push -u origin main
```

不要把占位地址直接执行。推送后检查：

1. GitHub Actions 的 Python/Spark 测试 Job 成功；
2. README Mermaid 架构图可正常渲染；
3. 相对链接能打开架构、面试指南和性能 JSON；
4. 仓库 About、Topics 和简介已经填写；
5. 置顶仓库只保留旗舰实时项目、共享单车离线湖仓和一个真正不同的项目。

## 建议展示截图

只截取可公开内容：

- 一键验收最终 PASS 摘要；
- 数据质量 JSON 的总体状态与 6 项检查；
- SCD2 两个版本的 `valid_from`、`valid_to` 示例；
- cohort 留存的 3 行手工验收样例；
- 基准报告环境、方法和三轮结果。

截图不得包含 Windows 用户目录、访问令牌、私有仓库地址或证书原图。

## 发布后的验证证据

记录以下内容，之后才能在简历或面试中说“公开仓库和远程 CI 已完成”：

- 远程仓库 URL；
- 首次成功 CI 的运行 URL 与提交 SHA；
- 发布日期；
- 若 CI 与本机结果不同，记录原因和修复提交。
