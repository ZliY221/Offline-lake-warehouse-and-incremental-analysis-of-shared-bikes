# 远程仓库发布清单

当前仓库已于 2026 年 10 月 4 日公开发布到 GitHub，代码、测试、质量报告、基准和面试材料均已同步。以下清单记录已完成的远程证据和仍需人工决定的发布事项。

## 推荐仓库信息

- 远程仓库：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes>
- 仓库名：`Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes`
- 简介：`Reproducible PySpark batch lakehouse with SCD2, data quality, backfill, cohort retention and benchmark evidence.`
- 建议 Topics：`pyspark`、`data-engineering`、`data-warehouse`、`parquet`、`scd2`、`data-quality`、`cohort-analysis`
- 默认分支：`main`
- 可见性：Public

## 发布前本地验收

```powershell
git status --short
python scripts/audit-publication.py
./scripts/run-portfolio-demo.ps1
git log --oneline -10
```

必须满足：

- `git status --short` 无输出；
- 发布审计不存在高置信密钥或超过 20 MiB 的跟踪文件；`PASS_WITH_REVIEW` 项逐条确认是合成测试内容；
- 一键演示最后输出 `"status": "PASS"`、`"automated_tests": "PASS"`、`"spark_plan_analysis": "PASS"`、`"spark_skew_join_analysis": "PASS"`；
- 质量检查数为 6，正式样例行数与 README 一致；
- `evidence/benchmark-10000-local.json` 能被 JSON 解析；
- `evidence/spark-plan-analysis-local.json` 同时包含 Sort-Merge、显式 Broadcast Hash Join、AQE 最终计划、分区合并与倾斜 Join 证据；
- 仓库内没有简历 PDF、证书原图、学籍验证码、手机号、邮箱密钥或 `.env`。

## 当前审计结果

2026 年 10 月 4 日已实际运行发布审计：没有高置信密钥、私钥、本机用户路径或超过 5 MiB 的 Git 跟踪文件。唯一人工复核项是 `tests/test_silver.py` 中的合成邮箱字符串，它用于验证非法匿名骑行者键会被拒绝，不是真实联系方式。

## 仍需本人决定

- 采用哪一种开源许可证。未确认前不自动添加许可证；
- 是否同时发布英文 README。当前中文 README 更适合国内实习投递。

## 后续推送与远程验证

`origin` 已指向上述 GitHub 仓库，后续提交使用：

```powershell
git remote -v
git status --short
git push origin main
```

推送后检查：

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

已核验：

- 远程仓库 URL：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes>
- 首次成功 CI：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes/actions/runs/37176098699>
- 首次成功提交：`548fdf9dd0f57fe8d3b390887407140170bcb64b`
- Node 24 Actions 升级后的成功 CI：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes/actions/runs/37186146080>
- 对应提交：`e864b1d9d8c4a7a7d75784bfa167c3bbe89448ce`
- 分区级增量回填成功 CI：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes/actions/runs/37251252105>
- 对应提交：`85e3dfffc47ecd4348e008d7835c9246d02f2ac8`；21 项测试通过，覆盖非目标分区文件哈希不变与跨分区主键写前失败。
- Cohort 旧/新归属依赖传播成功 CI：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes/actions/runs/37252400028>
- 对应提交：`eabfe8cdc38078f9b570da779280250e5904fb62`；验证只写受影响 cohort 分区，同时明确计算仍为完整 Silver 扫描。
- Spark 物理计划证据成功 CI：<https://github.com/ZliY221/Offline-lake-warehouse-and-incremental-analysis-of-shared-bikes/actions/runs/37253997852>
- 对应提交：`aed97c1d05398dbcea2486c0c51ae43880d721cb`；23 项测试通过，并保存 Sort-Merge、显式 Broadcast Hash Join 与 AQE 的真实格式化计划。该证据只证明计划选择和结果等价，不作为生产性能结论。
- 发布与首次成功日期：2026 年 10 月 4 日
- 若 CI 与本机结果不同，记录原因和修复提交。
