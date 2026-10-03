# Top100 无用测试产物清理与同步记录（2026-10-03）

## 范围

本次只清理可明确判定无用的测试产物：生成式缓存，以及已有完整替代版的中止/废弃 L8 运行目录。有效回归测试源码、历史审计证据、完整的 2026-10-03 参数扫描与正式 v2.0/v2.3/v2.5 交付文件均保留。

## 清理及恢复点

- `20261003_v20_l8_integrated_adversarial`、`20261003_v23_l8_integrated` 标记为中止；`20261003_v25_l8_integrated_adversarial` 标记为由 `_v2` 替代。三目录共 65 个文件、约 25.26 MiB，已移至 `D:\Codex\archives\microcap\20261003_unneeded_tests\superseded_runs`；`superseded_manifest.json` 逐文件记录原位置、大小及 SHA-256，移动后逐项读回一致。三个完整 `_v2` 运行目录保留。
- 在活动工作区内清理 `__pycache__`、`.pytest_cache`、`.ruff_cache`、`.mypy_cache`、`.hypothesis` 等生成式缓存，共 59 个目录、250 个文件、约 5.51 MiB。既有备份目录 `.codex_backups/` 未动；清理清单为 `D:\Codex\archives\microcap\20261003_unneeded_tests\cache_cleanup_manifest.json`。
- 12 个原有已修改的跟踪文件在本次整组刷新前，已逐文件备份并核对 SHA-256，清单为 `D:\Codex\archives\microcap\20261003_unneeded_tests\pre_sync_tracked_manifest.json`。

## 保留与验证

- `tests/test_v2_5_scan_integrity.py` 是仍在使用的回归测试；三份 v2.5 研究扫描脚本中对年化与最大回撤口径的修正也保留。该测试文件 **27 项通过**。
- `quant_param_scan_runs/20261003_v25_logwls_lb5_100_step5/` 与本轮半衰期扫描目录包含逐日与独立复算证据，不属于无用文件。适合远端阅读的结论、完整指标宽表与图已整理到 [v2.5 参数研究记录](microcap_v25_parameter_sensitivity_20261003.md)。
- `python -X utf8 scripts/top100_delivery.py refresh-all` 与随后独立 `check` 通过，`scope=whole_workspace_delivery`、`expected_date=2026-09-30`；正式三版最终收益流均到该日。代理换仓事件末日为 2026-09-17，属于事件表。
- L1 文档修正的新增相对链接已逐项检查，所需证据与文档一同纳入版本控制；未将 1.9 GiB 历史扫描目录或整批 326 MiB 本地诊断中间文件推入 Git。

## 生产边界

本次无生产策略参数变更。公开/本地代理数据、历史样本与实际可成交性仍沿用既有标注；文件同步、测试通过和邮件自动化的实际发送是不同验收项。
