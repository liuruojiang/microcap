# 2026-11-01 发布与日报同步

## 已完成

- v2.3 波动率约束已取消；v2.0、v2.3、v2.5 的共享历史修复已正式迁移，正常重跑的历史改写检查通过。
- 三个版本均使用用户选择的 `delist_zero_writeoff_equal_survivors_v1`：退市日剩余价值计零并释放仓位；保守模型假设，非真实成交。
- 正式代码已推送 `main`。生产核心最后变更为 `1559dba5f0b3349f66e631ac5cc2e83390c3f615`；云端固定使用可从正式分支到达的 `600286d6db41feeef980a91c0169fa85496bdb86`。后续策略提交仅涉及测试、启动器和交接说明。
- [完整状态包](https://github.com/liuruojiang/microcap/releases/tag/top100-shared-history-20261101)已发布，数据截至 2026-10-30；ZIP SHA-256 为 `8d6af3322aeb693f0bcee2e61281d6389c21b496b8305f3e1f1c27834fdc8664`。
- 主工作区使用 `scripts/run_top100_daily_synced.ps1` 完成原有 `refresh-all`、`check`，两份报告均 `ok=true`、`errors=[]`，进程退出码为 0。日期依据为实时读取的独立交易历史，三个版本均截至 2026-10-30。
- 本地凭据：`research_reports/20261029_st_history_repair/final_delivery/local_portable_acceptance_20261101.json`；原始日志为同目录 `local_synced_portable_20261101.log`。

## 云端日报

- 正式自动化配置已切换到上述策略提交和完整状态包，工作流保持启用。
- [运行 41304793756](https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/41304793756)已完整成功，自动化提交为 `78de9a15361b1e90473cbe45ee1ba35415307be9`。三版最终 CSV 的版本、日期、持仓、退市模型和全部成员操作字段与本地一致；整包范围为 `whole_workspace_delivery`，包含 98,844 个文件。
- 使用 `validation_only=true`、`publication_mode=close_confirmed`；摘要状态为 `OK`，`Send Gmail` 明确跳过。当前证明覆盖部署、刷新、三版输出及日报生成，不声明本轮有新的 SMTP 或 Gmail 收件。
- [交付凭据](daily_sync_completion.json)记录本地两项检查、最终 CSV 身份与哈希、云端策略与自动化提交、工作流及邮件边界。
- 修复了云端脚本包导入路径，以及周末校验日期与交易日期不同导致的重复名单抓取。仅复用已通过完整验证、且覆盖独立最新完成交易日的状态；进入新交易日仍要求刷新，原有最终数据检查保留。
- Windows 启动器与云端共用进程内运行配置：CSV 使用 CRLF；不修改系统环境或策略历史。新机器需提供 `-AutomationRoot`，或填写忽略 Git 的 `.microcap_index_cache/local_daily_launcher.json`。

## 邮件与保留事项

- 本轮不追加验收邮件，不下单。此前唯一获批验收邮件：`microcap-20261029-noheat-01`，Gmail `1a18678165c2e298`，运行 `40831593852`；它早于本次退市模型修复，不能代替新版收件证明。
- 用户要求暂缓小问题：保留晚发布的非 ST 监管警示 `AN202611011831295457` 的真实原始记录，以精确证书记录本次延后处理；其他公告变动仍按原检查处理。证书 SHA-256 为 `1b883caf14d9ab433ccab128448dac7fef804b7ec37439327246f4313206d9e9`。
- 历史来源为实际取得的公开数据查询范围，不宣称已取得完整 Wind 历史认证。备份、研究与原始证据保留，其他历史问题不在本次继续扩展。
