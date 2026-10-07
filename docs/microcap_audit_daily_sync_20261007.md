# 2026-10-07 审计修正远端同步与日报验收

本记录对应先前仅生成日报的维护验收。后续已完成新的多智能体对抗修复及真实SMTP收发验收，当前发布身份和收件证明见[实际发送验收](microcap_daily_delivery_adversarial_20261007.md)。

已同步 v2.0、v2.3、v2.5 的审计修复，并完成本地整体交付检查及云端实际日报生成验收。当前交易所日历确认的最近已完成交易日为2026-09-30；本次维护验收使用该收盘状态。

## 发布身份

| 对象 | 实际身份 |
|---|---|
| 审计策略提交及云端三版固定版本 | `501b269b9f51a83f05f3b71488143b4da285cd0b` |
| 日报自动化提交 | `ed6005d16816ca7b84aa95f0a9aab73f13b6232f` |
| 实际云端验收 | [37631244056](https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/37631244056)，completed / success |
| 本地定时任务 | 微盘股与 IC/IM 每日实时信号（独立）；ACTIVE，原工作日14:15调度保留 |
| 验收数据日 | 2026-09-30 |

修复源码、新增共享数据合同、审计回归和审计结论已推送策略仓库 main。日报仓库的三个隔离 checkout 同时固定到上述策略提交；冷启动配置同时改用新代码对应的整包恢复工件。工作流检查与本地定时任务均加入 `test_script_delivery_adversarial.py`；本地任务其他提示词、IC/IM设置、调度、模型及通知配置已逐项核对保持原值。

## 本地与云端证据

- 本地正式 `top100_delivery.py refresh-all` 和随后独立 `check` 均退出0，完整清单为complete；`top100_cloud_delivery.py sync --expected-date 2026-09-30` 通过并复用existing_whole_delivery。
- 本地三个真实官方“信号”入口均退出0；用日报仓库真实构建器生成明确标为2026-09-30收盘确认的验收预览，status=OK。
- 单独查询更新摘要新鲜度证明中的面板文件mtime，曾使旧整组清单的三个summary哈希失效；门禁正确拒绝。本次随后再次运行完整refresh-all和独立check通过，未手写清单或放松验证。该过程未改变持仓、费用、收益或NAV。
- 本地日报限时专项173 PASS，33.27秒；日报仓库专项156 PASS及38个subtests PASS。
- 云端交易日检查、自动化回归、策略回归、状态刷新、整包恢复到隔离目录、三个正式信号、整组交付打包和日报生成均成功。云端XML读回为自动化127 PASS，策略448 PASS、1 SKIP、0 FAIL、0 ERROR。
- 该SKIP是冷启动回归阶段尚无真实v2.3产物的可选身份检查；之后工作流真实生成了v2.3最终产物，已在下载工件上独立验证真实身份和执行字段。
- 云端摘要status=OK，publication_mode=close_confirmed，signal_date=2026-09-30。SMTP、发送意图、收据和送达标记步骤全部skipped。本次无邮件投递，也无实盘操作。

## 最终工件核对

下载本次运行的 `microcap-whole-delivery-validation-state`，独立验证整包14982项文件哈希及完整清单，并逐日核对三版真实成本净值。源码和全部底层输入内容哈希与本地完全一致。三版每行版本、持仓、下一持仓、执行规模、费用、净收益及净值均相同，六项数值字段最大绝对差均为0。最终CSV的版本修订、交易状态及带日期的成员行动字段也一致。

| 版本 | 最终收益流行数 | 末日 | 版本修订 | 成员行动 |
|---|---:|---|---|---|
| v2.0 | 4033 | 2026-09-30 | plain_mom16_fixed1_20260904 | actionable=False；signal=2026-09-17；execution=2026-09-18 |
| v2.3 | 3988 | 2026-09-30 | plain_lb25_hl2p5_r2off_vol10_26_20_20260904 | actionable=False；signal=2026-09-17；execution=2026-09-18 |
| v2.5 | 3988 | 2026-09-30 | plain_lb20_hl3_entry0_exit0_20260905 | actionable=False；signal=2026-09-17；execution=2026-09-18 |

上述成员变更日期是历史验收字段。当前成员集合与信号日期分开保存，历史名单变化未作为今天的交易指令。

面板8738行、代理指数4067行、基础成本净值4050行均截至2026-09-30；换手表为432条事件，最后实际事件为2026-09-17。正式刷新前后的三版逐日历史净收益、净值和成本差异均为0，历史改写审计均为clean。

## 冷启动与恢复

恢复发布：[microcap-audit-20261007](https://github.com/liuruojiang/microcap/releases/tag/microcap-audit-20261007)。对应整包 `microcap-approved-state-20260930-501b269b.zip`，70,850,779字节，SHA256=`d6c5e5833d3384fb9990c79567315f7e845b8dacfd4e7b6a651c9c0aa5a12fbe`，已与GitHub资产digest独立核对。

同步前61个相关源码、状态及最终文件已逐一备份并核验，位于 `.codex_backups/20261007_213127`。日报仓库被修改的工作流、配置及测试另有逐文件备份。本地定时任务原配置保存在上述备份中的 `local_automation_before.json`。恢复仍需匹配源码、批准authority及整组状态，再执行正式整体门禁。

## 验收范围

本次完成代码、状态和日报生成链的同步与验证。2026-10-07为已确认休市日，正常定时日报仍按休市规则停发；手动维护运行使用validation_only，不生成当日实时交易指令。后续正常交易日仍须通过动态交易日、前收盘锚点、当日报价和ST门禁；本次未进行邮件实际送达或成交认证。

完整证据见 [final_acceptance.json](../research_reports/20261007_remote_daily_sync/final_acceptance.json)、[云端逐日比较](../research_reports/20261007_remote_daily_sync/cloud_readback_comparison.json)、[本地最终检查](../research_reports/20261007_remote_daily_sync/local_final_check.json) 及 [前后历史对照](../research_reports/20261007_remote_daily_sync/local_historical_parity.json)。
