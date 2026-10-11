# L7 独立审计 B：正式交付身份与邮件链（2026-10-03）

**结论：2026-09-30 的远端收盘交付包及三个最终 CSV 可复核，属于旧固定策略 SHA 的历史成功运行；当前本地主工作区整组交付门禁 FAIL。** 云端 run 成功和邮件步骤成功不能证明当前未提交源码已经部署，也不能将旧数据模型升格为历史准确性认证。没有发送新邮件、运行 `refresh-all`、恢复或覆盖任何正式产物。

## 范围与结果

| 项目 | 实测结果 |
| --- | --- |
| 默认主线 | v2.0；同组核 v2.3、v2.5。三个正式信号及最终成本流终点为 2026-09-30。 |
| 本地逐项读回 | `inspect_outputs(..., '2026-09-30')`：16 条所需流，零内部错误；panel 8,738 行、代理 4,067 行、基础 costed 4,050 行、v2.0 4,033 行、v2.3/v2.5 各 3,988 行；turnover 432 条事件，末次 09-17。现有 `top100_delivery_manifest.json` 标记 complete，验证时间 2026-10-02 07:39:14 UTC。 |
| 本地整组清单 | `validate_manifest` **FAIL**：`inputs` 与当前三个策略源码哈希不一致。已保存的流与清单中 `artifacts` 本身无差异。随后实际运行 `python -X utf8 scripts/top100_delivery.py check`，退出码 1，首错 `Local core/authority differs from remote release: microcap_top100_mom16_biweekly_live_v2_0.py`。不能称主工作区已同步。 |
| 远端源码 | `origin/main=7dc9d934f7f24fade5236247db71ec4fd7f18471`；自动化的 09-30 工作流 head `b1158bfe896554198edd51b9f1f2d44fe11ec463`，三个隔离目录均 checkout 固定策略 `db889f7d1517763b3d1ff99c3ae024e338514ce9`。该固定提交是 main 的祖先，main 比它多一提交；两提交间的正式策略源码和冻结 authority 未变，交付脚本 `scripts/top100_cloud_delivery.py` 则增加了下载重试/续传。固定提交中的三源码哈希与本地旧 complete 清单完全相等，但与当前主工作区三源码均不等。[机器核对](CHECKS.json)。 |
| 远端实际运行 | 最后一个收盘交付运行 [36686990486](https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/36686990486)，2026-09-30 `completed/success`，`send` job 成功，三个信号、整组打包、邮件发送、SMTP accepted 工件、送达标记步骤均成功。[run 记录](remote_run_36686990486.json)、[artifact 列表](remote_artifacts_36686990486.json)。10-01、10-02 的同工作流运行虽然总体 success，`send` job 均 skipped；不能当作新邮件或新正式产物。 |
| 远端整包读回 | 下载 `microcap-whole-delivery-state.zip`，SHA-256 `df0f71ccd346ef09a192288a6bd66a3d0031356c0f291f736754f66f708cf28c`；`_verify_bundle_manifest` 严格核过 14,982 文件，包内交付清单 `complete`、日期 09-30、策略 SHA `db889f7d`。包内 v2.0/v2.3/v2.5 三个 `latest_signal.csv` 与同次运行摘要附件的对应 CSV **全字段相等**。[核验数据](CHECKS.json)、[整包](remote_whole/microcap-whole-delivery-state.zip)。 |
| 最终 CSV 身份/动作 | 三 CSV 版本分别为 2.0、2.3、2.5，日期均 09-30；v2.0 多空持有→多空持有，v2.3 空仓→多空开仓，v2.5 微盘多头持有→持有。下一交易日执行比例各为 1.0；成员换仓来源日 09-17、执行日 09-18，`member_rebalance_actionable=False`，因此 09-30 **无当日成员换仓指令**。邮件正文将 v2.3 列为下个交易日开仓，其他两版无操作，与最终 CSV 相符。[邮件正文](run_digest/artifacts/microcap_realtime_signal_digest_2026-09-30.md)。 |
| 邮件证明边界 | 下载的 `smtp-accepted` 工件含邮件元数据、`status=OK`、日期和模式；工作流步骤 `Send Gmail`、`Preserve accepted SMTP receipt`、`Mark digest delivered` 成功。该工件没有独立的 `Message-ID` 或收件箱阅读回执，因此这里只确认自动化发送及接受步骤和工件，不能独立确认收件端已读。[工件](smtp_receipt/metadata.json)、[marker](delivery_marker/run-url.txt)。 |

## 首差与反例

- 当前本地主工作区与旧 complete 清单的**首个阻断**是 `inputs` 中三个源码内容指纹。v2.0 的旧清单 SHA-256 `d86b53a2…`、当前 `e84cefc6…`；v2.3 `5f0ac783…`→`b3ca6503…`；v2.5 `110ae5b3…`→`7b3f942c…`。完整值见 [CHECKS.json](CHECKS.json)。这正是独立 `check` 对远端正式代码的失败项。
- 同日期本地最终信号与 09-30 远端附件按**全字段**比较：v2.0 有 3 个诊断数值差异，首差 `return_gross_base` 为本地 −0.0010923265118282274、远端 −0.00109210888330387；v2.3 有 10 个诊断数值差异，首差 `annualized_log_wls_score` 本地 2.0751617477012、远端 2.0751591770661793；v2.5 零差。三版版本、日期、持仓、动作、执行比例和成员换仓日期字段一致。数值差异表见 [CHECKS.json](CHECKS.json)；不能把“动作一致”写成整个最终 CSV 同字节。
- `realtime_state_bundle.py validate` 只核基础状态；09-30 工作流旧 SHA 的成功、09-30 整包的完整性或 10-02 manifest 的 complete 标记，都不能取代**当前**主工作区 `refresh-all` 后独立 `check` 的通过。当前历史血缘修复仍未获正式迁移批准，因此本审计未尝试重新生成或覆盖正式历史产物。

## 复核命令

```text
python -X utf8 research_reports/20261003_l7_diagnostic/delivery_audit/audit_delivery.py
python -X utf8 scripts/top100_delivery.py check
git ls-remote origin refs/heads/main
gh run view 36686990486 -R liuruojiang/codex-daily-automation-probe --json headSha,event,status,conclusion,createdAt,updatedAt,jobs
gh api repos/liuruojiang/codex-daily-automation-probe/actions/runs/36686990486/artifacts
```

L7 其余批量/增量与恢复逻辑由本层其他独立审计覆盖。本报告是交付链 B 审计，未声称真实订单、券商成交、SMTP 收件箱送达或 L1–L5 历史准确性 PASS。
