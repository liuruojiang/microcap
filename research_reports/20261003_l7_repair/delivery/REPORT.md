# L7 修复：交付链独立只读审计（2026-10-03）

**结论：L7 的本地身份检查器修复尚未形成可验收的正式交付。** 当前策略仓库远端 `main` 为 `7dc9d934f7f24fade5236247db71ec4fd7f18471`，自动化仓库远端 `main` 为 `ded918a16b134f55936ad6269b93632700e76d77`。自动化远端正式 `microcap-realtime-digest.yml` 中 v2.0、v2.3、v2.5 三处 checkout 均固定 `db889f7d1517763b3d1ff99c3ae024e338514ce9`；该旧策略提交是当前策略远端 `main` 的祖先。当前主工作区三份策略源码又各自不同于远端及 10-02 本地 `complete` 清单。因此现在运行 `scripts/top100_delivery.py refresh-all`，会先在 `verify_release` 阻断，不能用旧清单或局部通过的检查冒充修复完成。

## 证据与适用范围

| 检查 | 只读证据 |
| --- | --- |
| 当前仓库身份 | `git ls-remote origin refs/heads/main`：策略 `7dc9d934…`、自动化 `ded918a16…`；两仓 `gh pr list --state open` 均为 `[]`。自动化本地 checkout 位于 `codex/weekly-equity-momentum-boundary-data-fallback`，其工作区 workflow pin 为 `b100646…`，**不是远端 main 的正式配置**，不能直接把本地分支当生产。 |
| 远端工作流 | 通过 GitHub API 读取自动化 `main` 上的 `.github/workflows/microcap-realtime-digest.yml`：三处 `repository: liuruojiang/microcap` 的 `ref` 全为 `db889f7d…`；`validation_only` 输入明示仅核刷新、信号和工件，不发邮件、不写交付 marker；`Send Gmail` 和 `Persist normal-send intent` 均有 `inputs.validation_only != true` 条件。 |
| 当前主工作区 | `research_reports/20261003_l7_diagnostic/root_manifest_diff.json`：三源码与 `outputs/top100_delivery_manifest.json` 中输入 SHA 不同；28 个记录产物 SHA 匹配。当前源码差异为 v2.0 的 2026-07-06 主板 ST 涨跌幅规则与收益指标年化定义，v2.3/v2.5 的收益指标年化定义、v2.5 的回撤峰值处理；不是单纯换行差异。`scripts/top100_delivery.py` 的 `verify_release` 先逐项比远端 main 的源码和 authority，再准许 `refresh-all` / `check`。 |
| 已有正式交付 | 最近有发送步骤的运行是 2026-09-30 的 [36686990486](https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/36686990486)，旧工作流 head `b1158bfe…`、旧策略 pin `db889f7d…`。三版最终 CSV 与同次摘要附件全字段相等，SMTP accepted 工件存在，但无独立收件箱确认。2026-10-01、10-02 的同工作流运行总体 success、`send` 均 skipped；不能写成新邮件。详见 L7 [交付审计](../../20261003_l7_diagnostic/delivery_audit/REPORT.md) 和 [机器证据](../../20261003_l7_diagnostic/delivery_audit/CHECKS.json)。 |

## 使修复成为正式可检查版本的最小顺序

1. **冻结并审阅当前改动。** 保留主工作区已有脏文件与可恢复备份；核定三份策略源码变动、L7 身份检查器及测试究竟属于本次正式修复。不要为消除 SHA 差而回退源码、直接改旧 manifest、复制单份 NAV，或把当前自动化本地分支的 `b100646…` pin 误作正式配置。
2. **先解决策略仓库发布与历史血缘。** 在隔离分支提交经审阅的源码、检查器和必要测试，验证策略主线/权威文件。若刷新将改变旧历史成员、代理或 NAV，生成旧/新精确哈希、逐日差异及原因报告，取得项目规则所需的确切迁移批准，再执行一次受限迁移，随后在**无迁移参数**下重跑并读回三个版本 `historical_rewrite_audit.status=clean`。目前 L1/L2 的 PIT/ST/权益数据问题仍在；不能把本轮 L7 发布解释为这些问题已经解决。策略提交须先进入远端 `main`，且三份正式源码与 authority 的内容指纹可由主工作区核对。
3. **再完成当前本地主目录整组同步。** 核对策略远端 `main` 与本地主目录三源码及 authority 一致；运行 `python -X utf8 scripts/top100_delivery.py refresh-all`，再单独运行 `python -X utf8 scripts/top100_delivery.py check`。两者必须退出 0；读回本次 `top100_delivery_manifest.json` 的 `complete`、最新已确认交易日、远端 release SHA、三版完整最终 CSV/NAV 的身份及动作、所有输入/产物哈希和历史改写审计。失败或中断留下的 `blocked` 清单不能当完成。当天独立刷新证明必须有效；`realtime_state_bundle.py validate` 仅覆盖基础状态。
4. **再更新自动化正式 pin。** 以已进入策略远端的确切提交 SHA 更新自动化仓库正式工作流的三处 `ref`，通过其回归检查后进入自动化远端正式分支。工作流使用固定 SHA，策略仓库单独更新 `main` 不会使邮件流程自动切换。部署前后应分别读取**远端** workflow 文件确认三处 pin 相同且该 SHA 可从策略远端到达，并记录策略 SHA 与自动化 SHA。
5. **最后验收云端，邮件单列。** 先用工作流 `validation_only=true` 验证同版整包、三最终 CSV、authority/历史审计与归档；该模式不发邮件。真正的收盘邮件须在适用交易日、正常交付门禁与同版正式 CSV 读回均通过后才触发；按执行日期解读成员调仓，不能把 09-17/09-18 的历史动作写成 09-30 当前交易。记录工作流 run、策略/自动化 SHA、ZIP/清单及 SMTP accepted；若需证实到达收件箱，还须独立收件证据。旧 run `36686990486` 仅证明旧版本当时的发送步骤。

**独立结论边界：** 上述是读源码、远端 API 和已有运行工件得出的当前阻断与修复顺序。本审计没有 push、建 PR、执行 `refresh-all`、运行工作流或发送邮件；也没有把纸面旧数据判为正式历史准确。
