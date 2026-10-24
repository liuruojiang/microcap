# 日报交付链独立审计（2026-10-24）

结论：存在发布配置待同步项，当前不能称为“本地/远端日报均已采用本轮最新版”。未发现本次真实旧整包反例能绕过当前完整状态门禁。仅修改策略远端不足以完成发布；新提交、完整状态包、自动化 pin/seed、测试清单和本地任务覆盖条款必须一起更新。本审计未 push、修改生产任务或发送邮件。

## 当前真实入口与证据

| 层 | 当前实际值 | 核验证据 |
| --- | --- | --- |
| 本地定时任务 | `D:/Codex/home/automations/ic-im/automation.toml`，id `ic-im`，ACTIVE，工作日北京时间14:30，模型 `gpt-6.1-sol` / high，项目 cwd 为 IC/IM 仓库 | `local_automation_readback.json`；只保存必要字段和微盘覆盖条款，未改任务 |
| 本地微盘消费者 | 任务要求从当次 `liuruojiang/codex-daily-automation-probe` 远端 main 固定 SHA 只读取得 `scripts/build_microcap_realtime_digest.py` 并记录哈希；20261015 冻结消费者不是永久入口 | 任务10/15覆盖条款，10/23最新条款仍只明确22文件及15:00修复 |
| 微盘策略远端 main | `1743ecbd74749f97ac82ef15bbd1a736b69f65f7` | 本轮 `gh api` 只读查询，`remote_snapshot.json` |
| 自动化远端 main | `61b5cf0e87dbd71d8fe559d37f8a42c69956db20` | 同上，11份活远端源文件冻结到 `remote_source/` |
| 云端微盘 workflow | `.github/workflows/microcap-realtime-digest.yml` 三 checkout 仍固定 `35327072c1872f964bba3e74d84036090ec0e68d` | workflow 186/194/202 行 |
| 云端触发 | Cloudflare `china-post-close-digests`，微盘 UTC08:00 即北京时间16:00；UTC12:00即20:00是IC/IM | `worker.js` 2–4行，`wrangler.jsonc` 7行；这里仅核Git配置，未读取Cloudflare运行面板 |
| 默认完整恢复包 | 10/15 release，SHA `b099ade27babde77c6d15f52a1beb1a417ea291387fb8ea2a28023270c590684`，旧 pin `35327072...` | `config/microcap_approved_state.json` 3–6行 |
| 最新正常日报 | run `39921723033`，10/23，旧策略 pin，workflow成功；intent、SMTP receipt、delivery marker齐全 | `latest_run*.json`，`latest_smtp_receipt_readback.json` |

10/23 SMTP receipt 显示 `smtp_accepted`，接受时间 `2026-10-23T08:09:44+00:00`，实际正文、主题和metadata SHA绑定全部吻合。这里只验证SMTP接受与工作流标记，本轮未核 Gmail INBOX，未把历史投递当作本轮新版本投递。

## 实际阻塞与最低修复项

1. **云端仍运行旧代码/旧历史种子。** 更新三个策略 checkout 到新正式提交，并同步 `config/microcap_approved_state.json` 中 strategy_sha、state_url、state_sha256、state_date。新包须在新正式策略 commit 后由真实 `top100_cloud_delivery.py pack` 生成，记录 release_sha 与完整 inputs/artifacts；不可用旧 HEAD 标记脏源码包。包括新600241 metadata、六基础文件、当前authority、三版最终流；不要把本轮 migration 报告作为日常开关。
2. **云端回归清单遗漏新验证。** 当前 `delivery-regression.yml` 123行开始为显式29文件、120秒；至少缺 `test_close_confirmation_1500_20261023.py`、`test_realtime_quote_integrity_20261024.py`、`test_st_notice_state_machine_20261024.py`，应由主/回归 agent 核定完整当前清单，并带上其实际依赖如 `synthetic_quote_provenance.py`、`immutable_seed_baseline.py`。本地最新授权为180秒。回归workflow从生产workflow解析策略pin，因此不要另造相互冲突的pin。
3. **本地任务覆盖条款未纳入10/24迁移与修复。** 追加最新正式commit/authority `lineage_revision=2026-10-24-600241-st-category`、批准摘要与新状态恢复规则、完整专项清单、180秒，覆盖旧22文件与旧seed约束；保留当前14:30/ACTIVE/high/cwd与不另发SMTP边界。澄清微盘云端16:00、IC/IM20:00，避免沿用历史统称“云端20:00”。
4. **新包冷启动正向验收仍需发布链执行。** 本轮之前的隔离官方迁移演练证明当前候选能真实生成三版最终CSV并clean，但尚不能代替新release整包在全新runner上的restore→refresh→三版信号→whole-delivery→digest链。发布后先 `validation_only=true` 验收该链；10/23正常marker已存在，普通dispatch可能合法跳过新策略，不能作为新发布验收。

## 恢复链与旧状态风险

真实顺序为：跨提交full raw cache或9/03冷启动缓存→显式批准整包，或当前strategy SHA的verified cache→同SHA durable artifact→配置中的批准release fallback→完整缓存验证→真实独立刷新→状态包→三版本隔离输出→whole delivery→digest→intent→SMTP receipt→完成marker。

- verified-state cache key（workflow 294/299行）及durable artifact（332行）都绑定strategy SHA。换pin后不自动采用旧35327072 verified state。
- full raw cache key（233/235/601行）没有SHA隔离，可能先还原旧600241 metadata。但完整新seed后置恢复会覆写全部metadata；`realtime_state_bundle.validate_security_metadata`（740–768行）校验整个metadata内容指纹，不把旧元数据只作日期可用即接受。若额外/缺失metadata导致不一致，会失败关闭；不要删改指纹来迁就缓存。
- `top100_cloud_delivery.restore`（152–167行）在临时stage验证完整manifest/current core/authority及metadata后才替换；旧包即使下载SHA合法，也不能匹配新源码/authority。
- 普通 `realtime_state_bundle.restore_state` 主要为状态包校验，不是完整发布认证；workflow中的同SHA cache/artifact隔离和最终whole-delivery门禁不可移除。
- 本次新authority保留现有v5兼容version并记录独立revision；原生整组校验与状态包身份需使用最终字节，不能重用变更前manifest。

## 本轮实际反例

`probe_old_release.py` 调用真实活远端 `load_release_config` 和当前 `top100_cloud_delivery.restore`，没有mock或替换行情来源：

- 旧config+35327072正控接受；旧config+1743ecbd拒绝：`Approved release config does not match the checked-out strategy SHA`。
- 已核SHA的原10/15完整包恢复到仅有当前核心源/authority的全新隔离根，拒绝：`delivery manifest mismatch: inputs; run refresh-all`。
- 拒绝前后隔离目标全部文件哈希一致；主outputs、元数据、authority未改变。隔离根与脚本证据见 `old_release_probe.json`。

## 发布验收建议顺序

当前源码专项通过→策略正式commit/push及远端逐文件核对→主目录refresh-all/check与三版最终CSV读回→打完整包并本地全新根恢复验收→发布固定SHA release资产→自动化三pin/config/测试清单更新与回归→无发送云端冷启动验收（固定新seed）→更新本地任务覆盖条款/同源消费者→分别报告代码、恢复、无发送预览与实际SMTP/INBOX状态。不得删除正常去重marker或以correction重发掩盖升级测试。

本报告是当次快照；实际发布时应再次解析远端main，避免其他IC/IM更新覆盖或污染本次微盘最小变更。

## 已完成的授权局部修改（尚未远端发布）

主agent随后授权在新干净worktree `D:/t/m24automation` 实施四文件最小修改，原文件逐字节备份见 `edit_backup_manifest.json`：

- `.github/workflows/delivery-regression.yml`：完整保留原29文件交付组，新增5文件修复组，两个独立pytest子进程分别180秒硬限。合并两个真实JUnit到原 `regression.xml`，拒绝空组和任何skip，原失败退出仍由shell `set -e`执行。
- 新组明确包括 close_confirmation、quote_integrity、st_notice_state_machine、st_notice_release_audit、realtime_commit_guard；完整精确路径和原逐文件耗时见 `regression_groups.json`。最后一个提交时序测试由独立quote审计agent新增，发布前必须确保其已写完、已入新策略提交。
- `scripts/build_microcap_realtime_digest.py`：仅邮件验收模式主题追加 `[新版验收]`，首段明确“本邮件为新版验收”，保留历史数据截至日、历史收盘样本、不作为今日交易指令和test ID。普通日报主题/正文不变。
- `tests/test_delivery_regression_contract.py` 与 `tests/test_microcap_delivery_acceptance.py`：验证分组边界、完整新增清单和新版验收文案。

真实针对验证148项通过、零失败/错误/skip，5.01秒；见 `automation_targeted.xml`、`automation_targeted.log`。未运行云端新策略两组，因为策略新提交/commit-guard测试还在合并；不能把这148项说成云端全链验收。最小差异已保存 `automation_changes.patch`。策略pin/config由主agent负责，本agent没有修改。

### 180秒的精确边界

从此前真实 `full_regression_final.xml` 汇总：完整55文件1645项为507.65秒，testcase累计502.934秒；其中 `test_doublecheck_contract_adversarial.py` 单文件278秒。完整本地回归不能放入一个180秒子进程。本次云端保留29文件的历史testcase合计54.951秒，新组已测旧文件合计10.694秒、另ST新增34项与新commit-guard项需本轮验证。每组180秒有合理余量但须以实际新runner执行确认，不能宣称Windows旧耗时保证Linux必过。

### 原工作区用户脏改动

经主agent澄清，真正需保留的是 `D:/Codex/workspaces/codex-daily-automation-probe`，并非两个D:/动量策略同名checkout。该工作树consumer与email-body tests确有未合入改动，已完整备份原字节及diff到 `preserved_user_changes/`，原文件未动。

这些改动新增“基础信号→过滤→最终”展示及强制DECISION_AUDIT_FIELDS，远端尚无同等实现。其中仍说明v2.0的60日过热锁定、v2.3的R²入场过滤；目前正式plain v2.0没有该overlay、v2.3 R²关闭。不能直接把旧模型解释或旧必填字段迁入本次发布。它们既未丢弃，也未声称已合入；保留供另行按当前正式身份审阅。

## 用户授权的一封新版验收邮件

主agent已转达用户明确批准一封“新版验收、历史收盘样本”云邮件。现有原生模式可满足，无需更改正常去重或新增发送路径：

```json
{
  "validation_only": false,
  "delivery_test_id": "mc24-lineage-acceptance-20261024",
  "publication_mode": "close_confirmed",
  "correction": false,
  "external_schedule": false
}
```

先独立完成validation_only无发送验收，再执行上述唯一test ID。主题格式为 `[发送验收测试][收盘确认][新版验收] 微盘股 v2.0/v2.3/v2.5 日报 - <实际数据日> - mc24-lineage-acceptance-20261024`。正文首段保留“历史收盘日报样本，不作为今日交易指令”。

其marker为 `microcap-v2-close_confirmed-delivery-test-mc24-lineage-acceptance-20261024`，intent和SMTP receipt使用该前缀独立派生。normal `microcap-v2-close_confirmed-digest-delivered-2026-10-23` 不改动；相同test ID重试继续去重。验收模式不写普通verified-state缓存，whole-state工件也用validation名称，避免测试覆盖日常恢复链。实际发送、SMTP receipt内容绑定与INBOX需由主agent执行后另行报告，本agent没有发送。
