# L7 修复独立审计 A：源码、authority 与交付身份差异

**结论（2026-10-03，只读）：** 当前主工作区三份策略源码的未提交修改是 L2 已记录的修复，不应通过回退源码、改写旧清单或拷贝旧 NAV 来消除 L7 报错。冻结尾段 authority 与远端相同。要让现行整组交付门通过，必须先把经审阅的三份源码及本地交付身份守卫发布到策略仓库，再用同一版源码完成有血缘依据的整组刷新和独立 `check`；云端自动化仍固定旧策略提交，若要宣称云端同步，还须单独更新并验证该固定提交。历史成员、交易日历、公司行为和账户缺口不会被这项同步自动修复。

## 精确身份

`HEAD=origin/main=7dc9d934f7f24fade5236247db71ec4fd7f18471`；本次实时 `git ls-remote origin refs/heads/main` 亦返回该 SHA。2026-09-30 交付工作流使用策略提交 `db889f7d1517763b3d1ff99c3ae024e338514ce9`。`db889f7d` 是 `7dc9d934` 的祖先，两提交间下表四个文件均未变。实时读取自动化仓库 main `ded918a16b134f55936ad6269b93632700e76d77` 的 `.github/workflows/microcap-realtime-digest.yml`，确认 v2.0、v2.3、v2.5 三处 `ref` 仍指向 `db889f7d`。哈希口径与 `scripts/top100_delivery.py::sha` 一致：先将 CRLF 归一为 LF，再 SHA-256。

| 文件 | 当前工作区 SHA-256 | 旧 manifest、远端 main 和云端固定提交 SHA-256 | 差异性质 |
| --- | --- | --- | --- |
| `microcap_top100_mom16_biweekly_live_v2_0.py` | `e84cefc6734bf88635e17e0cda3bc5e4bfa7181e7f2922e969c0b219329bdeef` | `d86b53a2e216261af55ab77182c1ba0c10e595732fbe4a5eb0ae47235802240a` | L2 的 ST 涨跌停和收益指标修复；未提交。 |
| `microcap_top100_mom16_biweekly_live_v2_3.py` | `b3ca6503fb561114ef9d673dcb59fe840aec79e3a09fa2aabed5c639c7e39b59` | `5f0ac783d5796ac6742fdbf391603d3bb6b529e784e7aaf89c478e64d69c52d8` | L2 的收益指标年化修复；未提交。 |
| `microcap_top100_mom16_biweekly_live_v2_5.py` | `7b3f942cb9dd218d406d0e382d90ad712a30e9ed31c5c54907964c81dd8ea499` | `110ae5b3334f1bebcf986750090dea9f8d2b7bdd0aeb7ae3414053d5f6909a6b` | L2 的年化及最大回撤初值修复；未提交。 |
| `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json` | `edf169eba5fc37244b683f002e8555a7ace11cae86ae7fb136e40ba51291afc9` | 同左 | 无改动；既有 approved seed / 冻结尾段身份仍为原值。 |

工作区 `git diff` 精确为 v2.0 `+8/-5`、v2.3 `+2/-2`、v2.5 `+4/-4` 行。v2.0 增加 2026-07-06 起主板 ST 10% 价格限制（此前 5%），通过 `get_price_limit_ratio` 被实际价格限制判断函数使用；其余改动将 `summarize_returns` / `summarize_yearly` 的年化分母由自然日改为 `N/TRADING_DAYS`（v2.0 为 `TARGET_VOL_TRADING_DAYS`），v2.5 回撤另将初始权益 1 纳入高水位。差异与 [L2 已实施修复](../../20261002_l2_full_repair/L2_REPAIR_DECISION.md)所记一致；无法从 Git 元数据判定每个未提交字节的个人作者，故归类为**既有工作区修复**，而不是擅自称为用户亲自编辑。

旧 `outputs/top100_delivery_manifest.json` 的 28 项产物指纹与磁盘相等，仅上述三份源码输入指纹不等；`validate_manifest` 因此 FAIL。`refresh-all` 在写入新 manifest 前先运行 `verify_release`，而 `verify_release` 直接与远端 main 对照三份源码及 authority，现状会先在 v2.0 源码阻断。将旧 manifest 重新签名而不重算产物，或把本地源码退回旧版，均不能证成当前修复版的正式输出。

## 保留现有改动的最小修复顺序

1. **保留并备份当前文件。** 锁定三份策略源码、`scripts/top100_delivery.py`、测试和现有未提交 `outputs/` 的完整当前内容；只审阅这组源文件的 diff。工作区另有大量 L1/L2/L3–L7 未提交证据和既有数据变动，不作 `reset`、全库 `add` 或从旧 ZIP 覆盖。
2. **完成源码验证。** 对 L2 三项真实修复和 L7 身份守卫做针对性回归，并保留当前版本的内容哈希。v2.0 的 ST 限制修复可能改变 2026-07-06 后历史代理的准入/成交，所以不能从仅指标公式变更推断旧 NAV 仍等价。
3. **发布精确源码版本。** 在受控分支/审阅合并中只纳入这三份正式源文件、L7 交付校验修复和对应测试，核远端 main 新 SHA 与四个源/authority 指纹；若走独立 worktree，回到这个主工作区验证其现有改动与已合并源码逐字节一致。原有数据及其血缘问题须独立处理，不能靠合并代码自动消除。
4. **重建正式状态时遵守血缘闸门。** 先补足并审阅 L1/L2 的 PIT 成员、休市伪行和公司行为等依赖；历史 lineage 需要精确旧/新哈希迁移报告及获准的迁移执行，随后第二次不带迁移选项完成 `historical_rewrite_audit=clean`。当前证据不足以直接批准或执行整条历史重写。`scripts/top100_delivery.py refresh-all` 不是迁移审批的替身。
5. **整组刷新与独立检查。** 当源代码已上远端且血缘/数据闸门满足后，在主工作区按 `refresh-all`→`check` 顺序执行；读回 panel、proxy、turnover、base costed、v2.0/v2.3/v2.5、最终 CSV 和 manifest 的截止日、行数、哈希，确认最新已完成收盘日及远端源码 SHA。不能把 `realtime_state_bundle.py validate` 或旧 `complete` manifest 当作整组通过。
6. **云端需要另行同版固定。** 自动化工作流三处固定 `ref` 更新为已经可从策略远端取得且验证过的同一新提交；先跑无邮件的验证模式，读回实际 checkout SHA、整包指纹及三个最终 CSV，再按发布权限与用户授权处理正式发送。旧 `db889f7d` 的 09-30 成功运行不能证明新版已部署。当前任务没有授权本审计修改自动化仓库或发送邮件。

## 风险与证据边界

- 源码/远端身份不一致是 L7 本地 `check` 的**直接首错**，而非唯一历史准确性问题。旧最终 CSV 与旧云端附件的动作字段虽一致，v2.0 有 3 个、v2.3 有 10 个诊断字段不同；版本同步后需重算并按同批文件重新比较，不能依旧附件续证。
- 旧正式流截至 2026-09-30。没有本次最新交易日的独立刷新证明；旧数据行数与相同截止日不构成当前新鲜度证明。ST/PIT、休市伪行、公司行为、固定股数/现金以及期货资金问题仍限制正式业绩认证。
- 本报告未修改源码、正式产物、manifest、远端仓库或邮件，也未运行 `refresh-all`/`check`；其“修复顺序”是由当前源码及守卫调用顺序推出，不是已完成部署。

复核：`git status --short --branch`、`git diff --` 三份策略源码、`git show <ref>:<file>`、`git ls-remote origin refs/heads/main`、`gh api repos/liuruojiang/codex-daily-automation-probe/contents/.github/workflows/microcap-realtime-digest.yml`、`research_reports/20261003_l7_diagnostic/root_manifest_diff.json`。旧云端包及逐字段读回证据在 [L7 交付独立审计](../../20261003_l7_diagnostic/delivery_audit/REPORT.md)。
