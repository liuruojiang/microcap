# 第三轮三版独立数学与账本审计

结论：本轮独立数学及必要防御回归 **170 PASS / 0 FAIL / 0 SKIP**，包含新增 25 项实际构建链测试。修复前新增套件为 **22 FAIL / 3 PASS**；主 Agent 修复后，22 项损坏反例全部按相应合同阻断，3 项正常边界继续通过。三版正常同批结果与本轮修复前完整冻结依赖栈关键列精确一致，独立价格、状态、费用账本重新通过。失败均来自隔离的人工损坏，不表示真实正式 NAV 曾发生这些损坏。本子 Agent 没有修改生产或正式产物。

仅审 v2.0 / v2.3 / v2.5，未涉及 v2.6、参数扫描、历史修正、策略晋级、上线、发信或下单。生产源码、正式 outputs、authority、缓存均只读。新增源码仅为 `tests/test_round3_math_ledger.py`，证据限本目录的 `math_*`。测试用 `python -B` 与 pytest `-p no:cacheprovider`；没有删除目录。

## 新鲜度与正式边界

本轮父 Agent 已实际尝试正规 `top100_delivery.py refresh-all`，因本地 core/authority 与远端 release 不同而阻断。整组 delivery manifest 保持 blocked，未绕过。本数学复核先等其新鲜度凭证，再按 `baseline_and_freshness.json` 独立读回 7 份真实文件的日期、行数和原始 SHA256；它们与同日真实刷新后的原件完全相同，日频流终点为最新完成收盘 2026-09-30。turnover 是 432 个调仓事件，最后真实事件为 2026-09-17，不补造假期事件。数据仅用于隔离 diagnostic，不宣称正式刷新、同步或邮件交付成功。

| 输入或收益流 | 行数 | 末日期 |
|---|---:|---|
| panel | 8,738 | 2026-09-30 |
| proxy index | 4,067 | 2026-09-30 |
| turnover | 432 | 2026-09-17（事件） |
| base costed NAV | 4,050 | 2026-09-30 |
| v2.0 正式 NAV | 4,033 | 2026-09-30 |
| v2.3 costed NAV | 3,988 | 2026-09-30 |
| v2.5 costed NAV | 3,988 | 2026-09-30 |

## 已复现遗漏

反例进入真实 `build_v2_3_result`、`build_v2_5_result` 或 v2.0 的实际 fixed-exposure dispatch，而非只检查独立的薄弱辅助函数。通过 monkeypatch 损坏上游输出，函数调用结束自动恢复；费用、日期与影响基于同批真实价格和事件。以下倍率是反例产物相对于正常同批产物的末 NAV 比率，仅用于证明损坏影响，不是策略业绩。

| 遗漏 | 最小反例与实际影响 | 已独立验证的修复 |
|---|---|---|
| v2.5 同步改写费用可以保持算术自洽 | 2010-05-28 将 `entry_exit_cost` 0.003 清零，同步更新 total/net/nav，仍通过组件合计与净收益恒等式；末 NAV 倍率 1.0030090270812513。2010-06-17 rebalance 0.00024 清零同样接受，倍率 1.0002400576138224 | 对照真实 gross 状态和事件日期独立推导应付费用，而不止检验组件相加 |
| v2.0 / v2.3 信任可损坏的映射费用 | v2.0 2010-03-11 0.00054 清零，倍率 1.000540291757547；v2.3 2010-07-01 0.00066 清零，倍率 1.0006604358876856。v2.3 同一费用延后一日，末 NAV 相同但单日净收益错误 | 在实际费用消费者中验证映射结果与原始事件合同；不能重复调用同一个已损坏 mapper 来充当独立证据 |
| v2.3 正有限价差 NAV 未在防御输入处核对 | 在原过热退出日 2020-09-10 将 gross.spread_nav 改 NaN / 文字，`pct_change().fillna(0)` 将坏值变成零波动；10 个实际持仓日改变，倍率 1.0093397772513324。非正及 inf 也被接受 | 正式 gross 必须保留完整日期、合法状态、正有限 spread_nav；有效暖期外缺失不能充当恢复信号 |
| 状态来源不受合同保护 | v2.5 cost 模块能更换 holding / next_holding / signal_on，真实 cash 日被写成 active 但无交易费用；v2.3 2010-08-02 动量退出日的 NaN / 外版 / 未知 next_holding 被填补或解释为 active，退出延后一天，单日净收益差 0.003009844021648833 | v2.5 成本依赖严格保留 gross 的执行状态；v2.3 正式构建只接受其合法状态并核对信号 |
| 索引一致检查仅覆盖 costed/gross | 从真实 gross 依赖删去无费用事件的 active 日，两版仍输出 3,987 行：v2.3 漏 2010-07-05 净收益 0.0034583093364737305，倍率 0.9965536093484926；v2.5 漏 2010-06-25 净收益 −0.021884838378879334，倍率 1.0223745007107468。上游 official index 漏首日也能静默缩短样本 | 正式 gross 输出与请求的 common_index 完整相等；official overlap 不得用缩短起点掩盖有效信号日期 |
| v2.3 批准的两项费率不属于依赖兼容合同 | 将 ENTRY_COST 或 EXIT_COST 0.003 改为 0，`validate_v2_0_contract` 仍通过，实际构建漏费；v2.5 已有对应常量合同 | 在 v2.3 正式依赖合同检查批准的 ENTRY/EXIT 0.003；保留底层研究成本模型的用途 |

完整原始失败证据：[math_before_complete.log](math_before_complete.log)、[math_before_complete.xml](math_before_complete.xml)、[math_faults.jsonl](math_faults.jsonl)。最初 20 项初跑、追加 v2.0 及索引/状态、费率合同的单独日志也保留。反例记录包含加载源 SHA、真实变动日、净值/费用/状态变化及算术自洽结果。

主 Agent 新增 shared `scripts/top100_data_contracts.py`，在实际消费者对照原始事件验证费用，在 v2.5 成本合同对照真实执行状态核对 entry/exit/rebalance，并保护状态来源；v2.3 校验价差 NAV、合法状态及批准费率，两版核对完整 gross 请求日期和 official coverage。最新 22 项拒绝记录已逐一读回：均由对应费用、状态、spread_nav、索引或批准费率合同拒绝，没有以无关刷新阻断冒充反例通过。

最终 [math_final_suite.log](math_final_suite.log) / [math_final_suite.xml](math_final_suite.xml) 为 **141 PASS**（新增 25 + 第二轮独立数学 68 + 第一轮数学 40 + 既有 v2.3 数学 8）；[math_defense_regression.log](math_defense_regression.log) / [math_defense_regression.xml](math_defense_regression.xml) 为 **29 PASS**。合计 170 项，均无失败、错误或 skip。

这些遗漏在第二轮源上存在；正常输入的独立账本通过，不能覆盖上游同时损坏所有相关字段的合同攻击。第二轮 PASS 未被用作本轮证据。

## 正常路径、边界与暖启动

`--real` 从本轮 7 份文件重新装载并重演。冻结对照为 `.codex_backups/20261007_200436` 的完整 v2.0/v2.3/v2.5 栈，两个旧外层版的 lazy loader 明确绑定旧 v2.0；不是只换旧外层接新基座。独立参考重新执行第二轮另写的成对 WLS 与价格/状态/费用账本函数，不读取其 PASS 或保存结果表。v2.0 重演正式第二次 Mom16 的 4,033 行调用链，未以 4,050 行基础流替代。

| 版本 | 同轮完整冻结栈关键列最大差 | 独立账本最大差 | 正式 CSV 读回最大差 |
|---|---:|---:|---:|
| v2.0 | 0 | 4.336808689942018e-19 | 3.552713678800501e-15 |
| v2.3 | 0 | 4.336808689942018e-19 | 3.552713678800501e-15 |
| v2.5 | 0 | 0 | 1.4210854715202004e-14 |

关键列包括完整日期索引、holding、next_holding、signal_on、执行尺度、费用组件、实际税前收益、return_net 与 nav_net。真实三版有 116 / 94 / 126 个 cash 且有合法费用的交易日；这些费用没有清零。cash 的实际市场收益均为 0，相同实际持仓路径的 costed NAV 不高于 no-cost NAV。正式输入与输出 7 份文件在测试前后 SHA 完全不变。

3 项正常边界测试通过：v2.3 精确 26% 触发、20% 恢复、下一行才切换实际持仓；被阻断现金日市场收益 0，同时保留现金重新入场的 0.003 费用。v2.5 的 close / 明确 next_open 均保留合法现金开平仓费用，不把现金有费当作损坏。

另记暖启动定义：当前正式 v2.3 从 gross 起点计算 vol10，首 9 行为暖期；完整信号前史在 2010-05-05 已有约 7.43% vol10。真实首 10 行完整前史波动均低于 26% 且正式 next_holding 仍为 cash，不构成本轮正常行为差异。更换暖启动定义会改变已批准语义，故本轮只记录，没有参数或历史变更。最终补丁后重新计算的明细在 [math_real.json](math_real.json)、[math_real_final.log](math_real_final.log)；修复前真实重演日志 [math_real_before.log](math_real_before.log) 亦保留。

最终真实重演记录的 5 份相关源 SHA 与回归结束后的当前磁盘源 SHA 已再次独立核对，完全相等：

| 生产依赖源 | SHA256 |
|---|---|
| shared base | `f6408e3b4191de78e8e4d7e14a1fea388d42f6ce55b67de38c92302384cb9710` |
| v2.0 | `87725ea0664ef529e360c6f4f1c9a07924c54f6b7e23462d9f85914ecbecb442` |
| v2.3 | `b933287913acec63f4e0027fd71ce4af4f4e72cc012bba7d58db7d89624eb48f` |
| v2.5 | `a5675e6d9a10379fd3f0ab7ffaac738d9983fa1a4413588b03fc2876667765db` |
| shared data/event contracts | `6c2303e6fa8298fe91320882d5cdd4436fd39fb2e8ca594a67cdec50f97b6288` |

最后一次 shared helper 仅追加 signal_date == anchor 的实时行日期合同，数学与账本逻辑没有再变。主 Agent 通知稳定后，本子 Agent 已实际重新执行最终 --real，绑定该最新依赖源；170 项数学与防御回归的既有有效证据保留，全库最终回归由主 Agent 负责。

## 复现与认证限制

在项目根目录执行：

```powershell
python -B -X utf8 -m pytest tests/test_round3_math_ledger.py -q -p no:cacheprovider --tb=short
python -B -X utf8 tests/test_round3_math_ledger.py --real
```

本报告认证的是已述源码上的隔离数学与合同行为。未认证 point-in-time 成员/ST、企业行为权益、交易日来源本身、停牌/涨跌停成交、成交价格/回执、股票股数、期货张数、保证金或现金账户，也未认证任意未知输入空间。固定对冲和 futures_drag 是源码收益模型假设。历史回写防线、production authority 与正式发布门仍由主 Agent 管理，本轮不据 builder 反例推断已经绕过正式发布门。

Full / 10Y / 5Y / 3Y / 1Y 年化收益与最大回撤均为 **N/A：本轮仅审脚本数学、时序和成本合同，不开展业绩研究或参数晋级**。最终测试、源与证据哈希在 [math_summary.json](math_summary.json)。
