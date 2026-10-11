# L5 修复：v2.3 固定外生信号绑定（隔离诊断）

## 裁决

正式 v2.3 的信号输入是独立的 Top100 代理指数，并非 v2.3 策略实股账户自身权益。源码在 `microcap_top100_mom16_biweekly_live_v2_0.py:5895-5910` 从基础代理指数载入 `microcap` 收盘；`microcap_top100_mom16_biweekly_live_v2_3.py:508-516,617-691` 以该序列构造 **always-on** 价差 NAV 和动量；过热模块再基于它产生 `next_session_actionable_scale` (`:732-851`)。2026-09-04 的正式参数记录 `docs/v23_plain_promotion_20260904.md` 也如此定义。因此，将策略实际成交权益回馈给下一日信号，反而会改变已批准的 v2.3 策略定义。L5 旧报告称外生 open-loop 本身是缺陷，这个归因应更正。

L5 真正的信号可比性错误：原近期诊断从两条不同资金合同的独立股票种子账户重建各自代理，导致同一正式 v2.3 在 217 个有效日中有 **15 日**目标不同，首差 **2026-02-27**。这可作为资金合同敏感性研究，不能作为正式 v2.3 的共同信号。本隔离修复将同一份冻结正式 v2.3 信号绑定给 `prefunded` 与 `netted`，再各自执行逐股账户；未改生产文件、正式参数或 NAV。

## 绑定与实测

信号唯一来源为 `outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv`，SHA-256 `9aa647b2f661ca3edb65b835870827ff4913b92044064c3ef25eccf7c31c82c5`，3,988 行，2010-05-05 至 2026-09-30。取 2025-10-09 至 2026-09-30 **241/241** 个严格同序日期，日期唯一、无缺日，尺度只取 0/1，并逐日核 `next_session_actionable_scale == (next_holding != cash)`。生成双方兼容 `strategy_replay.run_account` 的 CSV：`date`、`hedge_close`、`v2_3_next_session_actionable_scale`、`v2_3_signal_at_close`，另留分数与来源目标供核查。

| 输入 | 两资金分支的目标差 | 对原新代理有效信号差 | 来源与用途 |
| --- | ---: | ---: | --- |
| 旧分资金代理 | 15 日，首差 2026-02-27 | — | 资金合同敏感性诊断 |
| 正式 ON 绑定 | **0 日** | `prefunded` 19 日、`netted` 8 日；两者与正式源首差均为 2025-11-24 | 修复后的共同正式目标输入 |
| 正式 OFF 绑定 | **0 日** | ON/OFF 49 日目标不同，首差 2026-03-25 | 同一外生代理上的单开关对照 |

正式 OFF 来源是同一 NAV 的 `base_next_holding`，即过热模块之前的 v2.3 核心目标 (`microcap_top100_mom16_biweekly_live_v2_3.py:740-741,831-851`)；L4/L5 已验证把过热触发阈值设为 `inf` 时 `next_holding == base_next_holding`。因此 OFF 仍使用正式 v2.3 的相同代理、25 日信号与时钟，改变的只有过热防御。ON 两文件字节 SHA 均 `68cd8ebd97b988f3a47107ee9fe3dd5ef2ef928adc4f6f1de1f577842bcc6b9b`；OFF 两文件字节 SHA 均 `c2a215823bd21313545c7d3920ea0c360b73da91d8bebf9db606588e12dd9bbf`。逐文件来源 SHA、行数与复算结果在 `binding_result.json`。

复现：`python -X utf8 research_reports/20261003_l5_repair/signal_audit/bind_official_v23_signal.py`。隔离绑定文件分别在 `prefunded/`、`netted/`、`prefunded_off/`、`netted_off/`。传给旧诊断 `strategy_replay.run_account` 时，分别将其 `HERE` 指向对应目录即可；函数实际只读取 `date`、`hedge_close`、`v2_3_next_session_actionable_scale`、`v2_3_signal_at_close` (`research_reports/20261002_l2_full_repair/version_integration/strategy_replay.py:146-160`)。该函数仍以 T 收盘信号冻结指令、T+1 收盘尝试成交。**单有绑定不能声称四账已重跑；须另行读取新逐股订单/权益/保证金账。**

## 拒单与现金目标核查

旧近期账户 `strategy_replay.py:95-143,219-263` 逐笔只记 `booked` 成交、拒单不扣费；目标现金时如还有股票，会在下一有效收盘信号再次冻结卖单。涨跌停或停牌造成的残余股票仍入股票市值，理论空头按实股重置 (`:231-246`)；所以“目标现金但仍持股”是可执行性暴露，不能强制净值清零。买单现金不足也被拒且不会凭空生成股数。当前诊断账本在这点没有找到应改的逐日算术错误。正式旧二值 NAV 在现金目标时不能代表拒单后的实际持仓/盈亏，这是正式回测与逐股账的模型差异，不能通过修改信号绑定掩盖。

`netted` 仅在股票账内轧差；理论互换保证金单列、可为负，3% 影子融资未从权益扣除 (`strategy_replay.py:216-219,269-274`)。这不是已验证的真实期货或经纪商融资合同。应由统一现金/保证金账与真实合约条款修复资金可执行性；固定信号输入不解决 2026-02-27 的资金缺口，也不解决旧 PIT/ST、公司行为和休市伪行。因此全部正式 Full、10Y、5Y、3Y、1Y 年化收益及最大回撤仍 **N/A**。
