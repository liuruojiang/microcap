# L2 范围对抗审计：三版公共输入与独立账户路径

**结论：只核 v2.0 无法给 v2.3／v2.5 的 L2 账户、成本或指标签字。** 三版共用 v2.0 的 Top100 代理收盘指数与换仓成本表，但各自重算信号、执行持仓和净收益。此处是旧输入上的只读、隔离诊断；L1 仍 FAIL，以下算术对齐不认证旧回测绩效，也没有刷新或重写正式产物。

审阅源码为当前 `HEAD 7dc9d934f7f24fade5236247db71ec4fd7f18471`。只读命令：`python -X utf8 research_reports/20260926_l2_diagnostic/scope_challenge/audit_readonly.py`。正式旧文件指纹如下；尾日为旧文件读回日期，不是本次刷新证明。

| 版本 | 正式 costed NAV（均在 `outputs/`） | 行数／首日／尾日 | SHA-256 |
| --- | --- | --- | --- |
| v2.0 | `microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv` | 4032／2010-03-01／2026-09-29 | `96cdd20f32b1bebcc02bc8b51f9c927866bb972a5e91bd0bcbf0e9a64b79c4cb` |
| v2.3 | `microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv` | 3987／2010-05-05／2026-09-29 | `148e06d1203b645d26ea585f0833a89ffcc22f26bea5ada73d9934024a5a097c` |
| v2.5 | `microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv` | 3987／2010-05-05／2026-09-29 | `80833bea717c320009fcd263e4b8a46e305c9b84a961dd8de5d744b83fc492b0` |

## 真实入口和不能外推的路径

| 版本 | 正式函数链 | 执行／成本差异 |
| --- | --- | --- |
| v2.0 | `microcap_top100_mom16_biweekly_live_v2_0.py` 的 `generate_v2_0_outputs()` → 共用 `embedded_context._load_embedded_base_context()` → `base_mod.run_signal()` → `apply_momentum_gap_exit_buffer()` → `apply_v2_0_execution()`；参见 14465–14480、13667–13710。 | 16 日相对动量，微盘多头／中证1000 空头比率 0.8，固定 1.0 执行，关闭目标波动、过热。结果从 `overlay_pre_cost_return`、`total_cost` 得到 `return_net`。 |
| v2.3 | `microcap_top100_mom16_biweekly_live_v2_3.py` 的 `_generate_v2_3_outputs_unlocked()` → v2.0 正式索引＋共用代理／换仓表 → `build_spread_log_wls_gross()` → `apply_overheat_defense()`；参见 1809–1816、617–716、732–877、1132–1150。 | 用 25 日对冲价差 NAV 的 log-WLS 信号，执行时仍为 0.8 对冲；10 日波动过热另有 `risk_off` 状态机，并自行决定持仓和入出／换仓成本。目标波动、空仓利息、融资均关闭（133–149、871–877）。 |
| v2.5 | `microcap_top100_mom16_biweekly_live_v2_5.py` 的 `_generate_v2_5_outputs_unlocked()` → v2.0 正式索引＋共用代理／换仓表 → `build_microcap_log_wls_gross()` → `apply_cost()` → `apply_no_target_vol()`；参见 2017–2024、682–756、1136–1240。 | 20 日纯微盘 log-WLS，信号和执行对冲比率均 0，持仓只有微盘／现金；继续调用 v2.0 嵌入式成本模型，但自己的转换次数、换仓是否计费、现金天数与 v2.0 不同。目标波动、空仓利息、融资均关闭（258–279）。 |

因此 v2.0 的 `return_net` 连乘通过，不推出 v2.3 过热离场／恢复正确；v2.0 的 0.8 对冲收益与资金使用，也不推出 v2.5 的零对冲微盘账户正确。三版皆无逐笔成交、股数、现金余额与对冲保证金的完整账本字段；`nav_net` 是日收益连乘，不能据此独立证明实际自融资和现金守恒。L2 若要按手册验收，应对三条正式流各自做独立事件／账户／指标复算，公共代理可共用一次证据，不能将 v2.0 PASS 外推。

## 最早能复现的时钟与成本边界

`outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv` 第一行是 2010-01-14：`rebalance_date=execution_date=2010-01-14`、`execution_timing=close`、`return_start_date=2010-01-15`。代理在同一日收盘价上排序并模拟换仓（v2.0 源码 4862–4915），这属于收盘后才能确定名单、却标为当日收盘成交的纸面假设；跨越至少多股成员，不是一个股票的个别误差。真实执行无法在获知当日完整收盘后再以已过去的同一收盘成交。正式净值中可见的首次 `cash→持仓` 与当天入场费：v2.0 为 2010-03-01，v2.3 为 2010-06-17，v2.5 为 2010-05-28；三行均为当天 `holding=cash`、`next_holding=持仓`、`entry_exit_cost=0.003`、`return_net=-0.003`。这 **并非单凭扣费即可判会计错误**：它与纸面收盘成交及次日持仓收益的内部时钟一致；问题在能否事前执行该收盘成交，且从日净值看不到真实委托／成交证明。v2.3 的当日过热特征同样由当日收盘价差计算并可改变该日收盘的下一状态（617–732、763–824）；因此对其应单独注入信号延迟反例。

三份旧 NAV 的只读算术检查：全部 `holding[t] == next_holding[t-1]`（跨日错配数各 0）；`nav_net` 与 `cumprod(1+return_net)` 最大绝对差依次为 `2.07e-12`、`1.84e-12`、`5.77e-12`；`return_net = (1+overlay_pre_cost_return)*(1-total_cost)-1` 最大差依次为 `2.92e-16`、`2.78e-16`、`1.06e-16`。这些仅证明旧文件内部算术，不证明应有的持仓、成本发生日、市场价格、现金与权益本身正确。尤其基础代理在 2010-01-14 已有同收盘换仓，比三版净值的首个入场事件更早。

**范围裁决：** L2 可以在旧冻结输入上做隔离的计算基座诊断，并把纸面时钟假设写清；但 L1 错误未修时，旧收益、回撤、成本总量和正式绩效均不能认证。若要 L2 整层 PASS，需先定义可复算的纸面账户边界，分别复核三版信号状态、成本、现金／权益与指标，另将同收盘可执行性判为未证。
