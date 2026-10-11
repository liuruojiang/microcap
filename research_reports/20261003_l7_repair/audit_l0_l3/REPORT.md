# L0–L3 独立只读脚本审计（2026-10-03）

**结论：尚不能判定前七层的脚本错误已全修。** L2 的三版专用年化指标、v2.5 初值回撤和 v2.0 内嵌的 2026-07-06 主板 ST 10% 限幅已经修复；其余下列纯代码错误仍在当前源码。现有 PR #68 不包含这些修复。此审计没有修改策略、缓存、正式输出或交付清单。

## 待修的代码错误

| 严重性 | 当前精确位置与已见反例 | 最小修法与边界 |
| --- | --- | --- |
| P1 历史 ST 分类 | `analyze_top100_rebalance_frequency.py:427-429`、v2.0 内嵌频率源码 `microcap_top100_mom16_biweekly_live_v2_0.py:2412-2414` 把 `S ST华新` 归成非 ST；相应当前发布门槛 `microcap_top100_mom16_biweekly_live.py:240-243`、v2.0 内嵌基础源码 `:3916-3919` 也漏判。当前实跑 `v2.freq_mod.is_st_name('S ST华新') == False`、`v2.base_mod.is_live_tradable_name('S ST华新') == True`。L1 已记录旧 `000010` 的同类历史简称。 | 采用 [`candidate_code_fix/REVIEW.md`](../../20260926_l1_repair/candidate_code_fix/REVIEW.md) 中已对抗测试的有界前缀正则，同时覆盖独立源码和内嵌源码；保留 `*ST TCL` 命中并避免把 `G STAR科技` 误判。新分类不能自动覆写旧历史名单：先独立重建、比较、制作精确哈希迁移报告。 |
| P1 限价分位误判 | 独立频率源码 `analyze_top100_rebalance_frequency.py:1025`、v2.0 内嵌 `microcap_top100_mom16_biweekly_live_v2_0.py:3050` 先用 float 计算限价，再 `Decimal(str(value))` 取分。真实 000632 2026-04-30 前收 4.10、当日收 3.90、适用 ST 5%：当前实跑计算得 3.89，`is_price_at_limit(3.90,4.10,0.05,-1) == False`。L1 独立重放发现该误判会改变一次成员阻卖和后续收益。 | 先把原前收和限幅分别转 `Decimal(str(...))`，在十进制域相乘后 `ROUND_HALF_UP` 到分；两份实现同时修，针对真实反例及价格/板块边界测试。其历史影响须走迁移门槛。 |
| P1 日历污染仍可进入正式流 | `analyze_top100_rebalance_frequency.py:364-369`、v2.0 内嵌 `:2353-2358` 直接以 panel 日期作为交易日；v2.0 内嵌 `:1249-1257` 还在无 panel 时用 `bdate_range` 代替交易所日历。全量重建入口 v2.0 `:5859-5865` 也直接使用 panel 日期。当前 panel/proxy 均含 2018-06-18 非交易行；L3 独立对抗只去掉该行即在 2018-06-29 翻转 Mom16 信号。交付 `scripts/top100_delivery.py:196-265` 只核最终日期，未核全史逐日会话。 | 复用 `scripts/exchange_calendar.py:22-38` 的 `sessions_for_day(end)`，一次取得覆盖全史的独立交易日集合。先在整组交付检查中对 panel、proxy、base costed 与三版最终 NAV 的**每一日期**验证为真实会话；发现伪行时抛错，不在检查中删行。再在全量重建入口及频率/指数日历装载处对非会话 fail-closed，且不能回退 `bdate_range`。`scripts/index_history_preflight.py:100-118` 已示范同一个独立会话源。加入防护会使当前旧流检查失败，正确后续是经批准的精确哈希历史迁移，而非静默过滤旧 CSV。 |
| P1 历史候选缺口仍静默缩池 | v2.0 内嵌 `list_backtest_universe_symbols` `:2333-2350` 与独立频率脚本 `analyze_top100_rebalance_frequency.py:353-361` 只取 `price_symbols & share_symbols & master_symbols`，无完整候选覆盖断言。L1 已实证 254 只样本内曾上市、现退市 A 股缺双缓存；旧历史目标由 4,975 只缓存可得证券排序。 | 在正式全史重建/交付发布路径要求候选覆盖证明，未覆盖证券按日期和原因列明并 fail-closed；不能把缺缓存当不存在。该代码闸门可以修，但补齐真正的原价、当时可知股本、ST 与上市状态仍需来源数据；当前旧 NAV 不能因为加入闸门而变正确。 |
| P1 未知历史 ST 被当作非 ST | `analyze_top100_rebalance_frequency.py:648-654` 的 `build_st_status_series` 在元数据缺失或 `st_intervals` 不存在时返回全 `False`；`:1213-1250` 用其保留市值候选。v2.0 内嵌同源函数具有相同逻辑。L1/L2 证明公告失败和后期 `600543` ST 区间污染，不能从空区间推断“非 ST”。 | 对正式历史候选引入单独的 `unknown` 状态并要求可核 PIT 区间/简称覆盖；未知时阻断正式认证，不得默认为 `False`。修守卫不等于取得缺失公告。 |
| P2 同仓库路径的限幅规则分叉 | v2.0 内嵌频率源码 `microcap_top100_mom16_biweekly_live_v2_0.py:3024-3037` 已按 2026-07-06 转 10%，但独立 `analyze_top100_rebalance_frequency.py:1000-1012` 仍固定 ST 5%，两条入口对 2026 年主板 ST 的可交易性可能给出不同答案。 | 把经验证的日期/板块分段规则同步至独立源码，测试切换日前后以及创业板、科创板、北交所保持原规则。不能把此同步视为历史 ST PIT 证据。 |
| P2 通用表现口径与新专用口径分叉 | v2.0 内嵌 `hedge_mod.calc_metrics` 在 `microcap_top100_mom16_biweekly_live_v2_0.py:513-520` 仍按首末**自然日**年化；通用 `build_performance_outputs` `:8826-8861` 用它写表现 CSV，旧 `analyze_top100_rebalance_frequency.py:1519-1525` 也调用它。v2.0 新专用 `summarize_returns/yearly` 已改成 `N/244`，两类报告会不一致，短窗首日尤其明显。 | 先确定通用表现入口是否属于当前交付合同；若是，将其指标函数统一到同一 `N/244` 定义，并检查首日、年度窗口、单日输入与五窗读回。此项主要改变指标展示，不应改写逐日净值。 |
| P2 尾端事件起算字段不随续接补齐 | v2.0 内嵌 `simulate_rebalance_path` `:3524-3547` 在样本最后一个调仓日把 `return_start_date` 留空。本地正式 `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv` 的 2026-09-17 最后一笔至今仍为空，但 proxy 已延至 2026-09-30。续接只延价格/NAV，没有补回已知下一交易日的事件起算字段，削弱事件链审计。 | 在已核独立交易日历、已读回下一收益日期且无新调仓的续接路径中，补该事件的 `return_start_date` 并验证等于实际 proxy 后一行日期；旧事件 CSV 更动也须纳入精确哈希迁移，不可猜下一工作日或直接改正式文件。 |

## 已修及未能凭脚本修复的边界

- 已修：v2.0 专用 `summarize_returns/yearly` `:13890-13995`、v2.3 `:1256,1341`、v2.5 `:1469,1474,1553,1555` 采用收益日数年化，v2.5 回撤含初始 NAV 1；v2.0 内嵌 `:2025,3036` 已实现主板 ST 10% 切换。`python -X utf8 -B -m pytest -q -p no:cacheprovider tests/test_top100_2026_st_limit_boundary.py tests/test_top100_l2_metric_contract.py` 实跑 **9 passed**。
- 退市候选原价/股本、PIT ST 公告实施区间、`600543` 2023–2024 与 2026 年污染、公司行为和停牌价格都依赖外部历史证据或账户重建，不能靠局部脚本补丁宣布 L1/L2 PASS。用户豁免的 2010 年早期单股不覆盖这些后期事项。
- L3 同收盘价建名单并同价纸面成交，以及 T 收盘信号让次日全天收益归新仓，是冻结模型与可执行 T+1 收盘账户之间的合同差异。应继续标作纸面诊断；若要可执行结论，应另定义成交时钟、价格和资金账户，不要在 L7 小补丁中暗改正式策略。

## 检查方式

只读核现有 L0/L1/L2/L3 裁决、L1 候选补丁、当前四份基础/主线源码、日历与交付检查源码。当前函数实跑得到 `st=False`、`limit=False`、`round=3.89`、`live_st=True`、`2026_ratio=0.1`。2018 伪行在本地 panel 第 6726 行、proxy 第 2055 行；其信号首差由 [`L3_DECISION.md`](../../20261003_l3_recheck/L3_DECISION.md) 的两条独立路径记录。未执行产物刷新、正式历史迁移或远端操作。
