# L2 固定股数纸面账：引擎状态

## 已完成的近期切片

冻结旧 L1 名单，以 2025-09-18 旧有效 100 只作为 2025-09-30 收盘独立起点，现金 1,000,000 元；2025-10-09—2026-09-30 共 241 个结算交易日。T 收盘冻结小数股数量，T+1 收盘旧股先承担全天价格变动，再按原冻结数量结算。买卖每侧 0.3% 纸面费用。分别输出仅用预备现金买入的 `prefunded` 和允许同一收盘理论卖款净额买入的 `netted`。后一合同不是实际交割证明。

两分支均完成 26 次纸面执行至 2026-09-30，无运行阻断。`recent_*_daily.csv` 保留现金、现金应收、待到账股份市值、逐股市值、总权益、费用、净日收益及仅加回当日费用的同路径单步收益；`recent_*_holdings.csv`、`recent_*_orders.csv`、`recent_*_rights.csv` 保留每日逐股与逐笔证据。`recent_result.json` 包含输入、源码、17 条公开真实成交补价、BaoStock 状态缓存及所有明细的 SHA-256。`recent_audit.json` 独立复核两个分支每天的逐股权益/现金守恒、订单费用、日收益和 003017 派息资格，结果为条件性纸面算术 PASS。

2026-09-29 `003017` 派息登记，`prefunded` 当日未持有，未记该权利；`netted` 当日持有 539.1375240684788 股，2026-09-30 记税前现金 161.7412572205437 元。隔离补价共有 17 个持仓/订单股日，全部发生于 2026-09-18，BaoStock 原始行均为 `tradestatus=1` 且成交量正数；正式 `.microcap_index_cache/prices_raw` 未修改。此为独立近期起点，不能与 2010 年旧账户直接拼接，也不是正式 v2.0/v2.3/v2.5 净值。

复现：

```powershell
python -X utf8 research_reports/20261002_l2_full_repair/engine/check_invariants.py
python -X utf8 research_reports/20261002_l2_full_repair/engine/recent_slice.py
python -X utf8 research_reports/20261002_l2_full_repair/engine/audit_recent.py
```

## 全史隔离回放状态（2026-10-03 封存）

`full_history.py` 从已冻结的 2010 纸面账户续跑，分别结算 `prefunded` 与 `netted`。两分支当前仍 **fail closed，未完成全史**：`prefunded` 最后结算日为 2018-12-14，首阻断 2018-12-17 `non_shareholder_reorg_not_entitlement:2018-12-17:600423`（发行人原件证明重整转增股**不归原股东**，冻结候选不可按普通送转入账）；`netted` 最后结算日为 2018-07-05，首阻断 2018-07-06 `unverified_missing_raw_close:2018-07-06:600232`（停牌期间每 10 股派 1 元，不能把零量参考价当作成交或未经证实的估值）。各分支首差发生在不同持仓路径，不能拼接或以较远分支替代较近分支。

隔离账已处理有原件支持的停牌除权估值、含税现金、待释放送转股和真实复牌盯市；明细在 `full_history_daily_account.csv`、`full_history_daily_positions.csv`、`full_history_settled_orders.csv`、`full_history_rights_events.csv`。除权停牌零量价只作受限估值，不作成交；新增股上市日是纸面可交易释放代理，实际中登或券商到账时刻未证。发行人公告和例外逐项列于 `../data/suspension_ex_evidence/EVIDENCE.md`。旧 600633 股改/停牌推断已在先前复核中撤回，见 `result_superseded_600633_block.json` 和 `research_reports/20260926_l1_repair/st_timing/600633_full_chain/600633_EVENT_CHAIN.md`。

冻结 proxy 中唯一非交易日 2018-06-18 已按独立 BaoStock 交易日历过滤，信号、结算和 `previous_market_day` 使用同一集合；原 proxy 文件保持原样，原 SHA-256 `bf880cd9fc630091e5a16ef32db7f5977b26e57e900941f819a09914a3e64973`，交易日历 SHA-256 `80cfc76c2a0f3ce46251f887103e68d494fb9c154735ee2a7b6df6268b230442`。该日无账、订单或 T+1 执行。

复现：

```powershell
python -X utf8 research_reports/20261002_l2_full_repair/engine/full_history.py
```

本次 `full_history_result.json` SHA-256 `992f5ddbfaa584befa593a221081eb70adefb4b910eba648cd369fa1d8c5896a`；引擎 SHA-256 `e36305d5bdca7b7556bd526f254c4d94fd040c098e0da5fbd4552928cfb1501b`。结果中的 960 项输入、全部明细、BaoStock 查询缓存和引擎代码哈希同批回读均一致。旧 L1 成员/ST/退市及点时完整性缺陷仍在冻结输入，诊断回放不能认证正式历史准确性；正式策略代码、`outputs/` 与正式原始价格缓存未因本次修改。
