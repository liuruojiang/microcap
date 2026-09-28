# L1 旧代理执行与收益全样本回放

## 结论

在保留已批准的 2026-09-03 有效持仓续接点后，432/432 次正式换仓的 `entry_count`、`exit_count`、`blocked_entry_count`、`blocked_exit_count`、`holding_count_after` 五项逐行一致；4064/4064 个代理交易日的 `daily_return` 一致，最大绝对差 `9.974659986866641e-17`。因此当前正式代理的**旧算法算术**可以由保存目标、真实 raw 收盘缓存、交易限制和已批准续接持仓复得。

该结果不证明历史候选全集、ST 标签或公司行为收益正确。独立 ST 公告核证已发现目标污染；raw close 未全面调整分红、送转。这些 L1 数据缺陷不会因旧算术对账通过而消失。

## 回放边界与续接点

- 初始状态为另一个诊断中重建的 2010-01-04 实际 100 只成员，源文件 `../corporate_actions/initial_2010_01_04_executed_seed.csv`；目标为正式 `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv`。这两项连同现有真实价格、股本与 ST 缓存，回放至 2026-09-03 前。
- 2026-09-03 起使用先前**已批准**的有效成员续接点，来自 `outputs/repair_20260917_cloud_evidence/cloud_20260916_whole.zip` 中的 `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv`，SHA-256 为 `9558082806163587d9813100bd19cafe92337a2c086e3fc8ac6aa8a71e5825ee`。审批链在 `research_reports/20260917_delivery_repair/candidate/user_approval.json` 与 `exact_hash_migration_review.json`；批准范围为冻结至 09-03、纠偏 09-04 至 09-17。这个先前批准仅解释现存正式曲线，**不批准本次 L1 的新历史改写**。
- 如果只沿保存目标从 2010 年连续回放，09-03 的 100 只与已批准的正式续接成员各差 21 只。这会造成 09-04 至 09-17 的 10 个日收益不一致，首日 09-04 差 `-0.0008770405956769383`；09-17 换仓会被误算成各 17 只而非正式各 19 只。注入精确续接点并补载该点独有的 `000995` 后，正式尾段一致。这是旧 lineage 的明确边界，不能把目标表机械串接后称为正式全历史实际持仓。
- 连续回放在 2010-01-05 至 2026-09-03 的 4049 个日收益吻合；09-04 至 09-17 由上述精确续接点再现 10 日；09-18 至 09-24 再现 5 日。换仓表 431 行截至 09-03，最后 1 行在 09-17。

## 涨停反例与执行回放

旧快速重建仅在换仓日及前一个市场交易日取样，遇长时间停牌会丢掉该股前次真实成交价。`600862` 于 2014-02-28 收 3.03，此后停牌，2014-09-18 复牌收 3.33，为按前次真实成交收盘价计算的 10% 涨停。该股在 09-18 目标第 74 名，原持仓中没有；正确回放的 `buyable=false`、`blocked_entry=true`，未买入。稀疏日历会误放行，之后引起 16 次换仓计数差异。此审计按每只股票的真实前次成交日期构建回放日期，再调用项目正式交易限制函数。

## 已证 ST 错误目标的实际进入情况

把 `../st_timing/certified_st_target_lower_bound.csv` 的 `(target_date, symbol)` 与本次回放有效成员的 `(rebalance_date, symbol)` 做唯一键一对一左连接：250 行、18 只经官方公告另行核证的错误目标中，235 行、18 只进入该日有效持仓，首例 2010-01-14。其余 15 行中有 5 行因正常交易限制等原因未进入；2026-09-03 的 10 行则不在已批准正式续接成员内。所有 235 行均早于 09-03 续接点，续接不会改变这些已进入的计数。连接明细在 `certified_st_effective_join.csv`。

这个连接证明错误目标确实进入旧代理的持仓状态；不单独量化它们造成的净值偏差，也不把回放自身当作 ST 身份的证明。ST 身份依据另一个审计的官方公告证据。

## 产物和方法限制

- `replay.py`、`summary.json`、`turnover_comparison.csv`、`members_by_rebalance.csv`：换仓与有效成员只读回放。它复用了项目的 `load_symbol_cache` 与 `apply_trade_constraints`，因此属于输入和状态链复核，并非完全独立实现交易规则。
- `check_all_returns.py`、`all_daily_returns.csv`、`all_returns_summary.json`：按有效成员另行从 raw 收盘价格日历计算全部 4064 日收益。历史入选股票没有前复权缓存；这里重算的是现有旧代理的 raw 收益口径。
- `check_key_returns.py`、`key_return_components.csv`：2014-09-18、2015-05-25、2015-07-10、2026-09-24 的成员逐股明细及关键日复算。2015-05-25 包含转增后的 `300092`，忠实复得旧代理值并不代表经济收益正确。
- `check_st_membership.py` 与对应 CSV/JSON：错误 ST 目标到实际有效成员的连接。

所有产物仅在本诊断目录；正式代码、缓存、代理和 NAV 未改写。L1 仍应判 FAIL，待缺失退市数据、公司行为与 ST 来源修复后重建候选并重新认证。
