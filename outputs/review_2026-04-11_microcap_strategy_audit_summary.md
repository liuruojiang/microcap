# 微盘股对冲策略评审记录

日期: 2026-04-11

## 范围

本次评审覆盖以下高风险项:

- `P0` 执行时序与收盘成交口径
- `P1` 复权收益链路
- 历史 `ST` 事件识别
- 历史可投资证券池 / 历史证券主表
- `1.0 / 1.1 / 1.2` 实际运行版本产物同步

本次未继续扩展到:

- 分钟级尾盘成交容量建模
- 官方日级 `ST` 状态库
- 全市场退市股历史缓存补全

## 主要修订

### 1. 执行口径修订

- 回测和实盘查询统一为 `T` 日信号、`T` 日收盘执行。
- 若 `T` 日收盘封涨停则不可买入，封跌停则不可卖出。
- 成本落点改为调仓信号日，不再落到 `T+1`。
- `live` 输出中的 `effective_date` 与收盘执行口径对齐。

相关文件:

- `analyze_top100_rebalance_frequency.py`
- `microcap_top100_mom16_biweekly_live.py`
- `scan_top100_momentum_costs.py`

### 2. 复权收益链路

- 回测加载器优先读取 `prices_qfq`，收益和动量改为优先使用复权价。
- 市值和涨跌停约束继续使用原始价，避免混淆成交约束。
- 最近候选池和近 `3Y` 范围的真实 `qfq` 缓存已补齐并验证。

相关文件:

- `fetch_wind_microcap_index.py`
- `analyze_top100_rebalance_frequency.py`

### 3. 历史 `ST` 事件

- 由“当前 `ST` 全样本剔除”升级为“简称变更 + `CNInfo` 公告”双源识别。
- 新增区间合并逻辑，避免重复撤销公告生成重叠 `ST` 区间。
- `security_meta` 升级到 `meta_version = 2`，旧缓存会懒重建。

相关文件:

- `analyze_top100_rebalance_frequency.py`

### 4. 历史证券主表

- 新增 `security_master.csv` 缓存。
- 主表优先使用沪市 / 深市 / 北交所当前清单和沪深退市清单。
- 对公开源未覆盖但本地确有真实缓存的代码，使用缓存首日补 `list_date`。
- 回测 `universe` 变为“主表符号集 ∩ 价格缓存 ∩ 股本缓存”。
- `load_symbol_cache()` 会按 `list_date / delist_date` 切掉上市前、退市后日期。

相关文件:

- `analyze_top100_rebalance_frequency.py`

### 5. 实际运行版本同步

- `1.0 / 1.1 / 1.2` 增加统一版本戳:
  - `research_stack_version = 2026-04-11-p0-p1-history-meta-master-stv2`
- 旧产物会被视为过期并重建。
- 已重新生成 `1.0 / 1.1 / 1.2` 的实际输出文件。

相关文件:

- `microcap_top100_mom16_biweekly_live.py`
- `microcap_top100_mom16_biweekly_live_v1_2.py`

## 关键输出文件

- `outputs/microcap_top100_mom16_biweekly_live_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_v1_1_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_v1_2_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_proxy_meta.json`
- `outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json`
- `outputs/microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_biweekly_thursday_16y_costed_nav.csv`

## 本次实际验证

已实际运行:

- `python -m unittest .\\test_p0_close_execution_timing.py .\\test_p1_adjusted_returns.py .\\test_history_meta_filters.py`
- `python -m py_compile '.\\microcap_top100_mom16_biweekly_live.py' '.\\microcap_top100_mom16_biweekly_live_v1_1.py' '.\\microcap_top100_mom16_biweekly_live_v1_2.py'`
- `python '.\\microcap_top100_mom16_biweekly_live.py'`
- `python '.\\microcap_top100_mom16_biweekly_live_v1_1.py'`
- `python '.\\microcap_top100_mom16_biweekly_live_v1_2.py'`

已确认:

- `1.0 / 1.1 / 1.2` summary 与 proxy meta 均写入新版本戳
- `execution_timing = close`
- `trade_constraint_mode = close`
- `security_meta_version = 2`
- 最新交易日为 `2026-04-10`

## 仍然存在的残余风险

- 历史价格 / 股本缓存本身未必完整覆盖全部退市股，生存者偏差已缓解但未彻底消失。
- 历史 `ST` 仍是公开源推断，不是官方逐日状态库。
- 尾盘成交容量、集合竞价成交能力和冲击成本仍未建模。
- `live` 路径仍以当前池为主，没有完全引入历史主表的全量治理，这属于有意分层，不是遗漏。

## 结论

本次评审中的高优先级口径问题已完成主要修正，当前版本可作为新的研究 / 运行基线继续使用。
