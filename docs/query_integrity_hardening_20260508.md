# 微盘查询一致性整改记录 2026-05-08

## 背景

本次整改源于 `1.6 + 1.8` 最近 1 年净值叠加图中，`v1.6` 回撤显著大于预期。复核后确认问题不是单一图表，而是查询入口缺少统一的数据血缘和一致性校验，导致旧输出、旧调仓流水、错误日期解析或历史净值回写都有机会被静默用于图表和表现查询。

## 已确认的问题

1. `表现 1年` 这种自然语言查询没有被稳定解析为最近 1 年窗口，存在落到全样本或非预期日期窗口的风险。
2. 表现输出缺少强制校验：没有统一确认 `return_net/nav_net`、源数据起止日期、窗口行数、重复日期数量。
3. `v1.0` 查询中 proxy 指数已经到 `2026-05-08`，但成本净值停在 `2026-04-30`。旧入口会尝试回落到慢全量刷新，实际查询可能长时间卡住。
4. 成本净值补尾曾从 `run_signal` 临时日期序列推导调仓日，和官方 proxy turnover 的调仓口径可能不一致。应以 `proxy_turnover.csv` 的 `rebalance_date` 为准。
5. `v1.6` 当前官方文件与旧备份之间存在历史行差异。整改后会阻止未来静默历史回写，但本次没有把旧备份强行恢复为真值。

## 代码整改

- `microcap_top100_mom16_biweekly_live.py`
  - `parse_date_range()` 支持裸写 `1年 / 1个月`，并把 `表现 1年` 解析为以源数据最后交易日为锚的最近 1 年。
  - `build_performance_outputs()` 统一调用 `validate_performance_frame()`，拒绝重复日期、缺列、空窗口，并写出 `performance_summary_manifest.json`。
  - `handle_performance_query_fast()` 在表现查询前先尝试轻量成本净值补尾，避免不必要的全量慢刷新。
  - `find_missing_cost_rebalances()` 优先读取官方 `proxy_turnover.csv` 的 `rebalance_date` 判断补尾期间是否跨调仓日，避免日期序列起点差异造成误判。
  - `assert_no_historical_rewrite()` 用于拦截历史净值被重写，必要时落审计 CSV。

- `microcap_top100_mom16_biweekly_live_v1_6.py`
  - 官方 `v1.6` costed NAV 写盘前增加历史回写防线。

- `microcap_top100_mom16_biweekly_live_v1_8.py`
  - 官方 `v1.8` costed NAV 写盘前增加历史回写防线。

- `tests/test_microcap_query_integrity.py`
  - 覆盖最近 1 年解析、重复日期拒绝、历史回写拒绝、`v1.6` target-vol 成本前收益口径，以及 proxy turnover 调仓日优先级。

## 验证结果

运行命令：

```powershell
python -m unittest discover -s tests -v
python -m py_compile microcap_top100_mom16_biweekly_live.py microcap_top100_mom16_biweekly_live_v1_6.py microcap_top100_mom16_biweekly_live_v1_8.py tests\test_microcap_query_integrity.py
python microcap_top100_mom16_biweekly_live.py "表现 1年"
python microcap_top100_mom16_biweekly_live_v1_6.py "表现 1年"
python microcap_top100_mom16_biweekly_live_v1_8.py "表现 1年"
```

结果：

| version | source | start | end | rows | total return | max drawdown |
| --- | --- | --- | --- | ---: | ---: | ---: |
| v1.0 | costed | 2025-05-08 | 2026-05-08 | 240 | 8.7490% | -13.4396% |
| v1.6 | costed_v1_6 | 2025-05-08 | 2026-05-08 | 240 | 16.7353% | -18.9350% |
| v1.8 | costed_v1_8 | 2025-05-08 | 2026-05-08 | 240 | 15.6826% | -21.2998% |

三个查询的 manifest 均显示：

- `return_column = return_net`
- `nav_column = nav_net`
- `duplicate_date_count = 0`
- `window_start_date = 2025-05-08`
- `window_end_date = 2026-05-08`
- `window_rows = 240`

## 留存输出

- `outputs/microcap_v1_6_v1_8_last_1y_overlay_20260508.png`
- `outputs/microcap_v1_6_v1_8_last_1y_overlay_20260508_summary.csv`
- `outputs/manual_v1_6_backup_vs_current_historical_rewrite_audit.csv`

## 当前解释

当前官方 `v1.6` 最近 1 年最大回撤仍是 `-18.9350%`。这不是新图表计算错，而是当前官方 `v1.6` costed NAV 本身给出的结果。由于已发现它与旧备份存在历史行差异，后续如果要判断“应该采用旧备份口径还是当前口径”，需要单独做一次数据血缘修复，不应在查询整改里静默恢复。
