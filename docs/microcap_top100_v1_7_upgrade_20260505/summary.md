# Top100 v1.7 升级说明

## 结论

`microcap_top100_mom16_biweekly_live_v1_7.py` 将 2026-05-05 研究中的最新候选参数固化为 v1.7。v1.7 不是替换 v1.6 文件，而是新增独立版本脚本，继续复用 v1.6 已验证的目标波动执行层、交易成本、融资成本和 v1.4 信号质量衰减执行层。

默认 Top100 主线仍按工作区约定保持 v1.0；查询 v1.7 时应显式指定 v1.7。

## 参数变化

| 项目 | v1.6 官方 | v1.7 |
|---|---:|---:|
| 动量回看期 | 16 | 12 |
| 入场阈值 | `momentum_gap > 0.0%` | `momentum_gap > 0.8%` |
| 退出缓冲 | `0.30%` | `0.35%` |
| 信号质量衰减阈值 | `25%` | `25%` |
| 信号质量恢复阈值 | `35%` | `25%` |
| 目标波动 | `25%` | `20%` |
| 波动估计窗口 | 60 日 | 40 日 |
| 缩放调仓触发阈值 | `10%` | `15%` |
| 最大杠杆 | `1.5x` | `1.5x` |
| 对冲比例 | `0.8` | `0.8` |

## 实现口径

- 新增脚本：`microcap_top100_mom16_biweekly_live_v1_7.py`
- 输出前缀：`microcap_top100_mom16_biweekly_live_v1_7`
- costed NAV：`outputs/microcap_top100_mom16_targetvol20_max1p5_v1_7_costed_nav.csv`
- 基准数据：官方默认代理指数 `outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv`
- 收益口径：`return_net/nav_net`，costed
- 比较窗口：`2010-03-04` 到 `2026-04-30`，3926 行，重复日期 0

## 官方口径对比

| 窗口 | v1.6 年化 | v1.7 年化 | v1.6 最大回撤 | v1.7 最大回撤 | v1.6 Sharpe | v1.7 Sharpe |
|---|---:|---:|---:|---:|---:|---:|
| 10年 | 33.35% | 35.09% | -18.85% | -17.61% | 1.95 | 2.23 |
| 5年 | 35.91% | 36.95% | -18.85% | -17.61% | 1.75 | 2.02 |
| 3年 | 47.48% | 49.92% | -18.85% | -15.79% | 2.30 | 2.78 |
| 1年 | 31.92% | 33.72% | -17.53% | -15.79% | 1.65 | 1.75 |
| 全共同区间 | 37.54% | 38.80% | -18.85% | -17.61% | 2.48 | 2.76 |

结果文件：

- `outputs/microcap_v1_6_official_proxy_candidate_vs_official_20260505_windows_compare.csv`
- `outputs/microcap_v1_6_official_proxy_candidate_vs_official_20260505_nav.csv`
- `outputs/microcap_top100_mom16_biweekly_live_v1_7_performance_summary.json`

## 口径纠偏记录

此前 `microcap_v1_6_verified_zero_missing_candidate_vs_official_20260505_*` 文件中的 “official_v1_6” 是在 `verified_zero_missing` 重建代理指数上反事实重算的序列，不是当前官方 v1.6 默认导出。该表把 v1.6 最大回撤放大到约 `-27.37%`，不能作为官方 v1.6 基准引用。

已核验当前官方 v1.6 默认导出的最大回撤为 `-18.8498%`，回撤低点为 `2024-05-13`。

## 验证

- `python .\tests\test_microcap_top100_v1_7_parameters.py`
- `python .\tests\test_top100_proxy_missing_return_weight.py`
- `python -m py_compile .\microcap_top100_mom16_biweekly_live_v1_7.py .\tests\test_microcap_top100_v1_7_parameters.py`
- `python .\microcap_top100_mom16_biweekly_live_v1_7.py`
- v1.7 `return_net` 与此前官方默认代理口径候选序列逐日对齐，`max_abs_diff = 0.0`

## 回滚

v1.7 是新增脚本和新增输出，不修改 v1.6 官方脚本。回滚时删除 `microcap_top100_mom16_biweekly_live_v1_7.py`、本目录文档、`tests/test_microcap_top100_v1_7_parameters.py` 以及对应 `outputs/*v1_7*` 导出即可。
