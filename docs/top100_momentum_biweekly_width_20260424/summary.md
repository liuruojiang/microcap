## 微盘 Top100 参数宽度记录

日期：2026-04-24

### 口径

- 主线脚本：`microcap_top100_mom16_biweekly_live.py`
- 主线参数：
  - `Top100`
  - `16日相对动量`
  - `双周周四`
  - `close` 执行口径
  - 成本来自 `microcap_top100_mom16_biweekly_live_proxy_turnover.csv`
- 数据：
  - `outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv`
  - `outputs/microcap_top100_mom16_biweekly_live_proxy_turnover.csv`
  - `mnt_strategy_data_cn.csv`
- 扫描区间：`lookback = 4..40`
- 基准参数：`lookback = 16`

### 带宽定义

- 严格带：Sharpe 保留至少 90%，且 MaxDD 不比基准差 3pct 以上
- 宽松带：Sharpe 保留至少 85%，且 MaxDD 不比基准差 5pct 以上

### 结果

| 窗口 | 基准16 年化 | 基准16 Sharpe | 基准16 MaxDD | 严格带 | 宽松带 |
|---|---:|---:|---:|---|---|
| 近1年 | 39.23% | 2.543 | -9.53% | `16,18,19,23,24,25,26` | `16,18,19,22,23,24,25,26` |
| 近3年 | 30.91% | 2.069 | -10.61% | `14,15,16,17` | `14,15,16,17` |
| 近5年 | 35.84% | 2.247 | -10.78% | `14,15,16,17` | `13,14,15,16,17` |
| 近10年 | 34.30% | 2.585 | -11.29% | `12,13,14,15,16,17` | `11,12,13,14,15,16,17,18` |
| 全样本 | 32.20% | 2.803 | -11.29% | `11,12,13,14,15,16,17` | `9,10,11,12,13,14,15,16,17,18` |

### 结论

- 微盘 Top100 主线的参数宽度明显好于 A / ADK。
- `3Y / 5Y` 的核心平台很干净，基本围绕 `14~17`。
- `1Y` 有右移迹象，偏向 `23~26`，但 `3Y / 5Y` 还没有跟着整体右移。
- 因此更像“中期主平台稳定，短期出现右移信号”，不是双边都脆的窄尖峰。

### 额外说明

- 仓库里旧的 `microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv` 与当前主线重建结果不一致，不应再作为当前基线。
- 本轮已用主线 `rebuild_costed_nav_from_proxy_turnover()` 口径确认，扫描结果与主线重建一致。

### 文件

- `top100_momentum_biweekly_width_fullscan.csv`
- `top100_momentum_biweekly_width_windows.csv`
- `top100_momentum_biweekly_width_bands.csv`
- `top100_momentum_biweekly_width_validation.json`
- `analyze_top100_momentum_biweekly_width.py`
