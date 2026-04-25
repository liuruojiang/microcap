# 2026-04-25 Microcap Top100 Buffer 更新说明

## 正式脚本

- `microcap_top100_mom16_biweekly_live.py`

## 变更

- 新增参数: `MOMENTUM_GAP_EXIT_BUFFER = 0.0025`
- 入场规则保持不变: `momentum_gap > 0`
- 退出规则改为非对称 buffer: 已持仓时仅在 `momentum_gap < -0.25%` 时退出。
- 成本模型保持不变, 仍使用 live turnover table 和 `scan_top100_momentum_costs.apply_cost_model()`。

## 研究依据

- 研究脚本: `analyze_top100_momentum_biweekly_buffer.py`
- 输出目录: `docs/top100_momentum_biweekly_buffer_20260425/`
- `buffer=0` 与 live costed path 校验: `max_abs_nav_diff = 0.0`, `max_abs_ret_diff = 0.0`
- `0.25%` 在 1Y/3Y/5Y/10Y/full_common 均提升年化收益和 Sharpe, 且最大回撤基本不变。

## 不采用对称 buffer 的原因

微盘股策略是 long/cash 二元开关。对称 buffer 会把入场门槛抬高到 `momentum_gap > +0.25%`, 容易错过反转初段。本次采用只保护退出的滞后带, 用于减少 0 附近抖动造成的频繁进出。
