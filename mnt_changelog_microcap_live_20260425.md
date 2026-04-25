# 2026-04-25 Microcap Top100 Buffer 更新说明

## 正式脚本

- `microcap_top100_mom16_biweekly_live_v1_4.py`

## 变更

- 新增参数: `V1_4_MOMENTUM_GAP_EXIT_BUFFER = 0.0025`
- 入场规则保持不变: `momentum_gap > 0`
- 退出规则改为非对称 buffer: 已持仓时仅在 `momentum_gap < -0.25%` 时退出。
- 基准脚本为 `microcap_top100_mom16_biweekly_live_v1_4.py`: 先在 v1.4 的底层动量信号加入退出 buffer, 再叠加 v1.4 原有 `momentum_gap` 峰值衰减去风险。
- `microcap_top100_mom16_biweekly_live.py` 作为 v1.0 基础脚本保留 `MOMENTUM_GAP_EXIT_BUFFER = 0.0`, 不作为本次 buffer 定版基准。
- Poe 默认版本切换为 v1.4; 输入不带版本号的“参数/信号/表现”默认走 v1.4。

## 研究依据

- 研究脚本: `analyze_top100_momentum_biweekly_buffer.py`
- 输出目录: `docs/top100_momentum_biweekly_buffer_20260425/`
- `buffer=0` 是 v1.4 原始规则; `buffer=0.25%` 与正式 v1.4 输出校验: `max_abs_nav_diff = 0.0`, `max_abs_ret_diff = 0.0`
- 是否采用 `0.25%` 以 v1.4 口径重跑后的 `docs/top100_momentum_biweekly_buffer_20260425/summary.md` 为准。

## 不采用对称 buffer 的原因

微盘股策略是 long/cash 二元开关。对称 buffer 会把入场门槛抬高到 `momentum_gap > +0.25%`, 容易错过反转初段。本次采用只保护退出的滞后带, 用于减少 0 附近抖动造成的频繁进出。
