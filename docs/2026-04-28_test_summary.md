# 2026-04-28 测试记录

## 结论

今天微盘股这边保留正式 v1.4 主线，不把 v1.6 buffer/target-vol 探索线推进为正式版本。正式同步内容集中在 v1.4 信号输出字段、Poe 版本路由/实时数据路径，以及 v1.4 buffer 文档结果刷新。

v1.4 的正式 buffer 仍使用 `0.25%`，并且已经和官方 v1.4 输出做过精确对齐。

## v1.4 Buffer 主线

- 正式脚本: `microcap_top100_mom16_biweekly_live_v1_4.py`
- 正式 Poe 入口: `poe_bots/microcap_top100_poe_bot.py`
- 基准: v1.4，`base_version=v1.1`，0.8x hedge，peak-decay derisk overlay
- Buffer 规则: 入场仍为 `momentum_gap > 0`；已持有微盘时，只有 `momentum_gap < -buffer` 才退出
- 正式默认: `V1_4_MOMENTUM_GAP_EXIT_BUFFER = 0.0025`
- 验证锚点: `buffer=0.0025` 对官方 v1.4 输出 `max_abs_nav_diff = 0.0`，`max_abs_ret_diff = 0.0`

保留的正式结果在 `docs/top100_momentum_biweekly_buffer_20260425/summary.md`。

关键窗口表现：

| 窗口 | 0.25% buffer CAGR | 相对 base | Sharpe | 相对 base | MaxDD | 相对 base | changes |
|---|---:|---:|---:|---:|---:|---:|---:|
| 近1年 | 40.58% | +5.09% | 3.112 | +0.402 | -8.14% | +0.90% | 175 |
| 近3年 | 40.26% | +1.55% | 2.859 | +0.113 | -12.21% | -0.31% | 175 |
| 近5年 | 45.40% | +1.53% | 3.091 | +0.108 | -12.21% | -0.31% | 175 |
| 近10年 | 39.12% | +1.17% | 3.129 | +0.097 | -12.21% | -0.31% | 175 |
| 全共同样本 | 38.35% | +1.52% | 3.488 | +0.150 | -12.21% | -0.31% | 175 |

解读：`0.25%` buffer 在所有窗口都改善 CAGR 和 Sharpe，最大回撤只比 base 多约 0.31 个百分点；它不是所有窗口的最大收益参数，但作为正式默认更稳，且已完成官方输出 parity。

## v1.6 / fine scan 探索线

今天临时扫过 v1.6 buffer / target-vol 相关探索，但没有推进为正式版本。原因是当前正式使用口径仍是 v1.4 buffer 主线；v1.6 需要独立定义清楚 baseline、目标波动、杠杆上限、成本和 Poe 默认路由后再评估。

已清理:

- `analyze_top100_momentum_biweekly_buffer_fine.py`
- `analyze_top100_momentum_biweekly_v1_6_buffer_scan.py`
- `docs/top100_momentum_biweekly_buffer_fine_20260428/`
- `docs/top100_momentum_biweekly_v1_6_buffer_scan_20260428/`
- `microcap_top100_mom16_biweekly_live_v1_6.py`
- `test_poe_bot_dedicated_versions.py`
- `test_v1_6_output_compatibility.py`
- Python `__pycache__`

已保留并同步:

- `microcap_top100_mom16_biweekly_live_v1_4.py`
- `poe_bots/microcap_top100_poe_bot.py`
- `poe_bots/microcap_top100_poe_bot_v1_4.py`
- `poe_bots/microcap_top100_poe_bot_v1_6.py`
- `test_poe_bot_fetch_universe_spot.py`
- `test_poe_bot_version_selection.py`
- `docs/top100_momentum_biweekly_buffer_20260425/`

修正: v1.4 / v1.6 的 Poe 专用入口脚本必须长期保留，分别作为固定版本机器人运行入口；不能作为临时测试文件清理。

## 验证与同步

验证:

- `python -m py_compile .\microcap_top100_mom16_biweekly_live_v1_4.py .\poe_bots\microcap_top100_poe_bot.py`
- `python .\test_poe_bot_fetch_universe_spot.py`，5 tests OK
- `python .\test_poe_bot_version_selection.py`，29 tests OK

同步:

- GitHub: `https://github.com/liuruojiang/microcap.git`
- Commit: `621da004 chore: sync microcap v1.4 poe updates`
