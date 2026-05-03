# 微盘 Top100 v1.0 / v1.4 / v1.6 修订记录

日期：2026-05-03  
仓库：`C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略`  
同步提交：`f25d46f3 Finalize microcap v1 overlays and cleanup`  
远端：`git@github.com:liuruojiang/microcap.git`

## 背景

本轮修订来自对 v1.6 目标波动率脚本的外部评审。评审过程中发现，部分工程和成本口径问题并不只存在于 v1.6，也会影响 v1.0/base 与 v1.4，因为 v1.6 的定位是：

`target-volatility overlay on top of v1.4`

也就是说，v1.6 应该以 v1.4 已经完成 momentum-gap buffer、signal-quality derisk、进出场状态和基础交易成本之后的收益流为底层，再叠加目标波动率缩放。最终修订范围覆盖：

- v1.0/base：基础 costed NAV 字段、overlay pre-cost return、signal-quality derisk 成本口径。
- v1.4：signal-quality derisk wrapper、fingerprint、正式/实时信号字段、缓存失效和 summary 说明。
- v1.6：target-volatility overlay 的执行 scale、下一交易日可执行 scale、腿部 turnover 成本、正式/实时信号区分、CSV/date 输出和 fingerprint。
- Poe 路由：同步版本选择和 v1.4/v1.6 查询兼容性。
- 目录清理：删除旧 Top50 生成导出和临时研究产物。

## 关键口径决定

### v1.6 的收益实现源与波动率估计源分离

最终确认：

- realized volatility 估计可以优先使用 `return_raw` / `base_gross_return` 等更纯净的底层风险收益源。
- 实际 P&L 缩放应使用 v1.4 overlay 后、但未扣 v1.6 target-vol 层成本前的收益流。
- v1.6 不应直接用 `return_raw` 作为实际收益实现源，否则会绕过 v1.4 的 momentum-gap exit buffer 与 signal-quality derisk 效果。

因此，v1.6 的实际收益缩放继续使用 v1.4 的 `overlay_pre_cost_return`；若缺失该字段，再按乘法成本口径由 `return_net` 和 `total_cost` 反推。

### v1.4 signal-quality derisk scale 成本

修订前，v1.4 在 signal-quality derisk 从 scale 1.0 降到 0.0 或从 0.0 恢复到 1.0 时，只缩放收益，没有为这次敞口变化计入交易成本。

修订后，在 v1.0/base 的 `apply_momentum_gap_peak_decay_derisk()` 中新增：

- `signal_quality_scale_turnover`
- `signal_quality_scale_cost`

成本模型：

- scale 下降：`abs(scale_delta) * EXIT_COST`
- scale 上升：`abs(scale_delta) * ENTRY_COST`

该模型沿用基础策略的整体进出场成本口径，即 `ENTRY_COST` / `EXIT_COST` 被视为整套微盘多头 + 中证1000对冲腿组合的进出成本。

### derisk 期间 rebalance cost 按有效敞口缩放

后续评审指出，如果 `DERISK_SCALE = 0.0`，策略实盘语义是完全空仓，此时不应继续扣微盘篮子成员 rebalance 成本。

修订后：

`rebalance_cost = rebalance_base_cost * max(previous_execution_scale, current_execution_scale)`

含义：

- derisk 到 0 后，空仓期间不扣成员调仓成本。
- derisk 到 0.5 等半仓状态，成员调仓成本按半仓扣。
- derisk / recovery 当天使用前后较大敞口，避免低估实际调仓摩擦。

### v1.6 target-vol 执行状态机

修订前，`target_vol_scale_next_session` 实际上是 shift 后的当前执行 scale，容易误导实盘执行。

修订后区分：

- `current_execution_scale`：当前行已经执行的 scale。
- `raw_next_target_scale` / `next_session_target_scale`：用当前收盘后 realized volatility 算出的原始下一目标 scale。
- `next_session_actionable_scale`：经过 rebalance threshold 过滤后，下一交易日真正应执行的 scale。
- `raw_scale_delta`：模型原始目标变动。
- `actionable_scale_delta` / `scale_delta`：实际需要执行的 scale 变动。
- `scale_trade_required`：是否需要 scale 调仓。

从 cash 入场时，`next_session_actionable_scale` 不受 rebalance threshold 阻挡；只要下一持仓不是 cash 且目标 scale 大于 0，就会进入目标 scale。

### v1.6 交易成本改为腿部 turnover 模型

修订前，target-vol scale 调整成本近似为：

`abs(diff(execution_scale)) * 10bp`

这把微盘多头 + 中证1000对冲空头误当作单腿资产，低估 scale 调整成本。

修订后使用腿部净敞口：

- 微盘多头：`+1.0 * scale`
- 中证1000对冲腿：`-BASE_HEDGE_RATIO * scale`

当 holding/scale 变化时：

`turnover = sum(abs(new_leg - old_leg))`

v1.6 的 overlay cost 只在持仓不变、仅 scale 调整时计入，避免与 v1.4 的进出场基础成本重复扣。信号字段也明确：

- `next_session_leg_turnover`
- `next_session_leg_cost_est_raw`
- `next_session_overlay_cost_est`
- `next_session_trade_cost_est_type = overlay_only`

### 正式信号和实时信号分离

正式收盘信号：

- `signal_timing = close_confirmed`
- `official_close_confirmed_signal = True`

实时信号：

- `signal_timing = intraday_hypothetical_if_now_close`
- `official_close_confirmed_signal = False`

v1.4 不再使用 `target_vol_signal_timing` 这类 v1.6 专属字段名，避免下游误读。

## 主要文件变化

### v1.0/base

文件：`microcap_top100_mom16_biweekly_live.py`

主要变化：

- 增加 `overlay_pre_cost_return` 统一字段。
- costed NAV 写出时保证 date 列可被 `parse_dates=["date"]` 读回。
- `apply_momentum_gap_peak_decay_derisk()` 增加 signal-quality scale 成本。
- derisk 期间 rebalance cost 按有效 execution scale 缩放。
- 正式/实时信号补齐 `signal_timing` 和 `official_close_confirmed_signal`。

### v1.4

文件：`microcap_top100_mom16_biweekly_live_v1_4.py`

主要变化：

- 增加 `ensure_output_dir()`。
- 增加 `validate_base_hedge_ratio()`，校验 v1.4 的 `BASE_HEDGE_RATIO = 0.8` 与 v1.1/base 底层一致。
- 调整生成顺序：先确保 v1.1 base 输出存在，再做 v1.4 fingerprint/invalidate。
- `current_base_fingerprint()` 增加：
  - `base_hedge_ratio`
  - `v1_4_overlay_engine_version`
  - `signal_quality_scale_cost_model`
  - `signal_quality_rebalance_cost_model`
  - `signal_quality_scale_cost_field`
  - `signal_quality_scale_turnover_field`
- `_file_sha1()` 对缺失文件返回 `"MISSING"`，避免外部单独调用 fingerprint 时报错。
- latest signal 输出：
  - `signal_quality_scale_turnover`
  - `signal_quality_scale_cost`
  - `entry_exit_cost`
  - `rebalance_cost`
  - `total_cost`
- summary 中写明 scale 成本和 rebalance 成本模型。

当前 v1.4 overlay engine：

`2026-05-03-sq-scale-rebalance-cost-v2`

### v1.6

文件：`microcap_top100_mom16_biweekly_live_v1_6.py`

主要变化：

- 新增正式 v1.6 脚本，定位为 v1.4 之上的 target-volatility overlay。
- 修复 `target_vol_scale_next_session` 语义，当前指向可执行下一交易日 scale。
- 新增 raw/actionable scale 拆分字段。
- 新增 next-session turnover 和 overlay-only cost 字段。
- target-vol cost 使用微盘多头 + 对冲腿净敞口 turnover。
- base P&L 使用 v1.4 overlay 后 pre-cost return，volatility source 与 P&L source 分离。
- 写出 CSV 使用 `index_label="date"`。
- `outputs/` 自动创建。
- 实时信号标记为盘中假设，不覆盖正式收盘信号。
- fingerprint 记录 target-vol 参数、成本模型、v1.4 fingerprint 和 hedge ratio。

## 最新回测快照

输出基于 2026-05-03 重新生成的 v1.4/v1.6 官方 costed NAV。

| 版本 | 起始日期 | 截止日期 | 交易日数 | 年化收益 | 最大回撤 | 最终净值 |
|---|---:|---:|---:|---:|---:|---:|
| v1.4 | 2010-02-02 | 2026-04-30 | 3943 | 24.957487% | -12.738344% | 37.260152 |
| v1.6 | 2010-03-04 | 2026-04-30 | 3926 | 37.538077% | -18.850855% | 172.333271 |

v1.6 近两年区间：

- 起始：2024-04-30
- 截止：2026-04-30
- 交易日数：485
- 区间收益：113.229402%
- 年化收益：46.366132%
- 最大回撤：-17.534007%

近两年曲线输出：

`outputs/microcap_top100_v1_6_last2y_nav_curve_20260503.png`

## 清理记录

本轮清理删除了：

- 旧 Top50 生成导出，包括 Top50 NAV、signal、performance、members、compare 文件。
- 旧 Top50 Wind cache CSV。
- 本地运行缓存：`__pycache__`。
- `gcm-diagnose.log`。
- 未跟踪的 EMA gap grid 临时研究脚本和文档目录。
- `outputs/` 中 v1.6 修订过程的临时 impact/before 输出。

保留：

- v1.0/v1.4/v1.6 正式脚本。
- 正式回归测试文件。
- `outputs/` 下当前版本的官方生成产物。
- `.codex_backups/` 本地备份目录。

旧 Top50 删除前备份：

`.codex_backups/20260503_142414`

## 验证命令

本轮关键验证命令：

```powershell
python -m unittest test_top100_forced_stop_loss.py test_v1_4_output_compatibility.py test_v1_6_output_compatibility.py test_top100_realtime_query_fast_path.py test_poe_bot_version_selection.py
```

结果：

`Ran 103 tests ... OK`

编译检查：

```powershell
python -m py_compile .\microcap_top100_mom16_biweekly_live.py .\microcap_top100_mom16_biweekly_live_v1_4.py .\microcap_top100_mom16_biweekly_live_v1_6.py .\poe_bots\microcap_top100_poe_bot.py .\poe_bots\microcap_top100_poe_bot_v1_4.py .\test_top100_forced_stop_loss.py .\test_v1_4_output_compatibility.py .\test_v1_6_output_compatibility.py .\test_top100_realtime_query_fast_path.py .\test_poe_bot_version_selection.py
```

结果：通过。

输出重建：

```powershell
python .\microcap_top100_mom16_biweekly_live_v1_4.py
python .\microcap_top100_mom16_biweekly_live_v1_6.py
python .\microcap_top100_mom16_biweekly_live_v1_6.py 信号
```

Git 同步：

```powershell
git push origin main
```

同步后：

`main...origin/main = 0 0`

## 当前 v1.6 最新信号

信号日期：2026-04-30  
信号类型：收盘确认信号

- 当前持仓：`long_microcap_short_zz1000`
- 下一交易日持仓：`long_microcap_short_zz1000`
- 交易状态：`hold`
- 持仓状态：`hold`
- scale 状态：`hold_scale`
- 当前执行 scale：`1.50`
- 60日年化波动率：`16.8279%`
- 原始下一目标 scale：`1.49`
- 下一实际执行 scale：`1.50`
- 下一交易日腿部 turnover：`0.0000`
- 下一交易日 overlay 成本估算：`0.0000%`
- `official_close_confirmed_signal = True`

## 后续注意事项

- v1.6 是研究/候选实盘脚本，不等同于主组合默认已接入 v1.6。
- v1.6 的成本模型已经比早期更接近微盘多头 + 对冲腿组合，但仍使用统一成本率；未来若要实盘化，应拆分微盘篮子成本、对冲腿滑点、期货基差/展期、融资成本和现金收益。
- v1.4/v1.6 的 fingerprint 已覆盖关键成本模型；以后修改成本口径时，应继续提升 overlay engine version，避免旧缓存误用。
- `overlay_pre_cost_return` 是后续版本叠加 overlay 的优先收益实现源，避免从 `return_net` 反复猜测成本扣除方式。
