# Top100 Power Momentum Method v2.4

Date: 2026-05-17

## Purpose

This note records the Power momentum signal formalized in Top100 microcap v2.4 so other strategies can reuse the method without copying temporary scan scripts.

## Signal Definition

The signal is calculated on the always-on 1:1 hedged spread curve:

- spread daily return = microcap daily return - 1.0 * CSI1000 hedge daily return - futures drag
- spread NAV = cumulative product of 1 + spread daily return
- Power score = annualized power-weighted average of spread daily returns

Formal v2.4 parameters:

- signal hedge ratio: 1.0
- execution hedge ratio: 0.8
- lookback: 20 trading days
- power: 0.75
- annualization days: repo trading-day constant from v2.0 target-vol module
- entry rule: score > 0
- exit buffer: remain active while score >= -0.18 after entry
- peak-decay derisk: decay ratio threshold 0.35, recovery threshold 0.40, derisk scale 0.0
- target volatility: 20%
- target-vol window: 60 trading days
- max leverage: 1.5x
- scale rebalance threshold: 10%
- cost model: official embedded transaction cost, target-vol scale-change cost, and financing cost above 1.0x

Weights are oldest to newest:

```text
w_i = i^0.75 / sum(j^0.75), i = 1..20
```

The most recent day receives the largest weight, but the curve is less front-loaded than linear weights.

## Why It Is Not Just A Better Raw Signal

In the cost-only raw layer, Power is not better than the v2.3 log-WLS signal. The reason v2.4 works is that the Power score gives the later overlay stack a better decay path:

- The raw Power signal is noisier near zero and needs the 0.18 exit buffer.
- The peak-decay layer benefits from the Power score's pullback shape, cutting weak trades more effectively.
- After peak-decay, the return stream has lower drawdown and better risk quality, so target-vol scaling can add exposure more efficiently.

This means the method should be reused as a signal-plus-overlay design, not as a naked `score > 0` replacement.

## Validation Summary

Validated from the current repository code and local embedded data through 2026-05-15. All performance numbers use `return_net`.

Power v2.4 risk-constrained candidate:

- raw signal: `power=0.75`, `lookback=20`
- overlay: `buffer=0.18`, `decay=0.35`, `recovery=0.40`, `target_vol=20%`, `scale_threshold=10%`

Comparison against formal v2.3:

| Window | v2.3 Ann / DD | v2.4 Power Ann / DD |
|---|---:|---:|
| 10Y | 32.75% / -13.50% | 38.52% / -13.07% |
| 5Y | 36.23% / -13.50% | 41.10% / -13.07% |
| 3Y | 39.33% / -10.65% | 45.87% / -10.68% |
| 1Y | 18.05% / -10.48% | 31.95% / -10.68% |

The return-max version with 35% target volatility was rejected because drawdown widened to about -15.55%.

## Reuse Guidance

Use this method when a strategy has:

- a clean spread or relative-strength return series;
- an overlay layer that reacts to score decay from recent peaks;
- a target-vol or exposure-scaling layer after signal-quality filtering.

Avoid using the Power score alone as a final selector without retesting costs and exit behavior. The raw signal can underperform log-slope methods before the overlay stack.

## Repository Artifacts

- Formal script: `microcap_top100_mom16_biweekly_live_v2_4.py`
- Ridge scan: `quant_param_scan_runs/20260517_microcap_top100_v2_3_power_raw_raw_signal_power_wma_ridge_width_cost_only/`
- Layered scan: `quant_param_scan_runs/20260517_microcap_top100_v2_3_power_raw_layered_overlay_power_p0p75_lb20_buffer_decay_targetvol/`
