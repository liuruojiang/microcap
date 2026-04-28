# Microcap Missed MFE Diagnostic - 2026-04-29

## Scope

This run tests the MAE/MFE diagnostic tool on microcap strategy cash periods.

- Source data: existing microcap costed NAV exports under `..\微盘股对冲策略\outputs`.
- Window: `2010-02-02` to `2026-04-28`.
- Metric: close-only missed MFE, not intraday high/low MFE.
- Definition: every consecutive `cash` segment is treated as a missed long trade in the strategy's shadow long-microcap/short-hedge spread.
- Shadow spread:
  - v1.0: `microcap_ret - 1.0 * hedge_ret - futures_drag`
  - v1.4/v1.6: `microcap_ret - 0.8 * hedge_ret - futures_drag`

## Full-Sample Summary

| strategy | cash segments | median missed MFE | p90 missed MFE | max missed MFE | avg final missed return | profit-then-loss rate |
|---|---:|---:|---:|---:|---:|---:|
| v1_0_live_costed | 75 | 0.20% | 2.85% | 5.81% | -0.48% | 14.67% |
| v1_4_buffer_derisk_costed | 71 | 0.65% | 3.14% | 6.94% | -0.11% | 16.90% |
| v1_6_targetvol15_max1p5_costed | 69 | 0.69% | 3.15% | 6.94% | -0.11% | 17.39% |

## Recent Windows

| window | strategy | segments | median missed MFE | p90 missed MFE | max missed MFE | avg final missed return |
|---|---|---:|---:|---:|---:|---:|
| 1Y | v1_0_live_costed | 8 | 0.95% | 2.32% | 2.35% | -0.46% |
| 1Y | v1_4_buffer_derisk_costed | 8 | 0.89% | 3.16% | 3.19% | 0.14% |
| 1Y | v1_6_targetvol15_max1p5_costed | 8 | 0.89% | 3.16% | 3.19% | 0.14% |
| 3Y | v1_0_live_costed | 17 | 1.31% | 3.35% | 4.66% | -1.00% |
| 3Y | v1_4_buffer_derisk_costed | 17 | 0.83% | 3.25% | 5.45% | -0.78% |
| 3Y | v1_6_targetvol15_max1p5_costed | 17 | 0.83% | 3.25% | 5.45% | -0.78% |
| 5Y | v1_0_live_costed | 29 | 1.31% | 4.17% | 5.81% | 0.17% |
| 5Y | v1_4_buffer_derisk_costed | 28 | 1.13% | 4.01% | 6.94% | 0.30% |
| 5Y | v1_6_targetvol15_max1p5_costed | 28 | 1.13% | 4.01% | 6.94% | 0.30% |

## Largest Missed Segments

| strategy | cash window | final missed return | missed MFE | best date | note |
|---|---|---:|---:|---|---|
| v1_4_buffer_derisk_costed | 2022-04-29 to 2022-05-24 | 5.92% | 6.94% | 2022-05-23 | largest close-only missed MFE |
| v1_6_targetvol15_max1p5_costed | 2022-04-29 to 2022-05-24 | 5.92% | 6.94% | 2022-05-23 | same base cash segment as v1.4 |
| v1_4_buffer_derisk_costed | 2021-10-28 to 2021-11-10 | 6.17% | 6.17% | 2021-11-10 | trend continued into exit |
| v1_6_targetvol15_max1p5_costed | 2021-10-28 to 2021-11-10 | 6.17% | 6.17% | 2021-11-10 | trend continued into exit |
| v1_0_live_costed | 2021-10-28 to 2021-11-10 | 5.81% | 5.81% | 2021-11-10 | largest v1.0 missed segment |

## Interpretation

The missed-MFE problem is real but not dominant in full sample. Most cash segments had small missed upside, while a few rebound windows after defensive exits created visible opportunity cost. The 2024-01-24 to 2024-03-07 segment is a useful warning case: it had positive missed MFE early, then ended deeply negative, so chasing every missed MFE would have increased drawdown risk.

Use these outputs for follow-up threshold research:

- `microcap_missed_mfe_reason_summary.csv`
- `microcap_missed_mfe_top20_segments.csv`
- `mae_mfe_trade_details.csv`
- `mae_mfe_scatter.png`
