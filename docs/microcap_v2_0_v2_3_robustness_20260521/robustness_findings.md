# v2.0 / v2.3 Robustness Findings

Run folder: `quant_param_scan_runs/20260521_microcap_top100_v2_0_v2_3_signal_targetvol_overlay_lookback_halflife_gap_targetvol_scale_hedge`

## Scope

- Source versions: `microcap_top100_mom16_biweekly_live_v2_0.py`, `microcap_top100_mom16_biweekly_live_v2_3.py`
- Return column: `return_net`
- Common close-confirmed window: 2010-05-05 to 2026-05-20
- Cost/execution path: `v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model` plus target-vol scale, financing, idle-cash, and scale-change cost logic.
- Grid rows:
  - v2.3: 4800 candidates.
  - v2.0: 960 requested-grid candidates plus the exact official default candidate.

## Official Points

| candidate | ann return | max DD | Sharpe | 8Y ann | 5Y ann | 3Y ann | avg TV turnover | cost total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v2.3 official lb17 h4 gap13 tv25 thr30 exec0.8 | 38.44% | -16.24% | 2.51 | 29.77% | 35.07% | 39.46% | 0.0340% | 1.625 |
| v2.0 official lb16 gap0.3% tv25 thr10 exec0.8 | 35.42% | -18.33% | 2.27 | 27.60% | 31.28% | 34.86% | 0.1363% | 1.858 |

## Distribution View

| version | candidates | median ann | p25-p75 ann | median DD | p25-p75 DD | median Sharpe | 3Y positive | DD < -30% share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v2.3 | 4800 | 33.53% | 30.76% to 35.66% | -18.14% | -23.25% to -16.24% | 2.29 | 100.0% | 2.42% |
| v2.0 | 961 | 35.03% | 32.33% to 38.76% | -33.48% | -41.61% to -27.53% | 2.04 | 100.0% | 64.83% |

## Parameter Behavior

- v2.3 is not just a single-point optimum on lookback: mean full CAGR is 32.17%, 33.47%, 33.79%, 33.45%, 33.17% for lookback 12/15/17/20/24.
- v2.3 half-life is smoother near 3-5 than 6-8: mean full CAGR is 34.04%, 33.61%, 33.21%, 32.94%, 32.26% for half-life 3/4/5/6/8.
- v2.3 target vol is the largest intentional risk knob: 15/20/25/30% raises mean full CAGR from 28.93% to 36.24%, while mean max DD worsens from -16.69% to -22.49%.
- v2.3 scale threshold is mild: 10/20/30/50% mean full CAGR stays near 33.13% to 33.26%.
- v2.3 execution hedge ratio has a visible platform around 0.8 to 1.0 for Sharpe, while 0.6 has materially worse drawdown.
- v2.0 requested-grid variants can produce higher return, but the drawdown surface is much less stable. 64.83% of v2.0 candidates have full max DD worse than -30%, versus 2.42% for v2.3.

## Decision

Research-only. Do not promote a new single best row from this run. The evidence supports v2.3 as the more robust risk-adjusted family; v2.0 is return-competitive but drawdown-sensitive under the requested wider buffers and target-vol settings.
