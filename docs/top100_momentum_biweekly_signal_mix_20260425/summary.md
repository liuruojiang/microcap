# Top100 Momentum Biweekly Signal-Mix Study

- Data source: live rebuild path from `microcap_top100_mom16_biweekly_live.py` via `load_close_df()`
- Cost path: `scan_top100_momentum_costs.apply_cost_model()` parity for `16/16`, then the same entry/exit/rebalance cost formula for mixed signals
- Baseline: `lookback=16` live semantics
- User-logic mix: average `momentum_gap = microcap_mom - hedge_mom` first, then select one binary long/cash signal
- Comparison note: older `top100_momentum_biweekly_mix_20260424` used weight-average semantics.
- Validation: `16/16` signal mix vs live costed path max_abs_nav_diff = `0.0`; max_abs_ret_diff = `0.0`

## Baseline 16
- last_3y: CAGR 30.91%, Sharpe 2.069, MaxDD -10.61%
- last_5y: CAGR 35.84%, Sharpe 2.247, MaxDD -10.78%
- full_common: CAGR 32.20%, Sharpe 2.803, MaxDD -11.29%

## Top Signal-Mix Pairs
- 14/18: 3Y CAGR 32.55%, Sharpe 2.148, MaxDD -12.77%; 5Y CAGR 38.47%, Sharpe 2.401, MaxDD -12.77%; Full CAGR 33.51%, Sharpe 2.910, MaxDD -14.40%
- 12/17: 3Y CAGR 30.89%, Sharpe 2.061, MaxDD -10.60%; 5Y CAGR 38.16%, Sharpe 2.393, MaxDD -10.60%; Full CAGR 33.57%, Sharpe 2.922, MaxDD -10.60%
- 13/16: 3Y CAGR 33.36%, Sharpe 2.216, MaxDD -12.74%; 5Y CAGR 37.99%, Sharpe 2.378, MaxDD -12.74%; Full CAGR 34.37%, Sharpe 2.996, MaxDD -12.74%
- 14/16: 3Y CAGR 32.43%, Sharpe 2.142, MaxDD -13.07%; 5Y CAGR 37.92%, Sharpe 2.372, MaxDD -13.07%; Full CAGR 33.70%, Sharpe 2.936, MaxDD -13.07%
- 11/17: 3Y CAGR 29.91%, Sharpe 2.010, MaxDD -10.14%; 5Y CAGR 37.22%, Sharpe 2.338, MaxDD -10.14%; Full CAGR 33.30%, Sharpe 2.906, MaxDD -10.38%
- 12/16: 3Y CAGR 30.54%, Sharpe 2.050, MaxDD -10.11%; 5Y CAGR 37.06%, Sharpe 2.334, MaxDD -10.11%; Full CAGR 34.21%, Sharpe 2.994, MaxDD -10.11%
- 11/15: 3Y CAGR 31.38%, Sharpe 2.110, MaxDD -10.01%; 5Y CAGR 36.95%, Sharpe 2.332, MaxDD -10.01%; Full CAGR 33.79%, Sharpe 2.952, MaxDD -10.18%
- 9/18: 3Y CAGR 29.87%, Sharpe 1.955, MaxDD -10.65%; 5Y CAGR 37.57%, Sharpe 2.329, MaxDD -10.65%; Full CAGR 34.11%, Sharpe 2.942, MaxDD -10.65%
- 13/15: 3Y CAGR 30.98%, Sharpe 2.104, MaxDD -13.05%; 5Y CAGR 36.75%, Sharpe 2.329, MaxDD -13.05%; Full CAGR 34.07%, Sharpe 2.992, MaxDD -13.05%
- 10/17: 3Y CAGR 28.35%, Sharpe 1.912, MaxDD -11.83%; 5Y CAGR 36.82%, Sharpe 2.322, MaxDD -11.83%; Full CAGR 33.50%, Sharpe 2.915, MaxDD -11.83%
- 7/18: 3Y CAGR 30.12%, Sharpe 1.988, MaxDD -12.70%; 5Y CAGR 36.92%, Sharpe 2.295, MaxDD -12.70%; Full CAGR 33.28%, Sharpe 2.865, MaxDD -12.70%
- 16/17: 3Y CAGR 29.95%, Sharpe 1.991, MaxDD -10.90%; 5Y CAGR 36.52%, Sharpe 2.284, MaxDD -10.90%; Full CAGR 32.60%, Sharpe 2.833, MaxDD -12.11%

## Focus Pairs
- 13/16: 3Y Sharpe 2.216 (+0.146 vs 16); 5Y Sharpe 2.378 (+0.131); Full Sharpe 2.996 (+0.194)
- 14/16: 3Y Sharpe 2.142 (+0.072 vs 16); 5Y Sharpe 2.372 (+0.125); Full Sharpe 2.936 (+0.133)
- 12/16: 3Y Sharpe 2.050 (-0.019 vs 16); 5Y Sharpe 2.334 (+0.087); Full Sharpe 2.994 (+0.191)
- 16/17: 3Y Sharpe 1.991 (-0.078 vs 16); 5Y Sharpe 2.284 (+0.037); Full Sharpe 2.833 (+0.031)
- 15/16: 3Y Sharpe 2.014 (-0.055 vs 16); 5Y Sharpe 2.250 (+0.003); Full Sharpe 2.880 (+0.077)
- 10/16: 3Y Sharpe 1.980 (-0.090 vs 16); 5Y Sharpe 2.243 (-0.004); Full Sharpe 2.917 (+0.114)
- 6/16: 3Y Sharpe 1.756 (-0.314 vs 16); 5Y Sharpe 2.168 (-0.079); Full Sharpe 2.789 (-0.014)
- 16/26: 3Y Sharpe 1.555 (-0.514 vs 16); 5Y Sharpe 2.162 (-0.085); Full Sharpe 2.718 (-0.084)
- 4/16: 3Y Sharpe 1.798 (-0.271 vs 16); 5Y Sharpe 2.140 (-0.107); Full Sharpe 2.770 (-0.033)
- 16/25: 3Y Sharpe 1.662 (-0.407 vs 16); 5Y Sharpe 2.126 (-0.121); Full Sharpe 2.749 (-0.054)
- 16/18: 3Y Sharpe 1.709 (-0.360 vs 16); 5Y Sharpe 2.121 (-0.126); Full Sharpe 2.722 (-0.081)
- 16/24: 3Y Sharpe 1.598 (-0.471 vs 16); 5Y Sharpe 2.121 (-0.126); Full Sharpe 2.722 (-0.081)
- 8/16: 3Y Sharpe 1.634 (-0.436 vs 16); 5Y Sharpe 1.991 (-0.256); Full Sharpe 2.762 (-0.041)

## Pairs With 16
- 13/16: 3Y CAGR 33.36%, Sharpe 2.216, MaxDD -12.74%; 5Y CAGR 37.99%, Sharpe 2.378, MaxDD -12.74%; Full Sharpe 2.996
- 14/16: 3Y CAGR 32.43%, Sharpe 2.142, MaxDD -13.07%; 5Y CAGR 37.92%, Sharpe 2.372, MaxDD -13.07%; Full Sharpe 2.936
- 12/16: 3Y CAGR 30.54%, Sharpe 2.050, MaxDD -10.11%; 5Y CAGR 37.06%, Sharpe 2.334, MaxDD -10.11%; Full Sharpe 2.994
- 16/17: 3Y CAGR 29.95%, Sharpe 1.991, MaxDD -10.90%; 5Y CAGR 36.52%, Sharpe 2.284, MaxDD -10.90%; Full Sharpe 2.833
- 15/16: 3Y CAGR 30.69%, Sharpe 2.014, MaxDD -14.42%; 5Y CAGR 36.16%, Sharpe 2.250, MaxDD -14.42%; Full Sharpe 2.880
- 10/16: 3Y CAGR 29.55%, Sharpe 1.980, MaxDD -10.01%; 5Y CAGR 35.65%, Sharpe 2.243, MaxDD -10.01%; Full Sharpe 2.917
- 11/16: 3Y CAGR 28.26%, Sharpe 1.909, MaxDD -10.58%; 5Y CAGR 35.26%, Sharpe 2.231, MaxDD -10.58%; Full Sharpe 2.965
- 9/16: 3Y CAGR 26.60%, Sharpe 1.744, MaxDD -11.27%; 5Y CAGR 35.09%, Sharpe 2.179, MaxDD -11.27%; Full Sharpe 2.853
- 16/23: 3Y CAGR 27.06%, Sharpe 1.689, MaxDD -21.67%; 5Y CAGR 36.12%, Sharpe 2.168, MaxDD -21.67%; Full Sharpe 2.728
- 6/16: 3Y CAGR 26.56%, Sharpe 1.756, MaxDD -10.69%; 5Y CAGR 34.76%, Sharpe 2.168, MaxDD -10.69%; Full Sharpe 2.789
- 16/26: 3Y CAGR 24.98%, Sharpe 1.555, MaxDD -24.11%; 5Y CAGR 36.12%, Sharpe 2.162, MaxDD -24.11%; Full Sharpe 2.718
- 4/16: 3Y CAGR 26.50%, Sharpe 1.798, MaxDD -14.24%; 5Y CAGR 33.74%, Sharpe 2.140, MaxDD -14.24%; Full Sharpe 2.770
- 16/21: 3Y CAGR 26.54%, Sharpe 1.654, MaxDD -17.05%; 5Y CAGR 35.46%, Sharpe 2.135, MaxDD -17.05%; Full Sharpe 2.721
- 16/20: 3Y CAGR 24.12%, Sharpe 1.510, MaxDD -17.95%; 5Y CAGR 35.32%, Sharpe 2.133, MaxDD -17.95%; Full Sharpe 2.762
- 16/25: 3Y CAGR 26.60%, Sharpe 1.662, MaxDD -21.26%; 5Y CAGR 35.15%, Sharpe 2.126, MaxDD -21.26%; Full Sharpe 2.749
- 16/18: 3Y CAGR 27.23%, Sharpe 1.709, MaxDD -15.90%; 5Y CAGR 35.01%, Sharpe 2.121, MaxDD -15.90%; Full Sharpe 2.722
- 16/24: 3Y CAGR 25.44%, Sharpe 1.598, MaxDD -22.99%; 5Y CAGR 35.20%, Sharpe 2.121, MaxDD -22.99%; Full Sharpe 2.722
- 16/22: 3Y CAGR 26.33%, Sharpe 1.649, MaxDD -19.36%; 5Y CAGR 34.65%, Sharpe 2.084, MaxDD -19.36%; Full Sharpe 2.721
- 16/19: 3Y CAGR 24.99%, Sharpe 1.571, MaxDD -18.28%; 5Y CAGR 34.13%, Sharpe 2.066, MaxDD -18.28%; Full Sharpe 2.701
- 5/16: 3Y CAGR 26.69%, Sharpe 1.745, MaxDD -11.99%; 5Y CAGR 32.31%, Sharpe 2.010, MaxDD -11.99%; Full Sharpe 2.674
- 8/16: 3Y CAGR 24.82%, Sharpe 1.634, MaxDD -12.33%; 5Y CAGR 32.00%, Sharpe 1.991, MaxDD -12.77%; Full Sharpe 2.762
- 7/16: 3Y CAGR 25.65%, Sharpe 1.683, MaxDD -11.96%; 5Y CAGR 31.82%, Sharpe 1.979, MaxDD -14.77%; Full Sharpe 2.736
