# Top100 Momentum Biweekly Buffer Scan

- Data source: live rebuild path from `microcap_top100_mom16_biweekly_live.py` via `load_close_df()`
- Baseline: `lookback=16`, long when `momentum_gap > 0`
- Buffer rule: entry remains `momentum_gap > 0`; when already long, exit only when `momentum_gap < -buffer`
- Cost path: `scan_top100_momentum_costs.apply_cost_model()` with the live turnover table
- Validation: `buffer=0` vs live costed path max_abs_nav_diff = `0.0`; max_abs_ret_diff = `0.0`

## last_1y
- buffer_0p001: CAGR 42.96% (+3.73%), Sharpe 2.765 (+0.222), MaxDD -9.03% (+0.51%), changes 182
- buffer_0p0015: CAGR 42.96% (+3.73%), Sharpe 2.765 (+0.222), MaxDD -9.03% (+0.51%), changes 180
- buffer_0p002: CAGR 42.96% (+3.73%), Sharpe 2.765 (+0.222), MaxDD -9.03% (+0.51%), changes 174
- buffer_0p0025: CAGR 42.96% (+3.73%), Sharpe 2.765 (+0.222), MaxDD -9.03% (+0.51%), changes 170
- buffer_0p003: CAGR 42.96% (+3.73%), Sharpe 2.765 (+0.222), MaxDD -9.03% (+0.51%), changes 164
- buffer_0p0035: CAGR 41.37% (+2.14%), Sharpe 2.653 (+0.110), MaxDD -9.03% (+0.51%), changes 158
- buffer_0p004: CAGR 41.37% (+2.14%), Sharpe 2.653 (+0.110), MaxDD -9.03% (+0.51%), changes 156
- base_0p000: CAGR 39.23% (+0.00%), Sharpe 2.543 (+0.000), MaxDD -9.53% (+0.00%), changes 206
- buffer_0p0045: CAGR 39.24% (+0.01%), Sharpe 2.504 (-0.039), MaxDD -10.39% (-0.86%), changes 152
- buffer_0p005: CAGR 39.24% (+0.01%), Sharpe 2.504 (-0.039), MaxDD -10.39% (-0.86%), changes 148
- buffer_0p040: CAGR 38.11% (-1.12%), Sharpe 2.323 (-0.220), MaxDD -11.85% (-2.31%), changes 54
- buffer_0p050: CAGR 38.11% (-1.12%), Sharpe 2.323 (-0.220), MaxDD -11.85% (-2.31%), changes 44
- buffer_0p0075: CAGR 36.33% (-2.90%), Sharpe 2.288 (-0.255), MaxDD -12.26% (-2.73%), changes 140
- buffer_0p010: CAGR 36.33% (-2.90%), Sharpe 2.288 (-0.255), MaxDD -12.26% (-2.73%), changes 126
- buffer_0p020: CAGR 34.96% (-4.27%), Sharpe 2.196 (-0.348), MaxDD -13.65% (-4.12%), changes 92
- buffer_0p030: CAGR 35.01% (-4.22%), Sharpe 2.178 (-0.366), MaxDD -15.38% (-5.84%), changes 70
- buffer_0p015: CAGR 34.18% (-5.05%), Sharpe 2.141 (-0.402), MaxDD -13.65% (-4.12%), changes 104

## last_3y
- buffer_0p001: CAGR 31.98% (+1.07%), Sharpe 2.135 (+0.066), MaxDD -10.61% (-0.00%), changes 182
- buffer_0p0015: CAGR 31.98% (+1.07%), Sharpe 2.135 (+0.066), MaxDD -10.61% (-0.00%), changes 180
- buffer_0p002: CAGR 31.98% (+1.07%), Sharpe 2.135 (+0.066), MaxDD -10.61% (-0.00%), changes 174
- buffer_0p0025: CAGR 31.98% (+1.07%), Sharpe 2.135 (+0.066), MaxDD -10.61% (-0.00%), changes 170
- buffer_0p003: CAGR 31.98% (+1.07%), Sharpe 2.135 (+0.066), MaxDD -10.61% (-0.00%), changes 164
- buffer_0p0035: CAGR 31.49% (+0.58%), Sharpe 2.100 (+0.030), MaxDD -10.61% (-0.00%), changes 158
- buffer_0p004: CAGR 31.49% (+0.58%), Sharpe 2.100 (+0.030), MaxDD -10.61% (-0.00%), changes 156
- base_0p000: CAGR 30.91% (+0.00%), Sharpe 2.069 (+0.000), MaxDD -10.61% (+0.00%), changes 206
- buffer_0p015: CAGR 27.45% (-3.46%), Sharpe 1.696 (-0.374), MaxDD -18.50% (-7.89%), changes 104
- buffer_0p020: CAGR 27.41% (-3.50%), Sharpe 1.694 (-0.375), MaxDD -18.50% (-7.89%), changes 92
- buffer_0p0045: CAGR 26.90% (-4.01%), Sharpe 1.689 (-0.380), MaxDD -16.40% (-5.80%), changes 152
- buffer_0p005: CAGR 26.84% (-4.07%), Sharpe 1.686 (-0.384), MaxDD -16.40% (-5.80%), changes 148
- buffer_0p030: CAGR 27.51% (-3.40%), Sharpe 1.673 (-0.397), MaxDD -21.45% (-10.84%), changes 70
- buffer_0p040: CAGR 27.28% (-3.63%), Sharpe 1.631 (-0.439), MaxDD -21.80% (-11.19%), changes 54
- buffer_0p0075: CAGR 25.95% (-4.96%), Sharpe 1.623 (-0.446), MaxDD -16.40% (-5.80%), changes 140
- buffer_0p010: CAGR 25.04% (-5.87%), Sharpe 1.553 (-0.517), MaxDD -18.50% (-7.89%), changes 126
- buffer_0p050: CAGR 25.27% (-5.64%), Sharpe 1.500 (-0.570), MaxDD -22.24% (-11.63%), changes 44

## last_5y
- buffer_0p0025: CAGR 37.44% (+1.60%), Sharpe 2.346 (+0.098), MaxDD -10.78% (-0.00%), changes 170
- buffer_0p040: CAGR 40.41% (+4.57%), Sharpe 2.345 (+0.098), MaxDD -21.80% (-11.02%), changes 54
- buffer_0p003: CAGR 37.21% (+1.37%), Sharpe 2.329 (+0.082), MaxDD -10.78% (-0.00%), changes 164
- buffer_0p0015: CAGR 37.07% (+1.23%), Sharpe 2.322 (+0.075), MaxDD -10.78% (-0.00%), changes 180
- buffer_0p002: CAGR 37.07% (+1.23%), Sharpe 2.322 (+0.075), MaxDD -10.78% (-0.00%), changes 174
- buffer_0p050: CAGR 40.35% (+4.51%), Sharpe 2.321 (+0.074), MaxDD -22.24% (-11.46%), changes 44
- buffer_0p0035: CAGR 36.90% (+1.07%), Sharpe 2.309 (+0.061), MaxDD -10.78% (-0.00%), changes 158
- buffer_0p004: CAGR 36.90% (+1.07%), Sharpe 2.309 (+0.061), MaxDD -10.78% (-0.00%), changes 156
- buffer_0p001: CAGR 36.64% (+0.80%), Sharpe 2.295 (+0.048), MaxDD -10.78% (-0.00%), changes 182
- buffer_0p030: CAGR 38.88% (+3.04%), Sharpe 2.284 (+0.037), MaxDD -21.45% (-10.67%), changes 70
- base_0p000: CAGR 35.84% (+0.00%), Sharpe 2.247 (+0.000), MaxDD -10.78% (+0.00%), changes 206
- buffer_0p020: CAGR 37.24% (+1.41%), Sharpe 2.223 (-0.024), MaxDD -18.50% (-7.72%), changes 92
- buffer_0p015: CAGR 36.19% (+0.35%), Sharpe 2.161 (-0.086), MaxDD -18.50% (-7.72%), changes 104
- buffer_0p0075: CAGR 34.73% (-1.10%), Sharpe 2.091 (-0.157), MaxDD -16.40% (-5.63%), changes 140
- buffer_0p0045: CAGR 34.11% (-1.73%), Sharpe 2.065 (-0.182), MaxDD -16.40% (-5.63%), changes 152
- buffer_0p005: CAGR 34.07% (-1.77%), Sharpe 2.063 (-0.184), MaxDD -16.40% (-5.63%), changes 148
- buffer_0p010: CAGR 34.37% (-1.47%), Sharpe 2.059 (-0.188), MaxDD -18.50% (-7.72%), changes 126

## last_10y
- buffer_0p0025: CAGR 35.87% (+1.56%), Sharpe 2.701 (+0.115), MaxDD -11.29% (+0.00%), changes 170
- buffer_0p003: CAGR 35.75% (+1.45%), Sharpe 2.691 (+0.105), MaxDD -11.29% (+0.00%), changes 164
- buffer_0p002: CAGR 35.68% (+1.38%), Sharpe 2.687 (+0.102), MaxDD -11.29% (+0.00%), changes 174
- buffer_0p004: CAGR 35.63% (+1.33%), Sharpe 2.680 (+0.094), MaxDD -11.29% (-0.00%), changes 156
- buffer_0p0035: CAGR 35.60% (+1.30%), Sharpe 2.678 (+0.093), MaxDD -11.29% (+0.00%), changes 158
- buffer_0p0015: CAGR 35.40% (+1.09%), Sharpe 2.667 (+0.081), MaxDD -11.29% (-0.00%), changes 180
- buffer_0p001: CAGR 35.26% (+0.96%), Sharpe 2.657 (+0.072), MaxDD -11.29% (+0.00%), changes 182
- base_0p000: CAGR 34.30% (+0.00%), Sharpe 2.585 (+0.000), MaxDD -11.29% (+0.00%), changes 206
- buffer_0p020: CAGR 35.10% (+0.80%), Sharpe 2.527 (-0.058), MaxDD -18.50% (-7.20%), changes 92
- buffer_0p015: CAGR 34.98% (+0.67%), Sharpe 2.525 (-0.061), MaxDD -18.50% (-7.20%), changes 104
- buffer_0p0075: CAGR 34.23% (-0.07%), Sharpe 2.499 (-0.086), MaxDD -16.40% (-5.11%), changes 140
- buffer_0p0045: CAGR 34.01% (-0.30%), Sharpe 2.495 (-0.090), MaxDD -16.40% (-5.11%), changes 152
- buffer_0p005: CAGR 33.94% (-0.36%), Sharpe 2.491 (-0.095), MaxDD -16.40% (-5.11%), changes 148
- buffer_0p010: CAGR 34.25% (-0.05%), Sharpe 2.482 (-0.103), MaxDD -18.50% (-7.20%), changes 126
- buffer_0p030: CAGR 34.84% (+0.54%), Sharpe 2.473 (-0.113), MaxDD -21.45% (-10.16%), changes 70
- buffer_0p040: CAGR 35.08% (+0.77%), Sharpe 2.449 (-0.136), MaxDD -21.80% (-10.50%), changes 54
- buffer_0p050: CAGR 34.82% (+0.52%), Sharpe 2.393 (-0.192), MaxDD -22.24% (-10.95%), changes 44

## full_common
- buffer_0p003: CAGR 33.52% (+1.32%), Sharpe 2.916 (+0.113), MaxDD -11.29% (+0.00%), changes 164
- buffer_0p0025: CAGR 33.48% (+1.29%), Sharpe 2.913 (+0.110), MaxDD -11.29% (-0.00%), changes 170
- buffer_0p002: CAGR 33.40% (+1.21%), Sharpe 2.907 (+0.104), MaxDD -11.29% (-0.00%), changes 174
- buffer_0p0035: CAGR 33.32% (+1.12%), Sharpe 2.894 (+0.091), MaxDD -11.29% (-0.00%), changes 158
- buffer_0p004: CAGR 33.30% (+1.10%), Sharpe 2.891 (+0.088), MaxDD -11.29% (+0.00%), changes 156
- buffer_0p0015: CAGR 33.13% (+0.93%), Sharpe 2.883 (+0.081), MaxDD -11.29% (-0.00%), changes 180
- buffer_0p001: CAGR 32.98% (+0.78%), Sharpe 2.871 (+0.068), MaxDD -11.29% (-0.00%), changes 182
- base_0p000: CAGR 32.20% (+0.00%), Sharpe 2.803 (+0.000), MaxDD -11.29% (+0.00%), changes 206
- buffer_0p020: CAGR 33.34% (+1.14%), Sharpe 2.774 (-0.029), MaxDD -18.50% (-7.20%), changes 92
- buffer_0p015: CAGR 33.07% (+0.88%), Sharpe 2.761 (-0.042), MaxDD -18.50% (-7.20%), changes 104
- buffer_0p010: CAGR 32.83% (+0.63%), Sharpe 2.754 (-0.049), MaxDD -18.50% (-7.20%), changes 126
- buffer_0p005: CAGR 32.38% (+0.18%), Sharpe 2.751 (-0.052), MaxDD -16.40% (-5.11%), changes 148
- buffer_0p0045: CAGR 32.21% (+0.01%), Sharpe 2.739 (-0.064), MaxDD -16.40% (-5.11%), changes 152
- buffer_0p030: CAGR 33.36% (+1.16%), Sharpe 2.737 (-0.066), MaxDD -21.45% (-10.16%), changes 70
- buffer_0p0075: CAGR 32.29% (+0.09%), Sharpe 2.724 (-0.078), MaxDD -16.40% (-5.11%), changes 140
- buffer_0p040: CAGR 33.29% (+1.09%), Sharpe 2.687 (-0.116), MaxDD -21.80% (-10.50%), changes 54
- buffer_0p050: CAGR 33.18% (+0.98%), Sharpe 2.641 (-0.162), MaxDD -22.24% (-10.95%), changes 44
