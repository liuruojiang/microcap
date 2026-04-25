# Top100 Momentum Biweekly Buffer Scan

- Baseline script: `microcap_top100_mom16_biweekly_live_v1_4.py`
- Baseline: v1.4 original rule, `base_version=v1.1`, 0.8x hedge, peak-decay derisk overlay
- Buffer rule: entry remains `momentum_gap > 0`; when already long, exit only when `momentum_gap < -buffer`
- Overlay path: buffer is applied before v1.4 peak-decay derisk, then v1.4 costed return is recomputed
- Validation: `buffer=0.0025` vs v1.4 official output max_abs_nav_diff = `0.0`; max_abs_ret_diff = `0.0`

## last_1y
- buffer_0p020: CAGR 39.38% (+6.71%), Sharpe 2.921 (+0.503), MaxDD -7.92% (+1.12%), changes 95
- buffer_0p015: CAGR 36.36% (+3.68%), Sharpe 2.712 (+0.294), MaxDD -7.92% (+1.12%), changes 109
- buffer_0p001: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 187
- buffer_0p0015: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 185
- buffer_0p002: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 179
- buffer_0p0025: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 175
- buffer_0p003: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 169
- buffer_0p0035: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 163
- buffer_0p004: CAGR 36.03% (+3.35%), Sharpe 2.658 (+0.240), MaxDD -8.14% (+0.90%), changes 161
- buffer_0p0045: CAGR 34.54% (+1.87%), Sharpe 2.542 (+0.124), MaxDD -9.15% (-0.11%), changes 157
- buffer_0p005: CAGR 34.54% (+1.87%), Sharpe 2.542 (+0.124), MaxDD -9.15% (-0.11%), changes 153
- buffer_0p0075: CAGR 33.82% (+1.14%), Sharpe 2.487 (+0.069), MaxDD -9.64% (-0.60%), changes 145
- buffer_0p010: CAGR 33.82% (+1.14%), Sharpe 2.487 (+0.069), MaxDD -9.64% (-0.60%), changes 131
- base_0p000: CAGR 32.68% (+0.00%), Sharpe 2.418 (+0.000), MaxDD -9.04% (+0.00%), changes 211
- buffer_0p030: CAGR 36.53% (+3.85%), Sharpe 2.397 (-0.021), MaxDD -8.30% (+0.74%), changes 73
- buffer_0p040: CAGR 37.13% (+4.45%), Sharpe 2.364 (-0.054), MaxDD -7.37% (+1.67%), changes 55
- buffer_0p050: CAGR 37.13% (+4.45%), Sharpe 2.364 (-0.054), MaxDD -7.37% (+1.67%), changes 45

## last_3y
- buffer_0p001: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p002: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p0025: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p003: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 38.73% (+0.99%), Sharpe 2.724 (+0.067), MaxDD -12.21% (-0.31%), changes 161
- base_0p000: CAGR 37.74% (+0.00%), Sharpe 2.657 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p020: CAGR 35.21% (-2.53%), Sharpe 2.286 (-0.371), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p0045: CAGR 33.99% (-3.75%), Sharpe 2.233 (-0.424), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p005: CAGR 33.99% (-3.75%), Sharpe 2.233 (-0.424), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p015: CAGR 34.22% (-3.52%), Sharpe 2.225 (-0.432), MaxDD -20.03% (-8.13%), changes 109
- buffer_0p0075: CAGR 33.75% (-3.99%), Sharpe 2.217 (-0.440), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 33.72% (-4.02%), Sharpe 2.215 (-0.442), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p030: CAGR 32.15% (-5.59%), Sharpe 1.977 (-0.680), MaxDD -19.08% (-7.18%), changes 73
- buffer_0p040: CAGR 32.31% (-5.43%), Sharpe 1.945 (-0.712), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 29.85% (-7.89%), Sharpe 1.776 (-0.881), MaxDD -21.92% (-10.01%), changes 45

## last_5y
- buffer_0p003: CAGR 44.45% (+1.38%), Sharpe 3.008 (+0.095), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 44.45% (+1.38%), Sharpe 3.008 (+0.095), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 44.45% (+1.38%), Sharpe 3.008 (+0.095), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p001: CAGR 44.26% (+1.19%), Sharpe 2.996 (+0.084), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 44.26% (+1.19%), Sharpe 2.996 (+0.084), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p002: CAGR 44.26% (+1.19%), Sharpe 2.996 (+0.084), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p0025: CAGR 44.24% (+1.18%), Sharpe 2.993 (+0.080), MaxDD -12.21% (-0.31%), changes 175
- base_0p000: CAGR 43.07% (+0.00%), Sharpe 2.912 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p020: CAGR 44.05% (+0.98%), Sharpe 2.864 (-0.048), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p015: CAGR 43.55% (+0.48%), Sharpe 2.839 (-0.073), MaxDD -20.03% (-8.13%), changes 109
- buffer_0p005: CAGR 42.79% (-0.28%), Sharpe 2.796 (-0.116), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0045: CAGR 42.44% (-0.63%), Sharpe 2.769 (-0.143), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p010: CAGR 41.95% (-1.12%), Sharpe 2.745 (-0.168), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p0075: CAGR 41.78% (-1.29%), Sharpe 2.732 (-0.180), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p030: CAGR 42.84% (-0.23%), Sharpe 2.665 (-0.247), MaxDD -19.08% (-7.18%), changes 73
- buffer_0p040: CAGR 41.50% (-1.57%), Sharpe 2.509 (-0.403), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 40.86% (-2.21%), Sharpe 2.441 (-0.472), MaxDD -21.92% (-10.01%), changes 45

## last_10y
- buffer_0p004: CAGR 38.99% (+1.19%), Sharpe 3.103 (+0.093), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p003: CAGR 38.91% (+1.10%), Sharpe 3.099 (+0.090), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 38.91% (+1.10%), Sharpe 3.099 (+0.090), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p002: CAGR 38.82% (+1.01%), Sharpe 3.093 (+0.083), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p0025: CAGR 38.81% (+1.00%), Sharpe 3.091 (+0.081), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p001: CAGR 38.60% (+0.79%), Sharpe 3.076 (+0.067), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 38.60% (+0.79%), Sharpe 3.076 (+0.067), MaxDD -12.21% (-0.31%), changes 185
- base_0p000: CAGR 37.81% (+0.00%), Sharpe 3.009 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p005: CAGR 38.36% (+0.56%), Sharpe 2.982 (-0.027), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0045: CAGR 38.26% (+0.46%), Sharpe 2.973 (-0.037), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p0075: CAGR 37.85% (+0.05%), Sharpe 2.943 (-0.067), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 37.86% (+0.05%), Sharpe 2.927 (-0.082), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p015: CAGR 37.64% (-0.16%), Sharpe 2.898 (-0.111), MaxDD -20.03% (-8.13%), changes 109
- buffer_0p020: CAGR 37.76% (-0.04%), Sharpe 2.895 (-0.114), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p030: CAGR 36.14% (-1.66%), Sharpe 2.675 (-0.334), MaxDD -19.08% (-7.18%), changes 73
- buffer_0p040: CAGR 36.30% (-1.51%), Sharpe 2.610 (-0.400), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 34.98% (-2.82%), Sharpe 2.471 (-0.538), MaxDD -21.92% (-10.01%), changes 45

## full_common
- buffer_0p004: CAGR 38.52% (+1.85%), Sharpe 3.491 (+0.176), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p0035: CAGR 38.46% (+1.79%), Sharpe 3.489 (+0.175), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p003: CAGR 38.32% (+1.65%), Sharpe 3.476 (+0.161), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0025: CAGR 38.09% (+1.42%), Sharpe 3.452 (+0.138), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p002: CAGR 38.00% (+1.33%), Sharpe 3.444 (+0.129), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p005: CAGR 38.48% (+1.81%), Sharpe 3.422 (+0.107), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0015: CAGR 37.62% (+0.95%), Sharpe 3.406 (+0.092), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p020: CAGR 38.86% (+2.19%), Sharpe 3.399 (+0.084), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p001: CAGR 37.51% (+0.84%), Sharpe 3.396 (+0.081), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0045: CAGR 38.12% (+1.45%), Sharpe 3.389 (+0.074), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p0075: CAGR 38.22% (+1.55%), Sharpe 3.389 (+0.074), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 38.41% (+1.74%), Sharpe 3.386 (+0.072), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p015: CAGR 38.44% (+1.77%), Sharpe 3.377 (+0.063), MaxDD -20.03% (-8.13%), changes 109
- base_0p000: CAGR 36.67% (+0.00%), Sharpe 3.315 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p030: CAGR 37.67% (+1.00%), Sharpe 3.188 (-0.126), MaxDD -19.08% (-7.18%), changes 73
- buffer_0p040: CAGR 37.12% (+0.45%), Sharpe 3.053 (-0.261), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 36.50% (-0.17%), Sharpe 2.955 (-0.359), MaxDD -21.92% (-10.01%), changes 45
