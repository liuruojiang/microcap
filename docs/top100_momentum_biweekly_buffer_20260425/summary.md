# Top100 Momentum Biweekly Buffer Scan

- Baseline script: `microcap_top100_mom16_biweekly_live_v1_4.py`
- Baseline: v1.4 original rule, `base_version=v1.1`, 0.8x hedge, peak-decay derisk overlay
- Buffer rule: entry remains `momentum_gap > 0`; when already long, exit only when `momentum_gap < -buffer`
- Overlay path: buffer is applied before v1.4 peak-decay derisk, then v1.4 costed return is recomputed
- Validation: `buffer=0.0025` vs v1.4 official output max_abs_nav_diff = `0.0`; max_abs_ret_diff = `0.0`

## last_1y
- buffer_0p0025: CAGR 40.58% (+5.09%), Sharpe 3.112 (+0.402), MaxDD -8.14% (+0.90%), changes 175
- buffer_0p003: CAGR 40.58% (+5.09%), Sharpe 3.112 (+0.402), MaxDD -8.14% (+0.90%), changes 169
- buffer_0p0035: CAGR 40.58% (+5.09%), Sharpe 3.112 (+0.402), MaxDD -8.14% (+0.90%), changes 163
- buffer_0p004: CAGR 39.81% (+4.32%), Sharpe 3.047 (+0.337), MaxDD -8.14% (+0.90%), changes 161
- buffer_0p001: CAGR 38.91% (+3.42%), Sharpe 2.962 (+0.252), MaxDD -8.14% (+0.90%), changes 187
- buffer_0p0015: CAGR 38.91% (+3.42%), Sharpe 2.962 (+0.252), MaxDD -8.14% (+0.90%), changes 185
- buffer_0p002: CAGR 38.91% (+3.42%), Sharpe 2.962 (+0.252), MaxDD -8.14% (+0.90%), changes 179
- buffer_0p0045: CAGR 38.28% (+2.79%), Sharpe 2.921 (+0.212), MaxDD -9.15% (-0.11%), changes 157
- buffer_0p005: CAGR 38.28% (+2.79%), Sharpe 2.921 (+0.212), MaxDD -9.15% (-0.11%), changes 153
- buffer_0p0075: CAGR 37.53% (+2.04%), Sharpe 2.863 (+0.153), MaxDD -9.64% (-0.60%), changes 145
- buffer_0p010: CAGR 37.53% (+2.04%), Sharpe 2.863 (+0.153), MaxDD -9.64% (-0.60%), changes 131
- buffer_0p015: CAGR 36.72% (+1.23%), Sharpe 2.726 (+0.016), MaxDD -7.92% (+1.12%), changes 107
- buffer_0p020: CAGR 36.72% (+1.23%), Sharpe 2.726 (+0.016), MaxDD -7.92% (+1.12%), changes 95
- base_0p000: CAGR 35.49% (+0.00%), Sharpe 2.710 (+0.000), MaxDD -9.04% (+0.00%), changes 211
- buffer_0p030: CAGR 32.58% (-2.91%), Sharpe 2.372 (-0.337), MaxDD -8.69% (+0.35%), changes 75
- buffer_0p040: CAGR 36.25% (+0.77%), Sharpe 2.301 (-0.409), MaxDD -7.37% (+1.67%), changes 55
- buffer_0p050: CAGR 36.25% (+0.77%), Sharpe 2.301 (-0.409), MaxDD -7.37% (+1.67%), changes 45

## last_3y
- buffer_0p0025: CAGR 40.26% (+1.55%), Sharpe 2.859 (+0.113), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p003: CAGR 40.26% (+1.55%), Sharpe 2.859 (+0.113), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 40.26% (+1.55%), Sharpe 2.859 (+0.113), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 40.01% (+1.29%), Sharpe 2.840 (+0.093), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p001: CAGR 39.71% (+0.99%), Sharpe 2.814 (+0.067), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 39.71% (+0.99%), Sharpe 2.814 (+0.067), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p002: CAGR 39.71% (+0.99%), Sharpe 2.814 (+0.067), MaxDD -12.21% (-0.31%), changes 179
- base_0p000: CAGR 38.71% (+0.00%), Sharpe 2.747 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p0045: CAGR 35.22% (-3.49%), Sharpe 2.332 (-0.414), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p005: CAGR 35.22% (-3.49%), Sharpe 2.332 (-0.414), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0075: CAGR 34.98% (-3.73%), Sharpe 2.316 (-0.431), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 34.95% (-3.76%), Sharpe 2.314 (-0.433), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p015: CAGR 34.35% (-4.36%), Sharpe 2.227 (-0.520), MaxDD -20.03% (-8.13%), changes 107
- buffer_0p020: CAGR 34.35% (-4.36%), Sharpe 2.227 (-0.520), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p030: CAGR 30.87% (-7.85%), Sharpe 1.950 (-0.797), MaxDD -19.08% (-7.18%), changes 75
- buffer_0p040: CAGR 32.04% (-6.67%), Sharpe 1.924 (-0.823), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 29.58% (-9.14%), Sharpe 1.756 (-0.991), MaxDD -21.92% (-10.01%), changes 45

## last_5y
- buffer_0p003: CAGR 45.60% (+1.73%), Sharpe 3.106 (+0.124), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 45.60% (+1.73%), Sharpe 3.106 (+0.124), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 45.44% (+1.57%), Sharpe 3.094 (+0.112), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p0025: CAGR 45.40% (+1.53%), Sharpe 3.091 (+0.108), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p001: CAGR 45.07% (+1.20%), Sharpe 3.067 (+0.085), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 45.07% (+1.20%), Sharpe 3.067 (+0.085), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p002: CAGR 45.07% (+1.20%), Sharpe 3.067 (+0.085), MaxDD -12.21% (-0.31%), changes 179
- base_0p000: CAGR 43.87% (+0.00%), Sharpe 2.982 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p005: CAGR 43.78% (-0.09%), Sharpe 2.877 (-0.106), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p015: CAGR 43.83% (-0.04%), Sharpe 2.856 (-0.127), MaxDD -20.03% (-8.13%), changes 107
- buffer_0p0045: CAGR 43.42% (-0.45%), Sharpe 2.850 (-0.133), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p020: CAGR 43.69% (-0.18%), Sharpe 2.842 (-0.141), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p010: CAGR 42.93% (-0.94%), Sharpe 2.825 (-0.158), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p0075: CAGR 42.76% (-1.11%), Sharpe 2.812 (-0.170), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p030: CAGR 42.20% (-1.67%), Sharpe 2.671 (-0.312), MaxDD -19.08% (-7.18%), changes 75
- buffer_0p040: CAGR 41.52% (-2.36%), Sharpe 2.509 (-0.473), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 40.87% (-3.00%), Sharpe 2.441 (-0.541), MaxDD -21.92% (-10.01%), changes 45

## last_10y
- buffer_0p003: CAGR 39.22% (+1.27%), Sharpe 3.138 (+0.106), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0035: CAGR 39.22% (+1.27%), Sharpe 3.138 (+0.106), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 39.23% (+1.28%), Sharpe 3.134 (+0.103), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p0025: CAGR 39.12% (+1.17%), Sharpe 3.129 (+0.097), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p002: CAGR 38.96% (+1.01%), Sharpe 3.115 (+0.084), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p001: CAGR 38.75% (+0.79%), Sharpe 3.099 (+0.067), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0015: CAGR 38.75% (+0.79%), Sharpe 3.099 (+0.067), MaxDD -12.21% (-0.31%), changes 185
- base_0p000: CAGR 37.95% (+0.00%), Sharpe 3.032 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p005: CAGR 38.60% (+0.65%), Sharpe 3.012 (-0.020), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0045: CAGR 38.50% (+0.55%), Sharpe 3.003 (-0.029), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p0075: CAGR 38.09% (+0.14%), Sharpe 2.972 (-0.059), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 38.10% (+0.14%), Sharpe 2.957 (-0.075), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p015: CAGR 37.54% (-0.42%), Sharpe 2.888 (-0.143), MaxDD -20.03% (-8.13%), changes 107
- buffer_0p020: CAGR 37.36% (-0.60%), Sharpe 2.864 (-0.167), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p030: CAGR 35.60% (-2.35%), Sharpe 2.667 (-0.365), MaxDD -19.08% (-7.18%), changes 75
- buffer_0p040: CAGR 36.07% (-1.88%), Sharpe 2.593 (-0.439), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 34.76% (-3.19%), Sharpe 2.454 (-0.577), MaxDD -21.92% (-10.01%), changes 45

## full_common
- buffer_0p0035: CAGR 38.72% (+1.90%), Sharpe 3.525 (+0.187), MaxDD -12.21% (-0.31%), changes 163
- buffer_0p004: CAGR 38.72% (+1.90%), Sharpe 3.522 (+0.184), MaxDD -12.21% (-0.31%), changes 161
- buffer_0p003: CAGR 38.57% (+1.75%), Sharpe 3.512 (+0.173), MaxDD -12.21% (-0.31%), changes 169
- buffer_0p0025: CAGR 38.35% (+1.52%), Sharpe 3.488 (+0.150), MaxDD -12.21% (-0.31%), changes 175
- buffer_0p002: CAGR 38.15% (+1.33%), Sharpe 3.468 (+0.130), MaxDD -12.21% (-0.31%), changes 179
- buffer_0p005: CAGR 38.68% (+1.86%), Sharpe 3.452 (+0.113), MaxDD -20.03% (-8.13%), changes 153
- buffer_0p0015: CAGR 37.77% (+0.95%), Sharpe 3.431 (+0.092), MaxDD -12.21% (-0.31%), changes 185
- buffer_0p001: CAGR 37.66% (+0.84%), Sharpe 3.420 (+0.082), MaxDD -12.21% (-0.31%), changes 187
- buffer_0p0045: CAGR 38.32% (+1.50%), Sharpe 3.419 (+0.081), MaxDD -20.03% (-8.13%), changes 157
- buffer_0p0075: CAGR 38.42% (+1.60%), Sharpe 3.419 (+0.080), MaxDD -20.03% (-8.13%), changes 145
- buffer_0p010: CAGR 38.61% (+1.79%), Sharpe 3.416 (+0.078), MaxDD -20.03% (-8.13%), changes 131
- buffer_0p020: CAGR 38.67% (+1.84%), Sharpe 3.383 (+0.045), MaxDD -20.03% (-8.13%), changes 95
- buffer_0p015: CAGR 38.43% (+1.61%), Sharpe 3.376 (+0.038), MaxDD -20.03% (-8.13%), changes 107
- base_0p000: CAGR 36.82% (+0.00%), Sharpe 3.338 (+0.000), MaxDD -11.90% (+0.00%), changes 211
- buffer_0p030: CAGR 37.39% (+0.57%), Sharpe 3.197 (-0.142), MaxDD -19.08% (-7.18%), changes 75
- buffer_0p040: CAGR 37.03% (+0.21%), Sharpe 3.047 (-0.291), MaxDD -19.13% (-7.22%), changes 55
- buffer_0p050: CAGR 36.42% (-0.40%), Sharpe 2.949 (-0.389), MaxDD -21.92% (-10.01%), changes 45
