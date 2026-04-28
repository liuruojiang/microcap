# Microcap v1.4 Reentry Scan

Source: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\outputs\microcap_top100_mom16_hedge_zz1000_0p8x_gapderisk_newpeak_v1_4_costed_nav.csv`

Execution: close signal, next-day exposure; entry cost = 0.30% * scale.

| variant                         |   cagr |   sharpe |   max_drawdown |   delta_cagr |   delta_sharpe |   delta_max_drawdown |   trigger_count |   overlay_days |
|:--------------------------------|-------:|---------:|---------------:|-------------:|---------------:|---------------------:|----------------:|---------------:|
| rebound_2%_scale_1.0            | 0.4096 |   2.6737 |        -0.4375 |       0.0258 |        -0.4039 |              -0.3153 |              39 |            353 |
| rebound_2%_scale_0.6            | 0.4002 |   2.9469 |        -0.3164 |       0.0165 |        -0.1308 |              -0.1942 |              39 |            353 |
| rebound_2%_gap_ge_-1%_scale_1.0 | 0.3986 |   3.0793 |        -0.1282 |       0.0149 |         0.0017 |              -0.0061 |              27 |            161 |
| rebound_2%_gap_ge_-1%_scale_0.6 | 0.3928 |   3.1055 |        -0.1236 |       0.0091 |         0.0278 |              -0.0014 |              27 |            161 |
| rebound_2%_scale_0.3            | 0.3924 |   3.0736 |        -0.2126 |       0.0086 |        -0.0041 |              -0.0905 |              39 |            353 |
| rebound_2%_gap_ge_+0%_scale_1.0 | 0.3917 |   3.0507 |        -0.1282 |       0.008  |        -0.0269 |              -0.0061 |              10 |             99 |
| rebound_3%_gap_ge_+0%_scale_1.0 | 0.3902 |   3.0931 |        -0.1378 |       0.0065 |         0.0154 |              -0.0156 |               5 |             35 |
| rebound_3%_scale_1.0            | 0.39   |   2.6318 |        -0.4448 |       0.0063 |        -0.4459 |              -0.3227 |              23 |            180 |
| rebound_2%_gap_ge_+0%_scale_0.6 | 0.3886 |   3.0818 |        -0.1236 |       0.0049 |         0.0041 |              -0.0014 |              10 |             99 |
| rebound_3%_scale_0.6            | 0.3884 |   2.9041 |        -0.3217 |       0.0047 |        -0.1735 |              -0.1996 |              23 |            180 |
| rebound_2%_gap_ge_-1%_scale_0.3 | 0.3883 |   3.102  |        -0.1228 |       0.0046 |         0.0244 |              -0.0007 |              27 |            161 |
| rebound_3%_gap_ge_-1%_scale_1.0 | 0.388  |   3.0713 |        -0.1378 |       0.0043 |        -0.0064 |              -0.0156 |              12 |             57 |
