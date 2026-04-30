# Top100 Microcap Broad Volume Overlay Scan

## Setup
- Baseline: official Top100 Mom16 biweekly Thursday costed NAV.
- Tested broad amount sources: SHCOMP, CSI300, CSI1000, CSI2000, ChiNext, plus CSI2000/ChiNext combinations.
- Timing: T-day index amount signal affects the next trading day only.
- Cost: extra scale changes pay `0.003 * abs(delta_scale)`.

## Baseline
- 31.07% annual, Sharpe 2.45, MaxDD -12.20%, sample 2010-02-02 to 2026-04-28.

## Top Candidates
- `zz2000_and_cyb_below_ma60_days15:scale0.00`: score 0.288, days 293; 10Y dAnn +1.74pp, dSharpe +0.22, dMaxDD +1.42pp; 5Y dAnn +3.84pp, dMaxDD +1.42pp; 3Y dAnn +2.71pp, dMaxDD +1.42pp; full dAnn +0.09pp; robust=True.
- `cyb_below_ma50_days20:scale0.00`: score 0.286, days 378; 10Y dAnn +1.71pp, dSharpe +0.22, dMaxDD +1.42pp; 5Y dAnn +6.96pp, dMaxDD +1.42pp; 3Y dAnn +4.27pp, dMaxDD +1.42pp; full dAnn -0.56pp; robust=True.
- `zz2000_and_cyb_below_ma60_days15:scale0.25`: score 0.254, days 293; 10Y dAnn +1.33pp, dSharpe +0.19, dMaxDD +1.90pp; 5Y dAnn +2.91pp, dMaxDD +1.90pp; 3Y dAnn +2.07pp, dMaxDD +1.90pp; full dAnn +0.09pp; robust=True.
- `cyb_below_ma50_days20:scale0.25`: score 0.249, days 378; 10Y dAnn +1.31pp, dSharpe +0.19, dMaxDD +1.90pp; 5Y dAnn +5.22pp, dMaxDD +1.90pp; 3Y dAnn +3.22pp, dMaxDD +1.90pp; full dAnn -0.40pp; robust=True.
- `zz2000_and_cyb_below_ma50_days20:scale0.00`: score 0.218, days 161; 10Y dAnn +1.06pp, dSharpe +0.12, dMaxDD +0.89pp; 5Y dAnn +2.91pp, dMaxDD +0.89pp; 3Y dAnn +4.14pp, dMaxDD +1.15pp; full dAnn +0.17pp; robust=True.
- `cyb_below_ma50_days20:scale0.50`: score 0.192, days 378; 10Y dAnn +0.88pp, dSharpe +0.14, dMaxDD +1.77pp; 5Y dAnn +3.48pp, dMaxDD +2.11pp; 3Y dAnn +2.15pp, dMaxDD +2.11pp; full dAnn -0.26pp; robust=True.
- `zz2000_and_cyb_below_ma60_days15:scale0.50`: score 0.189, days 293; 10Y dAnn +0.90pp, dSharpe +0.14, dMaxDD +1.65pp; 5Y dAnn +1.96pp, dMaxDD +1.65pp; 3Y dAnn +1.40pp, dMaxDD +1.65pp; full dAnn +0.07pp; robust=True.
- `zz2000_and_cyb_below_ma50_days20:scale0.25`: score 0.184, days 161; 10Y dAnn +0.81pp, dSharpe +0.10, dMaxDD +1.02pp; 5Y dAnn +2.20pp, dMaxDD +1.02pp; 3Y dAnn +3.11pp, dMaxDD +1.70pp; full dAnn +0.13pp; robust=True.
- `zz2000_and_cyb_below_ma50_days20:scale0.50`: score 0.142, days 161; 10Y dAnn +0.54pp, dSharpe +0.07, dMaxDD +1.16pp; 5Y dAnn +1.47pp, dMaxDD +1.16pp; 3Y dAnn +2.08pp, dMaxDD +2.11pp; full dAnn +0.09pp; robust=True.
- `zz2000_below_ma60_days15:scale0.25`: score 0.117, days 453; 10Y dAnn -0.51pp, dSharpe +0.15, dMaxDD +2.25pp; 5Y dAnn +0.80pp, dMaxDD +2.25pp; 3Y dAnn -1.01pp, dMaxDD +2.41pp; full dAnn -0.94pp; robust=True.
- `zz2000_and_cyb_below_ma50_days15:scale0.25`: score 0.112, days 254; 10Y dAnn +0.32pp, dSharpe +0.10, dMaxDD +1.67pp; 5Y dAnn +1.10pp, dMaxDD +1.67pp; 3Y dAnn +0.50pp, dMaxDD +1.67pp; full dAnn -0.36pp; robust=True.
- `zz2000_and_cyb_below_ma50_days15:scale0.00`: score 0.107, days 254; 10Y dAnn +0.41pp, dSharpe +0.11, dMaxDD +1.42pp; 5Y dAnn +1.43pp, dMaxDD +1.42pp; 3Y dAnn +0.62pp, dMaxDD +1.42pp; full dAnn -0.50pp; robust=True.
- `zz2000_below_ma60_days15:scale0.50`: score 0.107, days 453; 10Y dAnn -0.31pp, dSharpe +0.13, dMaxDD +1.78pp; 5Y dAnn +0.57pp, dMaxDD +1.78pp; 3Y dAnn -0.63pp, dMaxDD +1.78pp; full dAnn -0.61pp; robust=True.
- `cyb_below_ma50_days20:scale0.75`: score 0.101, days 378; 10Y dAnn +0.45pp, dSharpe +0.07, dMaxDD +1.05pp; 5Y dAnn +1.74pp, dMaxDD +1.05pp; 3Y dAnn +1.08pp, dMaxDD +1.05pp; full dAnn -0.12pp; robust=True.
- `zz2000_and_cyb_below_ma60_days15:scale0.75`: score 0.100, days 293; 10Y dAnn +0.46pp, dSharpe +0.08, dMaxDD +0.82pp; 5Y dAnn +0.99pp, dMaxDD +0.82pp; 3Y dAnn +0.71pp, dMaxDD +0.82pp; full dAnn +0.04pp; robust=True.

## Robust Rules
- `zz2000_and_cyb_below_ma60_days15:scale0.00`
- `cyb_below_ma50_days20:scale0.00`
- `zz2000_and_cyb_below_ma60_days15:scale0.25`
- `cyb_below_ma50_days20:scale0.25`
- `zz2000_and_cyb_below_ma50_days20:scale0.00`
- `cyb_below_ma50_days20:scale0.50`
- `zz2000_and_cyb_below_ma60_days15:scale0.50`
- `zz2000_and_cyb_below_ma50_days20:scale0.25`
- `zz2000_and_cyb_below_ma50_days20:scale0.50`
- `zz2000_below_ma60_days15:scale0.25`
- `zz2000_and_cyb_below_ma50_days15:scale0.25`
- `zz2000_and_cyb_below_ma50_days15:scale0.00`
- `zz2000_below_ma60_days15:scale0.50`
- `cyb_below_ma50_days20:scale0.75`
- `zz2000_and_cyb_below_ma60_days15:scale0.75`

## Files
- `top100_broad_volume_overlay_summary.csv`: all scanned variants.
- `top100_broad_volume_overlay_top150.csv`: ranked top variants.
- `top100_broad_volume_overlay_robust.csv`: robust-pass variants.
- `top100_broad_volume_overlay_group_summary.csv`: family-level comparison.
- `top100_broad_volume_overlay_top_curves.csv`: daily curves for top variants.
