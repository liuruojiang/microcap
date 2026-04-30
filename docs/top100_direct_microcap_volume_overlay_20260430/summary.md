# Top100 Direct Microcap Volume Overlay Scan

## Setup
- Baseline: official Top100 Mom16 biweekly Thursday costed NAV.
- Direct index source: 同花顺微盘股 `883418.TI` from QVeris when cached locally.
- Tested fields: `amount` and `volume` contraction/expansion versus moving average.
- Timing: T-day index signal affects the next trading day only.
- Cost: extra scale changes pay `0.003 * abs(delta_scale)`.
- Wind 868008.WI was not tested because WindPy is unavailable in this environment.

## Baseline
- 24.55% annual, Sharpe 1.54, MaxDD -12.20%, sample 2023-07-04 to 2026-04-28.

## THS Source
- Rows: 685, sample 2023-07-04 to 2026-04-30.
- Public THS history starts in 2023-07 here, so this scan is short-window only.
- Any `robust=True` flag below is only the mechanical filter inside this short sample, not a 5Y/10Y robustness finding.

## Top Candidates
- `ths_volume_below_ma50_days17:scale0.00`: score 0.284, days 22; 10Y dAnn +4.08pp, dSharpe +0.28, dMaxDD -0.00pp; 5Y dAnn +4.08pp, dMaxDD -0.00pp; 3Y dAnn +4.08pp, dMaxDD -0.00pp; full dAnn +4.08pp; robust=False.
- `ths_volume_below_ma50_days17:scale0.25`: score 0.224, days 22; 10Y dAnn +3.07pp, dSharpe +0.22, dMaxDD +0.00pp; 5Y dAnn +3.07pp, dMaxDD +0.00pp; 3Y dAnn +3.07pp, dMaxDD +0.00pp; full dAnn +3.07pp; robust=True.
- `ths_volume_below_ma55_days9:scale0.00`: score 0.212, days 110; 10Y dAnn +0.18pp, dSharpe +0.18, dMaxDD +2.06pp; 5Y dAnn +0.18pp, dMaxDD +2.06pp; 3Y dAnn +0.18pp, dMaxDD +2.06pp; full dAnn +0.18pp; robust=True.
- `ths_volume_below_ma55_days9:scale0.25`: score 0.206, days 110; 10Y dAnn +0.20pp, dSharpe +0.16, dMaxDD +2.32pp; 5Y dAnn +0.20pp, dMaxDD +2.32pp; 3Y dAnn +0.20pp, dMaxDD +2.32pp; full dAnn +0.20pp; robust=True.
- `ths_volume_below_ma55_days9:scale0.50`: score 0.169, days 110; 10Y dAnn +0.17pp, dSharpe +0.13, dMaxDD +2.30pp; 5Y dAnn +0.17pp, dMaxDD +2.30pp; 3Y dAnn +0.17pp, dMaxDD +2.30pp; full dAnn +0.17pp; robust=True.
- `ths_volume_below_ma50_days17:scale0.50`: score 0.155, days 22; 10Y dAnn +2.05pp, dSharpe +0.16, dMaxDD -0.00pp; 5Y dAnn +2.05pp, dMaxDD -0.00pp; 3Y dAnn +2.05pp, dMaxDD -0.00pp; full dAnn +2.05pp; robust=False.
- `ths_amount_above_ma20_days15:scale0.00`: score 0.140, days 15; 10Y dAnn +2.13pp, dSharpe +0.14, dMaxDD -0.00pp; 5Y dAnn +2.13pp, dMaxDD -0.00pp; 3Y dAnn +2.13pp, dMaxDD -0.00pp; full dAnn +2.13pp; robust=False.
- `ths_volume_above_ma40_days11:scale0.00`: score 0.133, days 65; 10Y dAnn +1.16pp, dSharpe +0.13, dMaxDD -0.00pp; 5Y dAnn +1.16pp, dMaxDD -0.00pp; 3Y dAnn +1.16pp, dMaxDD -0.00pp; full dAnn +1.16pp; robust=False.
- `ths_volume_below_ma30_days25:scale0.00`: score 0.128, days 6; 10Y dAnn +2.06pp, dSharpe +0.13, dMaxDD -0.00pp; 5Y dAnn +2.06pp, dMaxDD -0.00pp; 3Y dAnn +2.06pp, dMaxDD -0.00pp; full dAnn +2.06pp; robust=False.
- `ths_volume_above_ma40_days11:scale0.25`: score 0.114, days 65; 10Y dAnn +0.89pp, dSharpe +0.11, dMaxDD -0.00pp; 5Y dAnn +0.89pp, dMaxDD -0.00pp; 3Y dAnn +0.89pp, dMaxDD -0.00pp; full dAnn +0.89pp; robust=False.
- `ths_amount_above_ma20_days15:scale0.25`: score 0.109, days 15; 10Y dAnn +1.61pp, dSharpe +0.11, dMaxDD -0.00pp; 5Y dAnn +1.61pp, dMaxDD -0.00pp; 3Y dAnn +1.61pp, dMaxDD -0.00pp; full dAnn +1.61pp; robust=False.
- `ths_volume_below_ma50_days15:scale0.00`: score 0.109, days 33; 10Y dAnn +0.68pp, dSharpe +0.11, dMaxDD -0.00pp; 5Y dAnn +0.68pp, dMaxDD -0.00pp; 3Y dAnn +0.68pp, dMaxDD -0.00pp; full dAnn +0.68pp; robust=False.
- `ths_volume_below_ma50_days19:scale0.00`: score 0.101, days 16; 10Y dAnn +1.19pp, dSharpe +0.10, dMaxDD -0.00pp; 5Y dAnn +1.19pp, dMaxDD -0.00pp; 3Y dAnn +1.19pp, dMaxDD -0.00pp; full dAnn +1.19pp; robust=False.
- `ths_volume_below_ma50_days21:scale0.00`: score 0.101, days 10; 10Y dAnn +1.38pp, dSharpe +0.10, dMaxDD -0.00pp; 5Y dAnn +1.38pp, dMaxDD -0.00pp; 3Y dAnn +1.38pp, dMaxDD -0.00pp; full dAnn +1.38pp; robust=False.
- `ths_volume_below_ma30_days25:scale0.25`: score 0.099, days 6; 10Y dAnn +1.55pp, dSharpe +0.10, dMaxDD -0.00pp; 5Y dAnn +1.55pp, dMaxDD -0.00pp; 3Y dAnn +1.55pp, dMaxDD -0.00pp; full dAnn +1.55pp; robust=False.
- `ths_volume_below_ma50_days15:scale0.25`: score 0.097, days 33; 10Y dAnn +0.54pp, dSharpe +0.10, dMaxDD -0.00pp; 5Y dAnn +0.54pp, dMaxDD -0.00pp; 3Y dAnn +0.54pp, dMaxDD -0.00pp; full dAnn +0.54pp; robust=False.
- `ths_volume_above_ma35_days11:scale0.00`: score 0.096, days 53; 10Y dAnn +0.89pp, dSharpe +0.10, dMaxDD -0.00pp; 5Y dAnn +0.89pp, dMaxDD -0.00pp; 3Y dAnn +0.89pp, dMaxDD -0.00pp; full dAnn +0.89pp; robust=False.
- `ths_volume_below_ma55_days9:scale0.75`: score 0.092, days 110; 10Y dAnn +0.10pp, dSharpe +0.07, dMaxDD +1.15pp; 5Y dAnn +0.10pp, dMaxDD +1.15pp; 3Y dAnn +0.10pp, dMaxDD +1.15pp; full dAnn +0.10pp; robust=True.
- `ths_volume_above_ma40_days11:scale0.50`: score 0.085, days 65; 10Y dAnn +0.61pp, dSharpe +0.08, dMaxDD -0.00pp; 5Y dAnn +0.61pp, dMaxDD -0.00pp; 3Y dAnn +0.61pp, dMaxDD -0.00pp; full dAnn +0.61pp; robust=False.
- `ths_volume_below_ma50_days19:scale0.25`: score 0.084, days 16; 10Y dAnn +0.91pp, dSharpe +0.08, dMaxDD -0.00pp; 5Y dAnn +0.91pp, dMaxDD -0.00pp; 3Y dAnn +0.91pp, dMaxDD -0.00pp; full dAnn +0.91pp; robust=False.

## Family Summary
- amount below scale 0.75: robust 37/143, best score 0.064, median score -0.039.
- amount below scale 0.5: robust 19/143, best score 0.079, median score -0.095.
- volume below scale 0.75: robust 6/143, best score 0.092, median score -0.109.
- amount below scale 0.25: robust 5/143, best score 0.065, median score -0.168.
- volume above scale 0.0: robust 4/143, best score 0.133, median score -0.133.
- volume below scale 0.25: robust 3/143, best score 0.224, median score -0.371.
- volume below scale 0.5: robust 3/143, best score 0.169, median score -0.225.
- volume above scale 0.25: robust 3/143, best score 0.114, median score -0.093.
- volume above scale 0.5: robust 2/143, best score 0.085, median score -0.059.
- amount below scale 0.0: robust 2/143, best score 0.008, median score -0.255.
- volume below scale 0.0: robust 1/143, best score 0.284, median score -0.518.
- amount above scale 0.0: robust 0/143, best score 0.140, median score -0.416.
- amount above scale 0.25: robust 0/143, best score 0.109, median score -0.284.
- amount above scale 0.5: robust 0/143, best score 0.076, median score -0.179.
- volume above scale 0.75: robust 0/143, best score 0.046, median score -0.028.
- amount above scale 0.75: robust 0/143, best score 0.043, median score -0.083.

## Files
- `top100_direct_microcap_volume_overlay_summary.csv`: all scanned variants.
- `top100_direct_microcap_volume_overlay_top200.csv`: ranked top variants.
- `top100_direct_microcap_volume_overlay_robust.csv`: robust-pass variants.
- `top100_direct_microcap_volume_overlay_group_summary.csv`: family-level comparison.
- `top100_direct_microcap_volume_overlay_top_curves.csv`: daily curves for top variants.
