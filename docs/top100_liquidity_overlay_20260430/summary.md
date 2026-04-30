# Top100 Microcap Liquidity Overlay Scan

## Setup
- Baseline: official Top100 Mom16 biweekly Thursday costed NAV.
- Tested liquidity proxies: rebalance turnover, blocked entry/exit counts, holding count, and target-list market cap.
- Timing: rebalance features affect returns from `return_start_date`; daily holding count is shifted one trading day.
- Cost: extra scale changes pay `0.003 * abs(delta_scale)`.

## Baseline
- 31.07% annual, Sharpe 2.45, MaxDD -12.20%, sample 2010-02-02 to 2026-04-28.

## Top Candidates
- `holding_count_below_95:scale0.75`: score -0.014, events 4, days 4; 10Y dAnn -0.18pp, dSharpe -0.01, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.11pp; robust=False.
- `holding_count_below_97:scale0.75`: score -0.017, events 5, days 5; 10Y dAnn -0.22pp, dSharpe -0.01, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.14pp; robust=False.
- `daily_holding_count_below_97:scale0.75`: score -0.020, events 45, days 45; 10Y dAnn -0.27pp, dSharpe -0.01, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.17pp; robust=False.
- `holding_count_below_99:scale0.75`: score -0.020, events 7, days 7; 10Y dAnn -0.27pp, dSharpe -0.01, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.16pp; robust=False.
- `daily_holding_count_below_95:scale0.75`: score -0.023, events 35, days 35; 10Y dAnn -0.31pp, dSharpe -0.02, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.19pp; robust=False.
- `holding_count_below_95:scale0.50`: score -0.027, events 4, days 4; 10Y dAnn -0.36pp, dSharpe -0.02, dMaxDD +0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.22pp; robust=False.
- `daily_holding_count_below_99:scale0.75`: score -0.030, events 64, days 64; 10Y dAnn -0.40pp, dSharpe -0.02, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.25pp; robust=False.
- `holding_count_below_97:scale0.50`: score -0.034, events 5, days 5; 10Y dAnn -0.44pp, dSharpe -0.02, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.27pp; robust=False.
- `holding_count_below_99:scale0.50`: score -0.040, events 7, days 7; 10Y dAnn -0.53pp, dSharpe -0.03, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.33pp; robust=False.
- `daily_holding_count_below_97:scale0.50`: score -0.041, events 45, days 45; 10Y dAnn -0.55pp, dSharpe -0.03, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.33pp; robust=False.
- `holding_count_below_95:scale0.25`: score -0.041, events 4, days 4; 10Y dAnn -0.55pp, dSharpe -0.03, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.33pp; robust=False.
- `daily_holding_count_below_95:scale0.50`: score -0.047, events 35, days 35; 10Y dAnn -0.63pp, dSharpe -0.03, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.38pp; robust=False.
- `holding_count_below_97:scale0.25`: score -0.051, events 5, days 5; 10Y dAnn -0.67pp, dSharpe -0.04, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.41pp; robust=False.
- `holding_count_below_95:scale0.00`: score -0.056, events 4, days 4; 10Y dAnn -0.73pp, dSharpe -0.04, dMaxDD -0.00pp; 5Y dAnn +0.00pp, dMaxDD +0.00pp; 3Y dAnn +0.00pp, dMaxDD +0.00pp; full dAnn -0.45pp; robust=False.
- `holding_count_below_100:scale0.75`: score -0.059, events 19, days 19; 10Y dAnn -0.56pp, dSharpe -0.03, dMaxDD -0.00pp; 5Y dAnn -0.41pp, dMaxDD +0.00pp; 3Y dAnn -0.52pp, dMaxDD +0.00pp; full dAnn -0.34pp; robust=False.

## Robust Rules
- No rule passed the robust filter.

## Files
- `top100_liquidity_overlay_summary.csv`: all scanned variants.
- `top100_liquidity_overlay_top120.csv`: ranked top variants.
- `top100_liquidity_overlay_robust.csv`: robust-pass variants.
- `top100_liquidity_overlay_group_summary.csv`: family-level comparison.
- `top100_liquidity_overlay_top_curves.csv`: daily curves for top variants.
