# Microcap v1.0 Vol-Scaling / Leverage Study

- Source: `outputs\microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv`.
- Baseline is the refreshed v1.0 costed strategy, not v1.4.
- Scaling is a research overlay: gross strategy return and existing trade cost scale with exposure; cash days remain cash.
- Financing sensitivity uses 3% annualized cost on exposure above 1.0x.

| Scenario | Window | Annual | Vol | Sharpe | Max DD | Avg active scale |
|---|---:|---:|---:|---:|---:|---:|
| baseline_v1_0_costed | full | 31.09% | 11.55% | 2.42 | -12.20% | 1.00x |
| baseline_v1_0_costed | 10y | 31.97% | 13.39% | 2.15 | -12.20% | 1.00x |
| baseline_v1_0_costed | 5y | 32.00% | 16.06% | 1.82 | -12.20% | 1.00x |
| baseline_v1_0_costed | 3y | 27.35% | 15.27% | 1.68 | -12.20% | 1.00x |
| baseline_v1_0_costed | 1y | 17.68% | 15.17% | 1.17 | -12.20% | 1.00x |
| fixed_1.5x_3pct_financing | full | 49.13% | 17.26% | 2.42 | -17.78% | 1.50x |
| fixed_1.5x_3pct_financing | 10y | 50.30% | 20.02% | 2.15 | -17.78% | 1.50x |
| fixed_1.5x_3pct_financing | 5y | 50.09% | 24.03% | 1.83 | -17.78% | 1.50x |
| fixed_1.5x_3pct_financing | 3y | 42.27% | 22.84% | 1.68 | -17.78% | 1.50x |
| fixed_1.5x_3pct_financing | 1y | 26.76% | 22.73% | 1.18 | -17.78% | 1.50x |
| targetvol_15_w60_max1p5_3pct_financing | full | 42.00% | 13.60% | 2.66 | -14.73% | 1.30x |
| targetvol_15_w60_max1p5_3pct_financing | 10y | 40.21% | 14.91% | 2.35 | -14.73% | 1.21x |
| targetvol_15_w60_max1p5_3pct_financing | 5y | 32.99% | 15.80% | 1.90 | -11.50% | 1.00x |
| targetvol_15_w60_max1p5_3pct_financing | 3y | 31.19% | 16.21% | 1.78 | -11.50% | 1.07x |
| targetvol_15_w60_max1p5_3pct_financing | 1y | 17.69% | 16.12% | 1.11 | -11.50% | 1.03x |
| targetvol_20_w60_max1p5_3pct_financing | full | 46.25% | 15.63% | 2.53 | -15.96% | 1.42x |
| targetvol_20_w60_max1p5_3pct_financing | 10y | 45.98% | 17.77% | 2.23 | -15.96% | 1.38x |
| targetvol_20_w60_max1p5_3pct_financing | 5y | 41.82% | 20.29% | 1.84 | -15.19% | 1.28x |
| targetvol_20_w60_max1p5_3pct_financing | 3y | 38.34% | 20.67% | 1.69 | -15.19% | 1.36x |
| targetvol_20_w60_max1p5_3pct_financing | 1y | 22.17% | 20.86% | 1.08 | -15.19% | 1.35x |
| targetvol_25_w60_max1p5_3pct_financing | full | 48.04% | 16.78% | 2.44 | -17.37% | 1.47x |
| targetvol_25_w60_max1p5_3pct_financing | 10y | 48.88% | 19.39% | 2.16 | -17.37% | 1.47x |
| targetvol_25_w60_max1p5_3pct_financing | 5y | 47.26% | 22.97% | 1.82 | -17.37% | 1.44x |
| targetvol_25_w60_max1p5_3pct_financing | 3y | 41.58% | 22.48% | 1.68 | -17.37% | 1.48x |
| targetvol_25_w60_max1p5_3pct_financing | 1y | 25.66% | 22.47% | 1.15 | -17.37% | 1.48x |
