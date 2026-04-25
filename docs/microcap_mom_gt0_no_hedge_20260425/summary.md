# Microcap v1.0 No-Hedge Absolute-Momentum Study

- Source: `outputs/microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv`, refreshed to 2026-04-24.
- Variant: remove the CSI 1000 hedge; hold the Top100 microcap basket only when its 16-day momentum is above 0, otherwise hold cash.
- Cost model: same close-execution entry/exit and rebalance cost table used by the v1.0 live path.

| Scenario | Window | Annual | Vol | Sharpe | Max DD | Total Return |
|---|---:|---:|---:|---:|---:|---:|
| current_v1_0_long_microcap_short_zz1000_costed | full | 31.09% | 11.55% | 2.42 | -12.20% | 7977.28% |
| current_v1_0_long_microcap_short_zz1000_costed | 10y | 31.97% | 13.39% | 2.15 | -12.20% | 1499.92% |
| current_v1_0_long_microcap_short_zz1000_costed | 5y | 32.00% | 16.06% | 1.82 | -12.20% | 299.99% |
| current_v1_0_long_microcap_short_zz1000_costed | 3y | 27.35% | 15.27% | 1.68 | -12.20% | 106.41% |
| current_v1_0_long_microcap_short_zz1000_costed | 1y | 17.68% | 15.17% | 1.17 | -12.20% | 17.61% |
| long_only_microcap_mom_gt_0_costed | full | 40.19% | 21.29% | 1.70 | -27.57% | 23882.40% |
| long_only_microcap_mom_gt_0_costed | 10y | 30.40% | 20.13% | 1.43 | -26.61% | 1320.44% |
| long_only_microcap_mom_gt_0_costed | 5y | 42.71% | 21.71% | 1.76 | -22.93% | 490.63% |
| long_only_microcap_mom_gt_0_costed | 3y | 35.93% | 23.07% | 1.46 | -22.93% | 151.01% |
| long_only_microcap_mom_gt_0_costed | 1y | 74.90% | 19.21% | 3.06 | -10.78% | 74.56% |

Interpretation: the no-hedge absolute-momentum variant has higher upside in strong microcap windows, but its long-run risk profile is materially weaker than the current hedged v1.0 mainline. It is better treated as a high-beta sleeve candidate than as a direct mainline replacement.
