# Microcap Top100 v2.5 Pool Size And Rebalance Frequency Test

## Conclusion

Keep formal `v2.5 Top100 biweekly + costed` as the mainline.

The tested alternatives do not improve the risk-adjusted profile enough to replace the current mainline:

- `Top50 biweekly`: higher full-sample annual return, but drawdown widens materially and the latest 1Y result is much weaker.
- `Top200 biweekly`: somewhat more defensive in recent windows, but long-run annual return drops too much.
- `Top100 weekly`: recent return is attractive, but full-sample and 10Y drawdowns deteriorate sharply.
- `Top100 monthly`: lower turnover and milder drawdown, but full-sample and 10Y annual returns are weaker.

Practical decision: official `v2.5 Top100 biweekly + costed` remains the default. The other branches are observation-only and should not be promoted without a separate turnover and drawdown attribution pass.

## Run Artifact

- Run folder: `quant_param_scan_runs/20260523_microcap_top100_v2_5_pool_rebalance_frequency`
- Script: `scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py`
- Official entrypoint refreshed first: `microcap_top100_mom16_biweekly_live_v2_5.py`
- Metrics window: `2010-05-05` to `2026-05-22`
- Trading dates used by rebuilt variants: `2010-02-02` to `2026-05-22`
- Loaded current-universe symbols: `4975`
- Rows per candidate: `3884`
- Artifact checks: strict quant-param-scan checker passed.
- Tests: `pytest tests\test_v2_5_formalization.py tests\test_v2_5_bias_overheat_scan.py` passed, `9 passed`.

## Important Caveat

The local-cache rebuilt `Top100 biweekly` line is not bit-identical to the official v2.5 output:

- Rebuilt baseline max absolute daily `return_net` difference vs official: `4.6485%`.
- Cause scope: the rebuilt variants use the local-cache reconstruction path for pool size and rebalance-frequency changes, while the official v2.5 output uses the formal refreshed v2.5 output path.

Therefore:

- Use official `v25_official_top100_biweekly` as the production reference.
- Use `rebuilt_top100_biweekly` as the apples-to-apples research baseline for comparing `Top50`, `Top200`, `weekly`, and `monthly`.

## Results

All numbers are costed. Annualization uses `244` trading days.

| candidate | top_n | frequency | full ann / mdd | 10Y ann / mdd | 5Y ann / mdd | 3Y ann / mdd | 1Y ann / mdd | avg scale | cost sum |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| official v2.5 Top100 | 100 | biweekly | 37.12% / -19.48% | 31.09% / -16.22% | 36.97% / -13.76% | 42.99% / -13.76% | 63.35% / -13.76% | 0.584 | 0.907 |
| rebuilt Top100 | 100 | biweekly | 36.72% / -21.91% | 29.67% / -16.78% | 35.80% / -14.49% | 36.96% / -14.49% | 57.51% / -14.49% | 0.589 | 0.930 |
| Top50 | 50 | biweekly | 39.55% / -25.39% | 28.44% / -25.39% | 37.65% / -16.09% | 36.30% / -16.09% | 37.96% / -16.09% | 0.607 | 0.983 |
| Top200 | 200 | biweekly | 30.78% / -23.87% | 23.79% / -18.43% | 34.78% / -12.97% | 39.18% / -11.37% | 43.91% / -11.37% | 0.573 | 0.911 |
| Top100 weekly | 100 | weekly | 38.29% / -25.55% | 30.94% / -25.55% | 36.35% / -13.92% | 39.80% / -13.92% | 63.06% / -13.92% | 0.605 | 0.980 |
| Top100 monthly | 100 | monthly | 33.61% / -20.14% | 27.66% / -18.19% | 36.69% / -14.48% | 38.97% / -12.59% | 62.44% / -12.59% | 0.579 | 0.855 |

## Interpretation

Against the rebuilt `Top100 biweekly` research baseline:

- `Top50 biweekly` adds `+2.83%` full-sample annual return, but worsens full-sample drawdown by `-3.48%`, worsens 10Y drawdown by `-8.61%`, and loses `-19.55%` annual return in the latest 1Y window. This is not a robust upgrade.
- `Top200 biweekly` gives better recent-window drawdown in 3Y and 1Y, but full-sample annual return falls by `-5.94%` and 10Y annual return falls by `-5.89%`. This is too defensive for the mainline.
- `Top100 weekly` improves 1Y annual return and 3Y annual return, but the full-sample drawdown and 10Y drawdown both worsen by about `-8.8%`. The higher rebalance frequency increases cost and crash sensitivity.
- `Top100 monthly` lowers cost and has better full-sample drawdown than rebuilt Top100, but full-sample annual return falls by `-3.11%` and 10Y annual return falls by `-2.01%`. This is a possible defensive reference, not a replacement.

## Decision

Do not change v2.5 defaults.

Keep:

- Strategy version: `v2.5`
- Pool: `Top100`
- Rebalance frequency: `biweekly`
- Performance caliber: `costed`

Observation-only branches:

- `Top100 monthly`: useful as a lower-turnover defensive comparator.
- `Top100 weekly`: useful only for further turnover and stress attribution, not for promotion.
- `Top50` and `Top200`: not mainline candidates from this pass.
