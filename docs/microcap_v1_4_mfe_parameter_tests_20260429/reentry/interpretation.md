# v1.4 Reentry Scan Interpretation

## Tested Rules

The scan tested defensive no-exposure days in official v1.4 costed NAV:

- source: `..\微盘股对冲策略\outputs\microcap_top100_mom16_hedge_zz1000_0p8x_gapderisk_newpeak_v1_4_costed_nav.csv`
- window: `2010-02-02` to `2026-04-28`
- baseline: official v1.4 costed `nav_net`
- execution: signal at close, exposure begins next trading day
- reentry cost: `0.30% * scale`
- no-exposure state: `holding == cash` or `execution_scale <= 0`

Trigger families:

- `gap`: reenter when `momentum_gap` rises above a threshold
- `rebound`: reenter when the shadow long-microcap / short-0.8x-CSI1000 spread rebounds from the no-exposure segment low
- `rebound_and_gap`: require both a spread rebound and a repaired `momentum_gap`

## Main Result

Pure rebound triggers improved full-sample return but created unacceptable drawdown. The highest-CAGR rule, `rebound_2%_scale_1.0`, lifted full-sample CAGR from `38.37%` to `40.96%`, but max drawdown worsened from `-12.21%` to `-43.75%`. That fails the risk test.

The best usable family is:

`rebound_2%_gap_ge_-1%`

This means: after v1.4 is in a no-exposure state, only reenter if the shadow spread has rebounded at least `2%` from the no-exposure segment low and `momentum_gap >= -1%`.

## Candidate Rows

| variant | CAGR | Sharpe | Max DD | Delta CAGR | Delta Sharpe | Delta Max DD | Triggers | Overlay Days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline_v1_4 | 38.37% | 3.0777 | -12.21% | 0.00% | 0.0000 | 0.00% | 0 | 0 |
| rebound_2%_gap_ge_-1%_scale_0.3 | 38.83% | 3.1020 | -12.28% | +0.46% | +0.0244 | -0.07% | 27 | 161 |
| rebound_2%_gap_ge_-1%_scale_0.6 | 39.28% | 3.1055 | -12.36% | +0.91% | +0.0278 | -0.14% | 27 | 161 |
| rebound_2%_gap_ge_-1%_scale_1.0 | 39.86% | 3.0793 | -12.82% | +1.49% | +0.0017 | -0.61% | 27 | 161 |

## Window Check

| variant | 1Y CAGR | 3Y CAGR | 5Y CAGR | 10Y CAGR | full CAGR |
|---|---:|---:|---:|---:|---:|
| baseline_v1_4 | 39.87% | 40.40% | 45.64% | 39.16% | 38.37% |
| rebound_2%_gap_ge_-1%_scale_0.3 | 38.23% | 39.81% | 45.67% | 39.39% | 38.83% |
| rebound_2%_gap_ge_-1%_scale_0.6 | 36.52% | 39.18% | 45.67% | 39.61% | 39.28% |
| rebound_2%_gap_ge_-1%_scale_1.0 | 34.16% | 38.30% | 45.63% | 39.88% | 39.86% |

The rule improves full and 10Y results, but weakens the latest 1Y and 3Y windows as scale increases. The 0.3x version is the most conservative; the 0.6x version is the best risk-adjusted compromise in full sample; the 1.0x version is too aggressive for recent windows.

## Recommendation

Do not change v1.4 mainline directly yet.

If we continue this path, the only rule worth a second-stage production-style test is:

`cash/no-exposure -> 0.6x reentry when shadow spread rebound >= 2% and momentum_gap >= -1%`

It should be tested next with:

- exact v1.4 source-level rebuild, not only output-file overlay
- rebalance-cost handling against actual Top100 member turnover
- 1Y/3Y/5Y/10Y windows
- a specific guard against the 2024-01 false-rebound episode
