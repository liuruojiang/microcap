# Microcap Top100 v2.0 Parameter Replacement - 2026-06-29

## Decision

The official `v2.0` default in `microcap_top100_mom16_biweekly_live_v2_0.py` has been replaced with the selected post-P0 low-drawdown line from the standard layered test process.

This replaces the old pure target-vol line:

- target volatility: 25%
- target-vol window: 60 trading days
- max leverage: 1.5x
- scale rebalance threshold: 10%
- no volatility-overheat exit
- official costed path: `outputs/microcap_top100_mom16_targetvol25_max1p5_v2_0_costed_nav.csv`

The new official `v2.0` line is:

- lookback: 16
- R2 filter: off
- momentum-gap exit buffer: 0.30%
- peak-decay derisk: off
- net-value drawdown stop: off
- target volatility: 15%
- target-vol window: 75 trading days
- max leverage: 1.5x
- scale rebalance threshold: 10%
- volatility-overheat exit: 60-day realized volatility of `microcap - 0.8x hedge` spread, threshold 23%
- overheat trigger constraint: trigger only when current trade return is positive
- reentry rule: after overheat exit, stay in cash until the base momentum signal resets
- volume/amount overlay: rejected, not included
- official costed path: `outputs/microcap_top100_mom16_targetvol15_max1p5_v2_0_costed_nav.csv`

The old `targetvol25` official artifact is now treated as a legacy path and is removed by official v2.0 regeneration. A pre-replacement backup was saved under `backups/v2_0_param_replacement_20260629_135425/`.

## Freshness Proof

Generated official outputs were read back after replacement.

| Stream | Latest date | Rows |
|---|---:|---:|
| v2.0 new costed NAV | 2026-06-26 | 3958 |
| base panel shadow | 2026-06-26 | 8671 |
| base proxy index | 2026-06-26 | 3992 |
| base proxy turnover | 2026-06-25 | 425 |
| base costed NAV | 2026-06-26 | 3975 |

The turnover latest date is the latest rebalance date; daily streams share the same latest close-confirmed trading date, 2026-06-26.

## Performance Comparison

Comparison is aligned on the common daily date range of old and new v2.0 costed streams: 2010-03-11 to 2026-06-26, 3958 rows. Annualization uses the repository calendar-year performance convention, with 244 trading days for volatility.

| Window | Old ann. | Old max DD | New ann. | New max DD | Ann. delta | DD improvement | Old vol | New vol | Old Sharpe | New Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Full | 33.52% | -20.15% | 28.04% | -14.58% | -5.48pp | +5.57pp | 15.77% | 12.21% | 2.13 | 2.30 |
| 10Y | 29.19% | -20.15% | 23.66% | -11.51% | -5.53pp | +8.63pp | 17.36% | 12.56% | 1.68 | 1.88 |
| 5Y | 32.04% | -20.15% | 22.15% | -11.51% | -9.89pp | +8.63pp | 19.56% | 12.44% | 1.64 | 1.78 |
| 3Y | 28.17% | -20.15% | 19.43% | -11.51% | -8.73pp | +8.63pp | 19.12% | 11.43% | 1.47 | 1.70 |
| 1Y | 3.56% | -19.57% | 4.65% | -10.74% | +1.09pp | +8.83pp | 17.28% | 10.54% | 0.21 | 0.44 |

## Candidate Match Check

The generated official stream was compared with the selected scan artifact:

`quant_param_scan_runs/20260629_microcap_top100_v2_0_post_p0_lineage_retest_three_direction_overheat_layer10/daily_oh_vol_w060_t23.csv`

Read-back checks on the common index:

- `return_net` max absolute difference: `3.02e-16`
- `nav_net` max absolute difference: `2.10e-12`
- `holding` mismatches: `0`
- `next_holding` mismatches: `0`
- `current_execution_scale` max absolute difference: `0`
- `target_vol_realized_vol` max absolute difference: `0`
- `overheat_metric` max absolute difference: `0`
- `overheat_triggered` mismatches: `0`
- `blocked_until_signal_reset` mismatches: `0`

## Downstream Notes

- `v2.3` uses the v2.0 target-vol helper and now validates against the promoted v2.0 target-vol window of 75.
- `v2.5` keeps its own target-vol window at 60, but its v2.0 dependency contract now validates the promoted v2.0 window of 75 instead of requiring equality with the v2.5 window.
- `v2.3` and `v2.5` should be rerun under this post-P0 v2.0 dependency before any new parameter promotion.
- Current outputs remain labeled as public/local proxy, not official Wind `868008.WI`.

## Verification

- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_5.py` passed.
- `python -m pytest tests/test_top100_data_guards.py -q` passed: 13 tests.
- `python microcap_top100_mom16_biweekly_live_v2_0.py` regenerated official v2.0 summary, latest signal, NAV, and costed NAV.
- Generation warning was expected: current performance source is public/local proxy, not official Wind `868008.WI`.
