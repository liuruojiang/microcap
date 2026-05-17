# Microcap Top100 v2.3 Formalization And Cleanup

Date: 2026-05-17

## Formal script

- Script: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Base implementation: imports and reuses `microcap_top100_mom16_biweekly_live_v2_0.py`
- Role: defensive optional branch, not the primary return leader in the v2.0-v2.3 comparison
- Output prefix: `microcap_top100_mom16_biweekly_live_v2_3`

## v2.3 parameters

- Signal model: annualized weighted log slope of always-on spread NAV
- Signal spread hedge ratio: `1.0`
- Execution hedge ratio: `0.8`
- Lookback: `17`
- Exponential half-life: `4.0`
- R2 gate: disabled
- Score exit buffer: `0.13`
- Peak-decay derisk: decay `0.35`, recovery `0.50`, derisk scale `0.0`
- Target volatility: `0.25`
- Scale rebalance threshold: `0.30`
- Cost model: costed `return_net`, including embedded-lineage trading cost, scale-change cost, financing cost above 1.0x exposure

## Source evidence

- Candidate scan: `quant_param_scan_runs/20260517_microcap_top100_v2_2_research_s1e08_logwls_exp4lb17_tv25_scale030_buffer013_peak_decay`
- Selected candidate: `new_exp_h4_lb17_tv25_scale030_buffer013_decay_0p35_recovery_0p5`
- Cross-version comparison: `quant_param_scan_runs/20260517_microcap_top100_v2_0_to_v2_3_full_costed_compare`
- Common comparison window: 2010-05-05 to 2026-05-15, rows `3882`

## Comparison snapshot

All rows used freshly rebuilt `return_net` on one common date index.

| version | annualized return full | max drawdown full | 10Y annualized return | 10Y max drawdown | 5Y annualized return | 5Y max drawdown | 3Y annualized return | 3Y max drawdown | 1Y annualized return | 1Y max drawdown |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v2.0 | 36.6649% | -15.5891% | 32.8401% | -15.5891% | 33.2555% | -15.4027% | 41.1000% | -15.3887% | 22.0069% | -12.1804% |
| v2.1 | 37.4650% | -16.9951% | 33.0519% | -16.9459% | 33.6090% | -16.2118% | 37.4383% | -13.8179% | 23.4422% | -13.8179% |
| v2.2 | 37.7330% | -13.5818% | 30.4714% | -13.5818% | 32.5699% | -13.5818% | 34.8521% | -12.4975% | 27.5771% | -11.9170% |
| v2.3 | 33.6891% | -13.4978% | 32.7486% | -13.4978% | 36.2269% | -13.4978% | 39.3290% | -10.6509% | 18.0482% | -10.4837% |

## Verification before cleanup

- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_3.py`
- `python tests\test_microcap_top100_v2_3_formal.py`
  - Result: 2 tests passed
  - Verified v2.3 constants and date-level parity against the selected candidate scan for `return_net` and `nav_net`

## Cleanup scope

Removed after backup:

- `tests/`
- `quant_param_scan_runs/`
- regenerated `__pycache__/`

Backup:

- `.codex_backups/20260517_160017/tests_quant_param_scan_runs.tar`

Preserved:

- formal strategy scripts
- `docs/`
- tracked `outputs/`
- `.codex_backups/`
- archived research folders
