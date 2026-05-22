# Microcap Top100 v2.4 Validation Cleanup Sync Record - 2026-05-23

## Scope

This record documents the local pre-live validation pass for
`microcap_top100_mom16_biweekly_live_v2_4.py`, cleanup of temporary validation
artifacts, and the intended sync target.

The validation used the real local v2.4 entrypoint, the embedded v2.0 base
context, and the current local output/data cache. The latest generated v2.4 NAV
date in this pass was `2026-05-22`.

## Validation Results

### 1. Exit Buffer Return Recalculation

The targeted Day 1 / Day 2 / Day 3 buffer scenario was checked through
`v2_0.base_mod.apply_momentum_gap_exit_buffer()` and the v2.4 derisk path.

Observed Day 3 result:

- `holding = long_microcap_short_zz1000`
- `next_holding = long_microcap_short_zz1000`
- `return_raw = 0.01816`
- `return = 0.01816`
- expected formula result: `microcap_ret - 0.8 * hedge_ret - futures_drag * 0.8 = 0.01816`
- absolute difference: `0.0`

Conclusion: when the buffer extends an existing position, the active daily
return is recomputed from the buffered holding state and is not left at zero.

### 2. Second-Run Stale Output And Historical Rewrite Guard

The v2.4 main script was run twice consecutively:

```powershell
python microcap_top100_mom16_biweekly_live_v2_4.py
python microcap_top100_mom16_biweekly_live_v2_4.py
```

Both runs completed and wrote:

- `outputs/microcap_top100_mom16_biweekly_live_v2_4_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_v2_4_latest_signal.csv`
- `outputs/microcap_top100_mom16_power_p0p75_lb20_signal1p0_exec0p8_gap18_decay35_recovery40_targetvol25_scale010_v2_4_costed_nav.csv`

A follow-up spy run on `generate_v2_4_outputs()` observed:

- `pre_stale_outputs = []`
- `COSTED_NAV_CSV` existed before generation
- `assert_no_historical_rewrite()` call count: `2`
- v2.4 guard call label: `v2.4 official costed NAV`
- v2.4 key columns: `return_net`, `holding`, `next_holding`, `base_pre_cost_return`
- v2.4 allowed tail rows: `40`
- `post_stale_outputs = []`
- output rows: `3884`
- latest NAV date: `2026-05-22`

Conclusion: the current v2.4 fingerprint is stable across consecutive runs, and
the historical rewrite guard is reached on the second clean run.

### 3. Scale Trade State Enumeration

The latest signal row reported:

- `scale_trade_state = hold_scale`
- `trade_state = hold`

The v2.4 NAV does not store a daily `scale_trade_state` column, so the production
signal formula was replayed across the full NAV:

- `hold_scale`: `3871`
- `rebalance_scale`: `13`

No derived `frozen_scale`, `not_applicable`, or other non-rebalance state was
observed. The current implementation can therefore keep treating any
non-`hold_scale` value as a scale rebalance trigger.

## Test Commands

```powershell
python -m pytest tests\test_v2_4_review_guardrails.py -q
python -m pytest tests\test_v2_4_priority_behaviors.py -q
```

Observed results:

- `8 passed`
- `22 passed`

## Cleanup

Temporary validation artifacts removed after documentation:

- `tests/test_v2_4_priority_behaviors.py`
- `tests/test_v2_4_review_guardrails.py`
- `quant_param_scan_runs/20260523_microcap_top100_v2_4_peak_decay_min_peak_to_arm_decay_fine/`
- `quant_param_scan_runs/20260523_microcap_top100_v2_4_strategy_layer_hedge_peak_score_buffer/`

Backup written before deletion:

- `.codex_backups/20260523_v2_4_validation_cleanup/`

Preserved:

- formal v2.4 strategy source
- existing tracked permanent tests under `tests/`
- `docs/`
- current official `outputs/`
- `archive/`
- prior `.codex_backups/`

## Sync Notes

Publish target:

- remote: `origin = git@github.com:liuruojiang/microcap.git`
- branch: `main`

The intended commit should include the v2.4 formal source update and this
validation/cleanup record only.
