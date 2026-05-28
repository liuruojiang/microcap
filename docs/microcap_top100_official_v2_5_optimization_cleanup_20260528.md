# Microcap Top100 Official v2.5 Optimization Cleanup - 2026-05-28

## Scope

This cleanup records the completed official v2.5 layered optimization pass and removes temporary test files used only to validate research scan helpers.

The production strategy file was not changed:

- `microcap_top100_mom16_biweekly_live_v2_5.py`

## Retained Evidence

The following artifacts remain as the source of truth for this pass:

- `docs/microcap_top100_official_v2_5_optimization_directions_20260528.md`
- `docs/microcap_top100_v2_5_abs120_tv40_max1p0_layer_handoff_20260528.md`
- `docs/microcap_top100_test_cleanup_sync_20260528.md`
- successful cleanup backup: `.codex_backups/20260528_193114_test_cleanup_sync`

The local ignored scan folders and untracked scan helper scripts were removed in
the final cleanup pass. They are preserved only in the backup above; durable
research conclusions are kept in the markdown records.

## Removed Temporary Tests

These root-level temporary test files were removed after their scan artifacts had been finalized and strict-checked:

- `test_official_v2_5_post_surge_score_deceleration.py`
- `test_official_v2_5_conditional_maxlev_cap.py`
- `test_official_v2_5_r2_quality_derisk.py`
- `test_official_v2_5_exit_hysteresis.py`
- `test_official_v2_5_target_vol_fine.py`
- `test_staged_entry_timebox.py`
- `test_entry_turnover_gate.py`
- `test_broad_volume_amount_overlay.py`
- `test_post_surge_cooldown_derisk.py`
- `test_momentum_stall_cooldown_derisk.py`

## Final Decision

No independent layer was promoted into official v2.5.

The only follow-up worth testing is a narrow combination of the two watchlist local layers:

- post-surge score deceleration cooldown
- conditional max-leverage cap

Global R2 quality derisk, exit hysteresis, and target-vol/max-leverage cuts were rejected for poor recent-window or all-window tradeoffs.

## Final Cleanup Pass

User request: clean all test files in this directory, record it, and sync to cloud.

Removed locally after backup:

- all untracked `scripts/*v2_5*.py` one-off scan/helper scripts from the 2026-05-28 test round;
- tracked `tests/test_realtime_refresh_cache_dirs.py` pulled from remote during sync;
- `TASK_STATE.md` scratch handoff state;
- all ignored `quant_param_scan_runs/20260528_*` local scan output folders;
- root `__pycache__/`, `scripts/__pycache__/`, and `.pytest_cache/`.

Preserved:

- production strategy files, including `microcap_top100_mom16_biweekly_live_v2_5.py`;
- current tracked `outputs/` artifacts;
- durable records under `docs/`;
- historical backups under `.codex_backups/`.

Additional official v2.5 checks completed before cleanup:

- Official v2.5 absolute-momentum filter scan: do not require `abs_momentum > 0`; the only mild recent-window candidate was `ABS160 > -20%`, but it was too small to promote.
- Official v2.5 raw NAV drawdown threshold scan: no drawdown exit/recovery threshold had positive contribution across 10Y, 5Y, 3Y, and 1Y; high thresholds did not trigger, lower thresholds damaged returns.

Final cleanup backup:

- `.codex_backups/20260528_193114_test_cleanup_sync/removed_manifest.txt`
