# Microcap Top100 Test Record, Cleanup, And Sync

Date: 2026-05-18
Workspace: Top100 microcap hedged strategy

## Scope

This record consolidates the 2026-05-17 and 2026-05-18 research/test runs before removing temporary scan artifacts from the workspace.

The raw-signal replacement tests used the strict cost-only research口径:

- Signal curve: always-on `microcap - 1.0 * ZZ1000` spread NAV.
- Execution return: `microcap - 0.8 * ZZ1000`.
- Return column: costed `return_net`.
- Kept: official transaction-cost model.
- Removed: buffer, R2 gate, absolute-momentum filter, peak-decay, target-vol scaling, scale-change cost, financing/futures drag, macro gate, and other non-cost overlays unless a run explicitly documents a layered overlay test.

## Raw Signal Test Summary

| Run | Window | Decision |
| --- | --- | --- |
| `20260517_microcap_top100_v2_3_raw_signal_1p0_spread_cost_only_raw_momentum` | 2010-05-05 to 2026-05-15, 3882 rows | Keep official `log-WLS h4/lb17` as the best raw cost-only signal; Power-WMA needs overlay context before it helps. |
| `20260517_microcap_top100_v2_3_raw_signal_strategy_a_b_cost_only` | 2011-09-09 to 2026-05-15, 3551 rows | Strategy B long ROC/EMA can raise return but drawdown expands to about -40%; Strategy A bias rejected; official log-WLS remains best risk/drawdown tradeoff. |
| `20260517_microcap_top100_v2_3_raw_signal_strategy_b_ema_halflife_cost_only` | 2010-12-02 to 2026-05-15, 3742 rows | Reject Strategy B EMA as raw replacement; h200/h100 style windows have unacceptable drawdown; h10 is only a watchlist point and still below log-WLS. |
| `20260518_microcap_top100_v2_3_raw_signal_power_wma_window_cost_only` | 2010-05-05 to 2026-05-15, 3882 rows | Reject Power/Sqrt/Linear WMA as raw replacement; best `p0.75/lb20` is below official log-WLS and has slightly larger drawdown. |
| `20260518_microcap_top100_v2_3_raw_signal_research_raw_signal_linear_wls_nav_slope_cost_only` | 2010-05-05 to 2026-05-15, 3882 rows | Reject NAV-slope/linear-WLS; `exp h3/lb24` has only a tiny full-sample return edge but worse drawdown and weaker recent windows. |
| `20260518_microcap_top100_v2_3_raw_signal_research_raw_signal_roc_window_cost_only` | 2011-09-09 to 2026-05-15, 3551 rows | Reject ROC windows; long ROC raises return with unacceptable -40% drawdown, short ROC controls drawdown but lags official log-WLS. |
| `20260518_microcap_top100_v2_3_raw_signal_research_raw_signal_strategy_a_bias_window_cost_only` | 2011-01-28 to 2026-05-15, 3702 rows | Reject Strategy A bias/bias-momentum; long-MA bias has large drawdown and weak recent windows, bias momentum has far lower return. |
| `20260518_microcap_top100_v2_3_raw_signal_research_raw_signal_risk_adjusted_momentum_cost_only` | 2011-09-09 to 2026-05-15, 3551 rows | Reject risk-adjusted momentum; volatility normalization preserves signal direction for mean/ROC, long EWMA Sharpe has -40% drawdown, short windows lag log-WLS. |

Final raw-signal conclusion: under the strict cost-only口径, `v2.3` official `log-WLS h4/lb17` remains the preferred raw signal anchor. No tested single-family raw signal replacement should be promoted.

## Power Signal And v2.4 Overlay Tests

| Run | Decision |
| --- | --- |
| `20260517_microcap_top100_v2_3_signal_model_1p0_spread_raw_momentum_methods` | Initial broad non-log grid promoted linear WMA candidates for follow-up, but this was superseded by later strict cost-only checks. |
| `20260517_microcap_top100_v2_3_signal_model_1p0_spread_power_wma_refine` | Narrow Power-WMA refinement identified a stable cluster around `p0.75..p1.25` and `lb20..lb26` for overlay follow-up. |
| `20260517_microcap_top100_v2_3_power_raw_raw_signal_power_wma_ridge_width_cost_only` | Power-WMA had a focused peak but broad shoulder; `p0.75/lb20` was selected as the cost-only center before overlay layering. |
| `20260517_microcap_top100_v2_3_power_raw_layered_overlay_power_p0p75_lb20_buffer_decay_targetvol` | Layered stack improved under risk-constrained target-vol 20%; return-max target-vol 35% was rejected for worse drawdown. |
| `20260517_microcap_top100_v2_4_power_layered_overlay_buffer_fine_nested_decay_targetvol` | Buffer mountain was wide; current `0.13` usable, but `0.17..0.19` was the better center after nested scan. |
| `20260517_microcap_top100_v2_4_power_walk_forward_buffer_train5y_test1y_fixed_stack` | Walk-forward supported v2.4 structure but not dynamic buffer chasing; fixed `buffer=0.18` was preferable to train-best selection. |
| `20260517_microcap_top100_v2_4_power_candidate_final_validation_buffer17_18_decay45_recovery45_tv25_scale20` | Initialized as a final-validation run but did not produce completed scan CSVs; superseded by the formal v2.4 note and completed layered/walk-forward runs. |

Formal v2.4 documentation is preserved in `docs/microcap_top100_power_momentum_method_v2_4_20260517.md`.

## Peak-Decay Timing Tests

| Run | Decision |
| --- | --- |
| `20260517_microcap_top100_v2_0_to_v2_4_peak_decay_overlay_no_peak_decay_compare` | Diagnostic only. Removing peak-decay was mixed by version/window and should not switch production defaults by itself. |

The close-execution timing issue and cross-version checklist are preserved in `docs/peak_decay_close_execution_timing_issue_20260517.md`.

## Validation Status

Completed quant-param-scan runs were finalized and strict-checked with:

```powershell
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\finalize_quant_param_scan_run.py <run_dir> ...
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\check_quant_param_scan_artifacts.py --phase complete --strict <run_dir>
```

All finalized runs listed above reported `PASS`. The peak-decay ablation was diagnostic, and the v2.4 final-validation folder was only initialized.

## Cleanup Record

Cleanup target:

- `quant_param_scan_runs/` temporary scan directories.
- Untracked verification files: `tests/test_shared_peak_decay_close_timing.py`, `tests/test_v2_0_v2_2_no_peak_decay_official.py`, `tests/test_v2_4_peak_decay_close_timing.py`.
- Python/pytest caches generated by the test runs.

Preserved:

- Formal strategy scripts.
- Existing tracked test `tests/test_realtime_anchor_quote_guard.py`.
- `docs/` records.
- Tracked `outputs/` and local cache files.

Backup:

- `.codex_backups/20260518_011105`

Cleanup verification:

- `quant_param_scan_runs/`: removed.
- New untracked verification files under `tests/`: removed.
- Python/pytest caches: removed.
- Existing tracked test `tests/test_realtime_anchor_quote_guard.py`: preserved.

## Sync Record

This document is included in the cleanup/sync commit on `main`.
