# Microcap v2.0/v2.3 Robustness Scan Cleanup Record

Date: 2026-05-21

## Scope

This cleanup follows the v2.0/v2.3 parameter robustness scan for:

- `microcap_top100_mom16_biweekly_live_v2_0.py`
- `microcap_top100_mom16_biweekly_live_v2_3.py`

The scan was research-only. No production strategy source files were intentionally changed by the cleanup step.

## Archived Evidence

Durable archive path:

- `docs/microcap_v2_0_v2_3_robustness_20260521/`

Archived files:

- `robustness_findings.md`
- `record.md`
- `scan_meta.json`
- `stability_overview.csv`
- `parameter_sensitivity.csv`
- `window_metrics.csv`
- `scan_summary.csv`
- `default_neighborhood_top50.csv`
- `run_v20_v23_robustness_scan.py`
- `command_log.txt`

The full candidate-level evidence was preserved in `scan_summary.csv` and `window_metrics.csv`.

## Cleanup Action

Removed scratch/test run directory after the archive copy was verified:

- `quant_param_scan_runs/20260521_microcap_top100_v2_0_v2_3_signal_targetvol_overlay_lookback_halflife_gap_targetvol_scale_hedge/`

## Preserved Files

The following existing modified files were not cleaned or reverted because they are either current core artifacts or unrelated pre-existing worktree changes:

- `microcap_top100_mom16_biweekly_live_v2_0.py`
- `microcap_top100_mom16_biweekly_live_v2_3.py`
- `microcap_top100_mom16_biweekly_live_v2_4.py`
- `outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json`
- `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv`
- `outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv`
- `tests/test_realtime_anchor_quote_guard.py`

## Verification

Before cleanup, the scan artifacts passed:

```powershell
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\check_quant_param_scan_artifacts.py --phase complete --strict .
```

After archiving, the durable docs copy should be treated as the source of record for this research pass.
