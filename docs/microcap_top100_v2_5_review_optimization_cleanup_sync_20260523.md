# Microcap Top100 v2.5 Review Optimization Cleanup Sync Record - 2026-05-23

## Scope

This record documents the v2.5 strategy-layer review fixes, the R2 scale-up gate
rollback decision, cleanup of temporary review files, and the cloud sync scope.

The formal source changes are limited to:

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- `microcap_top100_mom16_biweekly_live_v2_0.py`

The comparison and validation used the real local v2.3/v2.5 entrypoints and
current local output/data cache. Both formal performance query outputs ended on
`2026-05-22`.

## Implemented v2.5 Fixes

- Added early `--bootstrap-deps` handling before importing heavy runtime
  packages.
- Added `--force-refresh` support in v2.5 and forwarded it into the embedded
  v2.0 runtime args.
- Propagated embedded v2.0 `force_refresh` into `_build_base_args()`.
- Added explicit v2.0 contract validation before v2.5 builds.
- Made `apply_target_vol()` fail fast when required cost/pre-cost columns are
  missing instead of silently zeroing cost fields.
- Made v2.5 use `overlay_pre_cost_return` directly for base pre-cost return.
- Split historical and realtime fingerprints so realtime options do not make
  historical summaries look incompatible.
- Added compatibility-audit JSON output only when summaries are stale or
  incompatible; successful rebuild removes the audit file.
- Clarified Sharpe fields as `sharpe_cagr`, `cagr_to_vol`, and `sharpe_mean`,
  while retaining `sharpe` as the backward-compatible CAGR/vol alias.
- Fixed realtime target-vol frozen-lag metadata:
  - `target_vol_frozen_lag_calendar_days`
  - `target_vol_frozen_lag_trading_days`
  - realtime freshness guard uses trading-day lag.

## Strategy Decisions

The R2 execution-scale gate was tested and removed from formal v2.5.

Reason:

- It is a strategy-layer change, not a pure script hardening fix.
- The measured comparison did not justify keeping it in the mainline.
- Formal v2.5 therefore remains: no R2 gate, no hedge, no stop-loss/DD/decay/
  overheat overlay, target volatility `30%`, max leverage `1.3x`, and scale
  rebalance threshold `0.30`.

The cleaned-up review scan artifacts are preserved under:

- `quant_param_scan_runs/20260523_microcap_top100_v2_5_strategy_layer_review_entry_exit_r2_scale_vol_confirm/`
- `quant_param_scan_runs/20260523_microcap_top100_v2_5_target_vol_r2_scale_up_gate_r2_scale_up_threshold_fine/`

## Formal Performance Snapshot

Command outputs refreshed before this cleanup:

```powershell
python microcap_top100_mom16_biweekly_live_v2_3.py 表现
python microcap_top100_mom16_biweekly_live_v2_5.py --force-refresh 表现
```

Full-sample costed performance:

| Version | Start | End | Rows | Final NAV | Annual Return | Max Drawdown |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| v2.3 | 2010-05-05 | 2026-05-22 | 3884 | 197.0465 | 38.99% | -16.99% |
| v2.5 | 2010-05-05 | 2026-05-22 | 3884 | 152.2009 | 36.77% | -19.48% |

Recent-window costed comparison:

| Window | Start | End | v2.3 Annual | v2.3 Max DD | v2.5 Annual | v2.5 Max DD |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Full | 2010-05-05 | 2026-05-22 | 38.99% | -16.99% | 36.77% | -19.48% |
| 10Y | 2016-05-23 | 2026-05-22 | 35.27% | -13.80% | 30.74% | -16.22% |
| 5Y | 2021-05-24 | 2026-05-22 | 35.36% | -13.80% | 36.25% | -13.76% |
| 3Y | 2023-05-22 | 2026-05-22 | 39.46% | -11.91% | 41.73% | -13.76% |
| 1Y | 2025-05-22 | 2026-05-22 | 20.15% | -11.91% | 58.86% | -13.76% |

## Cleanup

Temporary review-only files removed after documentation:

- `scripts/run_microcap_v2_5_r2_scale_up_fine_scan.py`
- `scripts/run_microcap_v2_5_strategy_layer_review_scan.py`
- `tests/test_v2_5_review_regressions.py`
- `tests/__pycache__/`

Backup written before deletion:

- `.codex_backups/20260523_202609/`

Preserved:

- formal v2.5 source
- embedded v2.0 force-refresh support
- existing tracked v2.5 research scripts under `scripts/`
- scan-run folders under `quant_param_scan_runs/`
- current official `outputs/`
- prior docs and backups

## Verification

Before deleting the temporary review test:

```powershell
python -m pytest tests\test_v2_5_review_regressions.py -q
```

Observed result:

- `20 passed`

Post-cleanup validation:

```powershell
python -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_5.py
python microcap_top100_mom16_biweekly_live_v2_5.py --force-refresh 信号
rg --files -g "test_*.py"
```

Observed results:

- `py_compile` completed successfully.
- `python microcap_top100_mom16_biweekly_live_v2_5.py --force-refresh 信号`
  completed successfully.
- Latest close-confirmed v2.5 signal: `cash -> cash`, `signal_date =
  2026-05-22`, `current_execution_scale = 0.00`.
- `rg --files -g "test_*.py"` returned no files.

## Sync Notes

Publish target:

- remote: `origin = git@github.com:liuruojiang/microcap.git`
- branch: `main`

The intended commit should include only the v2.0/v2.5 source changes and this
cleanup record.
