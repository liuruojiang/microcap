# Microcap Top100 Test Cleanup Sync Record - 2026-05-22

## Scope

This cleanup removes active test-only and scan-only artifacts from the last two work days, covering 2026-05-21 and 2026-05-22 local workspace activity.

The cleanup intentionally avoids deleting durable research records under `docs/`, formal strategy scripts already tracked by the repository, and current core artifacts that are part of the normal workspace state.

## Removed

Tracked scan-run artifacts removed from the working tree:

- `quant_param_scan_runs/20260521_microcap_top100_v2_0_targetvol_overlay_target_vol/`
- `quant_param_scan_runs/20260521_microcap_top100_v2_3_targetvol_overlay_target_vol/`

Untracked or ignored active test artifacts removed locally:

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- `tests/test_v2_5_standalone_params.py`
- `quant_param_scan_runs/20260522_microcap_top100_v2_0_v2_3_signal_model_lookback_10_12/`
- `outputs/*v2_5*`
- `outputs/*20260522*`
- `.microcap_index_cache/realtime/*v2_5*`
- `__pycache__/`
- `tests/__pycache__/`
- `.pytest_cache/` when present

The tracked output files touched by local signal/performance refresh commands were restored to the repository version after cleanup:

- `outputs/microcap_top100_mom16_biweekly_live_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json`
- `outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv`

## Preserved

- `docs/` research records and prior cleanup records
- tracked permanent tests still present under `tests/`
- formal tracked strategy entrypoints
- current core output artifacts required by the workspace defaults
- `.codex_backups/` historical backups

## Backup

Before deletion, active source/test/scan targets and tracked outputs were backed up to:

- `.codex_backups/20260522_234230`

Ignored runtime outputs under `outputs/` matching `*v2_5*` or `*20260522*` were treated as disposable generated artifacts.

## Verification

Commands used:

```powershell
git status --short --branch
git ls-files --others --exclude-standard
Get-ChildItem outputs -File | Where-Object { $_.Name -like '*v2_5*' -or $_.Name -like '*20260522*' }
```

Observed before this record was added:

- no untracked files remained
- no `outputs/*v2_5*` or `outputs/*20260522*` files remained
- the only tracked cleanup delta was removal of the two 2026-05-21 scan-run directories

## Sync Notes

Publish target:

- `origin = git@github.com:liuruojiang/microcap.git`
- branch: `main`

This record is intended to be committed together with the tracked scan-run deletions and pushed to `origin/main`.
