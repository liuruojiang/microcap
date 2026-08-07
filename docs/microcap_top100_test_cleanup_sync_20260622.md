# Microcap Top100 Test Cleanup Sync Record - 2026-06-22

## Scope

This closes the temporary realtime hedge quote date parsing test pass and removes
leftover one-off research/test artifacts from the active workspace.

Production strategy logic was not changed in this cleanup. The retained branch
logic remains in `microcap_top100_mom16_biweekly_live_v2_0.py`.

## Backed Up First

Backed up removable active artifacts to:

- `.codex_backups/20260622_213446_test_cleanup_sync`

The backup manifest is:

- `.codex_backups/20260622_213446_test_cleanup_sync/removed_manifest.txt`

## Removed From Active Workspace

- `tests/test_realtime_hedge_quote_date_parsing.py`
- `tests/__pycache__/`
- root `__pycache__/`
- `scripts/__pycache__/`
- `scripts/run_microcap_v2_3_gated_by_v2_5.py`
- `quant_param_scan_runs/20260617_microcap_top100_v2_3_gated_by_v2_5/`

## Preserved

- formal strategy scripts, including `microcap_top100_mom16_biweekly_live_v2_0.py`
- durable markdown records under `docs/`
- historical archives under `archive/` and Chinese-named legacy archive folders
- prior safety backups under `.codex_backups/`
- current tracked `outputs/` artifacts

## Verification

- no active root `test_*.py` files remain
- no active `tests/test_*.py` files remain
- no active untracked `scripts/run_microcap_v2_3_gated_by_v2_5.py` remains
- no active `quant_param_scan_runs/20260617_microcap_top100_v2_3_gated_by_v2_5/` folder remains
- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py`
- root `__pycache__/` created by compilation was removed after verification

## Sync Plan

Commit and push the cleanup to `origin/codex/fix-realtime-hedge-date`.
