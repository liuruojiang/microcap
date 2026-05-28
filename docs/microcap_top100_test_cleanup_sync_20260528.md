# Microcap Top100 Test Cleanup Sync Record - 2026-05-28

## Scope

This closes the 2026-05-28 official v2.5 and ABS-branch research/test round.

Production strategy logic was not changed in this cleanup. The cleaned files were
local test artifacts: one-off scan scripts, ignored scan-run folders, scratch
handoff state, and Python test caches.

## Removed Locally

Backed up first to:

- `.codex_backups/20260528_193114_test_cleanup_sync`

Removed from the active workspace:

- `81` paths listed in `.codex_backups/20260528_193114_test_cleanup_sync/removed_manifest.txt`
- all untracked `scripts/*v2_5*.py` scan/helper scripts from this test round
- all ignored `quant_param_scan_runs/20260528_*` folders
- tracked test file pulled from remote during sync: `tests/test_realtime_refresh_cache_dirs.py`
- `TASK_STATE.md`
- `.pytest_cache/`
- `__pycache__/`
- `scripts/__pycache__/`

## Preserved

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- tracked formal scripts already in the repository
- current tracked `outputs/` artifacts
- durable markdown records under `docs/`
- historical archives and backups

## Research Decisions Recorded

Official v2.5 remained the source of truth. No tested overlay or filter was
promoted into production.

Rejected or watchlist-only layers from this round:

- post-surge score-deceleration cooldown: watchlist only, not promoted
- conditional max-leverage cap: watchlist only, not promoted
- R2 quality derisk: rejected
- exit hysteresis: rejected
- global target-vol/max-leverage fine scan: rejected
- official absolute-momentum filter: rejected as a production filter
- raw official v2.5 NAV drawdown threshold: rejected

The final raw NAV drawdown scan found no candidate with positive annualized-return
contribution across 10Y, 5Y, 3Y, and 1Y. High drawdown thresholds were mostly no-op
rows; lower thresholds reduced returns materially.

## Verification

Post-cleanup checks:

- no active root `test_*.py` files remain
- no active `tests/test_*.py` files remain
- no active untracked `scripts/*v2_5*.py` files remain
- no active `quant_param_scan_runs/20260528_*` folders remain
- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_5.py`

## Sync Plan

Commit and push the cleanup records plus preserved current tracked output changes
to `origin/main`.
