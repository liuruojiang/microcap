# Microcap Top100 v2.5 Review Cleanup Sync Record - 2026-05-30

## Scope

This closes the 2026-05-30 v2.5 script-level review hardening round.

The synced production file is `microcap_top100_mom16_biweekly_live_v2_5.py`.
The active temporary pytest suite was used for review verification only and was
removed before sync, matching the current workspace cleanup convention.

## Removed Locally

Backed up first to:

- `.codex_backups/20260530_115620`

Removed from the active workspace:

- `tests/test_v25_cash_day_yield.py`
- `tests/`
- `TASK_STATE.md`
- `.pytest_cache/`
- `__pycache__/`
- active non-backup `__pycache__` / `.pytest_cache` folders under the workspace

Backup snapshots under `.codex_backups/` were preserved.

## Preserved And Synced

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv`
- this cleanup record

## Review Fixes Preserved In Source

- v2.5 contract validation now covers the actual v2.0 dependencies used by the
  wrapper, including `ENTRY_COST` and `EXIT_COST`.
- Full cash days receive idle cash yield and the version note now matches that
  behavior.
- The target-vol scale-change cost uses the embedded microcap one-side entry and
  exit cost rates, and the note formats the rate from constants.
- v2.5 performance query generation and performance output now share the same
  output lock and preserve the `date` index contract.
- The realtime v2.5 path uses the v2.0 official historical index for anchor
  consistency while preserving an appended intraday snapshot row.
- Official-index holes emit a warning rather than blocking generation.
- The historical rewrite audit tail is sized from the signal and target-vol
  dependency span, with a note about possible path-dependent target-vol resync.

## Verification Before Cleanup

The temporary pytest suite was run before removal:

- `python -m pytest tests/test_v25_cash_day_yield.py -q` -> `16 passed`
- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_5.py tests/test_v25_cash_day_yield.py`
- `python -c "import microcap_top100_mom16_biweekly_live_v2_5 as v25; v25.validate_v2_0_contract(); ..."` -> `contract ok`

Lightweight realtime official-index check:

- rows: `3924`
- start: `2010-03-04`
- end: `2026-05-27`

## Post-Cleanup Checks

- no active `tests/` directory remains
- no active `TASK_STATE.md` remains
- no active non-backup `.pytest_cache/` or `__pycache__/` folders remain
- `python -m py_compile microcap_top100_mom16_biweekly_live_v2_5.py`

## Sync Plan

Commit and push the v2.5 source hardening, tracked refreshed v2.0 panel row, and
this cleanup record to `origin/main`.
