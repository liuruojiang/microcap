# Microcap Top100 Test Cleanup Sync Record - 2026-05-21

## Scope

This cleanup removes active test-only files from the working tree after the v2.0 / v2.3 realtime signal console-output fix and the broader v2.0 / v2.3 / v2.4 robustness work.

## Removed

- `tests/`
- `.pytest_cache/`
- `__pycache__/`
- `scripts/__pycache__/`

The tracked active test file removed by this pass was:

- `tests/test_realtime_anchor_quote_guard.py`

## Preserved

- `.codex_backups/` historical backups
- `archive/` and other historical archive folders
- `outputs/` official strategy artifacts
- `docs/` research and cleanup records
- Formal strategy scripts and realtime workflow scripts

Archived or backup files whose names include `test` were intentionally preserved as historical evidence, not active test files.

## Backup

Before deletion, the active cleanup targets were backed up to:

- `.codex_backups/20260521_120332`

## Verification

After test cleanup, no permanent active test suite remains in the repository root. Verification therefore uses compile checks on the surviving formal Python entrypoints touched by the current work:

- `microcap_top100_mom16_biweekly_live_v2_0.py`
- `microcap_top100_mom16_biweekly_live_v2_3.py`
- `microcap_top100_mom16_biweekly_live_v2_4.py`

Observed verification:

```powershell
python -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_4.py
```

Result: passed.

Active test-file scan after cleanup returned no active matches outside preserved backup/archive locations.

## Sync Notes

The intended publish target is:

- `origin = git@github.com:liuruojiang/microcap.git`
- branch `main`

The final commit and push status should be checked with:

- `git status --short --branch`
- `git rev-list --left-right --count origin/main...HEAD`

Pre-commit remote divergence check after fetch:

- `origin/main...HEAD = 0 0`
