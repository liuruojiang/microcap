# 2026-05-12 cleanup and cloud sync record

## Scope

- Repository: microcap Top100 strategy workspace.
- Branch: `main`.
- Goal: remove active test files and first-party Python cache directories, then sync the repository to `origin/main`.

## Pre-cleanup checks

- `git status -sb` showed local edits in `microcap_top100_mom16_biweekly_live.py` and the active `tests/` suite.
- `git rev-list --left-right --count origin/main...HEAD` returned `0 0` before cleanup.
- `git ls-files` matched these active tracked test files:
  - `tests/test_microcap_query_integrity.py`
  - `tests/test_top100_realtime_anchor_refresh.py`
  - `tests/test_top100_realtime_resilience.py`
- A recursive scan found no root-level `test_*.py` or `*_test.py` files outside preserved archives/backups.

## Deleted

- `tests/`
- Root `__pycache__/`
- `docs/microcap_biweekly_anchor_comparison_20260512/__pycache__/`

## Preserved

- Formal strategy entrypoints and versioned scripts, including `microcap_top100_mom16_biweekly_live.py` and `microcap_top100_mom16_biweekly_live_v1_6.py`.
- `docs/`, `outputs/`, `archive/`, `归档/`, `.codex_backups/`, cache data directories, `poe_bots/`, `scripts/`, and `wheelhouse/`.
- Historical test-like files inside `归档/` and backup copies inside `.codex_backups/`.
- Research helpers such as `analyze_*`, `scan_*`, `compare_*`, `validate_*`, and `verify_*`.

## Backup

- Active `tests/` was backed up before deletion to `.codex_backups/20260512_183959`.

## Verification

- `python -m py_compile` passed for:
  - `microcap_top100_mom16_biweekly_live.py`
  - `microcap_top100_mom16_biweekly_live_v1_1.py` through `microcap_top100_mom16_biweekly_live_v1_8.py`
  - `run_top100_v1_6_v1_8_realtime_signals.py`
  - `poe_bots/microcap_top100_poe_bot.py`
  - `scripts/verify_top100_v14_adapter_equivalence.py`
- `git diff --check` passed.
- A post-cleanup scan found no active test files outside preserved archives/backups.
- Regenerated `__pycache__/` directories were removed from the repo root, `poe_bots/`, and `scripts/`.

## Sync result

- Push target: `origin/main`.
- Final remote sync is performed by the cleanup commit containing this record.
