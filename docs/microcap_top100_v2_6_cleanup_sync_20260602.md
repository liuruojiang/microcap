# Microcap Top100 v2.6 Cleanup Sync Record - 2026-06-02

## Scope

This cleanup records the formal promotion of the 75/75 combo into `v2.6`, documents the validation results, and removes temporary test/runtime files before cloud sync.

## Preserved

- `microcap_top100_mom16_biweekly_live_v2_6.py`, the formal v2.6 entrypoint.
- Scan scripts that produced the target-vol and combo comparison artifacts.
- Updated freshness/refresh rules in `AGENTS.md`.
- The v2.0 refresh optimization in `microcap_top100_mom16_biweekly_live_v2_0.py`.
- Current refreshed core output files already tracked by the repository.
- Generated v2.6 and chart outputs under `outputs/` remain local disposable artifacts unless explicitly force-added later.

## Removed Locally

- Temporary pytest file: `tests/test_v26_combo.py`.
- Current workspace Python bytecode caches:
  - `__pycache__/`
  - `scripts/__pycache__/`
  - `tests/__pycache__/`
- `.pytest_cache/` if present.

Historical backups under `.codex_backups/` were not cleaned.

## Verification Before Cleanup

Commands run before removing the temporary test file:

```powershell
python -m pytest tests/test_v26_combo.py -q
python -m py_compile microcap_top100_mom16_biweekly_live_v2_6.py
python microcap_top100_mom16_biweekly_live_v2_6.py
python microcap_top100_mom16_biweekly_live_v2_6.py signal
python microcap_top100_mom16_biweekly_live_v2_6.py performance
```

Observed:

- pytest temporary regression passed before cleanup.
- py_compile passed.
- v2.6 generated 3885 rows from `2010-05-05` to `2026-06-01`.
- v2.6 latest close-confirmed signal on `2026-06-01`: current `cash`, next `cash`, trade state `hold`.
- v2.6 performance query: annualized 44.36%, max drawdown -17.72%, Sharpe 2.668.

## Backup

Before strategy-source edits, source files were backed up to:

- `.codex_backups/20260602_025438`

## Sync Target

- Remote: `origin = git@github.com:liuruojiang/microcap.git`
- Branch: `codex/v25-review-fixes`

This record is intended to be committed with the v2.6 source, documentation, refresh-rule update, refresh optimization, and supporting scan scripts.
