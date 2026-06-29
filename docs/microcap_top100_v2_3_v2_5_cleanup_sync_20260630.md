# Microcap Top100 v2.3/v2.5 Cleanup Sync Record - 2026-06-30

## Scope

This closes the 2026-06-29 post-data-rebuild retest round for `v2.3`,
`v2.5`, and the `v2.3/v2.5` 50:50 portfolio comparison.

The retest uses the rebuilt historical constituent/ST-aware data lineage, not
the older "current stock list projected backward" base. The promoted source
defaults are:

- `v2.3`: `lb25`, half-life `2.5`, R2 window `25`, gap/entry buffers `0.08`,
  spread-vol overheat window `10`, trigger `0.26`, recovery ratio `0.75`,
  execution hedge `0.8`.
- `v2.5`: `lb17`, half-life `3`, entry threshold `0.46`, exit threshold
  `0.25`, no target-vol overlay.

## Freshness Proof

Latest locally available close-confirmed trading date: `2026-06-29`.

| Artifact | Rows | Start | End |
| --- | ---: | --- | --- |
| base panel | 8672 | 1990-12-19 | 2026-06-29 |
| proxy index | 3993 | 2010-01-15 | 2026-06-29 |
| proxy turnover | 425 | 2010-01-28 | 2026-06-25 |
| base costed NAV | 3976 | 2010-02-09 | 2026-06-29 |
| v2.0 costed stream | 3959 | 2010-03-11 | 2026-06-29 |
| v2.3 costed stream | 3922 | 2010-05-05 | 2026-06-29 |
| v2.5 costed stream | 3922 | 2010-05-05 | 2026-06-29 |
| 50:50 combo aligned stream | 3922 | 2010-05-05 | 2026-06-29 |

The proxy turnover end date is the latest rebalance date; the daily streams
share the same close-confirmed end date.

## Implemented Fixes

- Added realtime anchor validation so a realtime quote must anchor to the
  immediately preceding completed trading day.
- Fixed v2.0 target-vol scale-change cost so exit days are not double-counted
  as scale changes.
- Added an audited v2.0 historical rewrite allowlist for the two intended
  target-vol cost corrections.
- Made `v2.3` and `v2.5` performance summaries emit the required `full`,
  `last_10y`, `last_5y`, `last_3y`, and `last_1y` windows.
- Fixed cash-day execution scale leakage in `v2.3` overheat defense and
  `v2.5` no-target-vol paths.
- Cleared inherited hedge/overheat/target-vol signal fields from the `v2.5`
  latest-signal output.
- Added stale legacy `v2.5` retest output discovery so old ignored outputs do
  not silently interfere with future tests.
- Added the reproducible final portfolio script:
  `scripts/run_microcap_v2_3_v2_5_combo50_comparison.py`.

## Cleanup

Removed disposable active caches:

- `.pytest_cache/`
- root `__pycache__/`
- `scripts/__pycache__/`
- `tests/__pycache__/`

Moved one-off layer scan scripts, comparison scratch scripts, and 2026-06-29
ignored scan-run directories out of the active workspace to:

- `.codex_backups/20260630_004310_v23_v25_test_cleanup_sync`

Backup manifest:

- `.codex_backups/20260630_004310_v23_v25_test_cleanup_sync/removed_manifest.csv`

Moved target count: `45`; backed-up files: `5065`; backed-up bytes:
`7402819999`.

Preserved in the active workspace:

- formal strategy sources for `v2.0`, `v2.3`, and `v2.5`
- regression tests in `tests/test_top100_data_guards.py`
- final 50:50 combo comparison entrypoint
- tracked refreshed `v2.0` base output artifacts
- ignored formal `outputs/` files, which remain local and reproducible from the
  strategy entrypoints

## Verification

- `python -B microcap_top100_mom16_biweekly_live_v2_0.py`
- `python -B microcap_top100_mom16_biweekly_live_v2_3.py`
- `python -B microcap_top100_mom16_biweekly_live_v2_5.py`
- `python -B scripts\run_microcap_v2_3_v2_5_combo50_comparison.py`
- `python -B -m pytest tests\test_top100_data_guards.py -q -p no:cacheprovider`
  -> `26 passed`
- `python -B -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_5.py scripts\run_microcap_v2_3_v2_5_combo50_comparison.py tests\test_top100_data_guards.py`
- `git diff --check`

## Sync Target

Commit and push this cleanup record, the source/test fixes, the final combo
script, and tracked refreshed artifacts to:

- `origin/codex/fix-realtime-hedge-date`
