# Microcap Top100 Test Cleanup and Sync Record - 2026-09-28

## Test Source Audit

- The active `tests/` suite contains 30 tracked Python files covering delivery state, data guards, realtime contracts, calendars, and version behavior. They remain active regression coverage and were retained.
- Isolated test scripts under `research_reports/20260926_l1_repair/` are part of the L1 audit evidence and were retained with their candidate findings.
- Historical test-like files in `archive/` were left as records.
- No test source file was confirmed unused, so no test source files were deleted.
- The workspace contains generated `__pycache__`, `.pytest_cache`, and `.ruff_cache` directories. Removal was attempted only for paths inside this repository, but the local command policy rejected recursive deletion; these caches remain untouched. `.hypothesis/` was also preserved.

## Delivery Gate

Both required commands passed:

- `python -X utf8 scripts/top100_delivery.py refresh-all`
- `python -X utf8 scripts/top100_delivery.py check`

The final manifest check reported `scope=whole_workspace_delivery`, `ok=true`, and `expected_date=2026-09-28`. It read back the panel (8,736 rows), proxy index (4,065 rows), and v2.0 base costed NAV (4,048 rows), each through 2026-09-28. The v2.0, v2.3, and v2.5 costed NAV and latest-signal streams also ended on 2026-09-28. Proxy turnover ended on 2026-09-17, the latest recorded rebalance date.

The run identifies the performance series as a public/local proxy, not official Wind `868008.WI`; this refresh does not certify historical performance or production deployment.

## Remote Sync

- Remote: `origin` (`git@github.com:liuruojiang/microcap.git`)
- Branch: `main`
- Before sync, local `main` and `origin/main` were at the same commit.
- Pending non-ignored worktree changes were included in the requested sync: research reports, refreshed core streams, and existing delivery-code changes.
