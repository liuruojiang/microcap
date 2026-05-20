# 2026-05-20 realtime signal latency fix record

## Scope

- Repository: microcap Top100 strategy workspace.
- Branch: `main`.
- Versions: `v2.0` and `v2.3`.
- Goal: fix the local and GitHub daily-report realtime signal path that could spend about 30 minutes in a signal query.

## Root Cause

The realtime signal command was not slow because the final signal calculation was expensive.
It was slow because the query path could enter the historical anchor refresh chain before producing the realtime signal.

The problematic local path was:

```text
python microcap_top100_mom16_biweekly_live_v2_0.py "实时信号"
-> build_realtime_v2_0_outputs()
-> realtime_core.load_realtime_base()
-> load_realtime_context()
-> refresh_history_anchor()
-> build_refreshed_panel_shadow() / strategy-state freshness work
```

That path can touch free data sources, cached panel refreshes, the proxy index, costed NAV, static member state, and member price cache validation. When free sources are slow or the local state is stale, a realtime query can stretch into a long refresh operation.

## Fixes

- `80bb4512 Speed up state-only realtime signals`
  - Added a state-only realtime context path that reads the already refreshed `panel_shadow`, proxy index CSV, costed NAV CSV, proxy members, and turnover state.
  - In state-only mode, the signal path no longer calls `refresh_history_anchor()`.
  - Added regression coverage so state-only realtime cannot silently re-enter historical anchor refresh.

- `316ed2bb Refresh local realtime state before signal queries`
  - Local `v2.0` / `v2.3` realtime signal commands now first run an explicit realtime state refresh.
  - After refresh, the command forces state-only signal output in the same process.
  - If state refresh fails or produces unusable state, the query refuses to output a realtime signal rather than using stale state.

- `62666f9 Force state-only microcap signal steps`
  - GitHub daily-report `v2.0` / `v2.3` signal steps now set `TOP100_REALTIME_REQUIRE_STATE=1`.
  - The workflow keeps the refresh step before signal generation.

- `f878410 Limit microcap signal step timeout`
  - GitHub daily-report signal-step timeout was reduced from 30 minutes to 8 minutes per version.
  - The refresh step retains its longer timeout because it is the explicit data-refresh phase.

## Accuracy Impact

The strategy math was not changed.
The fixes changed only data-preparation routing for realtime queries.

The new contract is:

1. Refresh selected realtime strategy state first.
2. Validate and reuse that refreshed state for signal generation.
3. Refuse output if required state is missing or stale.

This preserves signal accuracy while preventing duplicate historical refresh work inside the signal step.

## Local Verification

Commands run after the local-path fix:

```powershell
pytest tests\test_realtime_anchor_quote_guard.py -q
```

Result:

```text
11 passed
```

Direct local realtime commands without setting `TOP100_REALTIME_REQUIRE_STATE`:

```powershell
python .\microcap_top100_mom16_biweekly_live_v2_0.py "实时信号"
python .\microcap_top100_mom16_biweekly_live_v2_3.py "实时信号"
```

Observed results:

| Version | Elapsed | Anchor | Quote date | Coverage |
| --- | ---: | --- | --- | --- |
| v2.0 | 36.87s | 2026-05-19 | 2026-05-20 | 100/100 |
| v2.3 | 35.73s | 2026-05-19 | 2026-05-20 | 100/100 |

## GitHub Daily Report Verification

Remote workflow checks after the automation fix:

```text
TOP100_REALTIME_REQUIRE_STATE count: 2
8-minute signal timeout count: 2
30-minute signal timeout count: 0
Refresh step 60-minute timeout: present
```

Automation test command:

```powershell
python -m unittest discover -s tests
```

Result:

```text
Ran 38 tests
OK
```

The latest GitHub Actions daily-report run before these fixes completed in under 5 minutes, but it was not a post-fix run. The next scheduled or manually dispatched run is the first full remote runtime confirmation for the new workflow.

## Cleanup

Generated output files changed by local realtime refresh verification were backed up and restored instead of committed.

Backup:

```text
.codex_backups/20260520_165506
```

Cleaned local generated caches:

- `.pytest_cache/`
- root `__pycache__/`
- `scripts/__pycache__/`
- `tests/__pycache__/`

Preserved:

- `tests/test_realtime_anchor_quote_guard.py`
- official strategy scripts
- tracked official outputs after restoring them to repository state
- `.codex_backups/`

## Sync Status

- microcap repo pushed to `origin/main` through commit `316ed2bb`.
- automation repo pushed to `origin/main` through commit `f878410`.
- This record is intended to be committed and pushed as the final cleanup/sync note.
