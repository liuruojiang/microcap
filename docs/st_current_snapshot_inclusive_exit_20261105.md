# ST withdrawal-day current-snapshot guard repair (2026-11-05)

## Observed failure

Normal cloud run `42016112300` restored the approved D4 package but stopped during staged metadata refresh for `300237` (ST美晨). The previous verified archive has three notices: 2025-09-27 entry, 2025-10-09 progress, and 2026-10-28 application. The new verified daily page adds `AN202611041831418741` (2026-11-05 withdrawal). `symbols[300237]` and `scope` are unchanged between archives. This is not lost historical evidence.

The existing `notice_date_exclusion_inclusive_exit_unchanged` policy produces `[2025-09-27, 2026-11-05]`, inclusive. The status function correctly excludes the stock on November 5 and clears it on November 6. The official issuer PDF (https://static.cninfo.com.cn/finalpage/2026-11-05/1225658367.PDF), independently read by the coordinating task, states suspension on November 5 and resumption/removal on November 6. No effective-date policy change is needed.

The two metadata builders instead required an open-ended interval for a current ST name, rejecting this valid inclusive end date. The error happened before staged `_history_equal` and cache installation.

## Minimal change and boundaries

Both the standalone builder and the v2.0 embedded `FREQ_SOURCE` now require an evidence interval covering the requested `notice_end`, inclusively. An explicit `as_of` determines that horizon; the last raw price (November 2 in the actual metadata) cannot stand in for November 5/6. Current names still cannot seed historical intervals. Parsers, announcement dates, member exclusion, history equality, source-failure guards and all strategy parameters remain unchanged.

The fixture contains the actual four provider titles and raw page SHA256/path references, for tests only. It is not an approved production registry entry and does not change the evidence/input closure. Tests execute both actual builder ASTs with fixed read-only archive adapters; archive authentication is separately evidenced by the real old-loader read and the run artifact. This is isolated software validation, not a full production replay or current signal.

## Verification

- Original-code red test: 2 failures (both real-case builders), 12 passes; lifecycle `7bb7bd336f924f6e89e28da3ea27e22e`.
- Patched affected regression: 97 passed, 60.92 seconds, no skips; lifecycle `cda8c0fdb9b74a5cb72a79d60ea39204`.
- Covers actual 3+1 rows, inclusive exit day, unchanged history through D4, stale-price/as-of separation, expired intervals, no evidence, future entry, future exit, conflicting same-day actions, and source errors. Existing category-state/partial withdrawal/overlap semantics remain covered by the unchanged state-machine suite.
- The first lifecycle invocation used an invalid wrapper argument; its nonzero receipt is retained as `fa191691f87f4bc6bbd239ac0e913be6`. It ran no tests.
- Pre-edit backup: `D:/Codex/workspaces/_repair_backups/intraday-micro-20261105/st-inclusive-exit-b657`, with before/after LF hashes.

## Deployment dependency (not yet accepted)

`analyze_top100_rebalance_frequency.py` is a RELEASE_FILE; v2.0 is a whole-delivery input and embedded runtime source. Neither belongs to `stock_input_contract.SOURCE_FILES`. Existing historical metadata, raw/share caches, authority, NAV and output CSVs were not modified.

The current original-source compatibility review binds the old v2.0 SHA exactly in `realtime_state_bundle.VALIDATION_SOURCE_REVIEW`. Therefore this candidate cannot be advertised as compatible merely by extending an automation repack allowlist or invoking `pack-archive`: `validation_source_review()` will reject the new v2.0 hash. A separately reviewed native source-successor certification retaining the original receipt and exact unchanged D4 data is required before old-package adoption. Do not edit a manifest/receipt or rebuild history to bypass this guard.

After source compatibility and required complete regression pass, resume the failed staged metadata refresh and its affected D5 incremental/three-version delivery gates. Successful source collection and old approved restoration can be reused only through their existing verified resume paths. No production refresh, restore, cloud dispatch, historical recomputation or email was performed by this patch task.
