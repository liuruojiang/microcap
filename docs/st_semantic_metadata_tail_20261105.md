# Verified metadata-equivalent native tail continuation (2026-11-05)

## Scope and real trigger

Cloud run 42016112300 failed on the real 300237 withdrawal-day guard. After its separate inclusive-day repair, the old metadata continuation still rejected `st_intervals: end=None -> 2026-11-05`, although the existing `_history_equal` proves identical ST/active semantics through both D4 and D5. Real D4 frozen-tail fallback also fails: authority seed is 2026-10-30 while saved tail starts 2026-10-29. The old route therefore requires a forbidden full historical proxy rebuild.

This patch only changes `scripts/st_metadata_refresh.py`. On a real proxy carrying its security metadata fingerprint, the original entrypoint now selects an authenticated, staged native tail continuation. It does not call `build_local_proxy_bundle`, recalculate historical membership or prices, change strategy parameters, edit frozen authority, or hand-edit a delivery manifest.

## Required proof and transaction

- Read the complete completed metadata marker, exact symbol set, old backup/current bytes, original/current archive hashes and horizons. Aggregate the actual backup bytes and require equality with the old proxy security fingerprint. Independently recompute the current native fingerprint.
- Require ST and listing/active semantics identical through the requested completed session. Only the exact final `end=None -> target` closure is permitted as an interval representation change; starts, sources, shape, unknown fields, evidence identity and dates remain guarded. Current ST protection remains resolved and the target is checked against the independent calendar.
- Authenticate the original execution model, stock-input fingerprint, consumption chain and its member/turnover hashes. Require a complete unique effective portfolio and a real no-rebalance tail.
- Copy only the required small proxy/NAV/member artifacts to a stage. Generate derived candidate proxy metadata through the existing native JSON writer, then run the existing native proxy and NAV tail functions. The historical proxy builder is forbidden on this route.
- Independently audit and retain the exact old proxy/NAV prefix bytes; member, turnover and effective-member bytes remain identical. Check final native fingerprints, source closure, marker/metadata/authority/panel/current-ST bytes and directory membership again before installation.
- Exclusive lock records PID/token and file identity using binary IO on Windows. Installation uses compare-and-swap checks, readback, and rollback on failures (including an exception after replacement). Unknown ownership or failed rollback retains the lock and fails closed; it cannot return success with another writer's lock.

The unchanged legacy replay implementation remains available to its original explicitly bounded inputs; production state with the formal security fingerprint enters the new route and cannot fall through into historical replay after a failed semantic proof.

## Evidence and tests

The real D4 300237 metadata bytes and SHA are saved in a test-only fixture. Its interval and dated four-notice evidence are the independently validated source case from the preceding fix. Native-tail tests use an explicitly synthetic two-member price/portfolio fixture and isolated authenticated-marker adapters: they test code contracts, not production 100-member coverage, live quotations, or whole-package acceptance.

- Original 22c8 continuation counterexample: 1 expected failure, lifecycle `4195184572384dda8cab131679413deb`.
- Native affected suites initial run: 195 passes / 7 failures, retained as `561b8ae8c656425e8a8b8fae388289b4`. Six failures were an operator error: source was edited while pytest was using `inspect.getsource`, causing line-offset disagreement. One was a mistaken test function name. Neither is hidden or counted as passed.
- After freezing/rechecking those cases: 24 passed (18 new + 6 existing turnover), 3.72 seconds, lifecycle `6c59cdf408654ae29126731c12011c61`.
- Final two concurrency additions and frozen runtime: all 20 new cases passed, 3.88 seconds, lifecycle `f802d290da9b45208d3c0ab4f07bb738`. Covers native proxy/NAV prefix, metadata transport fingerprint, source/unknown/history/future/missing/rebalance failures, current-ST/directory/concurrent changes, partial replace, receipt failure, rollback failure, and owner replacement after successful receipt.
- Collection proves all 20 new nodes enter exactly once through existing daily core `tests/test_realtime_state_guard_20260915.py`; the helper is not a separate discovered test file. Prior builder 97 and prior cloud full-suite evidence were not rerun.
- Backups: `D:/Codex/workspaces/_repair_backups/intraday-micro-20261105/metadata-tail-22c8`.

No full-pool stress test or production timing was run. The proven saving is removal of historical proxy rebuilding on this strictly equivalent path; synthetic test seconds are not a production-runtime promise.

## Exact release dependency and remaining acceptance

LF-normalized SHA256 for the only changed runtime file:

- old: `26537556d2cc58d899052ca0027f575b9f6d64fe03ca75651bf9f5f9a17dd0e8`
- new: `5ecf65d6124310a777f62f3c8ac76c72000acce6482b99ce2359f52ffc34a992`

This file belongs to both whole-delivery inputs and `stock_input_contract.SOURCE_FILES`. The original validation-source review does not authorize this additional code hash. An explicit reviewed code-only successor must preserve original data/file-count/keyset identity and the historical review before the restored D4 state may use this runtime. The helper deliberately continues to reject an unrecognized stock-input fingerprint. A whole-input repack allowlist alone does not solve that gate.

The coordinating task owns that compatibility review, final frozen complete regression, approved-package restore/repack and real D5 continuation/whole delivery. This patch does not claim those steps have passed. No production state, historical output, source registry, authority, email, cloud dispatch or shared automation was modified here.
