# Microcap delivery adversarial audit — 2026-09-04

Scope: isolated worktree at published strategy `57908e584a292aecea5991750f8c082760f4a539`; delivery helpers/tests only. No core strategy, authority, production state, refresh, orders, or email was changed by this audit agent.

## Observed failures and narrow corrections

- Future-dated price/panel/NAV anchors passed state validation with warnings. They now fail even when the optional age limit is disabled.
- Cloud restore/sync imposed five calendar days despite an exactly verified caller-supplied completed trading date. Transport now checks exact delivery date and integrity without this calendar-age shortcut; it never declares a signal ready.
- A different complete local delivery on the same session could be replaced by an older ZIP. Restore now rejects differing same-session completed inputs/artifacts; byte-identical completed delivery is reused, preserving its newer proof.
- KeyboardInterrupt during restoration could leave a complete marker and remove the lock. Interrupted applies now write blocked status; group refresh uses the same interruption discipline. Abrupt OS termination still leaves the lock for explicit review.
- Two ZIP filenames differing only in case could alias on Windows. Such aliases and trailing-dot/space/ADS paths are rejected before extraction.
- Windows CopyFile2 failed on long backup paths. Copies and backup directory creation now use Windows extended paths, with a long-path roundtrip test.
- Remote main advancing to an object missing locally caused `git show` failure. Release verification fetches only that immutable object; no checkout/merge or worktree mutation.
- Whole-delivery validation did not require the close-confirmed dated fields when a member action was true. The gate now requires today's rebalance signal, explicit required/official flags, and a valid later execution date. The real core function's legitimate next-session action is tested and accepted. It is **not** reinterpreted as today's executable trade.

## Evidence

Initial fault-injection run: six failures / four passes. Two interruption/rollback cases first exposed the Windows backup-path failure; after fixing that copy issue, both independently reproduced their intended state-integrity failures. Additional invalid member-contract cases failed before their fix.

Focused command (all offline, synthetic fixtures explicitly diagnostic):

`python -X utf8 -m pytest -q tests/test_adversarial_microcap_delivery.py tests/test_top100_cloud_delivery.py tests/test_top100_delivery.py tests/test_realtime_preflight.py tests/test_realtime_member_action_contract.py tests/test_realtime_member_snapshot_fallback.py --tb=short`

Result: **90 passed, one expected duplicate-ZIP warning**, 6.45 seconds. Existing Hypothesis payload-tamper properties remain enabled.

The actual already accepted GitHub restore directory `D:/Codex/home/automations/ic-im/artifacts/2026-09-04/github-whole-delivery-restore` was inspected read-only with the patched validators: whole manifest passed, 16 streams, 100 effective members, panel/proxy/costed NAV through 2026-09-04 and turnover through 2026-09-03. No new strategy performance is claimed.

Two pre-existing tests unexpectedly called live universe discovery in an empty checkout. Their policy-comparison and frozen-extension tests now isolate only those unrelated dependencies. A separate test still runs the actual metadata fingerprint algorithm and proves that changing a security name to ST changes its digest.

Backups: `.codex_backups/20260904_164209` (three delivery helpers), `.codex_backups/20260904_165057` (existing test file). Git diff/core hashes provide the release rollback boundary; filesystem copies preserve original helper bytes.

## Caller integration and remaining boundaries

Codex preflight should pass `--expected-date YYYY-MM-DD`, obtained from the formal exchange calendar, not from state files. That date must independently match the historical loader. A calendar/history mismatch leaves the old proof untouched. Long-age proof eligibility additionally requires the current day's 100/100 non-ST name evidence and exact matching anchors.

The frozen core's five-calendar-day stale guard is deliberately unchanged. The policy characterization test confirms that a September 30 anchor remains blocked on October 8 even with a calendar showing no intervening session. Transport and proof correctness therefore **do not** promise a realtime signal after a long holiday. Changing that core restriction requires a separately approved, versioned policy change; no `allow-stale` workaround is introduced.

External data/GitHub outages still block. ZIP hashes establish integrity, not independent authenticity against a malicious actor who controls the trusted repository. No unsent/Gmail success claims arise from this offline audit; the root agent must run the final pinned cloud acceptance.
