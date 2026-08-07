# Microcap production incident reconciliation — 2026-08-07

## Impact

The Microcap daily email repeatedly mixed local, production, and historical states. It reported list changes without clearly leading with current holdings, repeated a dated list adjustment as if it were still actionable, and could run older v2.0/v2.3 code from `main` even after a correct local sample had been reviewed. Later repair attempts also exposed Windows atomic-output path failures, so a formally generated signal could fail before the email was built.

## Root causes

1. The reviewed v2.0/v2.3 lineage lived on an unmerged branch while the production workflow checked out `main`.
2. Acceptance checked that an email was delivered, not that the checked-out strategy SHA and final CSV identities matched the reviewed versions.
3. Static list changes had no strict signal-date/execution-date/actionable contract and were repeated on later sessions.
4. The email body did not fail closed when the final CSV was missing or contradictory, and it did not consistently lead with current holdings.
5. Atomic generation used long public artifact basenames for internal staging, promotion, and rollback files. In a long Windows worktree those internal paths crossed the legacy 260-character boundary.
6. Synchronization was previously reported without proving the remote `main` SHAs and reading back the delivered correction.

## Reconciled production identities

- v2.0: volatility-overheat exit, 60-session window, 23% trigger, positive-trade and signal-reset conditions retained, followed by 15% target volatility over 75 sessions with 1.5x maximum leverage.
- v2.3: spread-NAV log-WLS, lookback 25, half-life 2.5, R2 entry gate 0.08, 1.0x signal hedge and 0.8x execution hedge; 10-session volatility trigger 26%, recovery 19.5%; no target-volatility overlay.
- v2.5: microcap-only log-WLS, lookback 17, half-life 3, entry 46%, exit 25%; no hedge, no overheat overlay, and no target-volatility overlay.

Each formal writer now emits stable `version`, `base_version`, and `overlay_type` fields. The email validates the exact identity from the readable, non-empty final CSV only; identity failure produces an abnormal report with holdings and actions redacted.

## Member-list timing

The close-confirmed list signal dated 2026-08-06 has execution/return start date 2026-08-07. It is therefore historical after the 2026-08-07 close and must not be presented as a new action on a later session. The realtime contract now carries separate required/actionable/official booleans plus signal and execution dates; an action is emitted only when all fields validate and the execution session is current.

## Freshness proof

All formal streams were refreshed and read back on 2026-08-07:

| Stream | Rows | Latest date |
| --- | ---: | --- |
| Refreshed panel | 8,701 | 2026-08-07 |
| Proxy index | 4,022 | 2026-08-07 |
| Proxy turnover | 427 | 2026-08-07 return start (2026-08-06 rebalance) |
| Base costed NAV | 4,005 | 2026-08-07 |
| v2.0 costed NAV | 3,988 | 2026-08-07 |
| v2.3 costed NAV | 3,951 | 2026-08-07 |
| v2.5 costed NAV | 3,951 | 2026-08-07 |

The 2026-08-07 close-confirmed holdings are:

- v2.0: cash to cash, execution scale 0.00.
- v2.3: cash to cash, execution scale 0.00; 10-session overheat feature 34.1139%, risk-off remains active.
- v2.5: long Microcap Top100 to long Microcap Top100, execution scale 1.00.

The current performance source is the documented public/local proxy rather than official Wind `868008.WI`; this warning remains visible and is not silently converted into an official-series claim.

## Preventive controls

- Immutable strategy SHA checkout in the automation workflow, with the checked-out SHA included in the email.
- Final-CSV-only exact identity validation for v2.0/v2.3/v2.5.
- Holdings-first email sentence; current action is separated from historical/preview list context.
- Strict dated member-action contract and fail-closed parsing of booleans and ISO dates.
- Markdown escaping on all CSV-derived warning and identity text.
- Atomic nine-artifact promotion with compact internal staging, promotion, and rollback names; seeded-existing-artifact rollback regressions cover v2.3 and v2.5.
- Correction and duplicate-delivery gates remain enabled.

## Verification record

- Formal lineage and identity suite: 102 tests passed after the final rollback-path fix.
- Automation focused suite: 24 tests plus 16 subtests passed in final review.
- Automation full suite: 103 tests plus 78 subtests passed; the sole remaining failure is an unrelated date-sensitive ETF deduplication fixture and does not touch the Microcap workflow.
- Formal v2.0/v2.3/v2.5 signal commands all completed successfully against 2026-08-07 data.
- A post-close `实时信号` call correctly blocked because a 2026-08-07 quote cannot use 2026-08-07 as its previous-session anchor. The correction must therefore be labelled close-confirmed rather than realtime.
