# Workspace Defaults

Use this repo-local file as the source of workspace guidance for the Top100 microcap strategy. Do not depend on a user-home `AGENTS.md` or any machine-local Codex configuration path.

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live_v2_0.py` (`v2.0`) unless the user explicitly asks for another version.
- For signal, realtime signal, performance, holdings, and member queries, default to `v2.0`.
- Before any Top100 test, signal output, chart generation, or parameter/combo comparison, refresh the selected strategy data to the latest locally available trading date and prove freshness before using metrics.
- Freshness proof is mandatory: read back and record the latest dates and row counts for the refreshed panel, proxy index, proxy turnover, base costed NAV, and every version/candidate return stream used in the test. These dates must share the same latest close-confirmed trading date unless the result is explicitly marked blocked.
- Do not use stale official costed NAV CSVs, older `outputs/` exports, or same-base helper recomputes as a substitute for refresh proof. If refresh cannot advance the base costed NAV and all compared streams to the latest available trading date, stop and report the refresh blocker; do not publish performance, leverage, holdings, or charts as formal results.
- When only a short tail is stale and no new rebalance date exists between the current costed NAV end and the target trading date, prefer an auditable no-new-rebalance tail extension over a broad `--force-refresh`; still verify the extended proxy index, base costed NAV, and downstream version streams by reading the written artifacts.
- The default practical/live performance caliber is `v2.0 + costed`; do not silently mix `gross` and `costed`.
- Prefer fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`; use the old full `build_base_context()` path only if requested or unavoidable.
- Treat `outputs/` as disposable export space. Keep current core artifacts for `v1.0`, `v1.1`, `v1.2`, `v1.6`, `v2.0`, `v2.3`, and `v2.5`.

# Member And Lineage Guardrails

- A published current Top100 list must contain exactly 100 unique ranked symbols and must have zero intersections with current `ST`, `*ST`, or `PT` names/codes. Treat this as a hard publish gate, not a warning.
- Historical backtests must use point-in-time ST entry/exit intervals under the current notice policy; never project the current ST snapshot backward through history.
- Build the historical member universe from the full historical security master, not the current listed universe or a narrow recent candidate set.
- Proxy compatibility must include the security-metadata content fingerprint. A metadata-content change requires rebuilding the proxy and every downstream formal stream.
- Historical rewrites remain fail-closed. A necessary lineage correction requires an approved exact-hash migration report, followed by a second run without the migration option that must finish with a clean rewrite audit.
- After a v2.0 lineage correction, rerun and read back v2.3/v2.5 before reporting them; do not reuse their earlier official outputs.

# Signal Query Defaults

- Return the final signal first; keep process details out unless there is a failure, stale-data risk, or version caveat.
- Distinguish `信号` from `实时信号`: `信号` is close-confirmed; `实时信号` needs same-day intraday snapshot, `snapshot_time`, and `latest_anchor_trade_date`.
- Never answer market-hours realtime requests with only the previous close-confirmed row.
- Before publishing any realtime signal, verify that `latest_anchor_trade_date` is the latest completed historical close immediately before the realtime `quote_trade_date` snapshot. If an intervening completed trading day is missing, refresh the selected version first; if refresh cannot rebuild the strategy NAV through that date, report the realtime signal as blocked and do not output a trading signal.
- For version-specific realtime without an official route, use the current fast realtime snapshot, then recompute the named version from real strategy functions and parameters.
- For `v2.0` + `v2.3` realtime signal requests, keep the fast answer path only after the anchor check above passes: run the official `v2.0` realtime command first; for `v2.3`, prefer reusing the freshly written `outputs/microcap_top100_mom16_biweekly_live_v2_0_realtime_signal.csv` plus local `outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv` and recompute with the real `v2.3` source functions. Do not trigger a full v2.3/v2.0 historical base refresh for a simple realtime query unless the fast path is unavailable, the anchor check fails, or the user explicitly asks to debug the official v2.3 entrypoint.
- If the official `v2.3` realtime entrypoint blocks on free index-history refresh such as `1.000852` / `sh000852` or leaves a stale base-build lock, report that as a route/data-source issue and fall back to the fast recompute path above only when the anchor check passes; otherwise block the realtime signal instead of publishing a stale-anchor result.
- For `v1.4 实时信号`, apply the v1.4 source logic on a realtime quote snapshot; do not treat the v1.4 close-confirmed script as realtime.
- Minimal signal reply: version, time, current holding, next holding, trade state, execution scale, microcap momentum, hedge momentum, momentum gap, quote coverage, and stale/fallback warning.

# Overlay Research Guardrails

- Before interpreting stop-loss, take-profit, drawdown, or reentry overlays, verify executed `holding` / `next_holding` state controls realized `return_net`.
- After overlay-triggered early exits, blocked `cash` days must have `return_net = 0` and must not inherit base `gross` return.
- Suspiciously strong overlay results require lagged-trigger or equivalent sanity checks before trusting the table.

# New Strategy Test Standard

- New strategy tests and candidate promotions must follow `docs/new_strategy_test_standard_process.md`; every display/report must include full sample, 10Y, 5Y, 3Y, and 1Y annualized return plus max drawdown, or explicit `N/A` reasons.

# Desktop And Git Defaults

- For local images/files in Codex desktop, prefer repo-relative paths or a documented local alias when one is explicitly provided in the current session.
- Keep remote `origin = git@github.com:liuruojiang/microcap.git` unless the user asks otherwise.
- Use the configured Git credential or SSH setup for this machine; do not hard-code a user-specific SSH key path in repo instructions.

# Production Email Deployment Gate

- Whole-workspace synchronization is NOT complete when only Git, a cloud run, or `realtime_state_bundle.py validate` passes. That validator covers base state only.
- After synchronizing code/authority or refreshing a historical base, use `python -X utf8 scripts/top100_delivery.py refresh-all`, then `python -X utf8 scripts/top100_delivery.py check`. Both must pass before claiming local synchronization complete or resuming parameter scans. The second command independently checks the latest completed session and live remote core/authority, and reads back all three final streams against the delivery manifest.
- Preserve dirty local changes with a verified backup and reviewed merge. Deployment in a separate worktree does not discharge synchronization of this primary workspace. Final NAVs are ignored by Git and must be regenerated/verified separately. Never reset or copy a single NAV to bypass lineage checks.
- A failed/interrupted group refresh leaves an incomplete delivery manifest. Never describe a partial version refresh, stale manifest, or `scope=base_state_only` result as whole-delivery success. Restoring an already approved seed requires exact base/source hashes and a backup; any new lineage needs new approval.

- A Microcap production-email change is not deployed merely because a local sample or workflow run succeeded. Before declaring synchronization complete, verify the exact strategy commit checked out by the automation workflow is reachable from the remote repository, read back the final CSV identity and dated member-action fields for v2.0/v2.3/v2.5, and record the remote strategy SHA, remote automation SHA, workflow run, and delivered message.
- The final CSV is the authority for strategy identity, holdings, and actions. Stdout may supply presentation fields but must never backfill a missing or contradictory final-CSV identity.
- A member rebalance is actionable only on its dated execution session. Historical and preview changes may be reported as context but must not be rendered as a current trade instruction.
- After the close, do not relabel a close-confirmed signal as realtime. A realtime publication must retain the previous-completed-session anchor invariant; use an explicitly close-confirmed correction when that invariant cannot hold.
