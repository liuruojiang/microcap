# Workspace Defaults

Common rules live in `C:\Users\Administrator.DESKTOP-95I7VVU\AGENTS.md`. This file only adds local Top100 microcap rules.

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live_v2_0.py` (`v2.0`) unless the user explicitly asks for another version.
- For signal, realtime signal, performance, holdings, and member queries, default to `v2.0`.
- Before any Top100 test, signal output, chart generation, or parameter/combo comparison, refresh the selected strategy data to the latest locally available trading date and prove freshness before using metrics.
- Freshness proof is mandatory: read back and record the latest dates and row counts for the refreshed panel, proxy index, proxy turnover, base costed NAV, and every version/candidate return stream used in the test. These dates must share the same latest close-confirmed trading date unless the result is explicitly marked blocked.
- Do not use stale official costed NAV CSVs, older `outputs/` exports, or same-base helper recomputes as a substitute for refresh proof. If refresh cannot advance the base costed NAV and all compared streams to the latest available trading date, stop and report the refresh blocker; do not publish performance, leverage, holdings, or charts as formal results.
- When only a short tail is stale and no new rebalance date exists between the current costed NAV end and the target trading date, prefer an auditable no-new-rebalance tail extension over a broad `--force-refresh`; still verify the extended proxy index, base costed NAV, and downstream version streams by reading the written artifacts.
- The default practical/live performance caliber is `v2.0 + costed`; do not silently mix `gross` and `costed`.
- Prefer fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`; use the old full `build_base_context()` path only if requested or unavoidable.
- Treat `outputs/` as disposable export space. Keep current core artifacts for `v1.0`, `v1.1`, `v1.2`, `v1.6`, and `v2.0`.

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

# Desktop And Git Defaults

- For local images/files in Codex desktop, prefer the ASCII alias `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\microcap_ascii\...` when available.
- Keep remote `origin = git@github.com:liuruojiang/microcap.git` unless the user asks otherwise.
- Prefer SSH key `C:\Users\Administrator.DESKTOP-95I7VVU\.ssh\codex_github_ed25519` for pushes.
