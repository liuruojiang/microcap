# Workspace Defaults

Common rules live in `C:\Users\Administrator.DESKTOP-95I7VVU\AGENTS.md`. This file only adds local Top100 microcap rules.

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live_v2_0.py` (`v2.0`) unless the user explicitly asks for another version.
- For signal, realtime signal, performance, holdings, and member queries, default to `v2.0`.
- Before any Top100 test, signal output, or chart generation, refresh the selected strategy data to the latest trading date.
- The default practical/live performance caliber is `v2.0 + costed`; do not silently mix `gross` and `costed`.
- Prefer fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`; use the old full `build_base_context()` path only if requested or unavoidable.
- Treat `outputs/` as disposable export space. Keep current core artifacts for `v1.0`, `v1.1`, `v1.2`, `v1.6`, and `v2.0`.

# Signal Query Defaults

- Return the final signal first; keep process details out unless there is a failure, stale-data risk, or version caveat.
- Distinguish `信号` from `实时信号`: `信号` is close-confirmed; `实时信号` needs same-day intraday snapshot, `snapshot_time`, and `latest_anchor_trade_date`.
- Never answer market-hours realtime requests with only the previous close-confirmed row.
- For version-specific realtime without an official route, use the current fast realtime snapshot, then recompute the named version from real strategy functions and parameters.
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
