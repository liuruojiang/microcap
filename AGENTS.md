# Workspace Defaults

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live_v1_6.py` (`v1.6`) unless the user explicitly asks for another version.
- If the user does not mention a Top100 version, treat the selected version as `v1.6` for all Top100 queries.
- `microcap_top100_mom16_biweekly_live_v1_1.py` and `microcap_top100_mom16_biweekly_live_v1_2.py` are backup/alternative scripts, not the default line.
- For signal, realtime signal, drawdown/performance, holdings, and member queries, default to `v1.6` unless the user explicitly specifies another version.
- Before any Top100 test, signal output, or chart generation, refresh the selected strategy data to the latest trading date first.
- The default practical/live performance caliber is `v1.6 + costed`. Do not silently mix `gross` and `costed`.
- When the user asks for a chart or image, regenerate it from refreshed source data instead of reusing an old export.
- For `信号`, `实时信号`, `成分股名单`, `进出名单`, `实时进出名单`, `净值图`, and `净值表现`, prefer the current fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`. Do not fall back to the old full `build_base_context()` query path unless the fast path is unavailable or the user explicitly asks to debug the old path.
- Treat `outputs/` as disposable export space. Keep the current core strategy artifacts for `v1.0`, `v1.1`, `v1.2`, and `v1.6`; test and comparison exports can be cleaned when requested.

# Signal Query Defaults

- When the user asks for any version's `信号` or `实时信号`, return the final signal first and keep process details out of the answer unless there is a failure, stale-data risk, or a version-specific caveat.
- Still verify from real source code and real data internally. "Skip middle steps" means skip verbose reporting, not skip validation.
- Distinguish `信号` from `实时信号`: `信号` is the latest close-confirmed signal; `实时信号` during A-share market hours must use a same-day intraday snapshot and must report `snapshot_time` plus `latest_anchor_trade_date`.
- Never answer a market-hours `实时信号` request with only the previous trading day's close-confirmed signal. If only a close-confirmed versioned script is available, build or reuse the current realtime snapshot first, then apply the selected version's strategy/overlay logic to that realtime close series.
- For version-specific realtime queries, use the official version realtime route when it exists. If it does not exist, use the current fast realtime snapshot from `microcap_top100_mom16_biweekly_live.py` (or an equivalent fresh realtime source) and then recompute the named version's signal from the real strategy functions and parameters.
- For `v1.4`, do not treat `microcap_top100_mom16_biweekly_live_v1_4.py 信号` as `实时信号`; that script only supports close-confirmed `信号 / 表现`. For `v1.4 实时信号`, use the realtime quote snapshot and then apply the v1.4 source logic: v1.1 base, `BASE_HEDGE_RATIO = 0.8`, `V1_4_MOMENTUM_GAP_EXIT_BUFFER = 0.0025`, `DECAY_RATIO_THRESHOLD = 0.25`, `DERISK_SCALE = 0.0`, and `RECOVERY_RATIO_THRESHOLD = 0.35`.
- Minimal signal reply format: version, snapshot/signal time, current holding, next holding, trade state, execution scale, microcap momentum, hedge momentum, momentum gap, realtime quote coverage when applicable, and any stale-data/fallback warning.

# Overlay Research Guardrails

- For any stop-loss, take-profit, drawdown overlay, or reentry overlay study, audit execution-layer invariants before interpreting performance. Do not explain a suspicious annual-return or drawdown result until the execution path is verified.
- Mandatory first checks for overlay studies:
  - after an overlay-triggered early exit, subsequent blocked `cash` days must have `return_net = 0` and must not inherit the base strategy `gross` return;
  - executed `holding` / `next_holding` state must match the realized daily return stream;
  - if results look surprisingly strong, verify with a lagged-trigger or equivalent sanity check before trusting the table.
- When an overlay result looks counterintuitive, treat the overlay output as suspect until these invariants are tested with explicit unit tests and a fresh rerun on real data.
- Do not rely only on trigger counts, event counts, or threshold-hit audits. Those are secondary checks; the primary check is whether the executed cash/holding state actually controls realized returns.

# Backtest Data Discipline

- Never use old comparison/export CSVs in `outputs/` as the source of truth for a new conclusion. Rebuild every compared series from the current official source functions in the same run, then compute metrics from those freshly rebuilt return series.
- For version comparisons, all variants must share one explicit baseline, one date index, one return column, one cost model, and one window definition. Report those source paths/functions when giving the result.
- Before reporting annual return, volatility, Sharpe, drawdown, or total return, print or internally verify: source function/file, return column, start date, end date, row count, duplicate-date count, and common-index row count.
- Do not mix `gross`, `return`, `return_net`, `nav`, and `nav_net` silently. If the answer is live/practical, use `return_net`/`nav_net` only unless the user explicitly asks for gross.
- Do not compare a current official version against a custom/export file that embeds an older baseline such as `return_net_v1_4`. If an embedded baseline is unavoidable for diagnosis, first compare it against the current official baseline and reject it if any material date-level mismatch exists.
- For target-volatility or leverage overlays, generate all target-vol variants from the same freshly rebuilt base result in memory. Do not mix a previously saved `targetvol` CSV with a newly generated v1.4/v1.6 file.
- Enforce cost sanity checks before accepting target-vol or overlay results: on the same base return stream, costed NAV must be less than or equal to no-cost NAV; entry/exit cost columns must affect `return_net`; and `cash -> long` entry days must deduct the configured entry cost even when exposure scale is zero for that day.
- If a recomputed table disagrees materially with an earlier table, stop and audit the data lineage first. Do not publish another performance table until the discrepancy is explained by source path, date window, cost model, or code change.
- Keep `outputs/` clean. Delete stale comparison, scan, custom, corrected, and temporary exports after the conclusion is superseded or documented; preserve only current official artifacts and required data/cache files.

# Desktop Response Paths

- In Codex desktop responses, prefer an ASCII-only local path alias when sending workspace files or images, so the UI can render and click them reliably.
- For this workspace, first prefer the persistent ASCII junction `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\microcap_ascii\...` for local image tags and file links.
- If that junction is missing, recreate an ASCII-only alias before replying with local image tags or file links. Use the desktop junction first; use a temporary `W:/...` mapping only as fallback.
- Do not default back to the original Chinese-character workspace path when sharing local charts or image files in desktop responses if an ASCII alias is available.

# Market Data Availability

- QVeris is no longer an available data source for future work in this workspace. Do not use QVeris discovery, tool execution, REST endpoints, or `QVERIS_API_KEY` as a fallback for live signals, pool checks, market-cap checks, history refreshes, or research inputs.
- Prefer free sources and validated local cache first: Xinhua Finance / CNFin, Sina, Eastmoney, Tencent, exchange/vendor CSVs already checked into the workspace, and fresh workflow state bundles.
- If free sources and local cache cannot provide the required field, freshness, or coverage, report the task as data-source blocked with the exact missing field/symbol/date. Do not silently fall back to QVeris or fabricate replacement data.
- Historical documents and old output filenames may mention QVeris as provenance. Treat those as archived evidence only, not as an approved source for new runs.

# Git Push Defaults

- Keep using remote `origin = git@github.com:liuruojiang/microcap.git` unless the user explicitly requests another target.
- For this workspace, prefer SSH push with key `C:\Users\Administrator.DESKTOP-95I7VVU\.ssh\codex_github_ed25519`.
- If sandboxed `git push` cannot read the SSH key, retry outside the sandbox using the same remote and key path instead of changing remotes or protocols.
