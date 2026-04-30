# Workspace Defaults

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live.py` (`v1.0`) unless the user explicitly asks for another version.
- `microcap_top100_mom16_biweekly_live_v1_1.py` and `microcap_top100_mom16_biweekly_live_v1_2.py` are backup/alternative scripts, not the default line.
- For signal, drawdown/performance, holdings, and member queries, default to `v1.0` unless the user explicitly specifies another version.
- Before any Top100 test, signal output, or chart generation, refresh the selected strategy data to the latest trading date first.
- The default practical/live performance caliber is `v1.0 + costed`. Do not silently mix `gross` and `costed`.
- When the user asks for a chart or image, regenerate it from refreshed source data instead of reusing an old export.
- For `信号`, `实时信号`, `成分股名单`, `进出名单`, `实时进出名单`, `净值图`, and `净值表现`, prefer the current fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`. Do not fall back to the old full `build_base_context()` query path unless the fast path is unavailable or the user explicitly asks to debug the old path.
- Treat `outputs/` as disposable export space. Keep the current core strategy artifacts for `v1.0`, `v1.1`, and `v1.2`; test and comparison exports can be cleaned when requested.

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

# Desktop Response Paths

- In Codex desktop responses, prefer an ASCII-only local path alias when sending workspace files or images, so the UI can render and click them reliably.
- For this workspace, first prefer the persistent ASCII junction `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\microcap_ascii\...` for local image tags and file links.
- If that junction is missing, recreate an ASCII-only alias before replying with local image tags or file links. Use the desktop junction first; use a temporary `W:/...` mapping only as fallback.
- Do not default back to the original Chinese-character workspace path when sharing local charts or image files in desktop responses if an ASCII alias is available.

# QVeris API Defaults

- Use QVeris only through the REST API at `https://qveris.ai/api/v1`.
- For live market data, total market capitalization checks, microcap Top100 pool checks, and version-specific realtime signal quote snapshots, prefer QVeris first when `QVERIS_API_KEY` is available. Use Tencent, Eastmoney, Sina, or local cache only as fallback, and report the fallback/source caveat in the answer.
- Never store QVeris API keys or other secrets in `AGENTS.md`, repo files, scripts, command lines, logs, or chat replies. Read the key from the `QVERIS_API_KEY` environment variable or a secure secret manager only.
- Authenticate with `Authorization: Bearer $QVERIS_API_KEY` and `Content-Type: application/json`.
- Capability discovery is free: `POST /search` with `{ "query": "...", "limit": 10, "session_id": "..." }`.
- Inspect tool details with `POST /tools/by-ids` and execute tools with `POST /tools/execute?tool_id=...`.
- Tool execution consumes credits. Before calling `/tools/execute`, search/inspect first, choose the smallest suitable tool, and report the expected operation clearly if the call may be expensive.
- For long tool responses, set `max_response_size` deliberately and use returned `full_content_file_url` only when needed.

# Git Push Defaults

- Keep using remote `origin = git@github.com:liuruojiang/microcap.git` unless the user explicitly requests another target.
- For this workspace, prefer SSH push with key `C:\Users\Administrator.DESKTOP-95I7VVU\.ssh\codex_github_ed25519`.
- If sandboxed `git push` cannot read the SSH key, retry outside the sandbox using the same remote and key path instead of changing remotes or protocols.
