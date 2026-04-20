# Workspace Defaults

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live.py` (`v1.0`) unless the user explicitly asks for another version.
- `microcap_top100_mom16_biweekly_live_v1_1.py` and `microcap_top100_mom16_biweekly_live_v1_2.py` are backup/alternative scripts, not the default line.
- For signal, drawdown/performance, holdings, and member queries, default to `v1.0` unless the user explicitly specifies another version.
- Before any Top100 test, signal output, or chart generation, refresh the selected strategy data to the latest trading date first.
- The default practical/live performance caliber is `v1.0 + costed`. Do not silently mix `gross` and `costed`.
- When the user asks for a chart or image, regenerate it from refreshed source data instead of reusing an old export.
- For `信号`, `实时信号`, `成分股名单`, `进出名单`, `实时进出名单`, `净值图`, and `净值表现`, prefer the current fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`. Do not fall back to the old full `build_base_context()` query path unless the fast path is unavailable or the user explicitly asks to debug the old path.
- Treat `outputs/` as disposable export space. Keep the current core strategy artifacts for `v1.0`, `v1.1`, and `v1.2`; test and comparison exports can be cleaned when requested.

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

# Git Push Defaults

- Keep using remote `origin = git@github.com:liuruojiang/microcap.git` unless the user explicitly requests another target.
- For this workspace, prefer SSH push with key `C:\Users\Administrator.DESKTOP-95I7VVU\.ssh\codex_github_ed25519`.
- If sandboxed `git push` cannot read the SSH key, retry outside the sandbox using the same remote and key path instead of changing remotes or protocols.
