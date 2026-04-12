# Workspace Defaults

- Top100 microcap mainline defaults to `microcap_top100_mom16_biweekly_live.py` (`v1.0`) unless the user explicitly asks for another version.
- `microcap_top100_mom16_biweekly_live_v1_1.py` and `microcap_top100_mom16_biweekly_live_v1_2.py` are backup strategy scripts, not the default line.
- For signal queries, drawdown/performance queries, and holdings/member queries, if the user does not explicitly specify a version, default to `v1.0`.
- Before any Top100 test, signal output, or chart generation, refresh the selected strategy data to the latest trading date first.
- The default practical/live performance caliber is `v1.0 + costed`; do not silently mix `gross` and `costed`.
- When the user asks for a chart or image, regenerate it from refreshed source data instead of reusing an old exported image.
- Treat `outputs/` as disposable export space. Keep the current core strategy artifacts for `v1.0 / v1.1 / v1.2`; test and comparison exports can be cleaned when requested.
