# POE Bot v1.5 Query Routing Record

**Date:** 2026-04-20

**Goal:** Extend the existing POE bot so `v1.0` remains the default, but explicit commands containing `1.5` or `v1.5` switch the bot to the formal `v1.5` signal and performance outputs.

## Implemented Scope

- Kept `v1.0` as the default strategy line.
- Extended version parsing so `1.5的信号`, `1.5 表现 近3年`, and similar prompts resolve to `v1.5`.
- Registered `v1.5` in the strategy table inside `poe_bots/microcap_top100_poe_bot.py`.
- Added `v1.5` attachment suffixing so exported files use `_v1_5`.
- Wired daily signal and performance queries to the official `v1.5` outputs instead of the default embedded `v1.0` performance payload.
- Generalized the official-output loader so both `v1.4` and `v1.5` are driven by strategy config instead of a hard-coded `v1.4` branch.

## Real Output Files Used

- `outputs/microcap_top100_mom16_biweekly_live_v1_5_latest_signal.csv`
- `outputs/microcap_top100_mom16_biweekly_live_v1_5_summary.json`
- `outputs/microcap_top100_mom16_biweekly_live_v1_5_performance_nav.csv`
- `outputs/microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_gapexit_newpeak_v1_5_costed_nav.csv`

## Code Changes

**Files:**
- `poe_bots/microcap_top100_poe_bot.py`
- `test_poe_bot_version_selection.py`

**Key implementation details:**
- Added `V1_5_SIGNAL_CSV`, `V1_5_SUMMARY_JSON`, `V1_5_COSTED_NAV_CSV`, and `V1_5_LIVE_NAV_CSV`.
- Expanded `VERSION_PATTERN` from `1.0|1.4` to `1.0|1.4|1.5`.
- Added a `STRATEGIES["1.5"]` entry with:
  - `cache_tag = "v1_5"`
  - `hedge_ratio = 0.8`
  - official signal and summary paths
  - formal performance paths
  - overlay label `v1.2 NAV节流 + 动量差峰值衰减退出`
  - official module/generator metadata for lazy output refresh
- Updated help and intro text to document explicit `1.5` switching.
- Refactored `ensure_selected_strategy_outputs()` to read the module and generator from strategy config.

## Tests Added

- `test_resolve_strategy_from_query_switches_to_v1_5`
- `test_v1_5_adds_version_suffix_to_attachment_name`
- `test_handle_signal_uses_official_v1_5_signal_output`
- `test_build_performance_outputs_refreshes_official_v1_5_outputs_first`

## Verification Run

Observed passing commands:

```powershell
python -m unittest test_poe_bot_version_selection.py -v
python -m unittest test_poe_bot_version_selection.py test_poe_bot_runtime_cache_and_stage_reuse.py test_poe_bot_signal_summary_labels.py -v
```

Observed smoke check:

```powershell
python - <<'PY'
from poe_bots import microcap_top100_poe_bot as bot
strategy, query = bot.resolve_strategy_from_query("1.5的信号")
signal_df, summary = bot.load_official_signal_bundle(force_refresh=False)
print(strategy["version"])
print(bot.versioned_attachment_name("microcap_top100_autorebuild_signal.csv"))
print(len(signal_df) if signal_df is not None else 0)
print(summary.get("version") if summary else None)
PY
```

Smoke-check observations:

- strategy version resolved to `1.5`
- attachment name became `microcap_top100_autorebuild_signal_v1_5.csv`
- official signal bundle loaded from current `outputs/`
- summary version read back as `1.5`

## Notes

- This change only affects POE query routing and formal-output selection.
- It does not change the underlying `v1.5` strategy logic, execution assumptions, or cost model.
- Matplotlib may still emit missing-glyph warnings for Chinese labels during test chart generation, but the test suite passed.
