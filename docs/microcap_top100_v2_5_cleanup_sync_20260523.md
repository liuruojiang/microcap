# Microcap Top100 v2.5 Cleanup And Sync Record

Date: 2026-05-23

## Scope

This record closes the v2.5 formalization and cleanup pass.

Kept as source/research artifacts:

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- `docs/microcap_top100_v2_5_warning_board_20260523.md`
- `docs/microcap_top100_v2_5_pool_rebalance_frequency_20260523.md`
- `quant_param_scan_runs/20260523_microcap_top100_v2_5_broad_volume_warning_overlay_zz2000_cyb_amount_ma_days_scale/`
- v2.5 research scripts under `scripts/`

Removed as test-only files:

- `tests/test_close_confirm_cutoff.py`
- `tests/test_refresh_price_cache_tail.py`
- `tests/test_top100_realtime_core_state_only.py`
- `tests/test_v2_0_state_only_anchor.py`
- `tests/test_v2_3_realtime_state_only.py`
- `tests/test_v2_3_script_maintenance.py`
- `tests/test_v2_5_bias_overheat_scan.py`
- `tests/test_v2_5_formalization.py`
- `tests/test_v2_5_staged_entry_scan.py`
- `tests/__pycache__/`
- `归档/Top100_v1_4_v1_5研究定版_2026-04-19/test_v1_4_output_compatibility.py`
- `归档/Top100_v1_4_v1_5研究定版_2026-04-19/test_v1_5_output_compatibility.py`

Backup before cleanup:

- `.codex_backups/20260523_182638`
- `.codex_backups/20260523_182752`

## Formal v2.5 Defaults

Keep the current v2.5 mainline as:

- Signal family: microcap-only log-WLS momentum.
- Lookback: `17`.
- Halflife: `3.0`.
- Entry threshold: `40%`.
- Exit threshold: `40%`.
- Target volatility: `30%`.
- Maximum leverage: `1.3x`.
- Scale rebalance threshold: `0.30`.
- Performance caliber: costed.

Excluded from formal v2.5 default logic:

- single-trade stop loss
- equity drawdown stop
- momentum decay exit/reentry
- overheat exit/reentry
- broad-volume environment reduction

## Warning Board

The warning board keeps only warning-only items. It must not change live signal,
realtime signal, sizing, or performance output unless promoted by a separate
source-change decision.

Current warning-board item:

- `MA60 bias overheat`, hot `35%`, cool `22%`.
- Definition: `microcap_close / MA60(microcap_close) - 1`.
- Latest close-confirmed row in the record: `2026-05-22`, current MA60 bias `+1.27%`, inactive and `33.73pp` below hot threshold.

## Research Decisions

Pool and rebalance-frequency pass:

- Keep official `Top100 biweekly`.
- `Top50`, `Top200`, `weekly`, and `monthly` are observation-only.

Broad-volume environment pass:

- Rule tested: reduce v2.5 exposure on the next trading day when CSI2000 amount and ChiNext amount are both below their moving averages for a sustained streak.
- Grid: MA `[45, 50, 55, 60, 65]`, consecutive days `10..20`, scale `[0.0, 0.25, 0.5, 0.75]`.
- Data sources:
  - CSI2000 amount: `akshare.stock_zh_index_hist_csindex(symbol='932000')`.
  - ChiNext amount: `akshare.stock_zh_index_daily_tx(symbol='sz399006')`.
- Latest data date: `2026-05-22`.
- Candidate count excluding baseline: `220`.
- Result: no candidate produced a positive annual-return delta in full, 10Y, 5Y, 3Y, or 1Y windows.
- Best broad-volume candidate by score: `zz2000_cyb_below_ma65_days19_scale0p75`.
  - Full annual-return delta: `-0.5888pp`.
  - Full max-drawdown delta: `+0.4255pp`.
  - 10Y annual-return delta: `-0.4999pp`.
  - 5Y annual-return delta: `-0.3428pp`.
  - 3Y annual-return delta: `-0.8762pp`.
  - 1Y annual-return delta: `-1.1097pp`.
- Decision: research-only; do not add to v2.5 mainline or warning board.

## Verification

Commands run during the closeout:

```powershell
python -m py_compile scripts\run_microcap_v2_5_zz2000_cyb_volume_scan.py
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\check_quant_param_scan_artifacts.py --phase complete --strict quant_param_scan_runs\20260523_microcap_top100_v2_5_broad_volume_warning_overlay_zz2000_cyb_amount_ma_days_scale
rg --files -g "test_*.py"
```

The compile and artifact checks passed. The final `rg --files -g "test_*.py"`
returned no files.
