# Quant Parameter Scan Record

## Run Metadata

- Run id: `20260521_microcap_top100_v2_0_v2_3_signal_targetvol_overlay_lookback_halflife_gap_targetvol_scale_hedge`
- Created at: 2026-05-21T10:44:36+08:00
- Project: microcap Top100
- Strategy or version: v2.0_v2.3
- Sleeve or subsystem: signal_targetvol_overlay
- Parameter group: `lookback_halflife_gap_targetvol_scale_hedge`
- Scan type:
- Repo or workspace path: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略`
- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_0.py; microcap_top100_mom16_biweekly_live_v2_3.py`
- Git branch: `main`
- Git commit: `0104f2d476e60fd605944d0479e50925b199f26a`
- Working tree status before:

```text
M microcap_top100_mom16_biweekly_live_v2_0.py
 M microcap_top100_mom16_biweekly_live_v2_3.py
 M microcap_top100_mom16_biweekly_live_v2_4.py
 M outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json
 M outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv
 M tests/test_realtime_anchor_quote_guard.py
```

## Research Question

- Baseline:
- Candidate grid:
- Decision target:
- Source-change rule: `research_only_no_source_change`
- Required windows: full, 10Y, 5Y, 3Y, 1Y
- Required metrics: annual return, annualized volatility, Sharpe, max drawdown, and strategy-specific exposure/cost metrics
- Promotion threshold:
- Rerun triggers:

## Implementation Anchor

- Official entrypoint:
- Function or command path:
- Existing loaders reused:
- Existing metrics reused:
- Default values and source locations:

| parameter | default | source location |
| --- | ---: | --- |
| `lookback_halflife_gap_targetvol_scale_hedge` |  |  |

## Data Snapshot

- Run timestamp:
- Raw data start:
- Raw data end:
- Metrics start after warmup:
- Metrics end:
- Latest trading date or snapshot:
- Data sources:
- Local cache paths:
- Cache write risk:
- Missing or stale data:
- Alignment rules:
- Adjustment mode:
- Trading calendar:
- Timezone assumptions:

## Cost and Execution Assumptions

- Commission:
- Slippage:
- Open-impact:
- Financing:
- Borrow or shorting cost:
- Rebalance timing:
- Fill timing:
- Leverage or sizing rules:
- Hedge assumptions:

## Runtime Override Plan

- Override mechanism:
- Values restored after each candidate:
- Default candidate included in same run:
- Parity check against official/default output:
- If parity check failed, explanation:

## Commands

```powershell
# Add scan commands here as they are run.
```

## Output Files

- `scan_summary.csv`:
- `window_metrics.csv`:
- `scan_meta.json`: this run metadata
- `command_log.txt`: initialization command and future scan commands

## Full-Sample Results

To be filled after the scan writes `scan_summary.csv`.

## Window Results

To be filled after the scan writes `window_metrics.csv`.

## Stability Classification

- Label:
- Evidence:
- Nearby-candidate behavior:
- Recent-window behavior:
- Cost sensitivity:
- Data sensitivity:
- Leverage or exposure caveat:

## Decision

- Decision:
- Recommended next action:

## User-Facing Summary

- Completed full-factorial robustness scan from existing `scan_summary.csv`: v2.3 4800 candidates, v2.0 960 candidates.
- Metrics use `return_net` on common close-confirmed index 2010-05-05 to 2026-05-20.
- Additional outputs: `parameter_sensitivity.csv`, `default_neighborhood_top50.csv`, `stability_overview.csv`.
- Decision: research evidence only; no source-code promotion in this run.

## Finalization

- Finalized at: 2026-05-21T11:26:29+08:00
- Decision: research_only_no_source_promotion; v2.3 shows better drawdown robustness, v2.0 has higher return but materially higher drawdown sensitivity
- Stability label: mixed_v23_risk_stable_v20_drawdown_sensitive
- Complete checker: PASS

## v2.0 Official Default Inclusion

- Added exact v2.0 official default candidate: `lookback=16`, `exit_buffer=0.003`, `target_vol=0.25`, `scale_threshold=0.10`, `execution_hedge_ratio=0.8`.

## Finalization

- Finalized at: 2026-05-21T11:28:11+08:00
- Decision: research_only_no_source_promotion; v2.3 shows better drawdown robustness, v2.0 has higher return but materially higher drawdown sensitivity
- Stability label: mixed_v23_risk_stable_v20_drawdown_sensitive
- Complete checker: PASS
