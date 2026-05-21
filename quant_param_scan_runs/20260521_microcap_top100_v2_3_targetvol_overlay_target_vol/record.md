# Quant Parameter Scan Record

## Run Metadata

- Run id: `20260521_microcap_top100_v2_3_targetvol_overlay_target_vol`
- Created at: 2026-05-21T18:15:34+08:00
- Project: microcap Top100
- Strategy or version: v2.3
- Sleeve or subsystem: targetvol_overlay
- Parameter group: `target_vol`
- Scan type:
- Repo or workspace path: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略`
- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Git branch: `main`
- Git commit: `a0e2be58b89b3f57f56e409f9c3e333063bb6f74`
- Working tree status before:

```text
M AGENTS.md
 M outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv
?? quant_param_scan_runs/
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
| `target_vol` |  |  |

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

## Scan Completion

- Command: `python run_v23_target_vol_scan.py`
- Common window: `2010-05-05` to `2026-05-20`.
- Official v2.3 latest nav date after refresh: `2026-05-20`.
- Fixed parameters: official v2.3 log-WLS signal, gap buffer, cost model, 60-day realized-vol window, 1.5x max leverage, 30% scale rebalance threshold.
- Best full-sample Sharpe row: `v2_3_tv10`.
- Current default row: `v2_3_tv25`.

## Finalization

- Finalized at: 2026-05-21T18:18:41+08:00
- Decision: recommend_tv10_for_risk_adjusted_selection; tv25_current_default_balances_return_and_drawdown; high_tv30_plus_cap_saturated_research_only_no_source_change
- Stability label: tv10_sharpe_best_tv25_practical_default_high_tv_cap_saturated
- Complete checker: PASS
