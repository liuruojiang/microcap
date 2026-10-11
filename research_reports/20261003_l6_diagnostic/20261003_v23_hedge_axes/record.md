# Quant Parameter Scan Record

## Run Metadata

- Run id: `20261003_v23_hedge_axes`
- Created at: 2026-10-03T12:01:20+08:00
- Project: microcap_top100
- Strategy or version: v2.3
- Sleeve or subsystem: hedge
- Parameter group: `SIGNAL_SPREAD_HEDGE_RATIO,EXECUTION_HEDGE_RATIO`
- Scan type: `single_parameter` twice, each holding the other hedge ratio at its formal value, plus a joint signal/execution ratio-zero control.
- Repo or workspace path: `D:\动量策略\微盘股对冲策略`
- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Git branch: `main`
- Git commit: `7dc9d934f7f24fade5236247db71ec4fd7f18471`
- Working tree status before:

```text
M microcap_top100_mom16_biweekly_live_v2_0.py
 M microcap_top100_mom16_biweekly_live_v2_3.py
 M microcap_top100_mom16_biweekly_live_v2_5.py
 M outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv
 M outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json
 M outputs/microcap_top100_mom16_biweekly_live_v2_0_base_summary.json
 M outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv
 M outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv
 M research_reports/20260926_l1_repair/L1_REPAIR_DECISION.md
 M research_reports/20260926_l1_repair/REPAIR_STATUS.md
 M research_reports/20260926_l1_repair/data_recovery/INITIAL_TWO_DATE_CLOSEOUT.md
 M scripts/run_microcap_v2_5_bias_overheat_scan.py
 M scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py
 M scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py
 M tests/test_v2_5_scan_integrity.py
?? research_reports/20260926_l1_repair/data_recovery/conditional_two_date_rank.csv
?? research_reports/20260926_l1_repair/data_recovery/conditional_two_date_rank.py
?? research_reports/20260926_l1_repair/data_recovery/conditional_two_date_rank_summary.json
?? research_reports/20260926_l1_repair/shadow_rank_gate/
?? research_reports/20260926_l1_repair/st_timing/000019_first_target_probe/
?? research_reports/20260926_l1_repair/st_timing/600137_2008_2010_window/
?? research_reports/20260926_l1_repair/st_timing/600603_initial_probe/
?? research_reports/20260926_l1_repair/st_timing/600633_full_chain/
?? research_reports/20260926_l1_repair/st_timing/600792_event_chain/
?? research_reports/20260926_l1_repair/st_timing/initial_old100_screen/
?? research_reports/20260926_l1_repair/st_timing/sse_yearbook_event_chain/
?? research_reports/20260926_l1_repair/st_timing/st_600137_window.py
?? research_reports/20260926_l1_repair/szse_old_member_names/
?? research_reports/20260926_l2_diagnostic/
?? research_reports/20260926_l2_repair/
?? research_reports/20261002_l2_full_repair/
?? research_reports/20261002_l3_diagnostic/
?? research_reports/20261003_l3_recheck/
?? research_reports/20261003_l4_diagnostic/
?? research_reports/20261003_l5_diagnostic/
?? research_reports/20261003_l5_repair/
?? research_reports/20261003_l6_diagnostic/
?? tests/test_top100_2026_st_limit_boundary.py
?? tests/test_top100_l2_metric_contract.py
```

## Research Question

- Baseline: current formal v2.3 signal ratio 1.0 / execution ratio 0.8, not the older v2.3 parameter tuple.
- Candidate grid: signal ratio 0, 0.8, 1.0, 1.2 with execution fixed 0.8; execution ratio 0, 0.7, 0.8, 0.9 with signal fixed 1.0; joint ratio-zero control. The shared formal point is calculated once.
- Decision target: `keep_default` or `rerun_required`, never automatic promotion.
- Source-change rule: `research_only_no_source_change`
- Required windows: full, 10Y, 5Y, 3Y, 1Y
- Required metrics: annual return, annualized volatility, Sharpe, max drawdown, and strategy-specific exposure/cost metrics
- Promotion threshold: none; prior L1/L2/L5 issues prevent promotion. Local shape uses the predeclared descriptive bounds in `../L6_PROTOCOL.md`.
- Rerun triggers: source/data/cost changes, failed official baseline parity, or corrected PIT/ST/hedge data.

## Implementation Anchor

- Official entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`.
- Function path: `build_v2_3_common_index` → `build_spread_log_wls_gross` → `apply_overheat_defense` → `summarize_returns`.
- Existing loaders reused: v2.0 embedded base context and source path resolver; the same close/index/turnover loaded in L4.
- Existing metrics reused: v2.3 `summarize_returns` on actual `return_net`; full daily files retained for independent calculation.
- Default values and source locations:

| parameter | default | source location |
| --- | ---: | --- |
| `SIGNAL_SPREAD_HEDGE_RATIO` | 1.0 | `microcap_top100_mom16_biweekly_live_v2_3.py:163` |
| `EXECUTION_HEDGE_RATIO` | 0.8 | `microcap_top100_mom16_biweekly_live_v2_3.py:164` |

## Data Snapshot

- Run timestamp: 2026-10-03 Asia/Shanghai.
- Raw data start/end: see exact panel and proxy boundaries/rows/SHA in `scan_meta.json`.
- Metrics start after warmup: 2010-05-05, 3,988 return rows per path.
- Metrics end/latest locally available close: 2026-09-30.
- Data: frozen local refreshed panel, Top100 proxy index and turnover event table, v2.0 base and v2.3 official costed NAV. Their exact hashes were checked before and after; all selected daily streams end on 2026-09-30. Turnover is event based, with last event 2026-09-17.
- Independent history check performed by the loader: Eastmoney request failed; Sina legacy succeeded, 157 overlap rows, maximum local precision difference 0.005 index points. It did not change the listed frozen input file hashes. This is a provenance observation, not a replacement for L1 PIT data.
- Adjustment mode: existing constructed proxy and hedge index close unchanged; no new corporate-action adjustment was applied.
- Trading calendar/timezone: frozen A-share daily index, Asia/Shanghai close-confirmed dates.

## Cost and Execution Assumptions

- Commission: 0.003 each entry/exit side plus existing member rebalance event cost.
- Slippage/open impact: no additional model beyond frozen official cost path.
- Financing: OFF in formal source; no real margin account.
- Borrow or shorting cost: theoretical futures drag 0.0003 per notional day through source functions.
- Rebalance and fill timing: T close state decision controls T+1 return; source event table supplies member cost.
- Leverage/sizing: active scale 1, no target-vol overlay.
- Hedge assumptions: signal ratio changes signal WLS and overheat feature; execution ratio changes active return and drag. Both are spot-index theoretical hedges, not IM contract fills.

## Runtime Override Plan

- Override mechanism: in-process temporary assignments to v2.3 module constants, with `finally` restoration. No source or formal output edit.
- Values restored: signal and execution hedge ratios plus reporting alias `BASE_HEDGE_RATIO`.
- Default candidate included: `formal_s1_e08`.
- Baseline parity: 3,988 dates; five numeric and six state fields vs frozen official v2.3 costed NAV; maximum numeric absolute difference 3.56e-15, state mismatches zero.
- Failure: none in this local paper-model baseline; formal historical certification remains blocked upstream.

## Commands

```powershell
python -X utf8 research_reports/20261003_l6_diagnostic/20261003_v23_hedge_axes/run_scan.py
```

## Output Files

- `scan_summary.csv`: 8 candidates × 5 windows, from same-run daily `return_net`.
- `window_metrics.csv`: eight wide candidate rows.
- `daily_outputs/*.csv.gz`: full official-function outputs per candidate with SHA in metadata.
- `scan_meta.json`: this run metadata
- `command_log.txt`: initialization command and future scan commands

## Full-Sample Results

| Candidate | Full annual return | Full maximum drawdown | Held days |
| --- | ---: | ---: | ---: |
| Formal signal 1 / execution 0.8 | 23.26% | −12.92% | 2,663 |
| Signal 0.8 / execution 0.8 | 22.80% | −14.73% | 2,694 |
| Signal 1.2 / execution 0.8 | 17.26% | −17.89% | 2,436 |
| Signal OFF / execution 0.8 | 3.68% | −20.84% | 1,518 |
| Signal 1 / execution 0.7 | 23.85% | −16.02% | 2,663 |
| Signal 1 / execution 0.9 | 22.62% | −15.34% | 2,663 |
| Signal 1 / execution OFF | 26.36% | −43.86% | 2,663 |
| Signal OFF / execution OFF (joint ratio zero) | 6.13% | −35.15% | 1,518 |

These are frozen old-input paper returns, not certified executable performance. Execution OFF raises full return in this history but increases drawdown sharply. The joint ratio-zero control removes the model's signal and execution hedge ratios; the legacy fixed entry/exit and member costs are unchanged and cannot isolate actual hedge trading costs.

## Window Results

The complete five-window annual return, drawdown, peak/trough dates, cost and holding days are in `scan_summary.csv` and `window_metrics.csv`. Signal 1.2 has last-1Y return −4.41% and drawdown −17.24% versus formal +6.45% / −9.27%; signal 0.8 has +8.43% / −8.26%. The last year does not rank candidates like the full sample.

## Stability Classification

- Label: `narrow_stable` for signal ratio, `peak_only` in full-sample drawdown for execution ratio; combined artifact uses `data_sensitive` because historical constituents and actual hedging are unverified.
- Evidence: signal 0.8 lies close to formal under the descriptive local bound, but signal 1.2 degrades sharply; execution 0.7/0.9 both worsen full drawdown by over 2 percentage points. No neighbor independently validates a true IM hedge.
- Recent-window behavior: signal 0.8 improves last-1Y paper return/drawdown, while signal 1.2 reverses them; not a broad flat platform.
- Cost sensitivity: same cost functions for all arms, but no new slippage or margin financing sensitivity.
- Data sensitivity: old Top100 PIT/ST and corporate actions remain disputed; local index cross-check uses a fallback source.
- Leverage/exposure caveat: signal hedge changes days held; execution hedge changes return even when signal/holding days match. The joint zero-ratio control turns off both modeled hedge legs but preserves fixed strategy cost rates, so it is not a real unhedged-stock execution account.

## Decision

- Decision: `keep_default` for this paper-model diagnostic; do not promote any hedge ratio from this historical sample.
- Recommended next action: include the independent audit and conditional results in the L6 layer verdict. Formal results remain N/A until upstream data and L5 account rules are repaired.

## Finalization

- Finalized at: 2026-10-03T12:05:11+08:00
- Decision: keep_default
- Stability label: data_sensitive
- Complete checker: PASS

## Finalization

- Finalized at: 2026-10-03T12:15:37+08:00
- Decision: keep_default
- Stability label: data_sensitive
- Complete checker: PASS
