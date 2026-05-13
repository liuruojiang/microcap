# v1.6 Target-Vol Vol-Proxy Source Comparison

## Run Metadata

- Strategy: microcap Top100 v1.6
- Entrypoint: microcap_top100_mom16_biweekly_live_v1_6.py
- Run folder: quant_param_scan_runs\20260513_microcap_top100_v1_6_target_vol_vol_proxy_source

## Research Question

Does the current production target-volatility volatility source, return_raw, materially differ from proposed
underlying-risk proxies for realized-vol and leverage sizing?

## Implementation Anchor

The base v1.4 path was rebuilt through the official v1.6 context. The production default was not changed.
Each candidate reuses v1.6 apply_target_vol_scaling with only _select_target_vol_return_source overridden at runtime.

## Data Snapshot

- Start: 2010-03-04
- End: 2026-05-12
- Full rows: 3921

## Cost and Execution Assumptions

- Target vol: 25.00%
- Window: 60
- Max leverage: 1.50
- Scale-change cost: 0.1000%
- Financing rate: 3.00%
- PnL source: v1_4_overlay_pre_cost_return_explicit_or_return_net_cost_reversal_fallback

## Runtime Override Plan

Runtime-only selector overrides were used. No production default selector was changed.

## Commands

- python scripts/analyze_v1_6_target_vol_proxy_sources.py --run-folder quant_param_scan_runs\20260513_microcap_top100_v1_6_target_vol_vol_proxy_source

## Output Files

- scan_summary.csv
- window_metrics.csv
- daily_comparison.csv

## Full-Sample Results

| candidate                 | target_vol_return_source         |   ann_return_pct |   max_dd_pct |   sharpe_repo |   final_nav |   avg_weight_pct |   max_leverage_day_ratio |
|:--------------------------|:---------------------------------|-----------------:|-------------:|--------------:|------------:|-----------------:|-------------------------:|
| current_return_raw        | return_raw                       |          37.5175 |     -18.8498 |        2.4872 |    173.7269 |         110.4103 |                   0.6669 |
| proposed_overlay_first    | overlay_pre_cost_return          |          38.3007 |     -18.8498 |        2.5102 |    190.4549 |         111.2140 |                   0.6940 |
| constructed_spread_always | constructed_microcap_minus_hedge |          36.7537 |     -15.5891 |        2.5572 |    158.7470 |         107.2672 |                   0.5950 |

## Window Results

See window_metrics.csv for full/10y/5y/3y/1y wide metrics.

## Stability Classification

Diagnostic comparison only. A production switch still requires user approval and a dedicated signal/NAV migration patch.

## Decision

Do not switch production default in this run.

## User-Facing Summary

This run measures the impact of candidate vol proxies without changing official v1.6 output semantics.

## Finalization

- Finalized at: 2026-05-13T17:56:23+08:00
- Decision: do_not_switch_default_yet
- Stability label: diagnostic_only
- Complete checker: PASS
