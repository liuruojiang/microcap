# Microcap v1.4 Parameter Scan Interpretation

## Scope

This scan uses the official v1.4 source path and cost model.

- Source module: `microcap_top100_mom16_biweekly_live_v1_4.py`
- Base context: `_load_base_v1_1_context()`
- Buffer path: `apply_momentum_gap_exit_buffer(...)`
- Peak-decay path: `apply_momentum_gap_peak_decay_derisk(...)`
- Cost model: the same entry/exit and rebalance-cost path used by v1.4
- Data window: `2010-02-02` to `2026-04-28`
- Grid size: 576 combinations

Validation against official current v1.4:

- current params: `buffer=0.0025`, `decay=0.25`, `scale=0.0`, `recovery=0.35`
- `max_abs_ret_diff = 9.97e-17`
- `max_abs_nav_diff = 2.84e-14`

## Current Baseline

| variant | CAGR | Sharpe | Max DD | Calmar | derisk triggers | zero-scale days |
|---|---:|---:|---:|---:|---:|---:|
| current v1.4 | 38.35% | 3.0777 | -12.21% | 3.1399 | 84 | 1279 |

## Main Finding

The parameter scan is more useful than the reentry overlay. Better results come from adjusting the existing v1.4 parameters, not adding a new reentry rule.

Two parameter families matter:

1. Aggressive return candidate:
   - `buffer=0.0035`, `decay=0.25`, `scale=0.0`, `recovery=0.25`
   - full CAGR improves by `+2.37 pct/year`
   - Sharpe improves by `+0.092`
   - Max DD worsens by `-1.31 pct`

2. Robust candidate:
   - `buffer=0.0035`, `decay=0.20`, `scale=0.0`, `recovery=0.25`
   - full CAGR improves by `+1.11 pct/year`
   - Sharpe improves by `+0.027`
   - Max DD is effectively unchanged
   - all tested windows have positive CAGR improvement and no drawdown deterioration

## Candidate Comparison

| params | CAGR | Sharpe | Max DD | Delta CAGR | Delta Sharpe | Delta Max DD |
|---|---:|---:|---:|---:|---:|---:|
| current: buf 0.25%, decay 25%, scale 0%, rec 35% | 38.35% | 3.0777 | -12.21% | 0.00% | 0.0000 | 0.00% |
| aggressive: buf 0.35%, decay 25%, scale 0%, rec 25% | 40.71% | 3.1701 | -13.52% | +2.37% | +0.0924 | -1.31% |
| robust: buf 0.35%, decay 20%, scale 0%, rec 25% | 39.46% | 3.1048 | -12.20% | +1.11% | +0.0271 | +0.01% |
| robust nearby: buf 0.40%, decay 20%, scale 0%, rec 25% | 39.38% | 3.0970 | -12.20% | +1.03% | +0.0193 | +0.01% |
| robust nearby: buf 0.30%, decay 20%, scale 0%, rec 25% | 39.18% | 3.0837 | -12.20% | +0.84% | +0.0061 | +0.01% |

## Window Check

| params | 1Y CAGR | 3Y CAGR | 5Y CAGR | 10Y CAGR | full CAGR |
|---|---:|---:|---:|---:|---:|
| current | 40.58% | 40.26% | 45.40% | 39.12% | 38.35% |
| aggressive | 46.56% | 43.20% | 48.44% | 41.64% | 40.71% |
| robust | 44.69% | 42.11% | 46.77% | 40.92% | 39.46% |

## Interpretation

The strongest pattern is not partial de-risking. The top candidates keep `scale=0.0`, meaning full no-exposure after the peak-decay trigger still works better than partial exposure in this grid.

The improvement comes from:

- slightly wider exit buffer: `0.25% -> 0.35%`
- earlier peak-decay trigger for the robust choice: `decay 25% -> 20%`
- faster recovery: `recovery 35% -> 25%`

The recovery threshold change is important. Current v1.4 waits for a stronger recovery before rearming. The scan says a quicker recovery threshold of `25%` captures more rebound without hurting drawdown when paired with `decay=20%`.

## Recommendation

If choosing a production candidate now, prefer the robust version:

`buffer=0.0035`, `decay_ratio_threshold=0.20`, `derisk_scale=0.0`, `recovery_ratio_threshold=0.25`

The aggressive version is worth keeping as a research candidate, but not as the default until it passes a more detailed 2024 false-rebound and recent-window review.
