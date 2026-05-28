# Microcap Top100 Official v2.5 Optimization Directions - 2026-05-28

## Purpose

This document resets the next research window back to official `v2.5`, because the current `ABS120 + TV40 + max1.0` branch is not beating official v2.5 outside selected recent drawdown windows.

Use this as a handoff note for a new Codex window. It is not a promotion decision and does not change production code.

## Official v2.5 Source Of Truth

Files:

- `microcap_top100_mom16_biweekly_live_v2_5.py`
- `outputs/microcap_top100_mom16_biweekly_live_v2_5_nav.csv`
- `outputs/microcap_top100_mom16_biweekly_live_v2_5_performance_summary.csv`
- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_optimization_directions/official_v2_5_drawdown_pockets.csv`
- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_optimization_directions/official_v2_5_pocket_worst_days.csv`

Core v2.5 knobs observed in source:

- Signal model: microcap-only exponential weighted log-WLS score.
- `LOOKBACK = 17`.
- `HALFLIFE = 3.0`.
- `ENTRY_THRESHOLD = 0.40`.
- `EXIT_THRESHOLD = 0.40`.
- `TARGET_VOL = 0.30`.
- `TARGET_VOL_MAX_LEVERAGE = 1.3`.
- `TARGET_VOL_WINDOW = 60`.
- `TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.30`.
- No hedge leg, no R2 gate, no stop-loss, no equity drawdown overlay, no momentum-decay overlay, no overheat overlay.

Official v2.5 current costed performance:

| Window | Annualized | Max Drawdown |
|---|---:|---:|
| Full sample | `36.61%` | `-19.48%` |
| 10Y | `30.54%` | `-16.22%` |
| 5Y | `35.36%` | `-13.76%` |
| 3Y | `40.73%` | `-13.76%` |
| 1Y | `58.05%` | `-13.76%` |

## Why The Current ABS120 Branch Is Not The Right Main Path

Latest measured comparison:

| Strategy | Full ann / DD | 10Y | 5Y | 3Y | 1Y |
|---|---:|---:|---:|---:|---:|
| Official v2.5 costed | `36.61% / -19.48%` | `30.54% / -16.22%` | `35.36% / -13.76%` | `40.73% / -13.76%` | `58.05% / -13.76%` |
| ABS120+TV40 max1.0 research baseline | `28.70% / -23.28%` | `21.20% / -23.28%` | `33.99% / -12.65%` | `39.34% / -12.65%` | `43.69% / -8.14%` |
| Post-surge + momentum-stall watchlist combo | `29.78% / -23.28%` | `22.64% / -23.28%` | `36.20% / -11.68%` | `43.80% / -11.12%` | `48.29% / -6.25%` |

Interpretation:

- The ABS120 branch wins on selected recent drawdown control, especially after the two event-specific watchlist layers.
- It still loses badly on full/10Y annualized return and full/10Y max drawdown.
- Official v2.5 is the better base for further optimization unless the objective is explicitly "sacrifice long-term return for recent drawdown control."

## Official v2.5 Drawdown Pockets

Measured from `outputs/microcap_top100_mom16_biweekly_live_v2_5_nav.csv` using costed `return_net`.

| Window | Peak | Trough | Max DD | Trading Days | Pocket Return | Avg Scale | Worst Day | Worst Day Return |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Full | `2012-03-13` | `2012-12-13` | `-19.48%` | `187` | `-18.98%` | `0.34` | `2012-04-23` | `-4.17%` |
| 10Y | `2017-09-11` | `2018-10-29` | `-16.22%` | `275` | `-14.98%` | `0.36` | `2018-03-23` | `-7.38%` |
| 5Y | `2025-08-11` | `2025-10-20` | `-13.76%` | `45` | `-11.71%` | `0.55` | `2025-08-27` | `-6.10%` |
| 3Y | `2025-08-11` | `2025-10-20` | `-13.76%` | `45` | `-11.71%` | `0.55` | `2025-08-27` | `-6.10%` |
| 1Y | `2025-08-11` | `2025-10-20` | `-13.76%` | `45` | `-11.71%` | `0.55` | `2025-08-27` | `-6.10%` |

Important observations:

- Recent official v2.5 drawdown is not the same as the 2026-05 pocket in the ABS120 branch. Official v2.5's 5Y/3Y/1Y max drawdown is the `2025-08-11` to `2025-10-20` pocket.
- The recent pocket starts from a very strong state: on `2025-08-11`, score was `1.7727`, scale was `1.3`, realized vol was `18.96%`, raw target-vol scale was capped at `1.3`, and MA40 bias was `13.55%`.
- The worst recent day, `2025-08-27`, occurred while still at `1.3x` scale, with score down to `0.3072`, R2 down to `0.0590`, MA40 bias still positive at `4.18%`, and next holding going to cash.
- The problem is not simply "late exit"; some large losses occur before the exit, while the model is still max-levered after a high-score surge.

## Priority Direction 1 - Official v2.5 Post-Surge / Score Deceleration Cooldown

Why this should be first:

- It directly targets the official v2.5 recent pocket.
- The pocket starts with high score, high MA bias, and max leverage.
- Similar event-specific cooldowns worked on the ABS120 branch, but must be re-run on official v2.5 because the base signal and target-vol cap differ.

Candidate scan:

- Trigger after close:
  - `microcap_ret >= 1.5% / 2.0% / 2.5%`
  - and/or `annualized_log_wls_score >= 1.0 / 1.3 / 1.6`
  - and/or `ma_bias_40 >= 8% / 10% / 12%`
  - optional R2 gate: `log_wls_r2 >= 0.6 / 0.75`.
- Action:
  - next `1/2/3` sessions scale multiplier `0.50 / 0.75`;
  - or cap max execution scale to `1.0 / 1.1` instead of multiplying.
- Promotion hurdle:
  - improve 5Y/3Y/1Y max DD by at least `1pp`;
  - keep 1Y annualized damage below `2pp`;
  - do not worsen full/10Y max DD.

Suggested script name:

- `scripts/run_microcap_v2_5_official_post_surge_score_deceleration_scan.py`

## Priority Direction 2 - Max-Leverage Conditional Cap

Why this is worth testing:

- Official v2.5's recent drawdown happens while `current_execution_scale = 1.3`.
- Target vol raw scale is also capped at `1.3` for much of the 2025 pocket, so normal target-vol mechanics cannot reduce exposure.
- A conditional cap is less destructive than lowering global `TARGET_VOL` or global `TARGET_VOL_MAX_LEVERAGE`.

Candidate scan:

- Trigger:
  - current scale at or near max: `current_execution_scale >= 1.25`;
  - plus one of:
    - `ma_bias_40 >= 8% / 10% / 12%`;
    - `score_chg_3d <= -0.5 / -0.8`;
    - `log_wls_r2 <= 0.30 / 0.50` after a high-score state.
- Action:
  - cap execution scale to `1.0 / 1.1 / 1.2`;
  - apply for next `1/2/3` sessions.
- Promotion hurdle:
  - must beat official v2.5 in 5Y/3Y/1Y drawdown;
  - annualized return cost should be smaller than a global max-leverage cut.

Suggested script name:

- `scripts/run_microcap_v2_5_official_conditional_maxlev_cap_scan.py`

## Priority Direction 3 - Low-R2 / Unstable-Score Derisk

Why this is plausible:

- The 2025 recent pocket shows score still positive while R2 collapses.
- On `2025-08-27`, score was still `0.3072`, but `log_wls_r2` was only `0.0590`.
- That is exactly the kind of "trend score is no longer reliable" state that a quality gate should catch.

Candidate scan:

- Trigger:
  - currently long;
  - `annualized_log_wls_score > 0` but `log_wls_r2 <= 0.10 / 0.20 / 0.30`;
  - optional score-drop gate: `score_chg_3d <= -0.5`.
- Action:
  - cap next-session scale to `0 / 0.5 / 0.75`;
  - or require R2 recovery above `0.30 / 0.50` before returning to full scale.
- Risk:
  - low R2 may also appear around profitable choppy recoveries, so this needs strict return-damage checks.

Suggested script name:

- `scripts/run_microcap_v2_5_official_r2_quality_derisk_scan.py`

## Priority Direction 4 - Exit Threshold / Hysteresis Recheck

Why this is lower priority:

- v2.5 uses `ENTRY_THRESHOLD = EXIT_THRESHOLD = 0.40`.
- The 2025 pocket includes repeated near-threshold flip behavior: cash, re-enter, cash, re-enter.
- But changing thresholds globally can easily reduce v2.5's strong 1Y annualized return.

Candidate scan:

- Keep entry threshold fixed at `0.40`, scan exit threshold:
  - `0.50 / 0.60 / 0.80`;
  - or exit after score drop rather than absolute score.
- Also test a "cooldown after exit" rule:
  - after an exit from a high-score state, block re-entry for `1/2/3` sessions unless score returns above `0.8 / 1.0`.

Promotion hurdle:

- Must improve the `2025-08` to `2025-10` pocket without losing more than `2pp` annualized in 1Y.
- Must not degrade full/10Y materially.

Suggested script name:

- `scripts/run_microcap_v2_5_official_exit_hysteresis_scan.py`

## Priority Direction 5 - Target-Vol Overlay Fine Scan

Why this is lower priority than conditional caps:

- A global target-vol/max-leverage cut will almost certainly reduce drawdown but may give up too much of official v2.5's strongest advantage: high full/10Y/1Y annualized return.
- Still worth testing as a benchmark, not as the first promotion candidate.

Candidate scan:

- `TARGET_VOL`: `25% / 27.5% / 30%`.
- `TARGET_VOL_MAX_LEVERAGE`: `1.0 / 1.1 / 1.2 / 1.3`.
- `TARGET_VOL_SCALE_REBALANCE_THRESHOLD`: `10% / 20% / 30%`.

Promotion hurdle:

- Any global reduction must be compared against the conditional cap in Direction 2.
- Do not promote a global cut if a conditional cap gets similar drawdown improvement with better annualized return.

Suggested script name:

- `scripts/run_microcap_v2_5_official_target_vol_fine_scan.py`

## Directions Not To Prioritize First

Do not start with these unless the first four directions fail:

- ABS120-style global absolute-momentum filters: already weak on the current branch versus official v2.5.
- Broad volume/amount regime filters: in the current branch, they did not improve all windows and mainly affected old full/10Y pockets.
- Equity drawdown exits and momentum decay as global layers: previous tests compressed old drawdowns but damaged recent windows or failed all-window improvement.
- Another full-cash global exit: official v2.5's strength is high return; global exits are likely to destroy the edge.

## Recommended Next Test Order

1. `official_post_surge_score_deceleration_scan`
2. `official_conditional_maxlev_cap_scan`
3. `official_r2_quality_derisk_scan`
4. `official_exit_hysteresis_scan`
5. `official_target_vol_fine_scan`

The first two should be run before any more ABS120 work.

## Reporting Standard For The New Window

Use the standard windows:

- full sample
- 10Y
- 5Y
- 3Y
- 1Y

Every completed layer should report:

- annualized return and max drawdown in all five windows;
- return delta and drawdown improvement versus official v2.5;
- trigger-day count by window;
- whether the 2025-08 to 2025-10 pocket improved;
- whether full/10Y got worse.

Record each completed layer back into this document or the active handoff document before moving to the next layer.

## Layer Update - Official Post-Surge Score Deceleration Cooldown

Added after the first official-v2.5 layer run.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_post_surge_score_deceleration`

Script and test:

- `scripts/run_microcap_v2_5_official_post_surge_score_deceleration_scan.py`
- `test_official_v2_5_post_surge_score_deceleration.py`

Layer tested:

- Official v2.5 costed NAV, not the ABS120 research branch.
- Trigger families: `ret`, `ret_score`, `ret_bias`, `ret_score_bias`, `score_bias`.
- Return thresholds: `1.5% / 2.0% / 2.5%`.
- Score minimums: `1.0 / 1.3 / 1.6`.
- MA40-bias minimums: `8% / 10% / 12%`.
- Optional R2 minimums: `0.60 / 0.75`.
- Cooldown length: next `1 / 2 / 3` sessions.
- Actions: execution-scale multiplier `0.50 / 0.75`, or cap at `1.0 / 1.1`.
- Trigger timing: close-confirmed T signal affects only subsequent sessions.
- Repricing: official v2.5 base pre-cost return, scaled base trade cost, target-vol scale-change cost, financing, and idle-cash yield.

Baseline after refresh:

- `official_v2_5`: full `36.61% / -19.48%`; 10Y `30.54% / -16.22%`; 5Y `35.36% / -13.76%`; 3Y `40.73% / -13.76%`; 1Y `58.05% / -13.76%`.

Representative rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | ann delta pp | DD improve pp |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ret_score_bias_ret1p5pct_score1p6_bias12p0pct_r20p75_d3_mul050` | `32.68% / -19.48%` | `31.54% / -16.22%` | `36.43% / -13.62%` | `42.59% / -12.70%` | `62.57% / -12.70%` | `-3.93/+1.00/+1.07/+1.86/+4.52` | `0/0/+0.14/+1.06/+1.06` |
| `ret_bias_ret1p5pct_scoreany_bias10p0pct_r20p75_d3_cap1p0` | `35.46% / -19.48%` | `30.88% / -16.22%` | `35.95% / -13.62%` | `41.17% / -12.70%` | `59.54% / -12.70%` | `-1.15/+0.34/+0.59/+0.44/+1.49` | `0/0/+0.14/+1.06/+1.06` |
| `ret_ret1p5pct_scoreany_biasany_r2any_d3_mul050` | `21.51% / -17.94%` | `19.30% / -17.45%` | `23.04% / -12.53%` | `22.28% / -12.53%` | `28.95% / -12.53%` | `-15.10/-11.23/-12.33/-18.45/-29.10` | `+1.54/-1.22/+1.23/+1.23/+1.23` |

Decision:

- `watchlist_post_surge_score_deceleration_improves_2025_pocket_but_fails_5y_hurdle`.
- Stability label: `watchlist_event_specific_not_promote_yet`.
- Promotion hurdle passers: `0`.
- The layer improves the 2025-08 to 2025-10 pocket and can improve 3Y/1Y max drawdown by about `1.06pp`.
- It does not meet the stated first-layer hurdle because the best balanced rows improve 5Y max drawdown by only about `0.14pp`, not `>= 1pp`.
- Broad ret-only rows can improve 5Y/3Y/1Y drawdown by more than `1pp`, but annualized return damage is too large.

Verification:

- Unit test: PASS.
- Official v2.5 refresh: PASS, latest trade date `2026-05-27`.
- Scan run: PASS, `2053` candidates.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Suggested next layer:

- Move to `official_conditional_maxlev_cap_scan`.
- Use this layer's useful pattern as a watchlist reference, but do not promote it before the conditional max-leverage cap layer is tested.

## Layer Update - Official Conditional Max-Leverage Cap

Added after the second official-v2.5 layer run.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_conditional_maxlev_cap`

Script and test:

- `scripts/run_microcap_v2_5_official_conditional_maxlev_cap_scan.py`
- `test_official_v2_5_conditional_maxlev_cap.py`

Layer tested:

- Official v2.5 costed NAV, not the ABS120 research branch.
- Trigger families: `global_cap`, `bias_hot`, `score_drop`, `low_r2_after_high_score`.
- Near-max leverage trigger: `current_execution_scale >= 1.25`.
- MA40-bias minimums: `8% / 10% / 12%`.
- Score-drop maximums: `-0.50 / -0.80`.
- R2 maximums: `0.30 / 0.50` after prior high-score states `1.0 / 1.3 / 1.6`.
- Cap length: next `1 / 2 / 3` sessions.
- Scale cap: `1.0 / 1.1 / 1.2`.
- Trigger timing: close-confirmed T signal affects only subsequent sessions.
- Repricing: official v2.5 base pre-cost return, scaled base trade cost, target-vol scale-change cost, financing, and idle-cash yield.

Baseline after refresh:

- `official_v2_5`: full `36.61% / -19.48%`; 10Y `30.54% / -16.22%`; 5Y `35.36% / -13.76%`; 3Y `40.73% / -13.76%`; 1Y `58.05% / -13.76%`.

Representative rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | ann delta pp | DD improve pp | exec days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `bias_hot_scale1p25_bias12p0pct_dropany_r2maxany_hiany_hilbany_d2_cap1p0` | `36.31% / -19.48%` | `30.74% / -16.22%` | `35.44% / -13.62%` | `40.86% / -12.70%` | `58.50% / -12.70%` | `-0.30/+0.20/+0.08/+0.13/+0.45` | `0/0/+0.14/+1.06/+1.06` | `43` |
| `bias_hot_scale1p25_bias10p0pct_dropany_r2maxany_hiany_hilbany_d1_cap1p1` | `36.53% / -19.48%` | `30.73% / -15.99%` | `35.34% / -13.06%` | `40.24% / -12.83%` | `56.41% / -12.83%` | `-0.08/+0.19/-0.00/-0.49/-1.64` | `0/+0.23/+0.55/+0.77/+0.77` | `114` |
| `global_cap1p0` | `30.89% / -17.65%` | `25.50% / -12.99%` | `30.06% / -10.66%` | `35.87% / -10.66%` | `46.54% / -10.66%` | `-5.72/-5.04/-5.30/-4.85/-11.51` | `+1.83/+3.24/+3.11/+3.11/+3.11` | `1526` |

Decision:

- `watchlist_bias_hot_conditional_maxlev_cap_improves_recent_pocket_but_5y_dd_gain_small`.
- Stability label: `watchlist_event_specific_scale_cap_not_promote_yet`.
- The cleanest row is the MA40-bias hot cap: near max leverage, MA40 bias above `12%`, cap next `2` sessions to `1.0`.
- It improves the 2025-08 to 2025-10 pocket and improves 3Y/1Y max drawdown by about `1.06pp` with no 1Y return damage.
- It is not promoted because 5Y max drawdown improves only about `0.14pp`, while full/10Y drawdown are unchanged.
- Global caps improve drawdown more, but they are too blunt and give up too much annualized return.

Verification:

- Unit test: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Suggested next layer:

- Move to `official_r2_quality_derisk_scan`.
- Keep the `bias_hot 12% / d2 / cap1.0` row as a watchlist reference for later combination tests, but do not merge it into mainline before the R2-quality and exit-hysteresis layers are tested.

## Layer Update - Official R2 Quality Derisk

Added after the third official-v2.5 layer run.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_r2_quality_derisk`

Script and test:

- `scripts/run_microcap_v2_5_official_r2_quality_derisk_scan.py`
- `test_official_v2_5_r2_quality_derisk.py`

Layer tested:

- Official v2.5 costed NAV, not the ABS120 research branch.
- Trigger families: `low_r2_positive_score`, `low_r2_score_drop`, `low_r2_recovery_block`.
- Low-R2 thresholds: `0.10 / 0.20 / 0.30`.
- Optional 3D score-drop thresholds: `-0.50 / -0.80`.
- Optional recovery block until R2 recovers above `0.30 / 0.50`.
- Cooldown length: next `1 / 2 / 3` sessions.
- Action: cap execution scale to `0 / 0.5 / 0.75`.
- Trigger timing: close-confirmed T signal affects only subsequent sessions.
- Repricing: official v2.5 base pre-cost return, scaled base trade cost, target-vol scale-change cost, financing, and idle-cash yield.

Refresh caveat:

- `python microcap_top100_mom16_biweekly_live_v2_5.py` timed out after 10 minutes during this layer and left stale generation locks.
- The timed-out Python processes were stopped and stale lock files were removed.
- Existing official v2.5 NAV was already through `2026-05-27`; this layer used that NAV.

Baseline:

- `official_v2_5`: full `36.61% / -19.48%`; 10Y `30.54% / -16.22%`; 5Y `35.36% / -13.76%`; 3Y `40.73% / -13.76%`; 1Y `58.05% / -13.76%`.

Representative rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | ann delta pp | DD improve pp | exec days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `low_r2_positive_score_r2max0p1_dropany_recoverany_d1_cap0p75` | `36.67% / -19.48%` | `30.54% / -16.22%` | `35.36% / -13.76%` | `40.72% / -13.76%` | `58.05% / -13.76%` | `+0.06/-0.00/-0.00/-0.00/0.00` | `0/0/0/0/0` | `70` |
| `low_r2_positive_score_r2max0p1_dropany_recoverany_d2_cap0p75` | `36.65% / -19.48%` | `30.51% / -16.22%` | `35.31% / -13.91%` | `40.64% / -13.91%` | `57.77% / -13.91%` | `+0.04/-0.02/-0.05/-0.09/-0.28` | `0/0/-0.15/-0.15/-0.15` | `137` |
| `low_r2_recovery_block_r2max0p3_dropany_recover0p3_d2_cap0p5` | old full/10Y DD improves, but recent windows do not improve |  |  |  |  | full ann roughly `-2.90pp` versus official | full DD roughly `+2.64pp` | `771` |

Decision:

- `reject_r2_quality_derisk_no_recent_drawdown_improvement`.
- Stability label: `reject_no_1y_or_2025_pocket_dd_improvement`.
- Mechanical passers: `0`.
- No candidate improved 1Y max drawdown or the 2025-08 to 2025-10 pocket.
- Tight R2 gates mostly do nothing to the recent max-DD event; longer recovery blocks improve some old full/10Y drawdowns but are too broad and do not solve the current official-v2.5 problem.

Verification:

- Unit test: PASS.
- Scan run: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Suggested next layer:

- Move to `official_exit_hysteresis_scan`.
- The R2-quality layer should not be combined into the watchlist stack unless the objective changes to old full/10Y drawdown compression.

## Layer Update - Official Exit Hysteresis

Added after the fourth official-v2.5 layer run.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_exit_hysteresis`

Script and test:

- `scripts/run_microcap_v2_5_official_exit_hysteresis_scan.py`
- `test_official_v2_5_exit_hysteresis.py`

Layer tested:

- Official v2.5 costed NAV, not the ABS120 research branch.
- Trigger families: `early_exit_threshold`, `post_exit_reentry_block`, `early_exit_and_reentry_block`.
- Early-exit score thresholds: `0.50 / 0.60 / 0.80`.
- Post-exit high-score minimums: `1.0 / 1.3 / 1.6`, using a 5-day high-score lookback.
- Reentry score minimums: `0.8 / 1.0`.
- Cooldown length: next `1 / 2 / 3` sessions.
- Action: cap execution scale to `0 / 0.5 / 0.75`.
- Trigger timing: close-confirmed T signal affects only subsequent sessions.
- Repricing: official v2.5 base pre-cost return, scaled base trade cost, target-vol scale-change cost, financing, and idle-cash yield.

Baseline:

- `official_v2_5`: full `36.61% / -19.48%`; 10Y `30.54% / -16.22%`; 5Y `35.36% / -13.76%`; 3Y `40.73% / -13.76%`; 1Y `58.05% / -13.76%`.

Representative rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | ann delta pp | DD improve pp | exec days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `early_exit_and_reentry_block_exit0p8_hi1p3_reentry0p8_d1_cap0p0` | `42.02% / -15.52%` | `31.68% / -15.52%` | `36.16% / -15.52%` | `42.18% / -10.20%` | `51.91% / -10.04%` | `+5.41/+1.14/+0.80/+1.46/-6.14` | `+3.96/+0.70/-1.76/+3.56/+3.72` | `475` |
| `early_exit_and_reentry_block_exit0p8_hi1p3_reentry0p8_d1_cap0p5` | improves full/10Y/3Y/1Y DD, but still worsens 5Y DD and costs more than `2pp` 1Y annualized return |  |  |  |  |  |  | `475` |
| `early_exit_and_reentry_block_exit0p8_hi1p3_reentry1p0_d3_cap0p0` | `36.55% / -16.63%` | `26.58% / -13.75%` | `29.53% / -13.75%` | `37.20% / -10.77%` | `35.14% / -10.77%` | `-0.06/-3.96/-5.83/-3.53/-22.91` | `+2.85/+2.47/+0.01/+2.99/+2.99` | `829` |

Decision:

- `reject_exit_hysteresis_no_acceptable_all_window_tradeoff`.
- Stability label: `reject_5y_or_1y_return_damage_not_promote`.
- Mechanical passers: `0`.
- The `exit0.8 / reentry0.8` family can improve the 2025-08 to 2025-10 pocket and substantially reduce 1Y drawdown, but it worsens 5Y max drawdown.
- The stricter `reentry1.0` family can barely improve 5Y drawdown, but only by causing excessive 1Y annualized return damage around `-22.9pp`.
- Therefore exit hysteresis is not a mainline promotion candidate under the current all-window hurdle.

Verification:

- Unit test: PASS.
- Scan run: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Suggested next layer:

- Move to `official_target_vol_fine_scan`.
- Treat exit hysteresis only as an optional special-objective branch if the goal changes to "reduce 2025 pocket even at the cost of 5Y consistency and 1Y return."

## Layer Update - Official Target-Vol Fine Scan

Added after the fifth official-v2.5 layer run.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_official_v2_5_target_vol_fine`

Script and test:

- `scripts/run_microcap_v2_5_official_target_vol_fine_scan.py`
- `test_official_v2_5_target_vol_fine.py`

Layer tested:

- Official v2.5 costed NAV, not the ABS120 research branch.
- Target vol values: `25% / 27.5% / 30%`.
- Max leverage values: `1.0 / 1.1 / 1.2 / 1.3`.
- Scale rebalance thresholds: `10% / 20% / 30%`.
- Each candidate reruns official v2.5 `apply_target_vol` with temporary parameter overrides, then restores module constants.
- Default-param parity row `tv300_max1p3_thr30` matched official v2.5 within numerical tolerance.

Baseline:

- `official_v2_5`: full `36.61% / -19.48%`; 10Y `30.54% / -16.22%`; 5Y `35.36% / -13.76%`; 3Y `40.73% / -13.76%`; 1Y `58.05% / -13.76%`.

Representative rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | ann delta pp | DD improve pp |
|---|---:|---:|---:|---:|---:|---:|---:|
| `tv300_max1p2_thr30` | `35.50% / -19.14%` | `29.17% / -15.19%` | `33.63% / -12.73%` | `39.84% / -12.73%` | `55.16% / -12.73%` | `-1.11/-1.37/-1.73/-0.88/-2.89` | `+0.35/+1.03/+1.03/+1.03/+1.03` |
| `tv300_max1p1_thr20` | `33.05% / -18.79%` | `27.17% / -14.07%` | `31.37% / -11.70%` | `37.07% / -11.70%` | `51.06% / -11.70%` | `-3.56/-3.37/-4.00/-3.65/-6.98` | `+0.69/+2.16/+2.06/+2.06/+2.06` |
| `tv275_max1p0_thr30` | `30.16% / -17.21%` | `24.67% / -12.75%` | `28.36% / -10.57%` | `33.33% / -9.90%` | `41.21% / -9.90%` | `-6.45/-5.87/-7.00/-7.39/-16.84` | `+2.27/+3.48/+3.19/+3.86/+3.86` |

Decision:

- `reject_global_target_vol_fine_no_acceptable_return_drawdown_tradeoff`.
- Stability label: `reject_global_leverage_cut_too_expensive`.
- Mechanical passers under the existing return-damage hurdle: `0`.
- The best practical global cut is `tv300_max1p2_thr30`: it improves all-window drawdowns by about `0.35pp` to `1.03pp`, but loses `2.89pp` annualized return in 1Y.
- Stronger global cuts reduce drawdown more but have much worse 1Y annualized return damage, roughly `-7pp` to `-17pp`.
- This makes the local event-specific watchlist rows from layers 1 and 2 better tradeoffs than a global target-vol/max-leverage cut.

Verification:

- Unit test: PASS.
- Scan run: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Suggested next step:

- Stop adding independent global layers for now.
- Summarize the official-v2.5 layer decisions and, if continuing, test a small combination of the two watchlist local layers only: post-surge cooldown and conditional max-leverage cap.
