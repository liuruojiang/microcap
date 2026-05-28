# Microcap Top100 v2.5 ABS120 TV40 Max1.0 Layer Handoff - 2026-05-28

## Current State

- Workspace: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略`.
- Research branch, not production mainline: `v2.5 cost-only research branch`.
- Current selected baseline: `abs120_gtm0p25_tv40_max1p0_score_gtp0`.
- Rule stack: `ABS120 > -25% + score > 0 + target vol 40% + max leverage 1.0 + scale threshold 30%`.
- Data through: `2026-05-27`.
- Default comparison windows: `full`, `last_10y`, `last_5y`, `last_3y`, `last_1y`.
- Baseline metrics:
  - Full: `28.70% / -23.28%`
  - 10Y: `21.20% / -23.28%`
  - 5Y: `33.99% / -12.65%`
  - 3Y: `39.34% / -12.65%`
  - 1Y: `43.69% / -8.14%`

Do not mix this baseline with official v2.0/v2.5 default outputs or older volume-overlay docs unless the candidate is rebuilt from the same `abs120_gtm0p25_tv40_max1p0_score_gtp0` shadow path.

## Source And Artifact Anchors

- Base candidate builder:
  - `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_score_threshold_scan.py`
- Current staged-entry layer:
  - `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_scan.py`
- Current timeboxed staged-entry layer:
  - `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_timebox_scan.py`
- Timebox behavior test:
  - `test_staged_entry_timebox.py`

Recent generated run folders:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_momentum_decay`
- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_equity_dd`
- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_ma_bias_overheat_derisk`
- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_high_vol_derisk`
- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry`
- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_timebox`

All formal runs above were finalized and strict-checked with `check_quant_param_scan_artifacts.py --phase complete --strict`.

## Layer Decisions

| Layer | Result | Reason |
|---|---|---|
| Equity drawdown exit/reentry | Reject / no promotion | Most useful rows were unchanged baseline; no practical layer. |
| Momentum decay exit/reentry | Reject | Can reduce 10Y/full DD, but 5Y/3Y weak and 1Y return damage too high. |
| MA-bias overheat derisk | Reject | Local derisk helped some 5Y/3Y rows but did not improve full/10Y/1Y drawdown. |
| Realized high-vol derisk | Reject | Almost no drawdown improvement; mostly reshuffles return/vol. |
| Staged entry, wait for down close | Watchlist only | First all-window DD-improving layer, but benefit is small and return damage is nontrivial. Best practical row was 95% initial fraction. |
| Timeboxed staged entry | Reject | Reduces return damage versus pure down-close wait, but loses all-window DD improvement. |

## Staged Entry Evidence

Down-close-only staged entry, best watchlist row:

- `staged_cash_only_frac095` and `staged_any_scaleup_frac095` are identical on this branch.
- Full/10Y/5Y/3Y/1Y: `28.32%/-23.05%`, `21.00%/-23.05%`, `33.57%/-12.17%`, `38.64%/-12.17%`, `42.87%/-8.08%`.
- Return delta pp vs baseline: `-0.37`, `-0.20`, `-0.42`, `-0.70`, `-0.82`.
- DD improvement pp vs baseline: `+0.22`, `+0.22`, `+0.49`, `+0.49`, `+0.05`.
- Decision: `watchlist_staged_entry_frac095_not_promote_yet`.

Timeboxed staged entry:

- `95%, max2d`: return damage is small, but 5Y/3Y/1Y DD improvement is effectively zero.
- `95%, max5d`: improves 5Y/3Y by about `+0.49pp`, but 1Y DD is unchanged.
- `90%, max5d` and `85%, max5d`: larger DD improvement in full/10Y/5Y/3Y, still no 1Y DD improvement and return damage grows.
- Decision: `reject_timeboxed_staged_entry_no_all_window_dd_improvement`.

## What Is Left

The next priority is volume/turnover/liquidity, but it is not correct to say it is the only possible remaining layer.

Why volume is next:

- Global exits have already failed or are too expensive.
- Generic local derisk by price overheat or realized volatility is weak.
- Staged-entry changes the execution profile, but the DD benefit is too small.
- A volume/turnover layer can be more local: only affect new entries, scale-ups, or crowded/illiquid days, without forcing long cash periods.

Important caveat:

- Existing folders under `docs/top100_*volume*` and older volume scripts are not on this current `ABS120 + TV40 + max1.0 + score > 0` baseline.
- Next run should adapt or rebuild the volume logic on the current shadow path, not reuse old results directly.

Recommended next layer name:

- `entry_volume_or_turnover_gate`

Recommended candidate families:

1. Entry-local low-turnover gate:
   - Only on new entry or scale-up days.
   - If turnover/volume rank is too low, use partial entry or delay one rebalance step.
   - Candidate knobs: rank window `40/60/120`, low percentile `20/30/40`, action `0.75x/0.5x/skip`.

2. Volume-spike overheat derisk:
   - Only when already active and price is stretched.
   - Combine volume spike with MA-bias overheat to avoid derisking ordinary high-volume trend days.
   - Candidate knobs: volume z/rank threshold, MA-bias hot threshold, derisk multiplier.

3. Liquidity-aware target-vol cap:
   - Do not change binary holding; cap execution scale when liquidity/turnover is unfavorable.
   - Candidate knobs: turnover percentile and max execution scale cap `0.75/0.85/0.95`.

Best first scan:

- Start with entry-local low-turnover gate because it is least likely to create long cash periods and most directly addresses whether poor fills/liquidity drive drawdown pockets.
- Include baseline unchanged row and report `full/10Y/5Y/3Y/1Y`.
- Require all-window DD improvement and annualized return damage preferably under `1pp` on 1Y and under `0.5pp` on 10Y/5Y before promotion.

## Suggested Next Script

Create:

- `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_entry_volume_turnover_gate_scan.py`

Suggested run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_entry_volume_turnover_gate`

Implementation guardrails:

- Build the shadow path through `run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_score_threshold_scan._build_candidate_frames`.
- Select exactly `BASE_CANDIDATE = "abs120_gtm0p25_tv40_max1p0_score_gtp0"`.
- Use `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv` only after confirming its date alignment with the shadow path.
- Reprice returns with the same v2.5 cost fields: base pre-cost return, scaled base trade cost, target-vol scale-change cost, financing, and idle-cash yield.
- Keep a no-overlay parity row with max absolute return diff near machine precision.
- Formalize with `scan_summary.csv`, `window_metrics.csv`, `candidate_navs.csv`, `latest_signals.csv`, `scan_meta.json`, `record.md`, and `command_log.txt`.
- Finalize and run strict artifact check before interpreting.

## Verification Commands From Last Layer

```powershell
python -m unittest test_staged_entry_timebox.py
python scripts\run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_timebox_scan.py
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\finalize_quant_param_scan_run.py quant_param_scan_runs\20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_timebox --decision reject_timeboxed_staged_entry_no_all_window_dd_improvement --stability-label no_all_window_dd_improvement
python C:\Users\Administrator.DESKTOP-95I7VVU\.codex\skills\quant-param-scan\scripts\check_quant_param_scan_artifacts.py --phase complete --strict quant_param_scan_runs\20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_staged_entry_timebox
```

Observed verification:

- Unit test: PASS.
- Scan run: completed.
- Finalize: PASS.
- Strict artifact check: PASS.

## Dirty Worktree Note

At handoff time, `outputs/` has modified refreshed data artifacts and many research scripts are untracked. Do not clean or revert them unless explicitly requested. The current handoff only documents research state; no commit/stage was requested.

## Layer Update - Entry Turnover Gate

Added after the initial handoff.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_entry_turnover_gate`

Script and test:

- `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_entry_turnover_gate_scan.py`
- `test_entry_turnover_gate.py`

Layer tested:

- Internal proxy only: `turnover_frac_one_side`, `buy_turnover_frac`, `blocked_entry_count`, `blocked_exit_count`.
- Rolling percentile windows: `20/40/80` rebalance rows.
- Entry fractions: `0.00/0.50/0.75`.
- Trigger scopes: `entry_only`, `entry_and_scaleup`.
- Mechanism: only scale the increment toward target exposure on unfavorable turnover/proxy days; no long cash exit layer.

Baseline:

- `no_entry_turnover_gate`: full `28.70% / -23.28%`; 10Y `21.20% / -23.28%`; 5Y `33.99% / -12.65%`; 3Y `39.34% / -12.65%`; 1Y `43.69% / -8.14%`.

Top score rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | return delta pp | DD improve pp |
|---|---:|---:|---:|---:|---:|---:|---:|
| `entry_and_scaleup_to_lowp30_lb40_frac075` | `28.61% / -22.63%` | `21.00% / -22.63%` | `33.68% / -12.65%` | `38.98% / -12.65%` | `43.00% / -8.14%` | `-0.09/-0.20/-0.31/-0.36/-0.69` | `+0.64/+0.64/0/0/0` |
| `entry_and_scaleup_to_lowp20_lb80_frac075` | `28.67% / -22.96%` | `21.07% / -22.96%` | `33.66% / -12.65%` | `38.88% / -12.65%` | `43.00% / -8.14%` | `-0.02/-0.12/-0.33/-0.46/-0.69` | `+0.32/+0.32/0/0/0` |

All-window mechanical pass rows:

| candidate | Return delta pp | DD improve pp | Interpretation |
|---|---:|---:|---|
| `entry_and_scaleup_blkexit_highp70_lb20_frac075` | `-0.46/-0.30/-0.68/-1.49/-1.34` | `+0.32/+0.32/+0.01/+0.01/+0.00` | Technically all windows improve, but 5Y/3Y/1Y improvement is negligible and return cost is too high. |
| `entry_and_scaleup_blkexit_highp70_lb20_frac050` | `-1.23/-0.99/-2.02/-3.95/-4.05` | `+0.64/+0.64/+0.32/+0.32/+0.00` | Larger drawdown improvement but unacceptable recent-window return damage. |

Decision:

- `reject_internal_entry_turnover_gate_no_acceptable_all_window_tradeoff`.
- Stability label: `weak_internal_turnover_proxy_not_promote`.
- Unit test: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Interpretation:

- Internal turnover/blocked proxy repeats the older 2026-04-30 finding: it is not the right source for this sleeve.
- The better candidates mainly improve full/10Y drawdown; 5Y/3Y/1Y are flat or nearly flat.
- Do not continue tuning internal turnover proxy unless the objective changes from all-window DD improvement to execution-friction diagnostics.

Next layer:

- Move to external/broad volume or amount regime on the same current baseline.
- Priority family from old research to adapt: `CSI2000 + ChiNext amount below moving average for sustained days`, especially the old ridge around `MA53 / 13 days`, with scale candidates `0.25/0.50/0.75`.
- Critical guardrail: rebuild it on `abs120_gtm0p25_tv40_max1p0_score_gtp0`; do not reuse the 2026-04-30 official-mainline results directly.

## Layer Update - Broad Volume Amount Regime

Added after the entry-turnover layer.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_broad_volume_amount`

Script and test:

- `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_broad_volume_amount_scan.py`
- `test_broad_volume_amount_overlay.py`

Layer tested:

- External amount regime: CSI2000 amount and ChiNext amount both below their own MA for sustained consecutive closes.
- MA grid: `45/47/49/51/53/55/57/59/61/63/65`.
- Consecutive-day grid: `10..20`.
- Scale grid: `0.00/0.25/0.50/0.75`.
- Trigger timing: close-T signal affects the next execution day.
- Repricing: current v2.5 shadow baseline is repriced with actual execution scale, target-vol scale-change cost, financing, idle-cash yield, and scaled base trade cost.

Data caveat:

- Amount source in this run was `akshare_live`.
- Amount-aligned full sample starts at `2013-12-31`, while the raw microcap cache starts at `2010-01-08`.
- Therefore the broad-volume `full` row is not directly comparable to older full-sample rows. The `10Y/5Y/3Y/1Y` rows remain the current baseline windows and are the key decision windows.

Baseline on amount-aligned sample:

- `no_broad_volume_amount`: full `30.92% / -23.28%`; 10Y `21.20% / -23.28%`; 5Y `33.99% / -12.65%`; 3Y `39.34% / -12.65%`; 1Y `43.69% / -8.14%`.

Top ranking rows:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | exec days |
|---|---:|---:|---:|---:|---:|---:|
| `no_broad_volume_amount` | `30.92% / -23.28%` | `21.20% / -23.28%` | `33.99% / -12.65%` | `39.34% / -12.65%` | `43.69% / -8.14%` | `0` |
| `zz2000_cyb_below_ma57_days17_scale0p75` | `30.57% / -23.18%` | `21.03% / -23.18%` | `33.66% / -12.65%` | `38.39% / -12.65%` | `42.68% / -8.14%` | `224` |
| `zz2000_cyb_below_ma57_days19_scale0p75` | `30.56% / -23.18%` | `21.01% / -23.18%` | `33.60% / -12.65%` | `38.36% / -12.65%` | `43.04% / -8.14%` | `189` |
| `zz2000_cyb_below_ma63_days19_scale0p75` | `30.47% / -23.18%` | `20.93% / -23.18%` | `33.68% / -12.65%` | `38.50% / -12.65%` | `43.04% / -8.14%` | `191` |

Best low-damage candidates with any drawdown improvement:

| candidate | Return delta pp | DD improve pp | Interpretation |
|---|---:|---:|---|
| `zz2000_cyb_below_ma63_days19_scale0p75` | `-0.45/-0.27/-0.31/-0.84/-0.65` | `+0.09/+0.09/0/0/0` | Lowest-damage useful-looking row, but improvement is only full/10Y and too small. |
| `zz2000_cyb_below_ma57_days19_scale0p75` | `-0.37/-0.19/-0.39/-0.98/-0.65` | `+0.09/+0.09/0/0/0` | Same pattern, no recent-window DD benefit. |
| `zz2000_cyb_below_ma57_days17_scale0p75` | `-0.35/-0.17/-0.33/-0.95/-1.01` | `+0.09/+0.09/0/0/0` | Ranking-score winner among candidates, still not promotable. |

Mechanical DD check:

- All-window DD-positive candidate count: `0`.
- Recent-window (`10Y/5Y/3Y/1Y`) DD-positive candidate count: `0`.
- Largest total DD improvement comes from full cash scale `0.0` rows, but those only improve full/10Y by about `2.08pp` each and cost roughly `10pp` to `18pp` annualized in recent windows, so they are not acceptable.

Decision:

- `reject_broad_volume_amount_no_all_window_dd_improvement`.
- Stability label: `weak_broad_volume_amount_not_promote`.
- Unit test: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Interpretation:

- This finishes the practical volume family on the current branch: internal turnover proxy failed, and external broad amount also failed.
- The broad amount regime can slightly reduce the old full/10Y drawdown pocket, but it does nothing for 5Y/3Y/1Y max drawdown unless the scale is made so harsh that returns are unacceptable.
- Do not spend more scan budget on this exact volume/amount formulation for promotion.

Suggested next layer:

- Move from candidate overlays to drawdown-pocket attribution before adding another rule.
- First locate the max-drawdown peak/trough dates for the current baseline in full/10Y/5Y/3Y/1Y, then inspect what common observable states exist around those pockets.
- If continuing with a new overlay, prioritize an event-specific local crash-day protective rule derived from those pockets, not another broad global exit or generic volume rule.

## Layer Update - Drawdown Pocket Attribution

Added after the broad-volume amount layer.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_drawdown_pocket_attribution`

Script:

- `scripts/analyze_microcap_v2_5_abs120_tv40_max1p0_drawdown_pockets.py`

Layer type:

- Attribution only, not an overlay promotion scan.
- Baseline: `abs120_gtm0p25_tv40_max1p0_score_gtp0`.
- Purpose: find the actual max-drawdown peak/trough pockets before inventing another rule.
- Real baseline range: `2010-07-09` to `2026-05-27`, rows `3838`.
- Amount context was read from cached `amount_factors.csv`; no live amount refresh was needed for attribution.

Pocket summary:

| window | peak | trough | max DD | trading days | pocket return | avg scale | worst day | worst day return |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| full | `2017-09-11` | `2018-10-29` | `-23.28%` | `275` | `-22.35%` | `0.48` | `2018-10-11` | `-8.37%` |
| 10Y | `2017-09-11` | `2018-10-29` | `-23.28%` | `275` | `-22.35%` | `0.48` | `2018-10-11` | `-8.37%` |
| 5Y | `2024-10-08` | `2024-10-11` | `-12.65%` | `4` | `-4.32%` | `1.00` | `2024-10-09` | `-10.34%` |
| 3Y | `2024-10-08` | `2024-10-11` | `-12.65%` | `4` | `-4.32%` | `1.00` | `2024-10-09` | `-10.34%` |
| 1Y | `2026-05-11` | `2026-05-21` | `-8.14%` | `9` | `-7.22%` | `1.00` | `2026-05-21` | `-4.00%` |

Observed context:

- Full/10Y pocket is a long 2017-2018 bear segment. Existing global exits and broad amount rules can reduce parts of it, but they do not solve 5Y/3Y/1Y.
- 5Y/3Y pocket is concentrated around `2024-10-08` to `2024-10-11`: after a high-bias surge, the strategy stayed at full scale and took the `2024-10-09` crash day.
- On `2024-10-09`, `return_net=-10.34%`, `microcap_ret=-10.34%`, `current_execution_scale=1.00`, `target_vol_realized_vol=44.80%`, `target_vol_scale_raw=0.893`, `long_only_bias_momentum=172.26`, `abs_momentum=10.79%`, `ma_bias_40=20.61%`.
- 1Y pocket is also full-scale but not a pure volume event: worst day `2026-05-21`, `return_net=-4.00%`, `target_vol_realized_vol=27.38%`, `long_only_bias_momentum=-8.64`, `abs_momentum=17.27%`, `ma_bias_40=-0.40%`.

Decision:

- `attribution_only_no_overlay_promotion`.
- No parameter was promoted.
- Outputs written: `drawdown_pockets.csv`, `drawdown_pocket_worst_days.csv`, `scan_meta.json`, `record.md`, `command_log.txt`.

Interpretation:

- The remaining drawdowns are not one homogeneous regime.
- The next promotable overlay should target the recent short-crash pockets first, because all prior global/long-regime layers mostly helped only full/10Y.
- The cleanest next scan is a post-surge cooldown: after an unusually large positive microcap day under high MA-bias/high score, reduce scale for the next `1/2/3` sessions.

Suggested next scan:

- Name: `post_surge_cooldown_derisk`.
- Trigger candidates: prior-day `microcap_ret >= 6%/8%/10%`, optionally with `ma_bias_40 >= 15%/20%/25%` or `long_only_bias_momentum >= 100/150`.
- Action candidates: next `1/2/3` sessions at scale `0.50/0.75`, repriced through the same target-vol cost model.
- Promotion hurdle: must improve 5Y/3Y max DD materially without damaging 10Y/5Y/3Y/1Y annualized return by more than roughly `1pp`; full/10Y should not deteriorate materially.

## Layer Update - Post-Surge Cooldown Derisk

Added after drawdown-pocket attribution.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_post_surge_cooldown_derisk`

Script and test:

- `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_post_surge_cooldown_derisk_scan.py`
- `test_post_surge_cooldown_derisk.py`

Layer tested:

- Event-specific cooldown after unusually strong microcap up-days.
- Trigger uses close-confirmed `microcap_ret >= threshold`, optionally gated by `ma_bias_40` and `long_only_bias_momentum`.
- Trigger day itself is not derisked; only the next `1/2/3` trading sessions are scaled down.
- Return threshold grid: `4%/6%/8%/10%`.
- MA-bias minimum grid: `any/15%/20%/25%/30%`.
- Score minimum grid: `any/100/150`.
- Cooldown length grid: `1/2/3` sessions.
- Scale grid: `0.50/0.75`.
- Repricing uses the same v2.5 cost model: base pre-cost return, target-vol scale-change cost, financing, idle-cash yield, and scaled base trade cost.

Baseline:

- `no_post_surge_cooldown`: full `28.70% / -23.28%`; 10Y `21.20% / -23.28%`; 5Y `33.99% / -12.65%`; 3Y `39.34% / -12.65%`; 1Y `43.69% / -8.14%`.

Top ranking representative:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | cooldown days |
|---|---:|---:|---:|---:|---:|---:|
| `surge08_bias30_score150_d3_scale050` | `29.25% / -23.28%` | `22.03% / -23.28%` | `35.84% / -11.68%` | `42.56% / -11.12%` | `43.69% / -8.14%` | `3` |

Delta versus baseline:

| candidate | Return delta pp | DD improve pp | Interpretation |
|---|---:|---:|---|
| `surge08_bias30_score150_d3_scale050` | `+0.56/+0.83/+1.85/+3.22/+0.00` | `0/0/+0.98/+1.53/0` | Very targeted; captures the 2024 post-surge crash pocket, does not address full/10Y or 1Y max DD. |
| `surge04_bias25_scoreany_d2_scale050` | `+0.14/+0.74/+1.64/+2.86/+0.00` | `0/0/+0.98/+1.53/0` | Broader trigger, same 5Y/3Y DD effect, more cooldown days. |
| `surge08_biasany_scoreany_d3_scale075` | `+0.10/+0.15/+0.34/+0.59/+0.00` | `0/0/+0.98/+1.53/0` | Gentler scale, keeps same DD improvement but smaller return benefit. |

Decision:

- `watchlist_post_surge_cooldown_improves_5y_3y_short_crash_not_all_windows`.
- Stability label: `watchlist_event_specific_recent_crash_protection`.
- Unit test: PASS.
- Scan run: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Interpretation:

- This is the first layer in this sequence with a useful positive result.
- It materially improves the 5Y/3Y short-crash drawdown pocket and also improves returns in those windows.
- It does not improve full/10Y max drawdown because that is the 2017-2018 long bear pocket.
- It does not improve 1Y max drawdown because the 2026-05 pocket was not a post-surge high-bias crash.
- Do not promote directly yet: it is event-specific and only fires a few days in the best representative row.

Suggested next scan:

- Target the remaining 1Y pocket separately.
- Candidate family: momentum-collapse cooldown or failed-breakdown derisk.
- Starting idea: when `long_only_bias_momentum` turns negative while still long and `abs_momentum` remains positive, scale down for `1/2/3` sessions, optionally gated by `ma_bias_40 <= 0%/5%` or recent multi-day loss.
- Goal: improve the 2026-05 drawdown without giving back the post-surge layer's 5Y/3Y improvement.

## Comparison - Official v2.5 vs Current Research Branch

Added after the post-surge cooldown layer.

Official v2.5 source:

- `outputs/microcap_top100_mom16_biweekly_live_v2_5_nav.csv`
- Costed `return_net` / `nav_net`.
- Full official range: `2010-05-05` to `2026-05-27`.

Research branch source:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_post_surge_cooldown_derisk/window_metrics.csv`
- Full research range: `2010-07-09` to `2026-05-27`.

Metrics:

| strategy | Full ann / DD | 10Y | 5Y | 3Y | 1Y |
|---|---:|---:|---:|---:|---:|
| Official v2.5 costed | `36.61% / -19.48%` | `30.54% / -16.22%` | `35.36% / -13.76%` | `40.73% / -13.76%` | `58.05% / -13.76%` |
| Current research baseline `ABS120+TV40 max1.0` | `28.70% / -23.28%` | `21.20% / -23.28%` | `33.99% / -12.65%` | `39.34% / -12.65%` | `43.69% / -8.14%` |
| Post-surge cooldown representative `surge08_bias30_score150_d3_scale050` | `29.25% / -23.28%` | `22.03% / -23.28%` | `35.84% / -11.68%` | `42.56% / -11.12%` | `43.69% / -8.14%` |

Delta vs official v2.5:

| strategy | Full | 10Y | 5Y | 3Y | 1Y |
|---|---:|---:|---:|---:|---:|
| Current research baseline, ann delta | `-7.91pp` | `-9.34pp` | `-1.37pp` | `-1.38pp` | `-14.36pp` |
| Current research baseline, DD improvement | `-3.80pp` | `-7.05pp` | `+1.11pp` | `+1.11pp` | `+5.63pp` |
| Post-surge representative, ann delta | `-7.36pp` | `-8.51pp` | `+0.48pp` | `+1.83pp` | `-14.36pp` |
| Post-surge representative, DD improvement | `-3.80pp` | `-7.05pp` | `+2.09pp` | `+2.64pp` | `+5.63pp` |

Interpretation:

- Official v2.5 is still much stronger on full/10Y and especially 1Y annualized return.
- The current research branch is only competitive in recent drawdown control.
- The post-surge cooldown layer makes the branch beat official v2.5 on 5Y/3Y drawdown and slightly on 5Y/3Y annualized return, but it still loses badly on full/10Y and 1Y annualized return.

## Layer Update - Momentum-Stall Cooldown Derisk

Added after the official v2.5 comparison.

Run folder:

- `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_momentum_stall_cooldown_derisk`

Script and test:

- `scripts/run_microcap_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_momentum_stall_cooldown_derisk_scan.py`
- `test_momentum_stall_cooldown_derisk.py`

Layer tested:

- Event-specific cooldown after fast momentum-score deterioration while ABS momentum remains positive.
- Trigger uses close-confirmed `long_only_bias_momentum <= score_max`, score drop over `3/5` sessions, `abs_momentum >= threshold`, optional `ma_bias_40` cap, and optional recent-loss gate.
- Trigger day itself is not derisked; only following `1/2/3` sessions are scaled down.
- Score max grid: `50/30/15/0`.
- Score-drop lookback grid: `3/5`; drop minimum grid: `20/30/40/50`.
- ABS momentum minimum grid: `0/10%/15%`.
- MA-bias max grid: `any/10%/5%/0%`.
- Recent-loss gates: `none`, `3D <= 0`, `3D <= -1%`, `5D <= -2%`.
- Cooldown length grid: `1/2/3`; scale grid: `0.50/0.75`.

Baseline:

- `no_momentum_stall_cooldown`: full `28.70% / -23.28%`; 10Y `21.20% / -23.28%`; 5Y `33.99% / -12.65%`; 3Y `39.34% / -12.65%`; 1Y `43.69% / -8.14%`.

Best representative:

| candidate | Full ann / DD | 10Y | 5Y | 3Y | 1Y | cooldown days |
|---|---:|---:|---:|---:|---:|---:|
| `scorele30_drop5_20_abs15_ma010_r3_000_d1_scale050` | `29.22% / -23.28%` | `21.80% / -23.28%` | `34.35% / -12.65%` | `40.55% / -12.65%` | `48.29% / -6.25%` | `252` |

Delta versus baseline:

| candidate | Return delta pp | DD improve pp | Interpretation |
|---|---:|---:|---|
| `scorele30_drop5_20_abs15_ma010_r3_000_d1_scale050` | `+0.53/+0.60/+0.35/+1.21/+4.60` | `0/0/0/0/+1.88` | Cleanly targets the 2026-05 pocket; no full/10Y/5Y/3Y DD effect, but return is non-worse across all windows. |

Decision:

- `watchlist_momentum_stall_cooldown_improves_1y_pocket_not_full_10y`.
- Stability label: `watchlist_event_specific_2026_05_protection`.
- Unit test: PASS.
- Scan run: PASS.
- Formal finalize: PASS.
- Strict artifact check: PASS.

Interpretation:

- This is the second useful event-specific layer.
- It solves the remaining 1Y pocket much better than global exits or volume rules.
- It does not address the old 2017-2018 full/10Y long-drawdown pocket.
- It should be treated as a watchlist component and then evaluated in combination with post-surge cooldown.

Combination quick check:

- Combined layers:
  - Post-surge representative: `surge08_bias30_score150_d3_scale050`.
  - Momentum-stall representative: `scorele30_drop5_20_abs15_ma010_r3_000_d1_scale050`.
- Output file: `quant_param_scan_runs/20260528_microcap_top100_v2_5_bias_ma40_mom20_exp_h5_abs120_tv40_max1p0_momentum_stall_cooldown_derisk/watchlist_combo_metrics.csv`.
- Post-surge execution days: `3`.
- Momentum-stall execution days: `252`.
- Overlap days: `0`.

| strategy | Full ann / DD | 10Y | 5Y | 3Y | 1Y |
|---|---:|---:|---:|---:|---:|
| Baseline | `28.70% / -23.28%` | `21.20% / -23.28%` | `33.99% / -12.65%` | `39.34% / -12.65%` | `43.69% / -8.14%` |
| Post-surge only | `29.25% / -23.28%` | `22.03% / -23.28%` | `35.84% / -11.68%` | `42.56% / -11.12%` | `43.69% / -8.14%` |
| Momentum-stall only | `29.22% / -23.28%` | `21.80% / -23.28%` | `34.35% / -12.65%` | `40.55% / -12.65%` | `48.29% / -6.25%` |
| Post-surge + momentum-stall | `29.78% / -23.28%` | `22.64% / -23.28%` | `36.20% / -11.68%` | `43.80% / -11.12%` | `48.29% / -6.25%` |

Combination interpretation:

- The two event-specific layers are complementary in this run.
- The combination preserves the post-surge 5Y/3Y drawdown improvement and adds the momentum-stall 1Y drawdown improvement.
- Still no improvement to full/10Y max drawdown; that remains the 2017-2018 long-bear pocket.

Suggested next scan:

- Stop adding more local event layers until deciding whether the full/10Y long-bear pocket matters for this branch.
- If it matters, next scan should be explicitly long-regime only, not another short cooldown:
  - candidate family: slow bear-regime scale cap using `abs_momentum < 0`, `score below 0`, or sustained negative MA-bias;
  - must avoid destroying the now-improved 5Y/3Y/1Y windows.
