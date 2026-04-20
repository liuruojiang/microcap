# v1.1 Momentum Gap Peak Decay Exit Design

**Goal**

Add a research overlay on top of the repaired `v1.1` baseline that exits an active trade when the trade's `momentum_gap` has decayed enough relative to that trade's own peak `momentum_gap`, while preserving the existing close-execution and costed-net framework.

**Scope**

- Baseline is `v1.1` only.
- This is a research overlay, not a new default production version yet.
- The first pass studies full exits only. No partial de-risking and no new reentry shortcuts.
- The first pass scans a decay ratio grid instead of choosing a single live threshold.

## Strategy Definition

### Baseline

- Use the repaired `v1.1` strategy as the signal source.
- Keep existing entry logic unchanged.
- Keep the existing stock-basket cost model and futures drag unchanged.
- Keep day-frequency close execution unchanged.

### Signal-Quality Exit

For each active trade:

1. Track `gap_peak`, defined as the highest observed `momentum_gap` since that trade became active.
2. On each day while the executed trade is active and the base signal still wants to remain active, compute:

`gap_decay_ratio = current_momentum_gap / gap_peak`

3. If `gap_decay_ratio <= decay_ratio_threshold`, treat this as signal-quality deterioration and force an exit at that day's close.

### Reset Rule

After a signal-quality exit:

- block the old signal;
- require the base signal to first pass through one `cash` period;
- only then allow the next new base `on` signal to enter.

This matches the stricter reset rule already used in prior overlay studies and avoids silent same-signal reentry.

## Execution Semantics

### Return Stream

The overlay must operate on the real executed holding state, not the base gross path.

Required invariant:

- once an overlay has exited early and the executed state is `cash`, subsequent blocked cash days must have `return_net = 0` except for explicit same-day exit costs;
- blocked cash days must not inherit the base strategy's `gross return`.

This invariant is mandatory because a recent bug in the execution helpers falsely allowed blocked cash days to continue collecting base returns, which invalidated earlier overlay conclusions.

### Costs

- Entry cost: unchanged existing one-side entry cost.
- Exit cost: unchanged existing one-side exit cost.
- Rebalance cost: unchanged existing rebalance turnover cost, but only while the executed trade remains active.
- If a signal-quality exit fires, the exit is booked at the same close and the day carries the exit cost.

### Trigger Timing

- Observe `momentum_gap` at the daily close.
- If the deterioration condition is met, exit at that same daily close in the backtest.
- No intraday or pre-close proxy logic is included in this version.

## Parameters

### Studied Parameter

`decay_ratio_threshold`

Interpretation:

- `0.2` means exit only after `momentum_gap` has decayed to 20 percent of its trade peak.
- `0.8` means exit after `momentum_gap` has decayed to 80 percent of its trade peak, which is a much tighter rule.

### First Scan

Run the first scan on:

- `0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8`

No extra positive-floor filter is included in the first pass. If results show that exits occur too late when `momentum_gap` is already near zero, a second-pass design may add a minimum positive gap floor.

## Code Placement

### Main Implementation

Modify:

- `microcap_top100_mom16_biweekly_live.py`

Add a new execution helper similar in style to the existing repaired overlay helpers. Recommended function name:

- `apply_momentum_gap_peak_decay_exit(...)`

Expected inputs:

- `gross_result`
- `turnover_df`
- `decay_ratio_threshold`
- optionally a switch for `require_signal_reset_after_exit`, defaulting to `True`

Expected output columns:

- `gap_peak`
- `gap_decay_ratio`
- `signal_quality_exit_triggered`
- `blocked_until_signal_reset`
- `signal_reset_seen`
- `trade_id`
- `trade_return_net`
- `entry_exit_cost`
- `rebalance_cost`
- `total_cost`
- `return_net`
- `nav_net`

### Tests

Modify:

- `test_top100_forced_stop_loss.py`

Add unit coverage for:

- trade-level peak `momentum_gap` tracking;
- exit when `current_gap / gap_peak` crosses the threshold;
- no exit when the ratio stays above the threshold;
- reset gating after a signal-quality exit;
- blocked cash days having `return_net = 0`.

## Research Outputs

### Primary Comparison

Compare each scanned overlay directly against repaired `v1.1 baseline` on the same refreshed data slice.

Report at minimum:

- full sample;
- 1 year;
- 3 year;
- 5 year;
- 10 year.

For each window report:

- annual return;
- max drawdown;
- total return;
- exit count.

### Output Artifacts

Write study outputs under `outputs/` as disposable research artifacts.

Recommended filenames:

- `outputs/microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected.csv`
- `outputs/microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected_windows.csv`

If a final point estimate is selected for a versioned strategy, that can be promoted into a dedicated version only after the research result is judged robust.

## Validation Requirements

### Unit Validation

Run:

- `python -m unittest .\test_top100_forced_stop_loss.py -v`

The new tests must fail before implementation and pass after implementation.

### Baseline Safety Validation

Also rerun:

- `python -m unittest .\test_top100_v1_1_output_compatibility.py .\test_top100_hedge_ratio_application.py -v`

This confirms the repaired `v1.1` baseline and the hedge-ratio path remain intact.

### Real-Data Validation

Before interpreting any results:

1. refresh the selected strategy data to the latest trading date;
2. rerun the repaired `v1.1` baseline;
3. run the decay-ratio scan on the refreshed real data;
4. compare apples-to-apples against the refreshed `v1.1 baseline`.

## Non-Goals

- No intraday signal-quality exit.
- No partial de-risking or multi-stage exits.
- No reentry based on renewed `momentum_gap` strength in this first pass.
- No direct integration into `v1.3` or another production version yet.

## Decision Rule After Study

Only promote this overlay beyond research if all of the following hold on the corrected execution path:

- it beats repaired `v1.1 baseline` on annual return or materially improves drawdown with acceptable return sacrifice;
- the result is not driven by one tiny threshold region only;
- the result survives window checks;
- the execution-layer invariants remain clean.
