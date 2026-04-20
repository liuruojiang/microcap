# v1.1 Momentum Gap Peak Decay Derisk Design

**Goal**

Add a research overlay on top of the repaired `v1.1` baseline that reduces gross exposure, instead of exiting to cash, when trade-level `momentum_gap` has decayed enough relative to that trade's own peak `momentum_gap`.

**Scope**

- Baseline is `v1.1` only.
- This is a research overlay, not a new production version yet.
- The first pass studies one-stage derisking only.
- The first pass scans both decay thresholds and derisk scales.

## Strategy Definition

### Baseline

- Use repaired `v1.1` as the base signal engine.
- Keep entry logic unchanged.
- Keep the existing stock-basket cost model and futures drag framework unchanged.
- Keep day-frequency close execution unchanged.

### Signal-Quality Derisk Rule

For each active trade:

1. Track `gap_peak`, defined as the highest observed `momentum_gap` since that trade became active.
2. Compute `gap_decay_ratio = current_momentum_gap / gap_peak` whenever the executed trade is active and `gap_peak > 0`.
3. If `gap_decay_ratio <= decay_ratio_threshold`, do not force exit to cash. Instead, switch that trade into a lower-risk state.

### Derisk State

- Base active state: `1.0x` execution scale on the `v1.1` gross return stream.
- Derisked state: `derisk_scale` execution scale on the same gross return stream.

For example:

- base `v1.1` is long microcap and short CSI1000 at the normal `v1.1` notional;
- under derisk, both the long stock basket and hedge leg are reduced proportionally to `30%`, `50%`, or `70%` of their usual active size.

This preserves the base strategy structure while expressing less conviction when signal quality deteriorates.

### Reset Rule

The first pass uses a simple one-way rule inside each trade:

- a trade starts at full scale;
- once the decay rule is hit, that trade remains at the chosen derisk scale until the base signal exits to `cash`;
- no same-trade recovery back to full scale is included in this version.

This keeps the first pass clean and avoids adding another re-escalation state machine.

## Execution Semantics

### Return Stream

The overlay must operate on the real executed state, not the base gross path alone.

Required invariants:

- if executed exposure scale is `0`, realized return must be `0` except explicit exit costs;
- if executed exposure scale is `derisk_scale`, realized spread return must equal `base gross return * derisk_scale`;
- blocked or inactive periods must not inherit the base strategy's full gross return.

### Costs

- Entry and exit costs keep the existing one-side stock cost model.
- Rebalance costs apply only while the trade remains active.
- When the overlay shifts from full scale to derisk scale, the first pass does not introduce an extra bespoke scale-switch cost. The purpose of the first pass is to test directional value before adding another friction layer.

## Parameters

### Decay Threshold Scan

Scan:

- `0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8`

Interpretation:

- `0.2` means derisk only after `momentum_gap` has decayed to 20 percent of its trade peak.
- `0.8` means derisk after only a modest decay from the peak.

### Derisk Scale Scan

Scan:

- `0.3, 0.5, 0.7`

Interpretation:

- `0.3` means keep only 30 percent of normal active exposure after derisking.
- `0.5` means keep half exposure.
- `0.7` means keep 70 percent of normal exposure.

## Code Placement

Modify:

- `microcap_top100_mom16_biweekly_live.py`
- `test_top100_forced_stop_loss.py`

Recommended new helper:

- `apply_momentum_gap_peak_decay_derisk(...)`

Suggested inputs:

- `gross_result`
- `turnover_df`
- `decay_ratio_threshold`
- `derisk_scale`

Suggested output columns:

- `gap_peak`
- `gap_decay_ratio`
- `signal_quality_derisk_triggered`
- `execution_scale`
- `trade_id`
- `trade_return_net`
- `entry_exit_cost`
- `rebalance_cost`
- `total_cost`
- `return_net`
- `nav_net`

## Research Outputs

Write disposable research artifacts under `outputs/`:

- `outputs/microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected.csv`
- `outputs/microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected_windows.csv`

Each result row should include:

- decay threshold;
- derisk scale;
- annual return;
- max drawdown;
- total return;
- derisk trigger count.

## Validation Requirements

### Unit Validation

Run:

- `python -m unittest .\test_top100_forced_stop_loss.py -v`

Add explicit tests for:

- scale switches from `1.0` to `derisk_scale`;
- derisked days using scaled returns, not full returns;
- trade-level peak tracking;
- no cash-day ghost returns.

### Baseline Safety Validation

Run:

- `python -m unittest .\test_top100_v1_1_output_compatibility.py .\test_top100_hedge_ratio_application.py -v`

### Real-Data Validation

Before reading conclusions:

1. refresh repaired `v1.1`;
2. rerun the gross path and turnover table;
3. run the threshold-scale grid on real local data;
4. compare against the refreshed repaired `v1.1 baseline`.

## Non-Goals

- No intraday logic.
- No re-escalation back to full scale inside the same trade.
- No separate hedge-leg-only adjustment in this version.
- No production promotion unless the corrected research result is clearly useful.
