# Peak-Decay / Recovery Close-Execution Timing Issue

Date: 2026-05-17
Workspace: Top100 microcap hedged strategy
Status: issue document for cross-strategy self-check

## Summary

The current peak-decay derisk layer can apply a close-confirmed signal-quality
change to the same day's return stream. Under a close-signal / close-execution
assumption, this is a timing error.

If the decay or recovery condition is known only from the current day's close,
then the trade can only be executed at that close. The current day should still
use the exposure that was in force before that close. The new exposure should
affect the next trading day's return.

The affected behavior is not only a display problem. It changes `return_net` and
`nav_net` when the peak-decay layer is active, and target-vol overlays then
compound the affected return stream.

## Concrete Example

Current v2.4 output around the latest case:

Source CSV:
`outputs/microcap_top100_mom16_power_p0p75_lb20_signal1p0_exec0p8_gap18_decay35_recovery40_targetvol20_scale010_v2_4_costed_nav.csv`

| date | observed state | key fields | current result |
| --- | --- | --- | --- |
| 2026-05-07 | peak-decay triggered | `signal_quality_derisk_triggered=True`, `gap_decay_ratio=0.320058`, `base_pre_cost_return=0.0`, `signal_quality_scale_cost=0.003` | The day's negative spread return is avoided and only the sell/derisk cost is charged. |
| 2026-05-08 | recovery triggered | `signal_quality_scale_turnover=1.0`, `gap_decay_ratio=0.496312`, `base_pre_cost_return=0.009758`, `signal_quality_scale_cost=0.003` | The strategy captures the 2026-05-08 daily return while also charging the buy/recovery cost. |

Why this is wrong under close execution:

- On 2026-05-07, the decay condition is known at the close. The strategy should
  still have been exposed during 2026-05-07, then sell at the close.
- On 2026-05-08, the recovery condition is known at the close. The strategy
  should not capture the 2026-05-08 daily return from a buy executed at that
  same close. It should mainly show the entry cost, and the restored exposure
  should begin from the next trading day.

## Root Cause

The shared peak-decay function makes the signal-quality decision from the
current row, then immediately applies that decision to the current row's return.

Relevant code path:

- `microcap_top100_mom16_biweekly_live_v2_0.py:5568`
  defines `apply_momentum_gap_peak_decay_derisk(...)`.
- `microcap_top100_mom16_biweekly_live_v2_0.py:5705`
  calculates `realized_daily_return = gross_daily_return * applied_scale`.
- `microcap_top100_mom16_biweekly_live_v2_0.py:5731`
  writes `next_holding` from `desired_next_active`, not from the derisk scale.

This means a same-day close-derived `applied_scale` changes the same day's PnL.
When `DERISK_SCALE = 0.0`, the code behaves like an intraday-known derisk
signal rather than a close-executed sell.

The target-vol layer then uses the already affected base return:

- `microcap_top100_mom16_biweekly_live_v2_0.py:10239`
  defines `apply_target_vol_scaling(...)`.
- `microcap_top100_mom16_biweekly_live_v2_0.py:10306`
  applies `base_pre_cost_return * execution_scale`.

## Affected Versions Found So Far

Affected because they call `apply_momentum_gap_peak_decay_derisk(...)`:

| version | affected | evidence |
| --- | --- | --- |
| v1.6 | yes | `microcap_top100_mom16_biweekly_live_v1_6.py:898` calls `v14_context.v1_1_mod.base_mod.apply_momentum_gap_peak_decay_derisk(...)`. |
| v2.0 | yes | `microcap_top100_mom16_biweekly_live_v2_0.py:10638` calls the shared function for the embedded lineage. |
| v2.1 | no for this specific layer | v2.1 explicitly excludes `momentum_gap_peak_decay_derisk`. |
| v2.2 | yes | `microcap_top100_mom16_biweekly_live_v2_2.py:259` and `:544` call the shared function. |
| v2.3 | yes | `microcap_top100_mom16_biweekly_live_v2_3.py:265` and `:558` call the shared function. |
| v2.4 | yes | `microcap_top100_mom16_biweekly_live_v2_4.py:246` and `:539` call the shared function. |

Other sub-strategies should self-check if they implement any similar close
signal that immediately changes the same day's return.

## Self-Check Checklist For Other Strategies

Search for all close-confirmed overlay decisions that are applied to the same
row's return:

1. A condition uses current-day close data, current-day momentum, current-day
   drawdown, current-day score, or current-day NAV.
2. The code immediately changes `holding`, `next_holding`, `execution_scale`,
   `applied_scale`, `weight`, or exposure for the same row.
3. The same row's `return`, `return_net`, `nav`, or `nav_net` is then calculated
   using that changed exposure.

Red flags:

- `triggered` or `recovery` is calculated from row `t`, and row `t` return is
  multiplied by the new scale.
- A close-triggered sell avoids row `t` negative return.
- A close-triggered buy captures row `t` positive return.
- `DERISK_SCALE = 0.0` appears in PnL, but `next_holding` still displays long.
- Entry/recovery cost is charged on row `t`, while row `t` also captures the
  newly restored exposure's return.

## Correct Close-Execution Semantics

For close-confirmed overlays:

- Row `t` signal is decided at the close of row `t`.
- Row `t` return should use the exposure decided at the close of row `t-1`.
- Row `t` close signal changes the target exposure for row `t+1`.
- Trading cost for the close trade may be charged on row `t`, but the new
  exposure should not receive row `t` intraday return.

For `DERISK_SCALE = 0.0`, the user-facing interpretation should be equivalent to
clear exposure / cash from the next executable period. If the implementation
keeps a separate signal-quality scale, reports must display the final effective
exposure:

`final_effective_exposure = target_vol_execution_scale * signal_quality_scale`

not only the target-vol scale.

## Recommended Fix Direction

Do not patch only display labels. The PnL path must be corrected first.

Recommended implementation approach:

1. Split decision state from execution state.
   - `signal_quality_scale_current`: exposure used for current row return.
   - `signal_quality_scale_next`: exposure to use from next row.
2. Evaluate decay/recovery using row `t` close data, but store it as the next
   row's execution state.
3. Apply row `t` return using the previously active state.
4. Charge close-trade costs on row `t` if that is the chosen accounting
   convention, but do not apply the new exposure to row `t` return.
5. Make `next_holding`, displayed scale, and operation lists reflect final
   effective exposure, not only the raw target-vol scale.

Minimum regression tests:

- A two-day fixture where day 1 close triggers derisk after a negative return:
  day 1 must still include that negative return before the close sell cost.
- A two-day fixture where day 2 close triggers recovery after a positive return:
  day 2 must not capture that positive return from the recovered exposure.
- A `DERISK_SCALE = 0.0` fixture must show final effective exposure as zero
  after the close-triggered derisk decision.
- Existing target-vol cost sanity checks must still pass after using the
  corrected base return stream.

## Commands Used For This Issue Note

```powershell
rg -n "apply_momentum_gap_peak_decay_derisk|realized_daily_return = gross_daily_return \* applied_scale|executed_next_holding.append|def apply_target_vol_scaling|base_pre_cost_return \* execution_scale" microcap_top100_mom16_biweekly_live_v1_6.py microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_2.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_4.py
```

```powershell
python - <<'PY'
import pandas as pd
p = "outputs/microcap_top100_mom16_power_p0p75_lb20_signal1p0_exec0p8_gap18_decay35_recovery40_targetvol20_scale010_v2_4_costed_nav.csv"
df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
print(df.loc[["2026-05-07", "2026-05-08"], [
    "holding", "next_holding", "active_spread_ret", "return_raw",
    "base_pre_cost_return", "execution_scale",
    "signal_quality_derisk_triggered", "signal_quality_scale_turnover",
    "signal_quality_scale_cost", "base_trade_cost_scaled",
    "return_net", "nav_net", "gap_decay_ratio",
]])
PY
```
