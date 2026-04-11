# ADK Pair Retention Prototype

## Date
2026-03-27

## Why This Test
ADK structural trading is dominated by pair switching, not by direction flips.

Baseline facts on the common sample:
- Structural trade days: `616`
- Pair change days: `610`
- Direction change days: `172`
- Median pair spell: `4` trading days

So the most natural way to reduce ADK structural trading is to add pair-retention hysteresis:
- keep the current pair unless a challenger beats it by a clear margin

## Tested Mechanism
Retention threshold on shifted pair signal strength:
- only switch if `challenger_score > current_score * threshold`

Scenarios tested:
- `1.02x`
- `1.05x`
- `1.10x`
- `1.20x`

## Baseline
- ADK annual return: `29.15%`
- ADK Sharpe: `1.43`
- ADK max drawdown: `-34.68%`
- Combined annual return: `19.43%`
- Combined Sharpe: `1.72`
- Combined max drawdown: `-10.82%`

## Results

### `1.02x`
- ADK structural trade days/year: `37.87`
- down from `40.22`
- Combined annual return: `19.00%`
- Combined Sharpe: `1.70`

### `1.05x`
- ADK structural trade days/year: `34.47`
- down from `40.22`
- Combined annual return: `19.01%`
- Combined Sharpe: `1.70`
- Combined max drawdown: `-10.54%`

### `1.10x`
- ADK structural trade days/year: `32.06`
- Combined annual return: `18.05%`
- Combined Sharpe: `1.63`

### `1.20x`
- ADK structural trade days/year: `29.38`
- Combined annual return: `17.59%`
- Combined Sharpe: `1.57`

## Takeaway
- Yes, ADK structural trading can be reduced.
- But a simple pair-retention threshold costs too much alpha.
- The mild version `1.05x` is the least bad:
  - structural trading down about `14%`
  - Combined performance only slightly worse
- Stronger thresholds are not attractive.

## Current Conclusion
- If the goal is only "cut ADK structural trading a bit without breaking the strategy", `1.05x` is a reasonable research backup.
- If the goal is "materially reduce ADK trading while preserving returns", this simple retention rule is not strong enough yet.
