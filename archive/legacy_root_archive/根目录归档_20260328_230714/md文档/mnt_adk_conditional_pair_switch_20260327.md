# ADK Conditional Pair Switch

## Date
2026-03-27

## Definition
Start from mild retention:
- keep current pair unless challenger beats it by `1.05x`

Add a forced switch rule:
- if current pair rank falls below a cutoff, switch immediately

Tested cutoffs:
- `rank > 2`
- `rank > 3`

## Baseline
- Structural trade days/year: `40.22`
- Combined annual return: `19.43%`
- Combined Sharpe: `1.72`

## Reference: simple retention `1.05x`
- Structural trade days/year: `34.47`
- Combined annual return: `19.01%`
- Combined Sharpe: `1.70`
- Combined max drawdown: `-10.54%`

## Conditional Results

### `rank > 2` force-switch
- Structural trade days/year: `35.45`
- Combined annual return: `19.10%`
- Combined Sharpe: `1.69`
- Combined max drawdown: `-10.68%`

### `rank > 3` force-switch
- Structural trade days/year: `34.80`
- Combined annual return: `18.88%`
- Combined Sharpe: `1.69`
- Combined max drawdown: `-10.54%`

## Conclusion
- This is not a clear improvement over simple `1.05x` retention.
- `rank > 2` keeps a little more return, but gives back some trading reduction and worsens drawdown slightly.
- `rank > 3` is basically not better than the simple retention version.
- So the best ADK trading-reduction candidate remains the simple mild retention rule, not this conditional variant.
