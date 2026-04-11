# Combined Drawdown Throttle Prototype

## Date
2026-03-27

## Goal
Test whether the portfolio should add a top-level risk contraction layer after combined drawdown, instead of relying only on sleeve-level vol scaling.

Current prototype keeps all sleeve logic unchanged and only applies a portfolio-level scale multiplier to combined daily returns.

## Important Assumptions
- Uses current baseline main-strategy setup with local frozen data
- Uses current Sub-B baseline, not the aggressive candidate
- When portfolio scale is below `1.0`, the trimmed portion is treated as flat `0` return
- Includes a small portfolio-level re-risk / de-risk trading cost of `2 bps` per unit of scale change

## Baseline
- Annual return: `19.43%`
- Volatility: `10.48%`
- Sharpe: `1.85`
- Max drawdown: `-10.82%`

## Tested Scenarios

### `mild_7_10`
- Trigger to `0.85x` risk at `7%` drawdown
- Trigger to `0.70x` risk at `10%` drawdown
- Recover to full risk once drawdown is back inside `4%`
- Step-up speed: `+0.01` per day

Result:
- Annual return: `18.78%`
- Sharpe: `1.83`
- Max drawdown: `-10.27%`

### `balanced_6_9`
- Trigger to `0.80x` risk at `6%` drawdown
- Trigger to `0.60x` risk at `9%` drawdown
- Recover threshold: `3%`
- Step-up speed: `+0.01` per day

Result:
- Annual return: `18.21%`
- Sharpe: `1.82`
- Max drawdown: `-9.86%`

### `gradual_5_8`
- Trigger to `0.80x` risk at `5%` drawdown
- Trigger to `0.55x` risk at `8%` drawdown
- Recover threshold: `2.5%`
- Step-up speed: `+0.005` per day

Result:
- Annual return: `17.48%`
- Sharpe: `1.82`
- Max drawdown: `-9.40%`

### `aggressive_4_7`
- Trigger to `0.75x` risk at `4%` drawdown
- Trigger to `0.50x` risk at `7%` drawdown
- Recover threshold: `2%`
- Step-up speed: `+0.005` per day

Result:
- Annual return: `16.33%`
- Sharpe: `1.82`
- Max drawdown: `-8.31%`

## Parameter Sweep Takeaway
A small grid search around mild settings did not find a configuration that beat baseline Sharpe.

Best mild candidate found in the sweep:
- `6% / 10%` thresholds
- `0.90x / 0.75x` scales
- recover at `3%`
- step-up `0.02`

Result:
- Annual return: `18.99%`
- Sharpe: `1.84`
- Max drawdown: `-10.36%`

This is a respectable trade-off, but still not better than baseline on efficiency.

## Current Conclusion
- The direction is valid as a risk-preference tool.
- It does reduce max drawdown.
- But under this simple prototype, it behaves more like "pay return to buy smoother equity" than "free lunch from better portfolio control."
- So it is not yet strong enough to justify changing the default strategy.

## Better Next Iterations
- Only activate portfolio throttle when multiple sleeves are simultaneously weak, instead of using drawdown alone
- Use slower monthly or weekly re-risking rather than a daily step-up loop
- Let the trimmed portion park in a more realistic sleeve-specific cash mix instead of flat zero
