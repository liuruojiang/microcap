# Combined Multi-Sleeve Guard

## Date
2026-03-27

## Goal
Test a better portfolio-level risk contraction layer:
- do not trigger on combined drawdown alone
- only contract risk when multiple sleeves are simultaneously weak
- park trimmed capital into `BIL` instead of assuming flat zero return

## Mechanism
- Compute each sleeve daily NAV drawdown from its own high
- Mark a sleeve as weak when its drawdown is below a threshold
- If weak sleeve count reaches:
  - `2`: moderate risk contraction
  - `3`: severe risk contraction
- Recover to full risk once weak sleeve count drops back to `<= 1`

## Baseline
- Annual return: `19.43%`
- Volatility: `10.48%`
- Sharpe: `1.85`
- Max drawdown: `-10.82%`

## Representative Results

### `weakdd6_mild`
- Weak threshold: sleeve drawdown `< -6%`
- Scale: `0.90x` at 2 weak sleeves, `0.75x` at 3 weak sleeves
- Recovery: weak count `<= 1`
- Re-risk frequency: daily
- Step-up: `0.02` per trading day

Result:
- Annual return: `18.57%`
- Volatility: `9.47%`
- Sharpe: `1.96`
- Max drawdown: `-8.74%`

### `weakdd5_mild`
- Weak threshold: sleeve drawdown `< -5%`
- Scale: `0.90x / 0.75x`
- Re-risk frequency: daily
- Step-up: `0.02` per trading day

Result:
- Annual return: `18.48%`
- Volatility: `9.30%`
- Sharpe: `1.99`
- Max drawdown: `-8.54%`

### `weakdd4_fast`
- Weak threshold: sleeve drawdown `< -4%`
- Scale: `0.90x / 0.70x`
- Re-risk frequency: daily
- Step-up: `0.02` per trading day

Result:
- Annual return: `17.86%`
- Volatility: `8.79%`
- Sharpe: `2.03`
- Max drawdown: `-8.13%`

### Weekly Re-risk Check
Weekly re-risk means:
- de-risking still happens immediately when weakness appears
- but risk is only allowed to increase once per week

Representative weekly versions:

`weakdd6_mild_weekly_rerisk`
- same weak threshold and scale as `weakdd6_mild`
- re-risk frequency: weekly
- step-up: `0.05` per week

Result:
- Annual return: `18.13%`
- Volatility: `9.34%`
- Sharpe: `1.94`
- Max drawdown: `-8.67%`

`weakdd5_mild_weekly_rerisk`
- same weak threshold and scale as `weakdd5_mild`
- re-risk frequency: weekly
- step-up: `0.05` per week

Result:
- Annual return: `18.05%`
- Volatility: `9.18%`
- Sharpe: `1.97`
- Max drawdown: `-8.54%`

## Weekly Re-risk Conclusion
- Weekly re-risk reduces the number of scale changes materially
- But in the tested settings it also gives back some return and does not improve Sharpe versus the daily re-risk mild versions
- So at the moment, daily re-risk still looks slightly better than weekly re-risk

## Why This Looks Better Than Pure Drawdown Throttle
- Pure combined-drawdown throttle reduced max drawdown, but did not beat baseline Sharpe
- Multi-sleeve guard does improve Sharpe in the tested prototype
- It behaves more like "detect broad internal weakness" than "sell after the portfolio is already hurt"

## Trade-off
- This is still not free alpha
- Higher Sharpe comes with lower annual return
- The stronger versions stay below full risk a lot of the time
  - for example `weakdd5_mild` is below full risk about `71%` of days

## Current Conclusion
- This direction is promising enough to keep
- It is materially stronger than the earlier pure drawdown-throttle prototype
- The most practical next candidate is still a mild daily re-risk version, probably closer to `weakdd6_mild` than the more aggressive `weakdd4_fast`
