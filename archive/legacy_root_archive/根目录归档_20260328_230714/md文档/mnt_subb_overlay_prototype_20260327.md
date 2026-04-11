# Sub-B Overlay Prototype

## Date
2026-03-27

## Goal
Test whether Sub-B should move from a standalone USD rotation sleeve toward a tactical overlay around Sub-C.

Current prototype kept main strategy code unchanged and only used local frozen data plus the current `mnt_bot plus 1 .py` logic.

## Prototype Definitions

### Baseline
- Current Sub-B full rotation
- Uses current main-script params: `20% target vol / 1.5x max lev / futures_only`

### Overlay A: `cash_residual`
- Map Sub-C strategic exposures into the closest Sub-B tradable sleeves:
  - `QQQ <- QQQ`
  - `EFA <- VEA`
  - `GLD <- GLD`
  - `TLT <- VGIT`
  - `BTC-USD <- BTC-USD`
- Build Sub-B tactical sleeve from the positive tilt of Sub-B vs mapped Sub-C anchor
- Unused capital stays in `BIL`

### Overlay B: `rescaled_active`
- Same positive-tilt idea
- But rescale active weights back to the original Sub-B gross exposure
- This behaves more like a concentrated active-risk sleeve than a cash-heavy overlay

## Reproducible Results

### Sub-B
- Full rotation: annual `16.13%`, max drawdown `-17.18%`, Sharpe `1.11`
- Overlay `cash_residual`: annual `8.78%`, max drawdown `-15.96%`, Sharpe `0.97`
- Overlay `rescaled_active`: annual `16.06%`, max drawdown `-20.87%`, Sharpe `1.03`

### Combined
- Full rotation: annual `19.43%`, max drawdown `-10.82%`, Sharpe `1.72`
- Overlay `cash_residual`: annual `17.70%`, max drawdown `-12.81%`, Sharpe `1.57`
- Overlay `rescaled_active`: annual `19.29%`, max drawdown `-12.30%`, Sharpe `1.69`

## Diagnostics
- Full rotation vs Sub-C daily correlation: `0.636`
- Overlay `cash_residual` vs Sub-C daily correlation: `0.625`
- Overlay `rescaled_active` vs Sub-C daily correlation: `0.638`

- Average full Sub-B gross: `1.15x`
- Average overlay gross:
  - `cash_residual`: `0.63x`
  - `rescaled_active`: `1.15x`

- Average overlap between Sub-B risky distribution and mapped Sub-C anchor: `43.5%`
- Average positive-tilt share: `55.9%`

## Current Conclusion
- The simple `cash_residual` overlay cuts too much exposure and weakens the whole combination.
- The `rescaled_active` overlay preserves return better, but it does not reduce correlation with Sub-C and worsens drawdown.
- Under the current reproducible prototype, "Sub-B as a simple overlay around Sub-C" is not yet a convincing upgrade over the current full-rotation sleeve.

## Next Better Iterations
- State-gated overlay: only switch Sub-B into overlay mode when Sub-C and Sub-B crowd into similar assets
- Symmetric active sleeve: allow both overweight and underweight expressions relative to the mapped anchor, not only positive tilts
- Use a richer anchor that includes VTI / DBMF information instead of only mapped overlaps
