# MNT 6.1 Stability Checklist

## Scope Freeze
- Keep the combo as four sleeves only: `Sub-A / Sub-A-DK / Sub-B / Sub-C`
- Keep stable weights at `15 / 15 / 40 / 30`
- Do not add new option, vol, or overlay modules into the main path

## Data Checks
- Verify CN and US source files load without fallback errors
- Check index/date alignment across all four sleeves before combining
- Confirm missing-data handling does not silently drop the latest period

## Strategy Checks
- Re-run the full backtest on the stable four-sleeve config
- Compare key metrics against the accepted 6.1 baseline
- Inspect the latest rebalance records for all four sleeves

## Robustness Checks
- Add trading-cost sensitivity runs
- Add start-date / end-date slice checks
- Add parameter perturbation checks for the existing core parameters only

## Execution Checks
- Verify exported performance Excel matches the charted NAV series
- Verify combined weights shown in outputs match the actual calculation
- Verify signal / journal outputs still reference the same stable version

## Operational Rule
- Treat further work as validation and monitoring only, not feature expansion
