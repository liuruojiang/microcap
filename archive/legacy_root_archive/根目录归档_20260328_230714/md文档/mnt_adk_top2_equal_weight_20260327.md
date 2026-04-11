# ADK Top-2 Equal Weight

## Date
2026-03-27

## Definition
- Each day hold the strongest two DK pairs
- Equal-weight their pair strategy returns
- No pair-retention threshold
- No weekly confirmation gate

## Baseline
- ADK annual return: `29.15%`
- ADK Sharpe: `1.43`
- ADK max drawdown: `-34.68%`
- ADK structural trade days/year: `40.22`
- Combined annual return: `19.43%`
- Combined Sharpe: `1.72`

## Top-2 Equal Weight Result
- ADK annual return: `23.87%`
- ADK Sharpe: `1.23`
- ADK max drawdown: `-39.42%`
- ADK structural trade days/year: `56.48`
- ADK total trade days/year: `77.11`
- Pair-set change days: `857`
- Median pair-set spell: `2` trading days

- Combined annual return: `17.94%`
- Combined Sharpe: `1.65`
- Combined max drawdown: `-10.78%`

## Conclusion
- This version is not attractive.
- It increases ADK structural trading a lot instead of reducing it.
- It also weakens both ADK and Combined performance.
- The likely reason is that the second-best pair introduces extra ranking noise, so the selected pair set changes even more frequently than Top-1.
