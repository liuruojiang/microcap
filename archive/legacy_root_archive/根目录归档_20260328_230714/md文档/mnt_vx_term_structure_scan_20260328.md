# VX1/VX2 Term Structure Scan

Data source: official Cboe VX monthly contract CSVs, stitched into daily VX1/VX2 continuous series.
Two prototypes are tested: directional VX1 and VX1-VX2 calendar spread.

## Top 15

| rank | mode | low_ratio | high_ratio | use_slope | annual | vol | sharpe | max_dd | active_days | long_days | short_days |
|---:|:---:|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | calendar | 0.93 | 0.98 | True | 3.97% | 25.72% | 0.15 | -69.26% | 1398 | 491 | 907 |
| 2 | calendar | 0.94 | 0.99 | True | 2.41% | 24.65% | 0.10 | -60.85% | 1475 | 408 | 1067 |
| 3 | calendar | 0.95 | 1.00 | True | -0.20% | 24.42% | -0.01 | -58.72% | 1530 | 340 | 1190 |
| 4 | calendar | 0.96 | 1.01 | True | -0.41% | 24.85% | -0.02 | -54.93% | 1591 | 277 | 1314 |
| 5 | calendar | 0.93 | 0.98 | False | -3.30% | 30.65% | -0.11 | -70.95% | 2117 | 778 | 1339 |
| 6 | calendar | 0.96 | 1.01 | False | -3.34% | 30.79% | -0.11 | -65.19% | 2516 | 416 | 2100 |
| 7 | calendar | 0.94 | 0.99 | False | -4.13% | 29.99% | -0.14 | -70.15% | 2269 | 628 | 1641 |
| 8 | calendar | 0.95 | 1.00 | False | -6.77% | 30.23% | -0.22 | -75.60% | 2399 | 516 | 1883 |
| 9 | directional | 0.93 | 0.98 | False | -21.23% | 77.87% | -0.27 | -97.91% | 2117 | 778 | 1339 |
| 10 | directional | 0.94 | 0.99 | True | -16.09% | 57.62% | -0.28 | -94.21% | 1499 | 404 | 1095 |
| 11 | directional | 0.96 | 1.01 | False | -19.82% | 70.89% | -0.28 | -96.29% | 2516 | 416 | 2100 |
| 12 | directional | 0.96 | 1.01 | True | -16.61% | 55.67% | -0.30 | -91.99% | 1617 | 272 | 1345 |
| 13 | directional | 0.93 | 0.98 | True | -20.36% | 58.17% | -0.35 | -97.54% | 1408 | 483 | 925 |
| 14 | directional | 0.95 | 1.00 | True | -20.58% | 55.95% | -0.37 | -95.28% | 1556 | 333 | 1223 |
| 15 | directional | 0.94 | 0.99 | False | -26.57% | 71.42% | -0.37 | -98.85% | 2269 | 628 | 1641 |
