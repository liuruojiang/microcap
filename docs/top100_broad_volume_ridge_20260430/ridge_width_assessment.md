# Ridge Width Assessment

Date: 2026-04-30

Scope: `zz2000_and_cyb_below` family from `top100_broad_volume_ridge_summary.csv`.

Scale means remaining Top100 exposure after the volume filter triggers:

- `scale 0.00`: turn off Top100 exposure;
- `scale 0.25`: keep 25%;
- `scale 0.50`: keep 50%;
- `scale 0.75`: keep 75%.

## MA53 / 13 Days Across Scale

| Scale | Trigger days | Full dAnn | 10Y dAnn | 5Y dAnn | 3Y dAnn | 10Y dMaxDD | 5Y dMaxDD | 3Y dMaxDD | Robust |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0.00 | 311 | +0.38pp | +2.00pp | +6.75pp | +6.68pp | +1.42pp | +1.42pp | +1.42pp | True |
| 0.25 | 311 | +0.30pp | +1.53pp | +5.08pp | +5.03pp | +1.90pp | +1.90pp | +1.90pp | True |
| 0.50 | 311 | +0.21pp | +1.03pp | +3.39pp | +3.36pp | +2.01pp | +2.01pp | +2.01pp | True |
| 0.75 | 311 | +0.11pp | +0.53pp | +1.70pp | +1.68pp | +1.00pp | +1.00pp | +1.00pp | True |

Interpretation: the exact `MA53 / 13 days` point is not dependent on a single de-risking scale. All tested scales pass. `scale 0.00` has the highest return uplift; `scale 0.50` has the largest drawdown improvement at that point.

## Tight Neighborhood: MA50-60 / Days11-15

This is the practical local ridge around `MA53 / 13`.

| Scale | Cells | Robust cells | Robust pct | Median score | Min score | Max score | Median full dAnn | Median 10Y dAnn | Median 5Y dAnn | Median 3Y dAnn | Median 10Y dMaxDD |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.00 | 55 | 38 | 69.1% | 0.221 | -0.059 | 0.446 | -0.40pp | +0.59pp | +3.55pp | +1.60pp | +1.42pp |
| 0.25 | 55 | 44 | 80.0% | 0.214 | +0.009 | 0.374 | -0.28pp | +0.47pp | +2.71pp | +1.24pp | +1.90pp |
| 0.50 | 55 | 51 | 92.7% | 0.176 | +0.030 | 0.276 | -0.18pp | +0.32pp | +1.83pp | +0.86pp | +1.65pp |
| 0.75 | 55 | 55 | 100.0% | 0.093 | +0.024 | 0.143 | -0.08pp | +0.17pp | +0.93pp | +0.44pp | +0.82pp |

Interpretation: this local ridge is wide enough. Even the minimum score inside the tight neighborhood is positive for `scale 0.25 / 0.50 / 0.75`; the only local negative cells are in the most aggressive `scale 0.00` setting.

## Local Neighborhood: MA48-58 / Days10-16

| Scale | Cells | Robust cells | Robust pct | Median score | Min score | Max score | Median full dAnn | Median 10Y dAnn | Median 5Y dAnn | Median 3Y dAnn | Median 10Y dMaxDD |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.00 | 77 | 50 | 64.9% | 0.138 | -0.239 | 0.446 | -0.48pp | +0.54pp | +2.61pp | +1.17pp | +1.42pp |
| 0.25 | 77 | 55 | 71.4% | 0.154 | -0.125 | 0.374 | -0.35pp | +0.42pp | +2.01pp | +0.93pp | +1.90pp |
| 0.50 | 77 | 63 | 81.8% | 0.129 | -0.054 | 0.276 | -0.23pp | +0.29pp | +1.38pp | +0.65pp | +1.43pp |
| 0.75 | 77 | 77 | 100.0% | 0.072 | -0.016 | 0.143 | -0.11pp | +0.16pp | +0.71pp | +0.34pp | +0.77pp |

Interpretation: the ridge remains broad when expanded to `MA48-58 / days10-16`, but aggressive scales begin to include weaker edges. `scale 0.50` is the best compromise between materiality and width.

## Wider Neighborhood: MA45-65 / Days10-20

| Scale | Cells | Robust cells | Robust pct | Median score | Min score | Max score | Median full dAnn | Median 10Y dAnn | Median 5Y dAnn | Median 3Y dAnn | Median 10Y dMaxDD |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.00 | 231 | 111 | 48.1% | 0.111 | -0.380 | 0.446 | -0.44pp | +0.60pp | +2.00pp | +1.34pp | +0.89pp |
| 0.25 | 231 | 119 | 51.5% | 0.105 | -0.245 | 0.374 | -0.32pp | +0.46pp | +1.52pp | +1.05pp | +1.02pp |
| 0.50 | 231 | 127 | 55.0% | 0.088 | -0.139 | 0.276 | -0.21pp | +0.32pp | +1.03pp | +0.72pp | +1.15pp |
| 0.75 | 231 | 146 | 63.2% | 0.051 | -0.059 | 0.143 | -0.10pp | +0.16pp | +0.52pp | +0.37pp | +0.63pp |

Interpretation: the signal is still visible over a very wide area, but the useful parameter platform is mainly centered around `MA50-60 / days11-15`. Moving out to `MA45-65 / days10-20` still works statistically, but the edges are not equally attractive.

## Practical Read

The ridge is wide enough to discuss as a candidate rule. The current best operational default is not necessarily `scale 0.00`.

Practical preference:

1. `MA53 / days13 / scale0.50`: best balance at the exact point, with full +0.21pp, 10Y +1.03pp, 5Y +3.39pp, 3Y +3.36pp, and MaxDD +2.01pp.
2. `MA53 / days13 / scale0.25`: more defensive return capture, still strong and wide.
3. `scale0.00`: strongest return uplift at the best cells, but more sensitive at the neighborhood edges.
4. `scale0.75`: widest pass rate, but effect size is smaller.

## Common-Round Parameter Check

`MA50 / 12 days` is a valid simpler alternative. It is weaker than `MA53 / 13 days`, but it sits inside the same ridge.

| Rule | Scale | Trigger days | Full dAnn | 10Y dAnn | 5Y dAnn | 3Y dAnn | 10Y dMaxDD | Robust |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| MA50 / 10 days | 0.00 | 411 | -2.23pp | -1.93pp | +1.56pp | -0.43pp | +1.42pp | False |
| MA50 / 10 days | 0.50 | 411 | -1.09pp | -0.92pp | +0.86pp | -0.13pp | +1.43pp | False |
| MA50 / 10 days | 0.75 | 411 | -0.54pp | -0.45pp | +0.45pp | -0.05pp | +1.17pp | True |
| MA50 / 12 days | 0.00 | 337 | -0.48pp | +0.50pp | +3.55pp | +0.75pp | +1.42pp | True |
| MA50 / 12 days | 0.25 | 337 | -0.34pp | +0.41pp | +2.71pp | +0.62pp | +1.90pp | True |
| MA50 / 12 days | 0.50 | 337 | -0.21pp | +0.29pp | +1.83pp | +0.45pp | +2.27pp | True |
| MA50 / 13 days | 0.50 | 305 | -0.06pp | +0.59pp | +1.93pp | +0.86pp | +1.25pp | True |
| MA53 / 13 days | 0.50 | 311 | +0.21pp | +1.03pp | +3.39pp | +3.36pp | +2.01pp | True |

Interpretation: `MA50 / 12 days` is not bad. `MA50 / 10 days` is too early and too broad: it cuts exposure for 411 days and gives up too much 10Y/full-sample return. The improvement from `MA50 / 12` to `MA53 / 13` is not about worshipping the exact numbers; it reflects that the ridge center starts around the 12-13 day confirmation band, and `MA53` is a nearby integer peak inside the `MA50-60` platform.
