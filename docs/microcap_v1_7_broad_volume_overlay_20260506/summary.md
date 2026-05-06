# Microcap v1.7 Broad Volume Overlay Scan

Freshly rebuilt v1.7 costed return stream plus AKShare index amount data.

## Baseline
- sample: 2010-06-01 to 2026-04-30; rows 3864; duplicate dates 0
- full annual: 39.74%; max drawdown: -17.61%; Sharpe: 2.81

## Warning-Board Candidate Family
- zz2000_and_cyb_below_ma53_days13:scale0.00: trigger 311 days; full dAnn -1.58pp; 10Y dAnn -0.42pp; 5Y dAnn +1.48pp; 3Y dAnn -0.21pp; 10Y dDD -0.00pp; robust=False.
- zz2000_and_cyb_below_ma53_days13:scale0.25: trigger 311 days; full dAnn -1.16pp; 10Y dAnn -0.29pp; 5Y dAnn +1.15pp; 3Y dAnn -0.12pp; 10Y dDD -0.00pp; robust=False.
- zz2000_and_cyb_below_ma53_days13:scale0.50: trigger 311 days; full dAnn -0.76pp; 10Y dAnn -0.17pp; 5Y dAnn +0.79pp; 3Y dAnn -0.05pp; 10Y dDD +0.00pp; robust=False.

## Top Family Width
- zz2000_and_cyb scale 0.00: robust 0/558; best score 0.250; p75 score -0.270.
- zz2000_and_cyb scale 0.25: robust 0/558; best score 0.226; p75 score -0.177.
- zz2000_and_cyb scale 0.50: robust 0/558; best score 0.174; p75 score -0.099.
- cyb scale 0.00: robust 0/558; best score 0.166; p75 score -0.393.
- cyb scale 0.25: robust 0/558; best score 0.144; p75 score -0.250.
- cyb scale 0.50: robust 0/558; best score 0.107; p75 score -0.125.
- zz2000_and_cyb scale 0.75: robust 0/558; best score 0.098; p75 score -0.041.
- cyb scale 0.75: robust 0/558; best score 0.075; p75 score -0.047.

## Files
- `microcap_v1_7_broad_volume_ridge_summary.csv`
- `microcap_v1_7_broad_volume_ridge_top100.csv`
- `microcap_v1_7_broad_volume_ridge_robust.csv`
- `microcap_v1_7_broad_volume_ridge_family_summary.csv`
- `microcap_v1_7_broad_volume_ridge_neighborhood_45_65_10_22.csv`
- `microcap_v1_7_broad_volume_selected_curves.csv`
- `meta.json`
