# Microcap Top100 v2.5 Warning Board

This board records warning-only research candidates for v2.5. Items here are not formal strategy logic and must not change live signal, realtime signal, sizing, or performance output unless promoted by a separate source-change decision.

## MA60 Bias Overheat Watch

- Status: warning-only / watchlist.
- Candidate: `MA60 bias overheat`, `hot=35%`, `cool=22%`.
- Definition: `ma60_bias = microcap_close / MA60(microcap_close) - 1`.
- Timing: close-confirmed; when `ma60_bias >= 35%`, flag overheat risk for the next session; when it cools to `<= 22%`, clear the warning.
- Baseline tested against: formal v2.5 costed no-overheat path.
- Evidence run: `quant_param_scan_runs/20260523_microcap_top100_v2_5_derived_microcap_only_ma_bias_overheat_ma40_ma60_high_bias_narrow/`.

Current close-confirmed status:

| Date | Microcap close | MA60 | Current MA60 bias | Warning hot bias | Gap to hot | Cool bias | Warning state |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-05-22 | 324020.187430 | 319952.603168 | +1.27% | +35.00% | 33.73pp below hot | +22.00% | Inactive; below cool threshold |

Measured behavior:

| Candidate | Full annual | Full max DD | 10Y annual | 5Y annual | 3Y annual | Trigger count | Risk-off days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v2.5 baseline | 37.12% | -19.48% | 31.09% | 36.97% | 42.99% | 0 | 0 |
| MA60 hot35/cool22 | 32.80% | -19.48% | 33.05% | 41.13% | 50.34% | 4 | 62 |

Trigger audit:

| Episode | Effect |
| --- | --- |
| 2015-04-10 trigger, risk-off 2015-04-13 to 2015-06-19 | Harmful. Missed a strong continuation rally; impact delta about `-96.57pp`. |
| 2015-11-25 trigger | Slightly helpful; impact delta about `+2.07pp`. |
| 2024-10-08 trigger | Helpful; impact delta about `+9.32pp`. |
| 2024-11-11 trigger | Helpful; impact delta about `+5.02pp`. |

Decision:

- Keep as a warning-board item only.
- Do not add to v2.5 formal logic.
- Do not treat as a drawdown-control layer; it did not improve full-sample max drawdown.
- User-facing wording should say: `MA60 bias overheat warning active/inactive`, not `exit signal`.
- If revisited later, compare it against the same v2.5 costed baseline and include the 2015 continuation-rally failure case in the report.
