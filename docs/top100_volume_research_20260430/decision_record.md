# Top100 Microcap Volume and Liquidity Overlay Decision Record

Date: 2026-04-30

## Scope

This records the final sleeve-level volume research pass after Sub-A, DK, and Sub-B. The tested baseline is the official Top100 microcap strategy used by V7.2:

- script family: `microcap_top100_mom16_biweekly_live.py`;
- baseline NAV: `outputs/microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv`;
- baseline sample: `2010-02-02` to `2026-04-28`;
- baseline metrics: `31.07%` annual, Sharpe `2.45`, MaxDD `-12.20%`.

No production strategy file was changed.

## Data and Timing

Local historical Top100 data contains:

- daily costed NAV and returns;
- rebalance turnover and blocked-entry/blocked-exit counts;
- target-list member market cap at each rebalance;
- daily holding count for the constructed Top100 proxy.

It does not contain a complete historical per-stock volume or amount matrix for all Top100 members. Therefore the research was split into:

1. internal liquidity proxies: turnover, blocked trade counts, holding count, and selected-member market cap;
2. broad market amount proxies: SHCOMP, CSI300, CSI1000, CSI2000, ChiNext, and CSI2000/ChiNext combinations.

Timing is no-lookahead:

- rebalance-date internal features affect returns from `return_start_date`;
- daily holding-count features are shifted one trading day;
- broad index amount signals at T close affect Top100 returns from T+1.

Extra exposure-scale changes pay `0.003 * abs(delta_scale)`.

## Internal Liquidity Proxy Scan

Output folder: `docs/top100_liquidity_overlay_20260430/`

Tested proxies:

- one-side rebalance turnover;
- blocked entry count;
- blocked exit count;
- total blocked count;
- holding count after rebalance;
- daily holding count;
- target-list mean, median, min, and max market cap.

Result: no robust candidate.

The best-scored rules were all negative and sparse. Reducing exposure when holding count dropped or turnover/blocked counts were high reduced returns and did not materially improve drawdown.

Decision: do not add an internal liquidity-proxy overlay.

## Broad Market Amount Scan

Output folder: `docs/top100_broad_volume_overlay_20260430/`

Tested sources:

- SHCOMP amount;
- CSI300 amount;
- CSI1000 amount;
- CSI2000 amount;
- ChiNext amount;
- CSI2000 and ChiNext amount combinations.

The first broad scan found a meaningful family: scale down Top100 when CSI2000 and/or ChiNext amount stays below its moving-average amount for a sustained period.

Examples from the coarse scan:

| Rule | Trigger days | Full annual delta | 10Y annual delta | 5Y annual delta | 3Y annual delta | MaxDD delta |
|---|---:|---:|---:|---:|---:|---:|
| CSI2000 and ChiNext amount < MA60 for 15 days, scale 0.00 | 293 | +0.09pp | +1.74pp | +3.84pp | +2.71pp | +1.42pp |
| ChiNext amount < MA50 for 20 days, scale 0.00 | 378 | -0.56pp | +1.71pp | +6.96pp | +4.27pp | +1.42pp |
| CSI2000 and ChiNext amount < MA50 for 20 days, scale 0.00 | 161 | +0.17pp | +1.06pp | +2.91pp | +4.14pp | +0.89pp to +1.15pp |

## Focused Ridge Scan

Output folder: `docs/top100_broad_volume_ridge_20260430/`

Focused grid:

- sources: CSI2000, ChiNext, and CSI2000 + ChiNext;
- amount MA range: `40..70`;
- consecutive-day range: `8..25`;
- scale: `0 / 0.25 / 0.50 / 0.75`.

The best cell:

| Rule | Trigger days | Full annual delta | 10Y annual delta | 5Y annual delta | 3Y annual delta | MaxDD delta |
|---|---:|---:|---:|---:|---:|---:|
| CSI2000 and ChiNext amount < MA53 for 13 days, scale 0.00 | 311 | +0.38pp | +2.00pp | +6.75pp | +6.68pp | +1.42pp |

Nearby defensive variants remain strong:

| Rule | Full annual delta | 10Y annual delta | 5Y annual delta | 3Y annual delta | MaxDD delta |
|---|---:|---:|---:|---:|---:|
| MA53 / 13 days / scale 0.25 | +0.30pp | +1.53pp | +5.08pp | +5.03pp | +1.90pp |
| MA53 / 13 days / scale 0.50 | +0.21pp | +1.03pp | +3.39pp | +3.36pp | +2.01pp |

Family-level ridge width:

| Family | Scale | Robust cells / tested |
|---|---:|---:|
| CSI2000 and ChiNext below MA | 0.75 | 264 / 558 |
| CSI2000 and ChiNext below MA | 0.50 | 181 / 558 |
| CSI2000 and ChiNext below MA | 0.25 | 169 / 558 |
| CSI2000 and ChiNext below MA | 0.00 | 149 / 558 |
| ChiNext below MA | 0.75 | 177 / 558 |

This is a materially wider ridge than the internal liquidity scan and is closer in shape to the useful A-share volume results.

## Interpretation

For the microcap sleeve, the useful volume information is not the strategy's own rebalance friction proxy. It is the broader small-growth liquidity regime:

- ChiNext captures growth liquidity/risk appetite;
- CSI2000 captures small-cap breadth/liquidity;
- their joint amount contraction is a plausible environment where Top100 microcap exposure should be reduced.

This result is stronger than Sub-B's volume result and directionally consistent with the earlier A-share sleeve work: A-share鎴愪氦棰?can matter, but the source must match the sleeve's market structure.

## Current Decision

Do not change the production microcap script yet.

Keep `CSI2000 and ChiNext amount < MA53 for 13 days` as the leading candidate family for the next implementation discussion. A practical default, if promoted later, should probably compare `scale 0.25` and `scale 0.50` against `scale 0.00`, because they keep most of the return improvement while producing equal or better drawdown improvement in the focused ridge.

## Files

- `analyze_top100_liquidity_overlay.py`
- `analyze_top100_broad_volume_overlay.py`
- `analyze_top100_broad_volume_ridge.py`
- `docs/top100_liquidity_overlay_20260430/summary.md`
- `docs/top100_broad_volume_overlay_20260430/summary.md`
- `docs/top100_broad_volume_ridge_20260430/summary.md`
