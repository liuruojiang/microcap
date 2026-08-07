# Microcap Top100 Post-P0 Lineage Note - 2026-06-29

## Current Data Lineage

The current v2.0/v2.3/v2.5 Top100 results use a locally reconstructed Top100 proxy, not the official Wind `868008.WI` series.

Post-P0 lineage:

- universe source: `backtest_cache_security_master`
- universe size: 4975 historical symbols
- current-ST filter: disabled for historical backtests
- historical-ST filter: enabled
- BSE filter: enabled
- proxy construction: local raw close, share-change, membership, and tradeability checks

This replaced the previous current-universe/current-ST path and the narrow recent-extension path. Older parameter research must not be mixed with this lineage without rerunning the candidate on the current data.

## Fixed Before Parameter Rescan

- v2.3/v2.5 summaries now carry explicit proxy-source warnings when the base microcap series is public/local proxy rather than official Wind.
- v2.3/v2.5 summaries now mark parameter retest as required before promoting a new parameter decision.
- Price-cache refresh now emits preflight and progress diagnostics, including symbol count, stale/missing count, workers, failures, and completion.
- Freshness proof remains mandatory for base panel, proxy index, proxy turnover, base costed NAV, and every compared version stream.

## Parameter Rescan Gate

Before any v2.3/v2.5 parameter result is promoted:

- Use only post-P0 lineage outputs.
- Include the current production/default parameter as the baseline.
- Report full sample, 10Y, 5Y, 3Y, and 1Y annualized return plus max drawdown.
- Preserve current execution timing, target-vol, hedge, financing, and existing cost fields.
- Label any result using public/local proxy as not official Wind `868008.WI`.

Cost/capacity/market-impact extensions are intentionally out of scope for this cleanup round.

## v2.0 Default Replacement

On 2026-06-29, official `v2.0` was replaced with the selected post-P0 low-drawdown line: target volatility 15%, target-vol window 75, max leverage 1.5x, scale threshold 10%, and a 60-day spread-volatility overheat exit at 23%. See `docs/microcap_top100_v2_0_param_replacement_20260629.md` for the full old/new comparison, freshness proof, and downstream v2.3/v2.5 notes.
