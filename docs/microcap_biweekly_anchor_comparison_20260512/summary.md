# Microcap Top100 Biweekly Anchor Comparison

- Source panel: `outputs\microcap_top100_mom16_biweekly_live_panel_refreshed.csv`
- Trading dates: 2010-01-08 to 2026-05-11 (3964 rows)
- Universe symbols: 4975 from `freq_mod.load_current_universe()`
- Current schedule: existing floating-week `build_biweekly_rebalance_dates()`
- Fixed schedule: absolute anchor `2016-01-07` with `W-WED` week buckets
- Cost model: `freq_mod.cost_mod.apply_cost_model()` on `return_net` / `nav_net`

## Latest Signal

| schedule                | date       | current_holding            | next_holding               | trade_state   |   microcap_mom |   hedge_mom |   momentum_gap |   nav_net |
|:------------------------|:-----------|:---------------------------|:---------------------------|:--------------|---------------:|------------:|---------------:|----------:|
| current_floating        | 2026-05-11 | long_microcap_short_zz1000 | long_microcap_short_zz1000 | hold          |       0.132739 |    0.078917 |      0.0538223 |   29.5984 |
| fixed_anchor_2016_01_07 | 2026-05-11 | long_microcap_short_zz1000 | long_microcap_short_zz1000 | hold          |       0.132739 |    0.078917 |      0.0538223 |   27.9935 |

## Annual Return (%)

| window   |   current_floating |   fixed_anchor_2016_01_07 |
|:---------|-------------------:|--------------------------:|
| 10y      |            22.9823 |                   23.0762 |
| 1y       |            10.04   |                   14.9114 |
| 3y       |            26.5283 |                   31.1305 |
| 5y       |            23.8312 |                   27.0294 |
| full     |            23.1509 |                   22.7296 |

## Max Drawdown (%)

| window   |   current_floating |   fixed_anchor_2016_01_07 |
|:---------|-------------------:|--------------------------:|
| 10y      |           -15.0075 |                  -17.5515 |
| 1y       |           -11.148  |                  -11.148  |
| 3y       |           -11.148  |                  -11.148  |
| 5y       |           -14.1476 |                  -15.4847 |
| full     |           -15.0075 |                  -17.5515 |

## Rebalance Schedule Diff

- Current rebalance count: 424
- Fixed rebalance count: 422
- Differing date rows: 676

First differing rows:

| date       | side         |
|:-----------|:-------------|
| 2010-01-08 | current_only |
| 2010-01-21 | current_only |
| 2010-02-04 | current_only |
| 2010-02-22 | current_only |
| 2010-03-04 | current_only |
| 2010-03-18 | current_only |
| 2010-04-01 | current_only |
| 2010-04-15 | current_only |
| 2010-04-29 | current_only |
| 2010-05-13 | current_only |
| 2010-05-27 | current_only |
| 2010-06-10 | current_only |
| 2010-06-24 | current_only |
| 2010-07-08 | current_only |
| 2010-07-22 | current_only |
| 2010-08-05 | current_only |
| 2010-08-19 | current_only |
| 2010-09-02 | current_only |
| 2010-09-16 | current_only |
| 2010-09-30 | current_only |

## Interpretation

This is a research archive only. Production code remains on the existing floating-week schedule.
The current floating-week schedule depends on the first week included in the input date range.
The fixed-anchor schedule removes that start-date dependence, but it is a strategy-definition change.
Adopting it should therefore be treated as a historical-result rewrite and compared before changing production defaults.
