# Microcap v2.0 / v2.3 NAV Refresh And Cleanup Record - 2026-05-20

## Scope

This record documents the refresh, validation, cleanup, and sync scope for the Top100 microcap v2.0 vs v2.3 one-year costed NAV overlay.

The user-facing chart was regenerated only after the selected strategy data was refreshed to the latest available close-confirmed trading date.

## Refreshed Artifacts

- `outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv`
- `outputs/microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv`
- `outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv`
- `outputs/microcap_top100_mom16_targetvol25_max1p5_v2_0_costed_nav.csv`
- `outputs/microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv`
- `outputs/microcap_v2_0_vs_v2_3_last1y_nav_overlay_20260520.png`
- `outputs/microcap_v2_0_vs_v2_3_last1y_nav_overlay_20260520.csv`
- `outputs/microcap_v2_0_vs_v2_3_last1y_nav_overlay_20260520_summary.csv`
- `outputs/microcap_v2_0_vs_v2_3_last1y_nav_overlay_20260520_summary.json`

Most generated outputs are ignored by git. The tracked sync payload is limited to the durable source/cache outputs already tracked by the repo plus this record.

## Data Refresh Notes

- The initial v2.0 / v2.3 rebuild only reached `2026-05-15`; this was not acceptable under the workspace rule requiring a fresh data refresh before charts.
- The refreshed panel already had CSI 1000 data through `2026-05-20`, but the Top100 proxy and costed NAV chain needed extension.
- Free sources used for stock-price backfill were Sina and Tencent. Eastmoney remained unreliable during this pass.
- The final rebuild confirmed the latest Top100 rebalance members had `100/100` price coverage through `2026-05-20`.
- QVeris was not used.

## Final Verification

Observed after the final rebuild:

| Artifact | End date | Rows | Duplicate dates |
| --- | --- | ---: | ---: |
| `wind_microcap_top_100_biweekly_thursday_16y_cached.csv` | 2026-05-20 | 3960 | 0 |
| `microcap_top100_mom16_targetvol25_max1p5_v2_0_costed_nav.csv` | 2026-05-20 | 3926 | 0 |
| `microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv` | 2026-05-20 | 3884 | 0 |

One-year costed `nav_net` overlay window:

| Version | Window | Days | Total return | Max drawdown |
| --- | --- | ---: | ---: | ---: |
| v2.0 | 2025-05-20 to 2026-05-20 | 232 | 10.9104% | -16.0407% |
| v2.3 | 2025-05-20 to 2026-05-20 | 232 | 14.6747% | -12.8209% |

The chart file was verified as a non-empty `2160 x 1170` PNG.

## Cleanup

Temporary refresh diagnostics removed after backup:

- `outputs/microcap_latest_member_price_refresh_20260520.csv`
- `outputs/microcap_price_cache_freshness_candidates_20260520.csv`
- `outputs/microcap_price_cache_freshness_candidates_20260520_after_sina.csv`
- `outputs/microcap_price_cache_freshness_candidates_20260520_current.csv`
- `outputs/microcap_price_cache_freshness_candidates_20260520_current2.csv`
- `outputs/microcap_price_cache_freshness_candidates_20260520_final.csv`
- `outputs/microcap_sina_price_refresh_audit_20260520.csv`

Backup directory:

- `.codex_backups/20260520_215759`

## Commands Used

- `python microcap_top100_mom16_biweekly_live.py 净值表现`
- `python microcap_top100_mom16_biweekly_live_v2_0.py`
- `python microcap_top100_mom16_biweekly_live_v2_3.py`
- Direct repo-function rebuild after targeted member-price refresh:
  - `extend_index_recent_window(...)`
  - `rebuild_costed_nav_from_proxy_turnover(...)`

## Follow-Up

For future chart refreshes where there is no new rebalance between the old NAV end date and the target date, first check whether the active 100-member basket plus hedge is sufficient to extend the tail. Only expand to the full recent candidate pool when a new rebalance date or member-list rebuild is required.
