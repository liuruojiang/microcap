# Microcap v2 Review Remediation (2026-07-11)

## Scope and preserved design decisions

- Reviewed and remediated the `v2.0`, `v2.3`, and `v2.5` strategy families and the active `v2.5` experiment scans.
- Preserved same-day close execution: realtime signals are queried before the close and modeled as executed at that day's close.
- Preserved full-capital allocation when fewer than 100 members are available.
- Kept the synthetic-basket execution abstraction; this work does not add a member-level fill simulator.

## Remediations

- Realtime output now fails closed when the independent latest-completed-trading-day anchor is missing or stale. Synthetic last-close member quotes remain preview-only and are excluded from genuine quote coverage.
- Formal output families are built in a staging directory, validated for freshness and schema consistency, then promoted transactionally with the summary written last.
- Cache parse/schema failures now identify the failed symbol instead of silently shrinking the candidate universe. Candidate pools below the requested portfolio size fail before constraints are applied.
- Recent proxy extensions preserve the frozen historical level and chain only new returns after the bridge date, keeping `close.pct_change()` consistent with `daily_return`.
- Historical rewrite audits remove stale audit files on a clean result, and approved metadata-only migrations are matched narrowly.
- `v2.3` and `v2.5` summaries and signal diagnostics now reflect their own parameters instead of retaining inherited `v2.0` metadata.
- `v2.5` scans load a fresh official baseline, record its fingerprint, enforce state-column consistency, and charge only incremental overlay turnover. Breadth uses adjusted prices with strict member coverage; volume features must reach the official NAV end date.
- Post-promotion freshness validation now executes while rollback backups are still live, so a failed readback restores the entire prior formal bundle.
- Embedded `v2.3`/`v2.5` invocations restore their output-path globals after custom-prefix runs.
- Staged-entry fills are charged once; breadth and volume overlays replay entry/exit state and costs instead of multiplying an already-costed return stream. Volume scans reject truncated or internally gapped full-sample features, and the bias-overheat scan uses the shared fresh official loader.

## Freshness proof

All formal streams below were read back after generation and end on the same close-confirmed trading date, `2026-07-10`.

| Artifact | Rows | Start | End |
|---|---:|---|---|
| Refreshed panel | 8,681 | 1990-12-19 | 2026-07-10 |
| Proxy index | 4,002 | 2010-01-15 | 2026-07-10 |
| Proxy turnover | 425 | 2010-01-28 | 2026-07-09 |
| Base costed NAV | 3,985 | 2010-02-09 | 2026-07-10 |
| v2.0 costed return stream | 3,968 | 2010-03-11 | 2026-07-10 |
| v2.3 costed return stream | 3,931 | 2010-05-05 | 2026-07-10 |
| v2.5 costed return stream | 3,931 | 2010-05-05 | 2026-07-10 |

The proxy turnover artifact is event-based, so its final row is the latest rebalance event (`2026-07-09`) rather than a daily close row.

## Verification

- `python -m pytest tests -q -p no:cacheprovider`: 68 passed.
- AST parsing succeeded for all 12 modified/new Python files.
- Proxy frozen-history comparison through `2026-07-02`: zero numeric drift.
- Appended proxy `close.pct_change()` versus `daily_return`: maximum absolute error below `1e-16`.
- Fresh official `v2.5` scan baseline smoke check: 3,931 rows, `2010-05-05` through `2026-07-10`, version `2.5`.
