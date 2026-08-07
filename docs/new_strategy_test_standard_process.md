# New Strategy Test Standard Process

Updated: 2026-06-29

This is the common required workflow for new strategy tests in the momentum strategy workspaces. Strategy-specific process documents may add stricter rules, but they must not weaken this standard.

## Scope

Use this process for new strategy research, replacement-signal tests, parameter scans, sleeve additions, overlay tests, portfolio combinations, and candidate promotion reviews across A-share, US, crypto, microcap, RSRS, Sub-D, and mixed-pool workspaces.

## Mandatory Display Windows

Every user-facing display, report, candidate table, layer summary, and promotion note must include annualized return and max drawdown for all of these windows:

- Full available formal sample.
- Last 10 years.
- Last 5 years.
- Last 3 years.
- Last 1 year.

If a window is not available, show `N/A` and state the reason, such as insufficient post-listing history, missing executable data, or invalid pre-publication vendor backfill. Do not omit a window because the table is wide.

## First-Pass Checks

Before running or comparing results:

- Identify the official entrypoint, report path, parameter file, and output path used by the strategy family.
- Record the data source, adjustment mode, trading calendar, timezone, fee/slippage model, execution timing, and latest available date.
- Confirm listing, inception, publication, or availability dates for every required index, ETF, stock pool, benchmark, hedge, or proxy.
- Set the formal sample start to the latest valid availability date among all required participants.
- Mark any pre-publication vendor backfill, proxy-only window, helper-run shortcut, or unreconciled baseline as diagnostic rather than formal.

## Baseline And Candidate Comparison

Every candidate shown to the user must be compared against the correct same-line baseline:

- For layered tests, use the previous formal layer's carried candidate unless the layer definition explicitly requires an always-on, no-signal, or original-strategy baseline.
- Original-strategy comparisons must progress with the tested layer. When the new branch reaches a condition that already exists in the original strategy, add the original strategy's corresponding condition back into the original-strategy baseline for that layer. For example, an R2/absolute-momentum layer should compare against the original strategy with its original R2/absolute-momentum gates; a target-vol layer should compare against the original strategy with its original target-vol parameters. Do not keep using a static stripped-down original baseline after later-layer conditions have been reached.
- For replacement tests, compare old and new logic on the same data slice, same costs, same execution timing, and same calendar.
- For each mandatory window, show candidate annualized return, candidate max drawdown, annualized-return delta in percentage points, and max-drawdown improvement in percentage points.
- A positive return delta means the candidate beat the baseline. A positive drawdown delta means the candidate had a shallower max drawdown.

## Layered Test Discipline

For layered or grid research:

- Test one layer at a time and stop after the layer summary unless the user explicitly approves continuing.
- Do not skip the agreed layer order. Later overlays may be run as side diagnostics only when clearly labeled.
- Layer 1 width standard: the first parameter-width test must show both sides of the recommended or peak tuple retaining at least 80% of the chosen width metric, meaning no more than 20% decay on either side. Both sides must appear as connected patches, not isolated one-point neighbors. Record the metric used for this 80% test and the left/right neighbor values.
- After the raw momentum signal and first-layer parameter-width test, run a dedicated signal-quality filter layer. For A-share momentum sleeves this layer must include R2 window and threshold tests before later overlays, liquidity/amount rules, target-vol, or risk gates are considered.
- For Layer 2 and every later added condition, the pass standard is not "best point improved." Compare each candidate to the same carried line before the new condition, on the same data slice, cost model, execution timing, and calendar. These layers are primarily drawdown-control layers: annualized return may be lower if the drawdown improvement is meaningful. Unless a run record pre-declares stricter or looser tolerances, use this default tolerance: full/10Y/5Y annualized return may lag the pre-condition line by at most 1 percentage point, and 3Y/1Y annualized return may lag by at most 3 percentage points. If full-sample max drawdown improves by at least 8 percentage points, the run record may pre-declare a wider return tolerance, but the trade-off must be shown in every mandatory window and cannot be inferred after seeing a single best point. The candidate must improve full-sample max drawdown and improve max drawdown in at least 3 of the 5 mandatory windows. Any larger return loss or short-window exception outside the pre-declared tolerance must be shown explicitly and labeled watch/diagnostic rather than promoted.
- Layer 2 and later width standard: do not apply the Layer 1 80% side-decay rule. The width test passes for any neighboring line, patch, or ridge that is stronger than the same carried line without the new condition. "Stronger" means the same-slice trade-off is preferable: max drawdown improves clearly, ideally in multiple mandatory windows, while annualized return loss remains small and pre-declared. A slightly lower return is acceptable when the drawdown improvement is meaningful and visible.
- Report ridge width, neighbor support, edge-of-grid status, and whether the recommended tuple is width-supported.
- Do not promote a thin maximum when a broader alternative exists and also passes the applicable width standard for that layer.
- Carry at least one primary line, one nearby confirmation line, and any user-requested return-heavy watchlist line until the user explicitly drops it.

## Required Artifacts

Formal or quasi-formal tests should write enough artifacts to reproduce the result:

- `record.md` or equivalent decision note.
- Candidate metrics with the mandatory windows.
- Daily curve or NAV series.
- Scan summary and ridge/width table when a grid was run.
- Metadata covering data source, sample dates, row counts, execution timing, costs, and command log.
- Parameter freeze notes when OOS or holdout-style windows are used to justify production status.
- Live data audit logs when realtime quote fields, volume units, or tentative intraday bars can change a production-facing signal.

Temporary helper scripts and test scaffolding may be deleted after promotion only if the retained final script, docs, and artifacts are enough to rebuild or audit the promoted result.

## Market And Execution Assumptions

Always state the assumptions that affect returns:

- A-share tests: fields used, adjustment mode, suspensions, price-limit executability, T+1 constraints, amount/volume source and unit normalization.
- US tests: adjusted close/open source, dividend/split handling, regular-session timing, US calendar, ETF inception, and any crypto calendar handling.
- Mixed-market tests: do not compress US or crypto data onto the A-share calendar unless explicitly marked diagnostic.
- Portfolio combinations: refresh every sleeve first, then record each sleeve's usable start/end date before combining.
- If a live/realtime signal is intentionally used before the close for manual same-day close execution, label the signal timestamp, confirmed/unconfirmed bar status, manual execution path, non-auto-ordering status, and whether `cost_rate` already includes that execution friction.
- Realtime volume-dependent filters must have a source-unit audit trail, or their intraday output must remain explicitly tentative until confirmed daily bars are available.
- Live trading calendars must cover the current live date and the next planned execution date; weekday fallback is a visible fallback, not a formal holiday calendar.

## Promotion Rules

A candidate can be promoted only when:

- The baseline is reconciled to the official output path or explicitly marked diagnostic.
- All mandatory windows are shown.
- Recent-window behavior is not hidden by full-sample strength.
- Costs, turnover, liquidity, and execution timing are included or explicitly excluded with a reason.
- The final user-visible surfaces, such as signal, live signal, params, and live params, are kept in sync when production logic changes.
- Any realtime signal surface that is advisory-only must state that orders are manual and not automatically submitted.
- Parameter changes require a new freeze note and same-path baseline comparison before promotion.

## Cleanup And Cloud Sync

Before cloud sync:

- Back up files before deleting temporary tests, scans, caches, or scaffolding.
- Remove disposable `tests/`, `.pytest_cache/`, `__pycache__/`, one-off analysis scripts, and rejected scratch outputs when their conclusions are preserved elsewhere.
- Keep final strategy scripts, source dependencies needed by those scripts, formal docs, final metrics, final daily curves, and rollback notes.
- Run the smallest real verification available after cleanup, normally `python -m py_compile <touched scripts>` plus `git diff --check`.
- Record what was deleted, what was preserved, the backup path, verification commands, and remote sync target.
