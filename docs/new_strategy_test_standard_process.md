# New Strategy Test Standard Process

Updated: 2026-06-13

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
- For replacement tests, compare old and new logic on the same data slice, same costs, same execution timing, and same calendar.
- For each mandatory window, show candidate annualized return, candidate max drawdown, annualized-return delta in percentage points, and max-drawdown improvement in percentage points.
- A positive return delta means the candidate beat the baseline. A positive drawdown delta means the candidate had a shallower max drawdown.

## Layered Test Discipline

For layered or grid research:

- Test one layer at a time and stop after the layer summary unless the user explicitly approves continuing.
- Do not skip the agreed layer order. Later overlays may be run as side diagnostics only when clearly labeled.
- Report ridge width, neighbor support, edge-of-grid status, and whether the recommended tuple is width-supported.
- Do not promote a thin maximum when a sufficiently wide secondary ridge exists.
- Carry at least one primary line, one nearby confirmation line, and any user-requested return-heavy watchlist line until the user explicitly drops it.

## Required Artifacts

Formal or quasi-formal tests should write enough artifacts to reproduce the result:

- `record.md` or equivalent decision note.
- Candidate metrics with the mandatory windows.
- Daily curve or NAV series.
- Scan summary and ridge/width table when a grid was run.
- Metadata covering data source, sample dates, row counts, execution timing, costs, and command log.

Temporary helper scripts and test scaffolding may be deleted after promotion only if the retained final script, docs, and artifacts are enough to rebuild or audit the promoted result.

## Market And Execution Assumptions

Always state the assumptions that affect returns:

- A-share tests: fields used, adjustment mode, suspensions, price-limit executability, T+1 constraints, amount/volume source and unit normalization.
- US tests: adjusted close/open source, dividend/split handling, regular-session timing, US calendar, ETF inception, and any crypto calendar handling.
- Mixed-market tests: do not compress US or crypto data onto the A-share calendar unless explicitly marked diagnostic.
- Portfolio combinations: refresh every sleeve first, then record each sleeve's usable start/end date before combining.

## Promotion Rules

A candidate can be promoted only when:

- The baseline is reconciled to the official output path or explicitly marked diagnostic.
- All mandatory windows are shown.
- Recent-window behavior is not hidden by full-sample strength.
- Costs, turnover, liquidity, and execution timing are included or explicitly excluded with a reason.
- The final user-visible surfaces, such as signal, live signal, params, and live params, are kept in sync when production logic changes.

## Cleanup And Cloud Sync

Before cloud sync:

- Back up files before deleting temporary tests, scans, caches, or scaffolding.
- Remove disposable `tests/`, `.pytest_cache/`, `__pycache__/`, one-off analysis scripts, and rejected scratch outputs when their conclusions are preserved elsewhere.
- Keep final strategy scripts, source dependencies needed by those scripts, formal docs, final metrics, final daily curves, and rollback notes.
- Run the smallest real verification available after cleanup, normally `python -m py_compile <touched scripts>` plus `git diff --check`.
- Record what was deleted, what was preserved, the backup path, verification commands, and remote sync target.
