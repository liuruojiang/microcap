# Member-basket daily delivery

This release separates daily observations from market-wide maintenance. It
retains the existing v2.0/v2.3/v2.5 calculations and their full price history.
It does not replace or rewrite the approved historical whole-delivery state.

## Daily path

1. Restore the small immutable checkpoint and verify its bound files.
2. Collect only the effective basket's prices and necessary adjustment factors.
   Close observations use the existing Tencent daily-bar / explicit suspension
   adapter and Sina ratio-adjustment checks. The index uses the independently
   validated index-history chain. Intraday uses native member quotes.
3. Compute the three existing models. A missing index affects the two hedged
   models, while v2.5 remains independently reportable. Missing member prices
   are disclosed; they are never replaced with zero or a smaller basket mean.
4. Save the report, source evidence and next checkpoint before attempting mail.
5. Send with an immutable report key. Recover only sending after a send failure.

The daily path does not invoke global-universe refresh, notice pagination,
security-metadata migration, historical performance regeneration or whole-state
packing. Full regression belongs to code changes, rather than each price query.

## Contracts

`member_period_v1` binds the existing effective and target members, dated
execution events, next rebalance, and source identity. `member_daily_checkpoint_v1`
binds period/history/turnover/version-state/member-raw-price files by SHA-256.
The complete small index history is retained to preserve hysteresis state;
there is no truncated-window restart from cash. A close may produce a new
candidate; intraday observations never advance the confirmed close checkpoint.

`member_daily_result_v1` carries per-version verified/partial/reference status.
An old basket after an unresolved period handoff is labelled reference. Market
data collection, model computation, maintenance status and SMTP delivery are
reported separately. Maintenance freshness must not be inferred from a previous
period's ST check. Report generation is not an account trade confirmation.

## Period handoff

Use the original 2016-01-07 Thursday-week biweekly calendar and close execution
logic, including blocked entries/exits. The old basket owns the rebalance day's
earned return. New effective members own returns from the next exchange session.
Bootstrap reads existing completed formal exports; it does not re-rank them.
Handoff verifies common earned history, dates, source and effective membership
before atomically publishing one complete version. A candidate never overwrites
an active checkpoint while a daily reader is using it.

## Scheduling

Cloud maintenance starts at 09:00 Beijing on weekdays, checking the exchange
calendar before doing incremental work. At 15:10 the independent selection task
checks whether this is the original model's actual rebalance session; non-event
days return immediately. Existing 16:00 microcap and 20:00 IC/IM trigger times
remain. The 10:00 local task should consume completed cloud maintenance/member
artifacts without repeating global work; IC/IM preparation is independent.

Maintenance retains the existing strict historical/source approval rules. A
failure preserves its previous published data and diagnostic evidence; it does
not hold the daily report hostage. The 15:50 handoff check is an observation
deadline, never proof that a candidate completed. Late results retain real times.

## Cutover

Initialize once from the already approved complete exports, verify parity of
the three model states, publish the small checkpoint, then enable the new daily
route. Keep the original workflow available for maintenance/recovery with its
normal automatic trigger disabled after cutover. Never run both normal send
routes for the same date. Preserve normal intent/receipt/marker uncertainty;
partial-information delivery uses its separate bounded key and cannot claim a
normal complete signal was delivered.
