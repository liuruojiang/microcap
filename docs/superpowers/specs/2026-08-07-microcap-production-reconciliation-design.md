# Microcap Production Reconciliation Design

## Goal

Restore one auditable production truth for Microcap Top100 v2.0, v2.3, and v2.5 so the strategy code, realtime CSVs, daily email, GitHub `main`, and user-facing action wording all describe the same state.

## Incident summary

The 2026-08-07 corrected email reported non-zero v2.0 and v2.3 holdings even though the approved overheat strategies remained in cash. Five failures combined:

1. The approved v2.0/v2.3 strategy work existed on `codex/fix-realtime-hedge-date` and PR #22 but was never merged into production `main`.
2. The production workflow always checks out `main`, so it ran the old v2.0/v2.3 models while the sample email was authored from the newer local branch.
3. The sample email's overheat state was not guarded by a production strategy-identity check. A formatting test fixture therefore looked authoritative even when production could not emit the same fields.
4. Static member changes were copied into every realtime signal until the next rebalance, so a prior 7-in/7-out list could be mislabeled as a new daily action.
5. Deployment verification stopped at successful workflow execution and email delivery. It did not read back the remote `main` SHA, strategy parameters, realtime holdings, overheat state, and rendered email as one acceptance bundle.

## Production strategy identity

Production must fail closed unless each version exposes the expected native identity.

### v2.0

- Base signal: the promoted post-P0 v2.0 lineage.
- Overheat metric: 60-session realized volatility of the microcap minus 0.8x hedge spread.
- Trigger threshold: 23%.
- Reentry: remain in cash until the base momentum signal resets.
- Target volatility: 15% target, 75-session window, 1.5x cap.
- Required realtime fields include `overheat_enabled`, `overheat_window`, `overheat_threshold`, `overheat_metric`, `blocked_until_signal_reset`, `current_holding`, `next_holding`, and `next_session_actionable_scale`.

### v2.3

- Signal: hedged-spread log-WLS, half-life 2.5, lookback 25, R2 entry gate 0.08, 1.0x signal spread, and 0.8x execution hedge.
- Overheat metric: 10-session spread-NAV realized volatility.
- Trigger threshold: 26%.
- Recovery threshold: 19.5%.
- No inherited v2.0 target-volatility, cash-yield, or financing overlay.
- Required realtime fields include `overheat_enabled`, `overheat_feature_window`, `overheat_trigger_threshold`, `overheat_recovery_threshold`, `overheat_feature_value`, `overheat_risk_off`, `current_holding`, `next_holding`, and `next_session_actionable_scale`.

### v2.5

- Keep the native unhedged v2.5 implementation already merged by PR #23.
- No overheat overlay is invented for v2.5.
- Required realtime fields remain native v2.5 holdings, scale, score, R2, and provenance.

## Realtime and member-action semantics

The email separates three concepts:

1. `current_holding -> next_holding`: the strategy direction and risk state.
2. Execution scale: the actionable next-session scale.
3. Member-list action: changes inside the Top100 basket.

A Thursday close-confirmed biweekly list becomes operational on the next trading row. The email may show the official 7-in/7-out action only on that first trading session, with both dates displayed:

- list signal date: 2026-08-06;
- execution session: 2026-08-07.

It must say that the action is based on the prior close and is due during the execution session, not after that execution session's close. On later sessions, the same static change table is historical context and must not make the subject `[需操作]`.

Intraday market-cap differences remain `intraday_preview` with `official_rebalance=False`. They may be retained in artifacts but never promoted to an official action in the email.

## Email contract

The body is decision-first:

1. A bold holdings sentence naming every version and its next-session scale.
2. A separate action sentence for holding changes, scale changes, and an official member action with signal/execution dates.
3. A compact version table with current-to-next holding, action, next-session scale, and native momentum score.
4. Active risk controls and data warnings only.
5. Data timestamp, quote coverage, production strategy SHA, and GitHub Run link.

If production strategy identity is missing or mismatched, the email subject is `[异常]`, the affected versions are marked unavailable, and no trading action is published.

## Deployment gates

No merge or corrected send is considered complete until all of the following pass:

1. Regression tests reproduce the old-main/non-zero-holding mismatch and the repeated-member-action bug before implementation.
2. Strategy tests verify the exact v2.0/v2.3 constants, overheat state machines, realtime fields, and cash scale behavior.
3. Automation tests verify holdings-first rendering, dated member-action gating, preview suppression, and fail-closed strategy identity.
4. A fresh real-data run proves panel, proxy index, proxy turnover, base costed NAV, and every compared version stream share the latest close-confirmed trading date.
5. The correct branch produces v2.0 `cash -> cash`, scale 0 and v2.3 `cash -> cash`, scale 0 for the 2026-08-07 snapshot unless fresh data genuinely changes the state.
6. Both pull requests are merged; the remote `main` SHAs are read back from GitHub and checked out by the production workflow.
7. The workflow artifact CSVs and generated email body are read back before the corrected email is accepted.

## Repository scope

### Microcap strategy repository

- Integrate the reviewed v2.0/v2.3 strategy lineage and later guard fixes into a clean branch based on current `origin/main`.
- Preserve the native v2.5 changes from PR #23.
- Add production identity and member-action timing fields/tests.
- Fix the standalone realtime refresh constant error without touching the user's dirty primary worktree.

### Automation repository

- Validate strategy identity before rendering actionable output.
- Render holdings before member changes.
- Gate member actions to their execution session and suppress previews.
- Preserve duplicate-delivery protection while allowing an explicitly labeled correction.

## Documentation and long-term guardrail

The final incident record belongs in `docs/`, not as a history block in `AGENTS.md`. `AGENTS.md` receives only one durable rule: any production Microcap email deployment must verify remote `main` strategy identities and read back the generated realtime CSVs before claiming success.

