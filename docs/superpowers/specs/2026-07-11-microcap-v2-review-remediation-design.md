# Microcap v2 Adversarial Review Remediation Design

## Goal

Repair the confirmed correctness, freshness, realtime-integrity, output-schema, and research-replay defects in the Top100 microcap v2.0, v2.3, and v2.5 family without changing the selected strategy parameters or the two execution assumptions explicitly accepted by the user.

## Approved Boundaries

The following are intentional strategy assumptions and will be preserved:

1. A realtime signal is queried before the close and execution is modeled at that trading day's close.
2. If fewer than 100 names can be held, the same total capital remains invested and is redistributed across the available names.
3. The formal historical strategy continues to use the existing synthetic-basket whole-position entry/exit abstraction. This remediation will label that abstraction explicitly; it will not introduce a member-level partial-fill portfolio engine or redefine the historical strategy.

The following are outside this remediation:

- changing v2.0, v2.3, or v2.5 signal thresholds, lookbacks, half-lives, hedge ratios, target-volatility targets, or selected official roles;
- promoting any new scan candidate;
- reporting new performance results before all required streams pass the repository freshness proof;
- refactoring the 12,000-line standalone v2.0 file into a new package architecture.

## Chosen Approach

Use narrow production-hardening changes with shared validation helpers where duplication has already caused divergent behavior. Preserve the official signal and execution abstractions, repair fail-open data paths and incorrect research replay, and add regression tests for every confirmed defect.

This approach was selected over:

- a member-level execution-engine rewrite, which would redefine historical returns and version meaning; and
- guard-only disabling of broken paths, which would leave the scan tools unusable and would not meet the requested full remediation scope.

## Formal Strategy Remediation

### Realtime integrity

- Synthetic last-close rows may be retained for diagnostic preview only. They must not count as genuine quote coverage, must remain visible through `member_quote_flat_fallback_count`, and must make the result non-actionable.
- The realtime anchor must be checked against an independently refreshed expected latest completed trading date. A calendar assembled only from the potentially stale strategy artifacts may not prove its own completeness. If the independent expected date is unavailable, the trading signal is blocked rather than guessed.
- v2.0, v2.3, and v2.5 signal CSVs and stdout must propagate `fallback_warning`, flat-fallback count, cache provenance/age when available, quote coverage, snapshot-row state, anchor date, and quote date.
- v2.3 realtime recomputation must use the freshly validated base/realtime historical calendar through the confirmed anchor. It must not require a stale v2.0 target-volatility costed stream merely to obtain a calendar.

### Freshness and output publication

- Candidate date coverage and continuity are validated before any official output path is replaced.
- All output files are first written to generation-scoped temporary paths. Only after candidate freshness, schema, historical-rewrite, and serialization checks succeed are files promoted under the version generation lock.
- The summary is promoted last and contains a generation identifier plus the exact read-back dates and row counts for the artifacts it describes.
- A failed generation leaves the previous official artifact set intact. Temporary files are removed on failure.
- After promotion, the written files are read back and checked. A read-back failure is reported and no performance or signal is presented as formal.

### Data-loading integrity

- `load_symbol_cache` distinguishes legitimate absence, such as a security not yet listed in the requested window, from parsing, schema, or corrupted-cache errors.
- Unexpected per-symbol failures are collected with symbol and reason and stop formal proxy generation. They are not silently converted into universe exclusion.
- Every rebalance must have at least `TOP_N` eligible ranked candidates before trade constraints are applied. The accepted post-trade holding count may remain below 100 and continues to be fully invested by design.
- Proxy metadata records requested, successfully loaded, legitimately unavailable, and failed symbol counts plus representative failure details.

### Version schemas and runtime isolation

- v2.3 explicitly overwrites every inherited v2.0 overheat and target-volatility field. Its generic fields may not contradict its native vol10/26%/19.5%-recovery rule or disabled target-volatility role.
- v2.3 keeps `momentum_gap` as a documented compatibility alias for `annualized_log_wls_score`. `microcap_mom` and `hedge_mom` become real component diagnostics calculated with the same log-WLS convention; they do not alter the native spread-NAV signal.
- v2.5 removes incompatible inherited summary keys such as the v2.0 hedge-version key and writes its native unhedged role consistently.
- v2.0 overlay output paths honor the configured output prefix and costed-NAV override rather than continuing to target official default paths.
- Version runtime overrides are applied through a scoped context that restores the prior v2.0 runtime configuration. Performance-title overrides remain scoped under a shared lock.

### Audit and lock behavior

- A clean historical-rewrite audit removes or atomically replaces stale failure rows and records a zero-change clean result.
- The overheat `blocked_until_signal_reset` output reflects the state after processing the trigger row, while the accepted return/execution timing stays unchanged.
- A lock owned by a confirmed dead PID is recoverable immediately. Age-based recovery remains the fallback for unreadable or unowned locks.
- The synthetic-basket entry/exit abstraction is emitted as explicit metadata so consumers do not interpret it as member-level fill simulation.

## v2.5 Research and Scan Remediation

### Shared official-baseline loader

Create a small scan-common helper used by the affected v2.5 scripts. It will:

- load the current official v2.5 stream through the real entrypoint contract;
- require panel, proxy index, proxy turnover, base costed NAV, v2.0, and v2.5 streams to share the same latest close-confirmed date;
- verify the official v2.5 fingerprint and role;
- reject static legacy preflight snapshots;
- expose canonical holding, scale, cost, and return fields for candidate replay.

### Cost and state replay

- Target-volatility replay preserves base entry and exit costs, including the first cash row after a next-open exit. Scale-change costs apply only to overlay-induced same-position scale changes.
- The staged-entry `none` candidate must match the official baseline within strict numeric tolerance. Parity failure aborts the scan.
- Cooldown replay must not charge a second entry/exit cost for a transition already charged in the baseline. It charges only incremental overlay trades.
- Candidate `holding`, `next_holding`, `current_execution_scale`, and `next_session_actionable_scale` must describe the state that generated candidate `return_net`.

### External feature integrity

- Volume scans require their external feature stream to cover the official NAV end date; they may not silently truncate the official tail.
- Breadth uses the repository's adjusted-return price source for return and moving-average features. Raw close may be retained only as a separately labeled diagnostic.
- Breadth and volume features enforce explicit daily coverage thresholds and fail with dates/counts when coverage is insufficient.
- Every scan writes source dates, row counts, adjustment mode, official baseline fingerprint, parity result, costs, and execution timing into its metadata.

## Error Handling

Formal generation and realtime trading output are fail-closed:

- incomplete quote coverage, synthetic fallback quotes, missing independent calendar confirmation, stale streams, internal date gaps, schema mismatch, failed historical audit, or failed read-back block formal output;
- diagnostic previews remain available only when clearly labeled non-actionable;
- scan inputs that fail freshness, coverage, role, or baseline parity checks abort before metrics are written;
- exceptions retain the responsible symbol, artifact, date, and validation name.

## Testing Strategy

Implementation follows red-green-refactor. Tests are added before production changes and must be observed failing for the intended reason.

Regression coverage will include:

- accepted close execution and full-capital redistribution remain unchanged;
- synthetic fallback quotes never become actionable or inflate genuine quote coverage;
- a calendar missing an intervening completed session blocks realtime output;
- failed freshness validation leaves all official artifacts byte-for-byte unchanged;
- corrupted per-symbol caches fail formal generation while legitimate pre-listing absence remains allowed;
- custom v2.0 output prefixes isolate all overlay outputs;
- clean rewrite audits cannot point to stale changed-row records;
- v2.3 signal rows contain one coherent overheat/target-volatility schema and real component diagnostics;
- v2.5 summaries contain only native role keys and realtime CSVs preserve warning/snapshot provenance;
- dead-PID locks recover without waiting for the stale-age threshold;
- target-volatility, staged-entry, and cooldown replay preserve official entry/exit costs and baseline parity;
- candidate canonical holdings/scales reproduce candidate returns;
- volume and breadth scans reject stale or under-covered external features.

Deterministic state-machine invariants will be exercised over generated short holding/scale sequences without adding a new property-testing dependency. The repository test suite remains the normal runner.

## Real-Data Verification

Before any Top100 test or formal result:

1. Back up every risky source file and every currently modified official artifact.
2. Refresh the selected data state to the latest locally available close-confirmed trading date. Prefer the auditable no-new-rebalance tail extension when its preconditions hold.
3. Read back and record dates and row counts for panel, proxy index, proxy turnover, base costed NAV, v2.0, v2.3, and v2.5 streams.
4. Run focused regression tests, then the full repository suite.
5. Run the official v2.0, v2.3, and v2.5 paths on real data and re-read their outputs.
6. Run baseline/parity smoke tests for each repaired scan family. No candidate performance is promoted by this remediation.

The current known state is blocked for formal results because the base streams end at 2026-07-02 while v2.0, v2.3, and v2.5 official streams end at 2026-06-29.

## Rollback

- Filesystem backups created by the quant-research backup helper are the primary rollback path for strategy and scan sources.
- Existing user modifications under `outputs/` are backed up and never discarded.
- Source changes remain narrow and version-specific so individual remediation groups can be reverted independently.
- No official artifact is deleted solely because a generation attempt fails.

## Completion Criteria

The remediation is complete only when:

- every confirmed non-design defect has a regression test that failed before its fix;
- all focused and full tests pass after freshness requirements are satisfied;
- the accepted close-execution and full-capital assumptions are unchanged and explicitly tested;
- formal v2.0, v2.3, and v2.5 outputs share the same latest close-confirmed date and pass read-back freshness proof;
- repaired scan baselines match the formal stream before any candidate calculation;
- no source or artifact changes outside the approved scope are included.
