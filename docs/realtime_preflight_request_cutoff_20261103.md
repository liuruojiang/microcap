# Preflight network cutoff

The native `preflight` entry defaults to `--publication-mode intraday`. It keeps
the existing state, independent calendar/history, same-day 100 member names,
ST, source/hash and proof checks. An explicit request guard rechecks the actual
Beijing clock after the potentially long initial validation and before every
history HTTP call, calendar subprocess, member quote batch and individual quote
worker HTTP call. A request already in progress may finish; no next request
starts at or after 15:00. The calendar child also checks the absolute deadline
after its imports, before invoking its existing source library.

The same guard checks history completion before member collection, the last
member response before certification, and the final state validation before
proof creation. Proof serialization happens in a temporary file; the guard runs
again immediately before atomic publication. A late response or validation
cannot overwrite the previous proof with an apparent intraday success.

`--publication-mode close-confirmed-check` is the explicit path for a readiness
check after 15:00. It allows the original data requests after the close while
retaining all date/completeness/source validation. It does not generate a signal
or assert that a close has complete data merely because the clock passed 15:00.

The local automation readiness helper must pass its mode as the native
`publication_mode` argument. No validator or HTTP transport is monkeypatched.
The request callback is optional on shared transports so the original refresh,
delivery-history, and other callers retain their existing behavior. The normal
intraday publication deadline remains separate and unchanged.

Tests extend the existing daily `test_realtime_preflight.py` group, including a
long validation crossing 15:00, history failover, actual calendar child admission,
each quote batch and queued workers in both standalone and embedded sources,
and explicit close-mode requests. Synthetic clocks and HTTP responses in these
software fault tests do not certify live quotes or a published signal.

The original validators can themselves acquire the exchange calendar. The
entire preflight therefore uses a native process-wide admission scope, including
worker threads. Independent tokens are removed in `finally`; nested/concurrent
scopes and explicit callbacks all apply, and the calendar child uses the earliest
deadline. The shared official realtime action uses the same scope for implicit
member/hedge requests, fallback hosts, batches and member previews. Outside an
intraday scope, close/diagnostic callers retain their original behavior.

## Current-session notices

An intraday preflight records one source-bound, read-only all-market notice
observation over calendar days D through T. Only risk notices touching the
canonical effective/current target member union require authenticated issuer and
body decisions. An unknown or risk-entry notice for those members blocks; an
unrelated nonmember risk notice does not block. The original 100 unique names,
same-day quotes and zero-ST tests remain required.

The observation stores immutable raw pages, receipts and manifest under
`.microcap_index_cache/realtime_notice_audits/`, and binds its reference in
`preflight_evidence.current_member_notice_audit`. It does not advance the
production notice archive or rewrite metadata, prices or NAVs.

All three state-only versions consume and revalidate the same audit identity,
member coverage, registry binding, raw/receipt closure and actual aware clocks.
The official native action scope enforces this even if report fields are
removed. The final CSV keeps the audit ID, policy revision, observation start/end
and manifest path/SHA256. Its atomic-publication guard checks the proof and those
fields again. This describes a shared observation time; it does not claim
continuous notice updates until every instant of publication. No TTL is added.

Transport validates the active reference's complete closure and preserves its
original day/clocks. An earlier-day audit can be restored as historical evidence
and is rejected as the next day's live proof. Explicit close-confirmed signals
keep the original complete-history archive path and never relabel a close
refresh proof as an intraday notice observation.

The existing whole-delivery source-dependency list explicitly binds the new
audit module and index-history preflight implementation. This source binding
does not make the read-only audit part of the economic strategy. For declared
schema3 native daily manifests and already-verified freshness query manifests,
transport retains the original complete validator and adds strict integer
pagination checks; it does not scan arbitrary legacy/CNINFO JSON.

## Replay compatibility and performance

The approved cached metadata replay compares both old and newly generated
turnover values after the same native CSV serialization/parser boundary, with
all columns and `check_exact=True`. It preserves old event-table bytes and adds
neither rounding nor numeric tolerance. `constraint_trade_date` uses the same
existing strict daily-date normalization as the other date columns.

The tradeability builder aligns its ST series lazily once per call instead of
once per row. Empty input behavior and the original separate standalone/embedded
return-price implementations remain unchanged. This verified optimization does
not replace the required full native replay or delivery acceptance.
