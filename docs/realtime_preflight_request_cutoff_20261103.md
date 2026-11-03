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
