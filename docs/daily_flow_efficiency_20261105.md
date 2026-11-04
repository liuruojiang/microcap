# Daily validation efficiency candidate, 2026-11-05

This change preserves strategy parameters, approved historical values, source/date/ST/raw-share gates and publication deadlines. It changes validation work only. Targeted tests are not production acceptance, a matching bundle, or a future intraday delivery guarantee.

## Changes

- `source_closure` collects exact path/SHA declarations, rejects conflicts and verifies every unique source on every call using four readers with at most eight pending tasks. Manifest bytes used to discover dependencies are authenticated before parsing and read back at the end. No persisted or mtime-only cache is introduced.
- Independent counterexamples found daily-manifest identity mixing. `verify_day` now hashes the exact bytes it parsed and rejects changes before returning; `source_closure` checks those bytes against the parent's declared SHA. Original failed counterexamples are retained in external audit records.
- `_read_csv_header_and_rows` reads every record, retaining only row count and the last row instead of allocating a list of all rows. Existing parsing/errors and return values remain unchanged.
- `verify_release` queries live remote main once per invocation, then reads all declared immutable Git blobs using one batch process. Raw notice evidence and the existing LF rules remain unchanged. Missing/non-blob/truncated/extra/bad-identity responses and process failure close the gate. Output is temporary-file backed; its own child is reaped on the 300-second timeout.

The return-basis memo still performs both complete byte scans. Removing the second scan would lose detection of an in-place content change during the first scan. The existing resident stage-resume mechanism remains the sole local retry path.

## Test organization

The original daily core file retains all original release guard assertions using a complete mocked Git batch response. It does not invoke external Git, which the certified daily test guard forbids. New real-Git, protocol and process-lifecycle tests are in the separate complete `test_release_batch.py` and `test_release_batch_independent.py` suites; they must run as additional repair acceptance, never be silently skipped in place of the core gate.

Additional independent tests are `test_source_closure_bounded.py`, `test_flow_state_independent.py`, and `test_csv_state_streaming.py`. The historical scoped-ST test fixture now installs all actually declared published evidence bytes while preserving its narrowed test registry; no assertion or production guard is removed.

Local test evidence is retained under `D:/Codex/test-lifecycle/records`. Final release requires the full required daily suites, authenticated immutable fixtures/consumer and formal source/state binding. Do not rewrite an old market manifest to make this code appear its original producer.

## Remaining boundary

An approved D4 bundle and completed historical computation should be reused. This patch does not authorize skipping historical recomputation when input identities genuinely differ. A future narrow metadata-only continuation would need exact old/new universe semantics, old metadata binding, unchanged complete historical inputs, authenticated outputs and real parity evidence. It is not implemented here.

End-to-end timing must include preflight, validate, whole check, all three official serial signals, result validation and actual Codex publication. A local speedup is not evidence of the entire report finishing within its window. Already delivered normal mail and its intent/receipt/marker must not be repeated for this acceptance.
